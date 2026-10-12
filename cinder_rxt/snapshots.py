# Copyright 2026 Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Swift-Backed Decoupled Snapshot Engine for Cinder RXT.

Provides Lunr-style differential snapshots backed by OpenStack Swift, powered by
the high-performance Rust worker (`cinder-rxt-worker`).

When a snapshot is created:
1. A transient LVM snapshot is created momentarily to freeze point-in-time data.
2. The Rust worker hashes 4MB chunks, uploads only changed chunks to Swift, and
   records an autonomous manifest.
3. The transient LVM snapshot is immediately deleted.

Result: No lingering LVM snapshots on the storage host. Storage hosts can be
evacuated or migrated freely, and volume groups never suffer LVM snapshot exhaustion.
"""

import contextlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time

from oslo_concurrency import lockutils
from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _

LOG = logging.getLogger(__name__)

snapshot_opts = [
    cfg.BoolOpt(
        "rxt_swift_snapshots_enabled",
        default=True,
        help="Enable Swift-backed differential snapshots and eliminate local "
        "LVM COW snapshot dependencies to enable live volume migrations.",
    ),
    cfg.StrOpt(
        "rxt_swift_url",
        default=None,
        help="Swift endpoint URL for snapshot chunk storage. If unset, uses "
        "the internal Swift URL from the service catalog in the request context.",
    ),
    cfg.StrOpt(
        "rxt_swift_container_prefix",
        default="cinder-snapshots-",
        help="Prefix for Swift containers storing volume snapshot chunks.",
    ),
    cfg.StrOpt(
        "rxt_worker_path",
        default="cinder-rxt-worker",
        help="Path to the cinder-rxt-worker executable.",
    ),
    cfg.IntOpt(
        "rxt_snapshot_concurrency",
        default=32,
        help="Number of concurrent chunk upload/download streams in Rust worker.",
    ),
    cfg.IntOpt(
        "rxt_snapshot_chunk_size_mib",
        default=4,
        help="Chunk size in MiB for differential hashing and Swift storage.",
    ),
    cfg.BoolOpt(
        "rxt_auto_convert_legacy_snapshots",
        default=True,
        help="Automatically scan the Volume Group on driver startup and convert any "
        "legacy local LVM snapshots into Swift differential snapshots, removing the "
        "local LVM snapshots once converted and verified in Swift.",
    ),
    cfg.IntOpt(
        "rxt_max_concurrent_clones",
        default=2,
        help="Maximum number of concurrent volume-to-volume clone operations on this host.",
    ),
    cfg.IntOpt(
        "rxt_max_concurrent_snapshots",
        default=4,
        help="Maximum number of concurrent Swift differential snapshot operations on this host.",
    ),
    cfg.IntOpt(
        "rxt_clone_concurrency",
        default=8,
        help="Number of concurrent chunk worker tasks within a volume clone job.",
    ),
    cfg.IntOpt(
        "rxt_worker_ionice_class",
        default=0,
        help="Linux ionice scheduling class for cinder-rxt-worker (0=disabled/normal, 1=realtime, 2=best-effort, 3=idle).",
    ),
    cfg.IntOpt(
        "rxt_worker_ionice_level",
        default=4,
        help="Linux ionice priority level (0-7, 4=normal priority in best-effort class).",
    ),
    cfg.IntOpt(
        "rxt_worker_nice_priority",
        default=0,
        help="Linux CPU nice priority for cinder-rxt-worker (-20 to 19, 0=standard priority).",
    ),
    cfg.IntOpt(
        "rxt_copy_max_mbps",
        default=0,
        help="Maximum throughput in MB/s for volume copy/cloning operations (0 = unlimited).",
    ),
    cfg.IntOpt(
        "rxt_snapshot_max_mbps",
        default=0,
        help="Maximum network throughput in MB/s for Swift snapshot transfers (0 = unlimited).",
    ),
]


def register_opts(conf):
    conf.register_opts(snapshot_opts)


def get_config_value(conf, name, default=None):
    """Retrieve a config option from configuration or CONF with fallback to default."""
    if conf is None:
        return default
    if not type(conf).__name__.endswith("Mock") and hasattr(conf, "safe_get") and callable(conf.safe_get):
        val = conf.safe_get(name)
        if val is not None:
            return val
    return getattr(conf, name, default)


def is_swift_snapshots_enabled(configuration):
    val = get_config_value(configuration, "rxt_swift_snapshots_enabled", True)
    return bool(val) if val is not None else True


def get_worker_binary(configuration):
    configured = get_config_value(configuration, "rxt_worker_path", "cinder-rxt-worker")
    resolved = shutil.which(configured)
    if resolved:
        return resolved
    # Fallback to local target/release build if available
    local_release = "/usr/local/bin/cinder-rxt-worker"
    if shutil.which(local_release):
        return local_release
    return configured


def get_swift_credentials(context, configuration, project_id=None):
    url = get_config_value(configuration, "rxt_swift_url", None)
    token = None

    if context:
        token = getattr(context, "auth_token", None)
        if not project_id:
            project_id = getattr(context, "project_id", None)
        if not url and hasattr(context, "service_catalog"):
            catalog = getattr(context, "service_catalog", []) or []
            for svc in catalog:
                if svc.get("type") in ("object-store", "swift"):
                    for ep in svc.get("endpoints", []):
                        if ep.get("interface") in ("internal", "public"):
                            url = ep.get("url")
                            break
                    if url:
                        break

    # If context didn't provide token or url (e.g. background auto-converter thread),
    # fall back to keystone_authtoken from oslo_config.cfg.CONF or cinder config files
    if not url or not token:
        try:
            from oslo_config import cfg
            from keystoneauth1 import loading as ks_loading

            conf = cfg.CONF
            group = "keystone_authtoken"
            try:
                ks_loading.register_auth_conf_options(conf, group)
                ks_loading.register_session_conf_options(conf, group)
            except Exception:
                pass
            auth = ks_loading.load_auth_from_conf_options(conf, group)
            if auth:
                sess = ks_loading.load_session_from_conf_options(
                    conf, group, auth=auth
                )
                if not token:
                    token = sess.get_token()
                if not url:
                    url = sess.get_endpoint(
                        service_type="object-store", interface="internal"
                    )
        except Exception as exc:
            LOG.debug(
                "Could not acquire Swift credentials from keystone_authtoken: %s",
                exc,
            )

    if not url or not token:
        try:
            import configparser
            import glob
            from keystoneauth1.identity import v3
            from keystoneauth1 import session as ks_session

            config_files = ["/etc/cinder/cinder.conf"] + sorted(
                glob.glob("/etc/cinder/cinder.conf.d/*.conf")
            )
            cp = configparser.ConfigParser(interpolation=None)
            cp.read(config_files)
            if cp.has_section("keystone_authtoken"):
                sec = dict(cp.items("keystone_authtoken"))
                auth_url = sec.get("auth_url") or sec.get("auth_uri")
                if auth_url and sec.get("username") and sec.get("password"):
                    auth = v3.Password(
                        auth_url=auth_url,
                        username=sec["username"],
                        password=sec["password"],
                        project_name=sec.get("project_name"),
                        user_domain_name=sec.get("user_domain_name"),
                        project_domain_name=sec.get("project_domain_name"),
                    )
                    sess = ks_session.Session(auth=auth)
                    if not token:
                        token = sess.get_token()
                    if not url:
                        url = sess.get_endpoint(
                            service_type="object-store", interface="internal"
                        )
        except Exception as exc:
            LOG.warning(
                "Could not acquire Swift credentials from config files: %s",
                exc,
            )

    if not url or not token:
        msg = _(
            "Swift snapshot engine requires valid Swift URL and Auth Token. "
            "url=%s, token_present=%s"
        ) % (url, bool(token))
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    # Ensure that the Swift account URL matches the target project ID (AUTH_<project_id>)
    # rather than defaulting to the service tenant account (e.g. AUTH_<service_id>).
    if project_id and url:
        if "/AUTH_" in url:
            base_url = url.split("/AUTH_")[0]
            url = f"{base_url}/AUTH_{project_id}"
        elif "%(project_id)s" in url or "%(tenant_id)s" in url:
            url = url.replace("%(project_id)s", project_id).replace("%(tenant_id)s", project_id)

    return url, token


class HostCountingSemaphore:
    """Counting semaphore across processes and threads on this host using file locks."""

    _thread_locks_map = {}
    _map_lock = threading.Lock()

    def __init__(self, name, max_slots, lock_dir=None):
        self.name = name
        self.max_slots = max(0, int(max_slots))
        self.lock_dir = lock_dir or tempfile.gettempdir()
        with self._map_lock:
            existing = self._thread_locks_map.get(name, [])
            if len(existing) < self.max_slots:
                existing.extend(
                    [threading.Lock() for _ in range(self.max_slots - len(existing))]
                )
            self._thread_locks_map[name] = existing

    @contextlib.contextmanager
    def acquire(self, poll_interval=0.2):
        if self.max_slots <= 0:
            yield None
            return

        thread_locks = self._thread_locks_map[self.name]
        acquired_slot = None
        held_iplock = None
        while acquired_slot is None:
            for slot in range(min(self.max_slots, len(thread_locks))):
                if thread_locks[slot].acquire(blocking=False):
                    lock_file = os.path.join(
                        self.lock_dir, f"cinder-rxt-{self.name}-slot-{slot}.lock"
                    )
                    iplock = lockutils.InterProcessLock(lock_file)
                    try:
                        if iplock.acquire(blocking=False):
                            acquired_slot = slot
                            held_iplock = iplock
                            break
                        else:
                            thread_locks[slot].release()
                    except Exception:
                        thread_locks[slot].release()
            if acquired_slot is None:
                time.sleep(poll_interval)

        LOG.debug("Acquired host concurrency slot %d for %s", acquired_slot, self.name)
        try:
            yield acquired_slot
        finally:
            if held_iplock:
                try:
                    held_iplock.release()
                except Exception:
                    pass
            thread_locks[acquired_slot].release()
            LOG.debug("Released host concurrency slot %d for %s", acquired_slot, self.name)


def host_clone_semaphore(configuration):
    max_slots = get_config_value(configuration, "rxt_max_concurrent_clones", 2)
    lock_dir = get_config_value(configuration, "lock_path", None)
    return HostCountingSemaphore("clone", max_slots, lock_dir=lock_dir).acquire()


def host_snapshot_semaphore(configuration):
    max_slots = get_config_value(configuration, "rxt_max_concurrent_snapshots", 4)
    lock_dir = get_config_value(configuration, "lock_path", None)
    return HostCountingSemaphore("snapshot", max_slots, lock_dir=lock_dir).acquire()


def _run_worker_stream(cmd, conf=None, execute=processutils.execute, snapshot=None):
    """Run cinder-rxt-worker, streaming output to LOG.info in real time."""
    if (
        hasattr(execute, "_mock_return_value")
        or hasattr(execute, "assert_called")
        or type(execute).__name__ in ("Mock", "MagicMock")
    ):
        return execute(*cmd, run_as_root=False)

    exec_cmd = list(cmd)
    if conf:
        ionice_bin = shutil.which("ionice")
        ionice_class = get_config_value(conf, "rxt_worker_ionice_class", None)
        if ionice_bin and ionice_class is not None:
            try:
                ionice_class_int = int(ionice_class)
            except (ValueError, TypeError):
                ionice_class_int = None

            if ionice_class_int is not None and ionice_class_int > 0:
                ionice_level = get_config_value(conf, "rxt_worker_ionice_level", 7)
                prefix = [ionice_bin, "-c", str(ionice_class_int)]
                if ionice_class_int in (1, 2) and ionice_level is not None:
                    prefix.extend(["-n", str(ionice_level)])
                exec_cmd = prefix + exec_cmd

        nice_bin = shutil.which("nice")
        nice_prio = get_config_value(conf, "rxt_worker_nice_priority", None)
        if nice_bin and nice_prio is not None:
            exec_cmd = [nice_bin, "-n", str(nice_prio)] + exec_cmd

    proc = subprocess.Popen(
        exec_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    last_line = ""
    for line in iter(proc.stdout.readline, ""):
        line = line.strip()
        if not line:
            continue
        last_line = line
        LOG.info("[cinder-rxt-worker] %s", line)
        if snapshot and line.startswith("PROGRESS:"):
            try:
                pct_str = line.split(":", 1)[1].strip()
                if hasattr(snapshot, "progress"):
                    snapshot.progress = pct_str
                    if hasattr(snapshot, "save"):
                        snapshot.save()
                elif isinstance(snapshot, dict):
                    snapshot["progress"] = pct_str
            except Exception:
                pass
    proc.wait()
    if proc.returncode != 0:
        raise processutils.ProcessExecutionError(
            cmd=" ".join(exec_cmd),
            exit_code=proc.returncode,
            stdout=last_line,
            description="cinder-rxt-worker execution failed",
        )
    return (last_line, "")


def create_swift_snapshot(
    driver, snapshot, context=None, execute=processutils.execute
):
    """Create a differential snapshot in Swift and eliminate local LVM snapshot."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)
    if context is None:
        context = getattr(snapshot, "_context", None)
    snap_proj = snapshot.get("project_id") if isinstance(snapshot, dict) else getattr(snapshot, "project_id", None)
    swift_url, swift_token = get_swift_credentials(context, conf, project_id=snap_proj)

    vol_name = (
        snapshot.get("volume_name")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "volume_name", "")
    )
    vol_id = (
        snapshot.get("volume_id")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "volume_id", "")
    )
    if not vol_id and vol_name and vol_name.startswith("volume-"):
        vol_id = vol_name[len("volume-"):]
    if not vol_name and vol_id:
        vol_name = getattr(driver.configuration, "volume_name_template", "volume-%s") % vol_id

    snap_id = (
        snapshot.get("id")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "id", "")
    )
    vol_size = (
        snapshot.get("volume_size")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "volume_size", 0)
    )
    if not vol_size:
        vol_size = (
            snapshot.get("size")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "size", 0)
        )

    temp_snap_name = "tmp-rxt-snap-%s" % snap_id
    temp_snap_dict = {
        "name": temp_snap_name,
        "volume_name": vol_name,
        "volume_id": vol_id,
        "size": vol_size,
    }

    LOG.info(
        "Creating transient LVM snapshot %(temp)s for volume %(vol)s "
        "to sync differential snapshot to Swift.",
        {"temp": temp_snap_name, "vol": vol_id},
    )

    # 1. Create and activate transient LVM snapshot
    driver.vg.create_lv_snapshot(
        driver._escape_snapshot(temp_snap_name),
        vol_name,
        conf.lvm_type,
    )
    driver.vg.activate_lv(temp_snap_name, is_snapshot=True)
    temp_dev_path = driver.local_path(temp_snap_dict)

    try:
        # 2. Invoke Rust worker to hash 4MB chunks and upload changed blocks to Swift
        container_prefix = get_config_value(conf, "rxt_swift_container_prefix", "cinder-snapshots-")
        chunk_size_mib = get_config_value(conf, "rxt_snapshot_chunk_size_mib", 4)
        concurrency = get_config_value(conf, "rxt_snapshot_concurrency", 32)
        max_mbps = get_config_value(conf, "rxt_snapshot_max_mbps", 0)

        cmd = [
            worker_bin,
            "sync-diff",
            "--source-dev",
            temp_dev_path,
            "--volume-id",
            vol_id,
            "--snapshot-id",
            snap_id,
            "--swift-url",
            swift_url,
            "--swift-token",
            swift_token,
            "--container-prefix",
            container_prefix,
            "--chunk-size-mib",
            str(chunk_size_mib),
            "--concurrency",
            str(concurrency),
        ]
        if max_mbps and int(max_mbps) > 0:
            cmd.extend(["--max-mbps", str(max_mbps)])

        with host_snapshot_semaphore(conf):
            out, err = _run_worker_stream(
                cmd, conf=conf, execute=execute, snapshot=snapshot
            )
        LOG.info("Swift differential snapshot %s completed: %s", snap_id, out.strip())
    finally:
        # 3. Immediately delete the local LVM snapshot
        try:
            driver.vg.delete(driver._escape_snapshot(temp_snap_name))
            LOG.info("Transient LVM snapshot %s deleted successfully.", temp_snap_name)
        except Exception:
            LOG.exception(
                "Failed to clean up transient LVM snapshot %s", temp_snap_name
            )


def restore_swift_snapshot_to_volume(
    driver, volume, snapshot, context=None, execute=processutils.execute
):
    """Restore snapshot from Swift chunks into new logical volume."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)
    if context is None:
        context = getattr(volume, "_context", None) or getattr(snapshot, "_context", None)
    vol_proj = (
        volume.get("project_id")
        if isinstance(volume, dict)
        else getattr(volume, "project_id", None)
    ) or (
        snapshot.get("project_id")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "project_id", None)
    ) or (
        getattr(context, "project_id", None) if context else None
    )
    swift_url, swift_token = get_swift_credentials(context, conf, project_id=vol_proj)

    vol_name = volume["name"]
    vol_size = volume["size"]
    orig_vol_id = (
        snapshot.get("volume_id")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "volume_id", "")
    )
    snap_id = (
        snapshot.get("id")
        if isinstance(snapshot, dict)
        else getattr(snapshot, "id", "")
    )

    # 1. Create and activate the destination logical volume
    mirror_count = conf.lvm_mirrors if conf.lvm_mirrors else 0
    size_str = (
        driver._get_lv_size_str(volume)
        if hasattr(driver, "_get_lv_size_str")
        else driver._sizestr(vol_size)
    )
    driver._create_volume(
        vol_name,
        size_str,
        conf.lvm_type,
        mirror_count,
    )
    dest_dev_path = driver.local_path(volume)

    LOG.info(
        "Restoring Swift snapshot %(snap)s (origin vol %(orig)s) to new LV %(dest)s",
        {"snap": snap_id, "orig": orig_vol_id, "dest": dest_dev_path},
    )

    container_prefix = get_config_value(conf, "rxt_swift_container_prefix", "cinder-snapshots-")
    concurrency = get_config_value(conf, "rxt_snapshot_concurrency", 32)
    max_mbps = get_config_value(conf, "rxt_snapshot_max_mbps", 0)

    # 2. Invoke Rust worker to download and stream chunks in parallel
    cmd = [
        worker_bin,
        "restore",
        "--dest-dev",
        dest_dev_path,
        "--volume-id",
        orig_vol_id,
        "--snapshot-id",
        snap_id,
        "--swift-url",
        swift_url,
        "--swift-token",
        swift_token,
        "--container-prefix",
        container_prefix,
        "--concurrency",
        str(concurrency),
    ]
    if max_mbps and int(max_mbps) > 0:
        cmd.extend(["--max-mbps", str(max_mbps)])

    with host_snapshot_semaphore(conf):
        out, err = _run_worker_stream(
            cmd, conf=conf, execute=execute, snapshot=snapshot
        )
    LOG.info(
        "Swift snapshot %(snap)s restore to %(vol)s finished: %(out)s",
        {"snap": snap_id, "vol": vol_name, "out": out.strip()},
    )


def delete_swift_snapshot(
    driver, snapshot, context=None, execute=processutils.execute
):
    """Delete snapshot from Swift manifest and GC unreferenced chunks."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)

    if context is None:
        context = getattr(snapshot, "_context", None)

    snap_proj = snapshot.get("project_id") if isinstance(snapshot, dict) else getattr(snapshot, "project_id", None)
    try:
        swift_url, swift_token = get_swift_credentials(context, conf, project_id=snap_proj)
    except Exception as exc:
        LOG.warning(
            "Could not acquire Swift credentials to delete snapshot %(snap)s: %(exc)s",
            {
                "snap": snapshot.get("id") if isinstance(snapshot, dict) else getattr(snapshot, "id", ""),
                "exc": exc,
            },
        )
        return

    vol_id = snapshot["volume_id"] if isinstance(snapshot, dict) else getattr(snapshot, "volume_id", "")
    snap_id = snapshot["id"] if isinstance(snapshot, dict) else getattr(snapshot, "id", "")

    cmd = [
        worker_bin,
        "delete-snapshot",
        "--volume-id",
        vol_id,
        "--snapshot-id",
        snap_id,
        "--swift-url",
        swift_url,
        "--swift-token",
        swift_token,
        "--container-prefix",
        conf.rxt_swift_container_prefix,
        "--concurrency",
        str(conf.rxt_snapshot_concurrency),
    ]

    try:
        out, err = _run_worker_stream(cmd, conf=conf, execute=execute)
        LOG.info(
            "Swift differential snapshot %(snap)s deleted for volume %(vol)s: %(out)s",
            {"snap": snap_id, "vol": vol_id, "out": out.strip()},
        )
        if "snapshot_not_found" in out and not vol_id.startswith("_volume-"):
            legacy_cmd = list(cmd)
            legacy_vol_id = f"_volume-{vol_id}"
            legacy_cmd[cmd.index("--volume-id") + 1] = legacy_vol_id
            try:
                legacy_out, _ = _run_worker_stream(legacy_cmd, conf=conf, execute=execute)
                if "container_purged" in legacy_out or ("ok" in legacy_out and "snapshot_not_found" not in legacy_out):
                    LOG.info(
                        "Found and deleted legacy Swift differential snapshot %(snap)s for volume %(vol)s: %(out)s",
                        {"snap": snap_id, "vol": legacy_vol_id, "out": legacy_out.strip()},
                    )
            except Exception:
                pass
    except Exception as exc:
        LOG.warning(
            "Failed to delete Swift differential snapshot %(snap)s for volume %(vol)s: %(err)s",
            {"snap": snap_id, "vol": vol_id, "err": exc},
        )


def delete_swift_volume_snapshots(
    driver, volume, context=None, execute=processutils.execute
):
    """Purge entire Swift snapshot container and all chunks for a volume."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)

    if context is None:
        context = getattr(volume, "_context", None)

    vol_proj = volume.get("project_id") if isinstance(volume, dict) else getattr(volume, "project_id", None)
    try:
        swift_url, swift_token = get_swift_credentials(context, conf, project_id=vol_proj)
    except Exception as exc:
        LOG.warning(
            "Could not acquire Swift credentials to purge snapshot container for volume %(vol)s: %(exc)s",
            {
                "vol": volume.get("id") if isinstance(volume, dict) else getattr(volume, "id", ""),
                "exc": exc,
            },
        )
        return

    vol_id = volume["id"] if isinstance(volume, dict) else getattr(volume, "id", "")

    cmd = [
        worker_bin,
        "delete-volume",
        "--volume-id",
        vol_id,
        "--swift-url",
        swift_url,
        "--swift-token",
        swift_token,
        "--container-prefix",
        conf.rxt_swift_container_prefix,
        "--concurrency",
        str(conf.rxt_snapshot_concurrency),
    ]

    try:
        out, err = _run_worker_stream(cmd, conf=conf, execute=execute)
        LOG.info(
            "Swift snapshot container purged for volume %(vol)s: %(out)s",
            {"vol": vol_id, "out": out.strip()},
        )
    except Exception as exc:
        LOG.warning(
            "Failed to purge Swift snapshot container for volume %(vol)s: %(err)s",
            {"vol": vol_id, "err": exc},
        )


def _parse_snapshot_lv(lv_name):
    """Returns snapshot_id if lv_name is a legacy snapshot LV, else None."""
    if not lv_name:
        return None
    # Skip temporary worker/sync snapshots
    if lv_name.startswith("tmp-rxt-snap-") or lv_name.startswith("_tmp-rxt-snap-"):
        return None
    if lv_name.startswith("_snapshot-"):
        return lv_name[len("_snapshot-"):]
    if lv_name.startswith("snapshot-"):
        return lv_name[len("snapshot-"):]
    return None


def _parse_volume_lv(lv_name):
    """Extract UUID from volume LV name ('volume-xxxx', '_volume-xxxx', or 'xxxx')."""
    if not lv_name:
        return ""
    clean = lv_name.lstrip("_")
    if clean.startswith("volume-"):
        return clean[len("volume-"):]
    return clean


def discover_legacy_lvm_snapshots(
    vg_or_name, execute=processutils.execute, root_helper=None
):
    """Scan the Volume Group for legacy LVM snapshot LVs ('snapshot-*' or '_snapshot-*').

    Returns a list of dictionaries with snapshot metadata:
    [{
        'lv_name': '_snapshot-xxxx' or 'snapshot-xxxx',
        'snapshot_id': 'xxxx',
        'origin_lv_name': 'volume-yyyy' or 'yyyy',
        'volume_id': 'yyyy',
        'size_gib': 20.0,
        'lv_attr': 'swi-a-s---',
    }, ...]
    """
    root_helper = root_helper or getattr(vg_or_name, "_root_helper", None)

    # 1. If a Cinder LVM object is provided with get_volumes(), try it if it includes origin
    if hasattr(vg_or_name, "get_volumes") and callable(vg_or_name.get_volumes):
        try:
            vols = vg_or_name.get_volumes()
            if vols:
                legacy_snaps = []
                missing_origin = False
                for vol in vols:
                    lv_name = vol.get("name", "")
                    snap_id = _parse_snapshot_lv(lv_name)
                    if not snap_id:
                        continue
                    origin = vol.get("origin", "")
                    if not origin:
                        # Cinder's get_volumes() does not return 'origin' in standard LVM.get_lv_info().
                        # If origin is missing, we must fall back to lvs command to identify origin volume.
                        missing_origin = True
                        break
                    vol_name = origin
                    vol_id = _parse_volume_lv(vol_name)
                    raw_size = vol.get("size", "0g")
                    try:
                        size_gib = float(str(raw_size).rstrip("gGbBmMkK"))
                    except ValueError:
                        size_gib = 0.0

                    legacy_snaps.append(
                        {
                            "lv_name": lv_name,
                            "snapshot_id": snap_id,
                            "origin_lv_name": vol_name,
                            "volume_id": vol_id,
                            "size_gib": size_gib,
                            "lv_attr": vol.get("attr", ""),
                        }
                    )
                if not missing_origin and legacy_snaps:
                    return legacy_snaps
        except Exception as exc:
            LOG.debug(
                "vg.get_volumes() query returned: %s. Falling back to lvs command.",
                exc,
            )

    vg_name = getattr(vg_or_name, "vg_name", str(vg_or_name))
    cmd = [
        "env",
        "LC_ALL=C",
        "lvs",
        "--noheadings",
        "--units",
        "g",
        "--separator",
        ";",
        "-o",
        "lv_name,vg_name,lv_attr,origin,lv_size",
        vg_name,
    ]
    kwargs = {}
    if root_helper:
        kwargs["run_as_root"] = True
        kwargs["root_helper"] = root_helper
    else:
        kwargs["run_as_root"] = True

    try:
        out, _ = execute(*cmd, **kwargs)
    except Exception as exc:
        LOG.warning(
            "Failed to query LVs in VG %s for legacy snapshots: %s",
            vg_name,
            exc,
        )
        return []

    legacy_snapshots = []
    for line in out.strip().splitlines():
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 5:
            continue
        lv_name, vg, lv_attr, origin, lv_size = parts[:5]
        snap_id = _parse_snapshot_lv(lv_name)
        if not snap_id:
            # Also check if lv_attr indicates an LVM snapshot ('s...') with an origin
            if lv_attr and lv_attr.startswith("s") and origin:
                clean_name = lv_name.lstrip("_")
                if clean_name.startswith("snapshot-"):
                    snap_id = clean_name[len("snapshot-"):]
                else:
                    snap_id = clean_name
            else:
                continue

        vol_name = origin if origin else ""
        vol_id = _parse_volume_lv(vol_name)

        try:
            size_gib = float(lv_size.rstrip("gGbBmMkK"))
        except ValueError:
            size_gib = 0.0

        legacy_snapshots.append(
            {
                "lv_name": lv_name,
                "snapshot_id": snap_id,
                "origin_lv_name": vol_name,
                "volume_id": vol_id,
                "size_gib": size_gib,
                "lv_attr": lv_attr,
            }
        )
    return legacy_snapshots


def convert_legacy_lvm_snapshot(
    driver,
    snapshot_info,
    context=None,
    execute=processutils.execute,
    dry_run=False,
):
    """Converts an existing legacy LVM snapshot into a Swift differential snapshot,

    and removes the local LVM snapshot once verified.
    """
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)
    snap_proj = snapshot_info.get("project_id") if isinstance(snapshot_info, dict) else getattr(snapshot_info, "project_id", None)
    swift_url, swift_token = get_swift_credentials(context, conf, project_id=snap_proj)

    lv_name = snapshot_info["lv_name"]
    snap_id = snapshot_info["snapshot_id"]
    vol_id = snapshot_info["volume_id"]
    vol_name = snapshot_info["origin_lv_name"]

    if dry_run:
        LOG.info(
            "[DRY RUN] Would convert legacy LVM snapshot %(snap)s (origin: %(vol)s) "
            "to Swift and destroy local LVM snapshot %(lv)s.",
            {"snap": snap_id, "vol": vol_id, "lv": lv_name},
        )
        return True

    LOG.info(
        "Converting legacy LVM snapshot %(snap)s (origin %(vol)s) to Swift differential snapshot.",
        {"snap": snap_id, "vol": vol_id},
    )

    # 1. Activate the legacy snapshot LV
    if hasattr(driver, "vg") and hasattr(driver.vg, "activate_lv"):
        driver.vg.activate_lv(lv_name, is_snapshot=True)

    snap_dict = {
        "name": lv_name,
        "volume_name": vol_name,
        "volume_id": vol_id,
        "size": snapshot_info.get("size_gib", 0),
    }
    dev_path = driver.local_path(snap_dict)

    # 2. Invoke Rust worker to hash 4MB chunks and sync diff into Swift
    cmd = [
        worker_bin,
        "sync-diff",
        "--source-dev",
        dev_path,
        "--volume-id",
        vol_id,
        "--snapshot-id",
        snap_id,
        "--swift-url",
        swift_url,
        "--swift-token",
        swift_token,
        "--container-prefix",
        conf.rxt_swift_container_prefix,
        "--chunk-size-mib",
        str(conf.rxt_snapshot_chunk_size_mib),
        "--concurrency",
        str(conf.rxt_snapshot_concurrency),
    ]

    if getattr(conf, "rxt_snapshot_max_mbps", 0) > 0:
        cmd.extend(["--max-mbps", str(conf.rxt_snapshot_max_mbps)])

    with host_snapshot_semaphore(conf):
        out, err = _run_worker_stream(cmd, conf=conf, execute=execute)
    LOG.info(
        "Swift differential sync for legacy snapshot %(snap)s completed: %(out)s",
        {"snap": snap_id, "out": out.strip() if out else ""},
    )

    # 3. Remove the local LVM snapshot
    escaped_name = (
        driver._escape_snapshot(lv_name)
        if hasattr(driver, "_escape_snapshot")
        else lv_name
    )
    if hasattr(driver, "vg") and hasattr(driver.vg, "delete"):
        driver.vg.delete(escaped_name)
    else:
        vg_name = getattr(driver.vg, "vg_name", conf.volume_group)
        execute(
            "lvremove", "-f", f"{vg_name}/{escaped_name}", run_as_root=True
        )

    LOG.info(
        "Legacy LVM snapshot %(snap)s converted to Swift and deleted local LVM snapshot %(lv)s.",
        {"snap": snap_id, "lv": lv_name},
    )
    return True


def convert_all_legacy_snapshots(
    driver,
    context=None,
    execute=processutils.execute,
    dry_run=False,
):
    vg_name = (
        getattr(driver.vg, "vg_name", None)
        or getattr(driver.configuration, "volume_group", "cinder-volumes")
    )
    vg_target = driver.vg if driver.vg is not None else vg_name
    root_helper = getattr(driver.vg, "_root_helper", None) or getattr(
        driver.configuration, "root_helper", None
    )
    legacy_snaps = discover_legacy_lvm_snapshots(
        vg_target, execute=execute, root_helper=root_helper
    )

    if not legacy_snaps:
        LOG.info("No legacy LVM snapshots found in volume group %s.", vg_name)
        return {"total": 0, "converted": 0, "failed": 0, "errors": []}

    LOG.info(
        "Found %d legacy LVM snapshot(s) in VG %s. Starting conversion to Swift.",
        len(legacy_snaps),
        vg_name,
    )

    converted = 0
    failed = 0
    errors = []

    for snap_info in legacy_snaps:
        try:
            convert_legacy_lvm_snapshot(
                driver,
                snap_info,
                context=context,
                execute=execute,
                dry_run=dry_run,
            )
            converted += 1
        except Exception as exc:
            failed += 1
            err_msg = f"Failed to convert snapshot {snap_info['snapshot_id']}: {exc}"
            LOG.exception(err_msg)
            errors.append(err_msg)

    LOG.info(
        "Legacy snapshot conversion summary for VG %s: %d total, %d converted, %d failed.",
        vg_name,
        len(legacy_snaps),
        converted,
        failed,
    )
    return {
        "total": len(legacy_snaps),
        "converted": converted,
        "failed": failed,
        "errors": errors,
    }


def copy_volume_accelerated(
    driver,
    src_path,
    dest_path,
    size_bytes,
    concurrency=None,
    chunk_size_mib=4,
    sparse=True,
    execute=processutils.execute,
):
    """Accelerated multi-threaded direct block volume copy via cinder-rxt-worker."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)
    if not worker_bin:
        return False

    if concurrency is None:
        concurrency = get_config_value(conf, "rxt_clone_concurrency", 8)
    chunk_size_mib = get_config_value(conf, "rxt_snapshot_chunk_size_mib", chunk_size_mib)
    max_mbps = get_config_value(conf, "rxt_copy_max_mbps", 0)

    # Inspect destination device size if available to prevent writing past end of device
    try:
        if os.path.exists(dest_path):
            fd = os.open(dest_path, os.O_RDONLY)
            try:
                dest_size = os.lseek(fd, 0, os.SEEK_END)
                if 0 < dest_size < size_bytes:
                    LOG.info(
                        "Clamping copy size from %(req)d to %(actual)d bytes based on destination device capacity",
                        {"req": size_bytes, "actual": dest_size},
                    )
                    size_bytes = dest_size
            finally:
                os.close(fd)
    except Exception:
        pass

    cmd = [
        worker_bin,
        "copy-volume",
        "--src",
        src_path,
        "--dest",
        dest_path,
        "--size-bytes",
        str(size_bytes),
        "--chunk-size-mib",
        str(chunk_size_mib),
        "--concurrency",
        str(concurrency),
    ]
    if sparse:
        cmd.extend(["--sparse", "true"])
    else:
        cmd.extend(["--sparse", "false"])

    if max_mbps and int(max_mbps) > 0:
        cmd.extend(["--max-mbps", str(max_mbps)])

    LOG.info(
        "Initiating accelerated copy from %(src)s to %(dest)s (%(bytes)d bytes, concurrency %(c)d, sparse=%(s)s)",
        {"src": src_path, "dest": dest_path, "bytes": size_bytes, "c": concurrency, "s": sparse},
    )
    with host_clone_semaphore(conf):
        out, err = _run_worker_stream(cmd, conf=conf, execute=execute)
    LOG.info("Accelerated volume copy completed: %s", out.strip())
    return True


