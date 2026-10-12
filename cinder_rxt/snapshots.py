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
2. The Rust worker hashes 4MB chunks, uploads only dirty chunks to Swift, and
   records an append-only binary diff.
3. The transient LVM snapshot is IMMEDIATELY removed.

Result: No lingering LVM snapshots on the storage host. Storage host migrations
are 100% unencumbered and volume groups never suffer LVM snapshot exhaustion.
"""

import json
import shutil

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _

LOG = logging.getLogger(__name__)

snapshot_opts = [
    cfg.BoolOpt(
        "rxt_swift_snapshots_enabled",
        default=False,
        help="Enable Swift-backed differential snapshots to eliminate host LVM "
        "snapshot boat-anchors and unblock live volume migrations.",
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
]


def register_opts(conf):
    conf.register_opts(snapshot_opts)


def is_swift_snapshots_enabled(configuration):
    return getattr(configuration, "rxt_swift_snapshots_enabled", False)


def get_worker_binary(configuration):
    configured = getattr(configuration, "rxt_worker_path", "cinder-rxt-worker")
    resolved = shutil.which(configured)
    if resolved:
        return resolved
    # Fallback to local target/release build if available
    local_release = "/usr/local/bin/cinder-rxt-worker"
    if shutil.which(local_release):
        return local_release
    return configured


def get_swift_credentials(context, configuration):
    url = getattr(configuration, "rxt_swift_url", None)
    token = None

    if context:
        token = getattr(context, "auth_token", None)
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

    if not url or not token:
        msg = _(
            "Swift snapshot engine requires valid Swift URL and Auth Token. "
            "url=%s, token_present=%s"
        ) % (url, bool(token))
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    return url, token


def create_swift_snapshot(
    driver, snapshot, context=None, execute=processutils.execute
):
    """Create a differential snapshot in Swift and eliminate local LVM snapshot."""
    conf = driver.configuration
    worker_bin = get_worker_binary(conf)
    swift_url, swift_token = get_swift_credentials(context, conf)

    vol_name = snapshot["volume_name"]
    vol_id = snapshot["volume_id"]
    snap_id = snapshot["id"]

    temp_snap_name = "tmp-rxt-snap-%s" % snap_id
    temp_snap_dict = {
        "name": temp_snap_name,
        "volume_name": vol_name,
        "volume_id": vol_id,
        "size": snapshot["volume_size"],
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
            conf.rxt_swift_container_prefix,
            "--chunk-size-mib",
            str(conf.rxt_snapshot_chunk_size_mib),
            "--concurrency",
            str(conf.rxt_snapshot_concurrency),
        ]
        out, err = execute(*cmd, run_as_root=False)
        LOG.info(
            "Swift differential snapshot %(snap)s completed: %(out)s",
            {"snap": snap_id, "out": out.strip()},
        )
    finally:
        # 3. Immediately destroy the local LVM snapshot!
        try:
            driver.vg.delete(driver._escape_snapshot(temp_snap_name))
            LOG.info(
                "Transient LVM snapshot %(temp)s destroyed successfully. "
                "Storage host remains completely unencumbered.",
                {"temp": temp_snap_name},
            )
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
    swift_url, swift_token = get_swift_credentials(context, conf)

    vol_name = volume["name"]
    vol_size = volume["size"]
    orig_vol_id = snapshot["volume_id"]
    snap_id = snapshot["id"]

    # 1. Create and activate the destination logical volume
    mirror_count = conf.lvm_mirrors if conf.lvm_mirrors else 0
    driver._create_volume(
        vol_name,
        driver._sizestr(vol_size),
        conf.lvm_type,
        mirror_count,
    )
    dest_dev_path = driver.local_path(volume)

    LOG.info(
        "Restoring Swift snapshot %(snap)s (origin vol %(orig)s) to new LV %(dest)s",
        {"snap": snap_id, "orig": orig_vol_id, "dest": dest_dev_path},
    )

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
        conf.rxt_swift_container_prefix,
        "--concurrency",
        str(conf.rxt_snapshot_concurrency),
    ]
    out, err = execute(*cmd, run_as_root=False)
    LOG.info(
        "Swift snapshot restore to %(vol)s finished: %(out)s",
        {"vol": vol_name, "out": out.strip()},
    )
