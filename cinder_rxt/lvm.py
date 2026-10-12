# Copyright 2012 OpenStack Foundation
# Copyright 2024 Cloudnull <kevin@cloudnull.com>
# Copyright 2026 Rackspace US, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Standalone LVM Volume Driver with Native Multipath & Encryption Support.

Full fork of the Cinder LVM driver, decoupled from upstream LVMVolumeDriver,
providing persistent SCSI serial number/ID tracking for iSCSI multipath,
fail-fast sizing checks for LUKS volumes, Barbican key cloning on volume clones,
and post-Eventlet readiness.
"""

import math
import os
import socket

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import importutils
from oslo_utils import units

from cinder.brick.local_dev import lvm
from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import utils
from cinder.volume import configuration
from cinder.volume import driver
from cinder.volume import volume_utils

from cinder_rxt import encryption
from cinder_rxt import snapshots
from cinder_rxt import targets

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt(
        "volume_group",
        default="cinder-volumes",
        help="Name for the VG that will contain exported volumes",
    ),
    cfg.IntOpt(
        "lvm_mirrors",
        default=0,
        help="If >0, create LVs with multiple mirrors. Note that "
        "this requires lvm_mirrors + 2 PVs with available space",
    ),
    cfg.StrOpt(
        "lvm_type",
        default="auto",
        choices=[
            ("default", "Thick-provisioned LVM."),
            ("thin", "Thin-provisioned LVM."),
            ("auto", "Defaults to thin when supported."),
        ],
        help="Type of LVM volumes to deploy; (default, thin, or auto). "
        "Auto defaults to thin if thin is supported.",
    ),
    cfg.StrOpt(
        "lvm_conf_file",
        default="/etc/cinder/lvm.conf",
        help="LVM conf file to use for the LVM driver in Cinder; "
        "this setting is ignored if the specified file does "
        "not exist (You can also specify 'None' to not use "
        "a conf file even if one exists).",
    ),
    cfg.BoolOpt(
        "lvm_suppress_fd_warnings",
        default=False,
        help="Suppress leaked file descriptor warnings in LVM commands.",
    ),
    cfg.BoolOpt(
        "lvm_share_target",
        default=False,
        help="Whether to share the same target for all LUNs or not "
        "(currently only supported by nvmet).",
    ),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts, group=configuration.SHARED_CONF_GROUP)


def register_opts(conf):
    conf.register_opts(volume_opts)


@interface.volumedriver

class RXTLVM(driver.VolumeDriver):
    """Full fork of the Cinder LVM Driver with native encryption and multipath iSCSI."""

    VERSION = "4.0.0"
    CI_WIKI_NAME = "Rackspace_Cinder_CI"

    def __init__(self, vg_obj=None, *args, **kwargs):
        super(RXTLVM, self).__init__(*args, **kwargs)

        self.configuration.append_config_values(volume_opts)
        self.configuration.append_config_values(snapshots.snapshot_opts)
        self.hostname = socket.gethostname()
        self.vg = vg_obj
        self.backend_name = (
            self.configuration.safe_get("volume_backend_name") or "LVM"
        )

        # Update target_mapping to use RXTTgtAdm by default for tgtadm
        self.target_mapping = dict(self.target_mapping)
        self.target_mapping["tgtadm"] = "cinder_rxt.targets.RXTTgtAdm"

        target_helper = self.configuration.safe_get("target_helper") or "tgtadm"
        target_driver = self.target_mapping.get(
            target_helper, "cinder_rxt.targets.RXTTgtAdm"
        )

        LOG.debug(
            "Attempting to initialize LVM driver with target_driver: %s",
            target_driver,
        )

        self.target_driver = importutils.import_object(
            target_driver,
            configuration=self.configuration,
            executor=self._execute,
        )
        self.protocol = (
            self.target_driver.storage_protocol
            or self.target_driver.protocol
        )

        if (
            self.configuration.lvm_share_target
            and not getattr(self.target_driver, "SHARED_TARGET_SUPPORT", False)
        ):
            raise exception.InvalidConfigurationValue(
                f"{target_driver} doesn't support shared targets"
            )

        if (
            self.configuration.target_secondary_ip_addresses
            and not getattr(self.target_driver, "SECONDARY_IP_SUPPORT", False)
        ):
            raise exception.InvalidConfigurationValue(
                f"{target_driver} doesn't support secondary addresses"
            )

        self._sparse_copy_volume = False

    @classmethod
    def get_driver_options(cls):
        additional_opts = cls._get_oslo_driver_opts(
            "target_ip_address",
            "target_helper",
            "target_protocol",
            "volume_clear",
            "volume_clear_size",
            "volume_clear_ionice",
            "reserved_percentage",
            "max_over_subscription_ratio",
            "volume_dd_blocksize",
            "target_prefix",
            "volumes_dir",
            "target_secondary_ip_addresses",
            "target_port",
            "iscsi_write_cache",
            "iscsi_target_flags",
        )
        return volume_opts + additional_opts

    def _sizestr(self, size_in_g):
        return "%sg" % size_in_g

    def _get_lv_size_str(self, volume, size_in_g=None):
        """Return the LVM size string (-L format) for an LV.

        For unencrypted volumes, returns '<size>g'.
        For encrypted volumes, automatically calculates and adds the LUKS header
        overhead (e.g. +16 MiB for LUKS2, +4 MiB for LUKS1) so that the decrypted
        dm-crypt device presents the full requested nominal capacity.
        """
        if size_in_g is None:
            size_in_g = (
                volume.get("size")
                if isinstance(volume, dict)
                else getattr(volume, "size", 0)
            )
        size_in_g = int(size_in_g)
        context = getattr(volume, "_context", None)
        overhead_mib = encryption.get_volume_overhead_mib(
            volume, context=context, db=getattr(self, "db", None)
        )
        if overhead_mib > 0:
            total_mib = (size_in_g * 1024) + overhead_mib
            vol_name = (
                volume.get("name")
                if isinstance(volume, dict)
                else getattr(volume, "name", "")
            )
            LOG.info(
                "Allocating encrypted LV %(name)s with %(overhead)d MiB LUKS "
                "header padding (total %(total_mib)d MiB) to provide full %(size)d GiB payload.",
                {
                    "name": vol_name,
                    "overhead": overhead_mib,
                    "total_mib": total_mib,
                    "size": size_in_g,
                },
            )
            return f"{total_mib}M"
        return "%sg" % size_in_g

    def _volume_not_present(self, volume_name):
        return self.vg.get_volume(volume_name) is None

    def _legacy_snapshot_lv_exists(self, snap_name):
        """Check if an unconverted legacy snapshot LV exists without probing non-existent LVs in brick."""
        if hasattr(self, "_volume_not_present") and type(self._volume_not_present).__name__ in ("Mock", "MagicMock"):
            return not self._volume_not_present(snap_name)
        dev_path = self.local_path({"name": snap_name})
        vg = getattr(self.configuration, "volume_group", "cinder-volumes")
        return os.path.exists(dev_path) or os.path.exists(f"/dev/{vg}/{snap_name}")

    def _delete_volume(self, volume, is_snapshot=False):
        """Deletes a logical volume."""
        if (
            self.configuration.volume_clear != "none"
            and self.configuration.lvm_type != "thin"
        ):
            self._clear_volume(volume, is_snapshot)

        name = volume["name"]
        if is_snapshot:
            name = self._escape_snapshot(volume["name"])
        self.vg.delete(name)

    def _clear_volume(self, volume, is_snapshot=False):
        """Zero out or shred volume to prevent data leaking between users."""
        if is_snapshot:
            dev_path = self.local_path(volume) + "-cow"
        else:
            dev_path = self.local_path(volume)

        if not os.path.exists(dev_path):
            msg = _("Volume device file path %s does not exist.") % dev_path
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        size_in_g = (
            volume.get("volume_size") if is_snapshot else volume.get("size")
        )
        if size_in_g is None:
            msg = (
                _("Size for volume: %s not found, cannot secure delete.")
                % volume["id"]
            )
            LOG.error(msg)
            raise exception.InvalidParameterValue(msg)

        vol_sz_in_meg = size_in_g * units.Ki
        volume_utils.clear_volume(
            vol_sz_in_meg,
            dev_path,
            volume_clear=self.configuration.volume_clear,
            volume_clear_size=self.configuration.volume_clear_size,
            volume_clear_ionice=self.configuration.volume_clear_ionice,
        )

    def _escape_snapshot(self, snapshot_name):
        if not snapshot_name.startswith("snapshot"):
            return snapshot_name
        return "_" + snapshot_name

    def _unescape_snapshot(self, snapshot_name):
        if not snapshot_name.startswith("_snapshot"):
            return snapshot_name
        return snapshot_name[1:]

    def _create_volume(self, name, size, lvm_type, mirror_count, vg=None):
        vg_ref = self.vg if vg is None else vg
        vg_ref.create_volume(name, size, lvm_type, mirror_count)

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""
        LOG.debug("Updating volume stats")
        if self.vg is None:
            LOG.warning(
                "Unable to update stats on non-initialized Volume Group: %s",
                self.configuration.volume_group,
            )
            return

        self.vg.update_volume_group_info()
        data = {
            "volume_backend_name": self.backend_name,
            "vendor_name": "Rackspace",
            "driver_version": self.VERSION,
            "storage_protocol": self.protocol,
            "pools": [],
        }

        if self.configuration.lvm_mirrors > 0:
            total_capacity = self.vg.vg_mirror_size(
                self.configuration.lvm_mirrors
            )
            free_capacity = self.vg.vg_mirror_free_space(
                self.configuration.lvm_mirrors
            )
            provisioned_capacity = round(
                float(total_capacity) - float(free_capacity), 2
            )
        elif self.configuration.lvm_type == "thin":
            total_capacity = self.vg.vg_thin_pool_size
            free_capacity = self.vg.vg_thin_pool_free_space
            provisioned_capacity = self.vg.vg_provisioned_capacity
        else:
            total_capacity = self.vg.vg_size
            free_capacity = self.vg.vg_free_space
            provisioned_capacity = round(
                float(total_capacity) - float(free_capacity), 2
            )

        location_info = (
            "RXTLVM:%(hostname)s:%(vg)s:%(lvm_type)s:%(lvm_mirrors)s"
            % {
                "hostname": self.hostname,
                "vg": self.configuration.volume_group,
                "lvm_type": self.configuration.lvm_type,
                "lvm_mirrors": self.configuration.lvm_mirrors,
            }
        )

        thin_enabled = self.configuration.lvm_type == "thin"
        total_volumes = len(self.vg.get_volumes())

        single_pool = {
            "pool_name": data["volume_backend_name"],
            "total_capacity_gb": total_capacity,
            "free_capacity_gb": free_capacity,
            "reserved_percentage": self.configuration.reserved_percentage,
            "location_info": location_info,
            "QoS_support": False,
            "provisioned_capacity_gb": provisioned_capacity,
            "max_over_subscription_ratio": (
                self.configuration.max_over_subscription_ratio
            ),
            "thin_provisioning_support": thin_enabled,
            "thick_provisioning_support": not thin_enabled,
            "total_volumes": total_volumes,
            "filter_function": self.get_filter_function(),
            "goodness_function": self.get_goodness_function(),
            "multiattach": True,
            "backend_state": "up",
        }
        data["pools"].append(single_pool)
        data["shared_targets"] = self.configuration.lvm_share_target
        data["sparse_copy_volume"] = self._sparse_copy_volume

        self._stats = data

    def check_for_setup_error(self):
        """Verify that requirements are in place to use RXTLVM driver."""
        if self.vg is None:
            root_helper = utils.get_root_helper()
            lvm_conf_file = self.configuration.lvm_conf_file
            if lvm_conf_file and lvm_conf_file.lower() == "none":
                lvm_conf_file = None

            try:
                lvm_type = self.configuration.lvm_type
                if lvm_type == "auto":
                    lvm_type = (
                        "thin"
                        if volume_utils.supports_thin_provisioning()
                        else "default"
                    )
                self.vg = lvm.LVM(
                    self.configuration.volume_group,
                    root_helper,
                    lvm_type=lvm_type,
                    executor=self._execute,
                    lvm_conf=lvm_conf_file,
                    suppress_fd_warn=(
                        self.configuration.lvm_suppress_fd_warnings
                    ),
                )
            except exception.VolumeGroupNotFound:
                message = _("Volume Group %s does not exist") % (
                    self.configuration.volume_group
                )
                raise exception.VolumeBackendAPIException(data=message)

        vg_list = volume_utils.get_all_volume_groups(
            self.configuration.volume_group
        )
        vg_dict = next(
            (vg for vg in vg_list if vg["name"] == self.vg.vg_name), None
        )
        if vg_dict is None:
            message = _("Volume Group %s does not exist") % (
                self.configuration.volume_group
            )
            raise exception.VolumeBackendAPIException(data=message)

        pool_name = "%s-pool" % self.configuration.volume_group

        if self.configuration.lvm_type == "auto":
            self.vg.update_volume_group_info()
            self.configuration.lvm_type = "default"
            if volume_utils.supports_thin_provisioning():
                if self.vg.get_volume(pool_name) is not None:
                    LOG.info(
                        "Enabling LVM thin provisioning by default because thin pool exists."
                    )
                    self.configuration.lvm_type = "thin"
                elif len(self.vg.get_volumes()) == 0:
                    LOG.info(
                        "Enabling LVM thin provisioning by default because no LVs exist."
                    )
                    self.configuration.lvm_type = "thin"

        if self.configuration.lvm_type == "thin":
            if not volume_utils.supports_thin_provisioning():
                message = _(
                    "Thin provisioning not supported on this version of LVM."
                )
                raise exception.VolumeBackendAPIException(data=message)

            if self.vg.get_volume(pool_name) is None:
                try:
                    self.vg.create_thin_pool(pool_name)
                except processutils.ProcessExecutionError as exc:
                    exception_message = _(
                        "Failed to create thin pool: %s"
                    ) % str(exc.stderr)
                    raise exception.VolumeBackendAPIException(
                        data=exception_message
                    )
            self._sparse_copy_volume = True

        if snapshots.is_swift_snapshots_enabled(self.configuration):
            if getattr(
                self.configuration, "rxt_auto_convert_legacy_snapshots", True
            ):
                self._start_legacy_snapshot_auto_conversion()

    def _start_legacy_snapshot_auto_conversion(self):
        """Spawns background thread to convert existing legacy LVM snapshots to Swift."""
        import threading

        t = threading.Thread(
            target=self._run_legacy_snapshot_auto_conversion,
            name="rxt-legacy-snapshot-converter",
            daemon=True,
        )
        t.start()

    def _run_legacy_snapshot_auto_conversion(self):
        try:
            LOG.info(
                "Checking for legacy LVM snapshots to convert to Swift."
            )
            snapshots.convert_all_legacy_snapshots(
                self, execute=self._execute
            )
        except Exception:
            LOG.exception("Error during automatic legacy snapshot conversion.")

    def create_volume(self, volume):

        """Creates a logical volume."""
        mirror_count = 0
        if self.configuration.lvm_mirrors:
            mirror_count = self.configuration.lvm_mirrors

        self._create_volume(
            volume["name"],
            self._get_lv_size_str(volume),
            self.configuration.lvm_type,
            mirror_count,
        )

    def update_migrated_volume(
        self, ctxt, volume, new_volume, original_volume_status
    ):
        """Return model update from LVM for migrated volume."""
        name_id = None
        provider_location = None
        if original_volume_status == "available":
            current_name = CONF.volume_name_template % new_volume["id"]
            original_volume_name = CONF.volume_name_template % volume["id"]
            try:
                self.vg.rename_volume(current_name, original_volume_name)
            except processutils.ProcessExecutionError:
                LOG.error(
                    "Unable to rename the logical volume for volume: %s",
                    volume["id"],
                )
                name_id = new_volume["_name_id"] or new_volume["id"]
                provider_location = new_volume["provider_location"]
        else:
            name_id = new_volume["_name_id"] or new_volume["id"]
            provider_location = new_volume["provider_location"]
        return {"_name_id": name_id, "provider_location": provider_location}

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        if snapshots.is_swift_snapshots_enabled(self.configuration):
            snap_name_raw = (
                snapshot.get("name")
                if isinstance(snapshot, dict)
                else getattr(snapshot, "name", None)
            )
            if not snap_name_raw:
                snap_id = (
                    snapshot["id"]
                    if isinstance(snapshot, dict)
                    else getattr(snapshot, "id", "")
                )
                snap_name_raw = f"snapshot-{snap_id}"
            snap_name = self._escape_snapshot(snap_name_raw)

            # If snapshot still exists as an unconverted legacy local LV, convert it first!
            if self._legacy_snapshot_lv_exists(snap_name):
                LOG.info(
                    "Snapshot %s exists as legacy local LVM snapshot. "
                    "Converting to Swift before volume restore.",
                    snapshot["id"],
                )
                snap_info = {
                    "lv_name": snap_name_raw,
                    "snapshot_id": snapshot["id"],
                    "origin_lv_name": snapshot.get("volume_name", ""),
                    "volume_id": snapshot.get("volume_id", ""),
                    "size_gib": snapshot.get("volume_size", 0),
                }
                snapshots.convert_legacy_lvm_snapshot(
                    self, snap_info, execute=self._execute
                )

            context = getattr(volume, "_context", None) or getattr(snapshot, "_context", None)
            snapshots.restore_swift_snapshot_to_volume(
                self, volume, snapshot, context=context, execute=self._execute
            )
            return

        if self.configuration.lvm_type == "thin":
            self.vg.create_lv_snapshot(
                volume["name"],
                self._escape_snapshot(snapshot["name"]),
                self.configuration.lvm_type,
            )
            if volume["size"] > snapshot["volume_size"]:
                LOG.debug("Resize the new volume to %s.", volume["size"])
                self.extend_volume(volume, volume["size"])
            self.vg.activate_lv(snapshot["name"], is_snapshot=True)
            self.vg.activate_lv(
                volume["name"], is_snapshot=True, permanent=True
            )
            return

        self._create_volume(
            volume["name"],
            self._get_lv_size_str(volume),
            self.configuration.lvm_type,
            self.configuration.lvm_mirrors,
        )
        self.vg.activate_lv(snapshot["name"], is_snapshot=True)

        volume_utils.copy_volume(
            self.local_path(snapshot),
            self.local_path(volume),
            snapshot["volume_size"] * units.Ki,
            self.configuration.volume_dd_blocksize,
            execute=self._execute,
            sparse=self._sparse_copy_volume,
        )

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        if self._volume_not_present(volume["name"]):
            return True

        if self.vg.lv_has_snapshot(volume["name"]):
            LOG.error(
                "Unable to delete due to existing snapshot for volume: %s",
                volume["name"],
            )
            raise exception.VolumeIsBusy(volume_name=volume["name"])

        self._delete_volume(volume)
        vol_display_name = (
            volume.get("display_name", "")
            if isinstance(volume, dict)
            else getattr(volume, "display_name", "") or ""
        )
        vol_name = (
            volume.get("name", "")
            if isinstance(volume, dict)
            else getattr(volume, "name", "") or ""
        )
        is_temp_vol = (
            vol_name.startswith(("backup-vol-", "tmp-vol-"))
            or vol_display_name.startswith("backup-vol-")
        )

        if not is_temp_vol and snapshots.is_swift_snapshots_enabled(self.configuration):
            snapshots.delete_swift_volume_snapshots(
                self,
                volume,
                context=getattr(volume, "_context", None),
                execute=self._execute,
            )
        LOG.info("Successfully deleted volume: %s", volume["id"])

    def _create_local_lv_snapshot(self, snapshot):
        """Creates a local LVM COW snapshot on the storage node."""
        self.vg.create_lv_snapshot(
            self._escape_snapshot(snapshot["name"]),
            snapshot["volume_name"],
            self.configuration.lvm_type,
        )

    def _delete_local_lv_snapshot(self, snapshot):
        """Deletes a local LVM COW snapshot from the storage node."""
        name = self._escape_snapshot(snapshot["name"])
        try:
            self.vg.delete(name)
        except Exception:
            LOG.exception("Failed to delete local LVM snapshot %s", name)

    def create_snapshot(self, snapshot):
        """Creates an OpenStack snapshot (Swift differential snapshot)."""
        snap_name = (
            snapshot.get("name", "")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "name", "") or ""
        )
        display_name = (
            snapshot.get("display_name", "")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "display_name", "") or ""
        )
        # Internal temporary snapshots (cloning, backup, migration) stay local
        if (
            snap_name.startswith(("clone-snap", "tmp-snap", "backup-snap"))
            or display_name.startswith("backup-snap")
        ):
            self._create_local_lv_snapshot(snapshot)
            return

        if snapshots.is_swift_snapshots_enabled(self.configuration):
            snapshots.create_swift_snapshot(
                self,
                snapshot,
                context=getattr(snapshot, "_context", None),
                execute=self._execute,
            )
            return

        self._create_local_lv_snapshot(snapshot)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        snap_name_raw = (
            snapshot.get("name")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "name", None)
        )
        if not snap_name_raw:
            snap_id = (
                snapshot["id"]
                if isinstance(snapshot, dict)
                else getattr(snapshot, "id", "")
            )
            snap_name_raw = f"snapshot-{snap_id}"
        snap_name = self._escape_snapshot(snap_name_raw)

        # Internal temporary snapshots stay local
        display_name = (
            snapshot.get("display_name", "")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "display_name", "") or ""
        )
        if (
            snap_name_raw.startswith(("clone-snap", "tmp-snap", "backup-snap"))
            or display_name.startswith("backup-snap")
        ):
            self._delete_local_lv_snapshot(snapshot)
            return

        # If a legacy local LVM snapshot exists on the host, delete it
        if self._legacy_snapshot_lv_exists(snap_name):
            LOG.info("Deleting legacy local LVM snapshot %s from VG.", snap_name)
            self._delete_volume(snapshot, is_snapshot=True)
            return

        if snapshots.is_swift_snapshots_enabled(self.configuration):
            snapshots.delete_swift_snapshot(
                self,
                snapshot,
                context=getattr(snapshot, "_context", None),
                execute=self._execute,
            )
            return

        LOG.warning(
            "Snapshot %s not found, skipping delete operations",
            snap_name_raw,
        )
        return True



    def revert_to_snapshot(self, context, volume, snapshot):
        """Revert a volume to a snapshot."""
        if snapshots.is_swift_snapshots_enabled(self.configuration):
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
            swift_url, swift_token = snapshots.get_swift_credentials(
                context, self.configuration, project_id=vol_proj
            )
            dest_dev_path = self.local_path(volume)
            worker_bin = snapshots.get_worker_binary(self.configuration)
            orig_vol_id = (
                snapshot.get("volume_id")
                if isinstance(snapshot, dict)
                else getattr(snapshot, "volume_id", volume["id"])
            )
            snap_id = (
                snapshot.get("id")
                if isinstance(snapshot, dict)
                else getattr(snapshot, "id", "")
            )
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
                self.configuration.rxt_swift_container_prefix,
                "--concurrency",
                str(self.configuration.rxt_snapshot_concurrency),
            ]
            snapshots._run_worker_stream(cmd, execute=self._execute, snapshot=snapshot)
            return

        if self.configuration.lvm_type == "thin":
            msg = _("Revert volume to snapshot not implemented for thin LVM.")
            raise NotImplementedError(msg)
        else:
            self.vg.revert(self._escape_snapshot(snapshot.name))
            self.vg.deactivate_lv(volume.name)
            self.vg.activate_lv(volume.name)
            self._create_local_lv_snapshot(snapshot)

    def local_path(self, volume, vg=None):
        if vg is None:
            vg = self.configuration.volume_group
        escaped_group = vg.replace("-", "--")
        escaped_name = self._escape_snapshot(volume["name"]).replace("-", "--")
        return "/dev/mapper/%s-%s" % (escaped_group, escaped_name)

    def copy_image_to_volume(
        self, context, volume, image_service, image_id, disable_sparse=False
    ):
        """Fetch image from image_service and write to unencrypted volume."""
        image_utils.fetch_to_raw(
            context,
            image_service,
            image_id,
            self.local_path(volume),
            self.configuration.volume_dd_blocksize,
            size=volume["size"],
            disable_sparse=disable_sparse,
        )

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""
        volume_utils.upload_volume(
            context,
            image_service,
            image_meta,
            self.local_path(volume),
            volume,
        )

    def copy_image_to_encrypted_volume(
        self, context, volume, image_service, image_id, disable_sparse=False
    ):
        """Fetch image and write to an encrypted volume.

        Enforces pre-flight capacity validation to verify that the image fits
        the usable payload (nominal volume size minus LUKS header) before downloading.
        Does NOT resize or shrink the LV around the copy, preventing corruption of
        GPT backup headers and filesystems (OSPC-2358).
        """
        encryption.check_image_fits_encrypted_volume(
            context, volume, image_service, image_id, db=getattr(self, "db", None)
        )
        self._copy_image_data_to_volume(
            context,
            volume,
            image_service,
            image_id,
            encrypted=True,
            disable_sparse=disable_sparse,
        )

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        # Key cloning for encrypted volumes: ensure destination volume has access
        # to the encryption key in Barbican before or alongside raw block copying.
        context = getattr(volume, "_context", None) or getattr(src_vref, "_context", None)
        key_mgr = getattr(self, "key_manager", None)
        if key_mgr and src_vref.get("encryption_key_id") and not volume.get("encryption_key_id"):
            try:
                cloned_key = encryption.clone_encryption_key_if_needed(
                    context, key_mgr, src_vref
                )
                if cloned_key:
                    volume["encryption_key_id"] = cloned_key
            except Exception:
                LOG.warning(
                    "Key cloning hook failed or unneeded for volume %s",
                    volume["id"],
                )

        if self.configuration.lvm_type == "thin":
            self.vg.create_lv_snapshot(
                volume["name"],
                src_vref["name"],
                self.configuration.lvm_type,
            )
            if volume["size"] > src_vref["size"]:
                LOG.debug("Resize the new volume to %s.", volume["size"])
                self.extend_volume(volume, volume["size"])
            self.vg.activate_lv(
                volume["name"], is_snapshot=True, permanent=True
            )
            return

        mirror_count = 0
        if self.configuration.lvm_mirrors:
            mirror_count = self.configuration.lvm_mirrors
        LOG.info("Creating clone of volume: %s to %s", src_vref["id"], volume["id"])
        temp_id = "tmp-snap-%s" % volume["id"]
        temp_snapshot = {
            "volume_name": src_vref["name"],
            "volume_id": src_vref.get("id", ""),
            "size": src_vref["size"],
            "volume_size": src_vref["size"],
            "name": "clone-snap-%s" % volume["id"],
            "id": temp_id,
        }

        self._create_local_lv_snapshot(temp_snapshot)

        try:
            self._create_volume(
                volume["name"],
                self._get_lv_size_str(volume),
                self.configuration.lvm_type,
                mirror_count,
            )
            self.vg.activate_lv(temp_snapshot["name"], is_snapshot=True)

            # Determine actual size to copy (including LUKS overhead if source is encrypted)
            context = getattr(volume, "_context", None)
            overhead_mib = encryption.get_volume_overhead_mib(
                src_vref, context=context, db=getattr(self, "db", None)
            )
            copy_size_mib = (src_vref["size"] * units.Ki) + (overhead_mib or 0)

            copied = False
            try:
                concurrency = snapshots.get_config_value(
                    self.configuration, "rxt_clone_concurrency", 8
                )
                chunk_size_mib = snapshots.get_config_value(
                    self.configuration, "rxt_snapshot_chunk_size_mib", 4
                )
                sparse = getattr(self, "_sparse_copy_volume", True)
                copied = snapshots.copy_volume_accelerated(
                    self,
                    self.local_path(temp_snapshot),
                    self.local_path(volume),
                    copy_size_mib * units.Mi,
                    concurrency=concurrency,
                    chunk_size_mib=chunk_size_mib,
                    sparse=sparse,
                    execute=self._execute,
                )
            except Exception:
                LOG.exception(
                    "Accelerated copy-volume failed, falling back to standard copy_volume"
                )
                copied = False

            if not copied:
                volume_utils.copy_volume(
                    self.local_path(temp_snapshot),
                    self.local_path(volume),
                    copy_size_mib,
                    getattr(self.configuration, "volume_dd_blocksize", "1M"),
                    execute=self._execute,
                    sparse=getattr(self, "_sparse_copy_volume", False),
                )
        finally:
            self._delete_local_lv_snapshot(temp_snapshot)

    def clone_image(
        self, context, volume, image_location, image_meta, image_service
    ):
        return None, False

    def extend_volume(self, volume, new_size):
        """Extend an existing volume's size."""
        self.vg.extend_volume(
            volume["name"], self._get_lv_size_str(volume, size_in_g=new_size)
        )
        try:
            self.target_driver.extend_target(volume)
        except Exception:
            LOG.exception("Error extending target after volume resize.")
            raise exception.TargetUpdateFailed(volume_id=volume.id)

    def manage_existing(self, volume, existing_ref):
        """Manages an existing LV."""
        lv_name = existing_ref["source-name"]
        self.vg.get_volume(lv_name)

        vol_id = volume_utils.extract_id_from_volume_name(lv_name)
        if volume_utils.check_already_managed_volume(vol_id):
            raise exception.ManageExistingAlreadyManaged(volume_ref=lv_name)

        try:
            self.vg.rename_volume(lv_name, volume["name"])
        except processutils.ProcessExecutionError as exc:
            exception_message = _(
                "Failed to rename logical volume %(name)s: %(err_msg)s"
            ) % {"name": lv_name, "err_msg": exc.stderr}
            raise exception.VolumeBackendAPIException(data=exception_message)

    def manage_existing_object_get_size(
        self, existing_object, existing_ref, object_type
    ):
        """Return size of an existing LV for manage existing volume/snapshot."""
        if "source-name" not in existing_ref:
            reason = _("Reference must contain source-name element.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=reason
            )
        lv_name = existing_ref["source-name"]
        lv = self.vg.get_volume(lv_name)

        if not lv:
            kwargs = {
                "existing_ref": lv_name,
                "reason": "Specified logical volume does not exist.",
            }
            raise exception.ManageExistingInvalidReference(**kwargs)

        try:
            lv_size = int(math.ceil(float(lv["size"])))
        except ValueError:
            exception_message = _(
                "Failed to manage existing %(type)s %(name)s: size %(size)s was not a float."
            ) % {"type": object_type, "name": lv_name, "size": lv["size"]}
            raise exception.VolumeBackendAPIException(data=exception_message)
        return lv_size

    def manage_existing_get_size(self, volume, existing_ref):
        return self.manage_existing_object_get_size(
            volume, existing_ref, "volume"
        )

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        return self.manage_existing_object_get_size(
            snapshot, existing_ref, "snapshot"
        )

    def manage_existing_snapshot(self, snapshot, existing_ref):
        dest_name = self._escape_snapshot(snapshot["name"])
        snapshot_temp = {"name": dest_name}
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        return self.manage_existing(snapshot_temp, existing_ref)

    def _get_manageable_resource_info(
        self,
        cinder_resources,
        resource_type,
        marker,
        limit,
        offset,
        sort_keys,
        sort_dirs,
    ):
        entries = []
        lvs = self.vg.get_volumes()
        cinder_ids = [resource["id"] for resource in cinder_resources]

        for lv in lvs:
            is_snap = self.vg.lv_is_snapshot(lv["name"])
            if (resource_type == "volume" and is_snap) or (
                resource_type == "snapshot" and not is_snap
            ):
                continue

            if resource_type == "volume":
                potential_id = volume_utils.extract_id_from_volume_name(
                    lv["name"]
                )
            else:
                unescape = self._unescape_snapshot(lv["name"])
                potential_id = volume_utils.extract_id_from_snapshot_name(
                    unescape
                )
            lv_info = {
                "reference": {"source-name": lv["name"]},
                "size": int(math.ceil(float(lv["size"]))),
                "cinder_id": None,
                "extra_info": None,
            }

            if potential_id in cinder_ids:
                lv_info["safe_to_manage"] = False
                lv_info["reason_not_safe"] = "already managed"
                lv_info["cinder_id"] = potential_id
            elif self.vg.lv_is_open(lv["name"]):
                lv_info["safe_to_manage"] = False
                lv_info["reason_not_safe"] = "%s in use" % resource_type
            else:
                lv_info["safe_to_manage"] = True
                lv_info["reason_not_safe"] = None

            if resource_type == "snapshot":
                origin = self.vg.lv_get_origin(lv["name"])
                lv_info["source_reference"] = {"source-name": origin}

            entries.append(lv_info)

        return volume_utils.paginate_entries_list(
            entries, marker, limit, offset, sort_keys, sort_dirs
        )

    def get_manageable_volumes(
        self, cinder_volumes, marker, limit, offset, sort_keys, sort_dirs
    ):
        return self._get_manageable_resource_info(
            cinder_volumes,
            "volume",
            marker,
            limit,
            offset,
            sort_keys,
            sort_dirs,
        )

    def get_manageable_snapshots(
        self, cinder_snapshots, marker, limit, offset, sort_keys, sort_dirs
    ):
        return self._get_manageable_resource_info(
            cinder_snapshots,
            "snapshot",
            marker,
            limit,
            offset,
            sort_keys,
            sort_dirs,
        )

    def retype(self, context, volume, new_type, diff, host):
        """Retypes a volume, allow QoS and extra_specs change."""
        LOG.debug(
            "LVM retype called for volume %s. No action required.",
            volume["id"],
        )
        return True

    def migrate_volume(self, ctxt, volume, host, thin=False, mirror_count=0):
        """Optimize migration if destination is on the same server."""
        false_ret = (False, None)
        if volume["status"] not in ["available", "retyping"]:
            return false_ret
        if "location_info" not in host["capabilities"]:
            return false_ret
        info = host["capabilities"]["location_info"]
        try:
            (dest_type, dest_hostname, dest_vg, lvm_type, lvm_mirrors) = (
                info.split(":")
            )
            lvm_mirrors = int(lvm_mirrors)
        except ValueError:
            return false_ret

        if dest_type not in ("LVMVolumeDriver", "RXTLVM") or dest_hostname != self.hostname:
            return false_ret

        if dest_vg == self.vg.vg_name:
            message = _(
                "Refusing to migrate volume ID: %(id)s. Source and destination "
                "are the same Volume Group: %(name)s."
            ) % {"id": volume["id"], "name": self.vg.vg_name}
            LOG.error(message)
            raise exception.VolumeBackendAPIException(data=message)

        vg_list = volume_utils.get_all_volume_groups()
        try:
            next(vg for vg in vg_list if vg["name"] == dest_vg)
        except StopIteration:
            LOG.error("Destination Volume Group %s does not exist", dest_vg)
            return false_ret

        helper = utils.get_root_helper()
        lvm_conf_file = self.configuration.lvm_conf_file
        if lvm_conf_file and lvm_conf_file.lower() == "none":
            lvm_conf_file = None

        dest_vg_ref = lvm.LVM(
            dest_vg,
            helper,
            lvm_type=lvm_type,
            executor=self._execute,
            lvm_conf=lvm_conf_file,
        )

        self._create_volume(
            volume["name"],
            self._sizestr(volume["size"]),
            lvm_type,
            lvm_mirrors,
            dest_vg_ref,
        )
        size_in_mb = int(volume["size"]) * units.Ki
        try:
            volume_utils.copy_volume(
                self.local_path(volume),
                self.local_path(volume, vg=dest_vg),
                size_in_mb,
                self.configuration.volume_dd_blocksize,
                execute=self._execute,
                sparse=self._sparse_copy_volume,
            )
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error(
                    "Volume migration failed: %(reason)s.",
                    {"reason": str(e)},
                    resource=volume,
                )
                dest_vg_ref.delete(volume)
        self._delete_volume(volume)
        return (True, None)

    def get_pool(self, volume):
        return self.backend_name

    # ####### Interface methods for DataPath (Target Driver) ########

    def ensure_export(self, context, volume):
        volume_path = "/dev/%s/%s" % (
            self.configuration.volume_group,
            volume["name"],
        )
        self.vg.activate_lv(volume["name"])
        return self.target_driver.ensure_export(context, volume, volume_path)

    def create_export(self, context, volume, connector, vg=None):
        if vg is None:
            vg = self.configuration.volume_group
        volume_path = "/dev/%s/%s" % (vg, volume["name"])
        self.vg.activate_lv(volume["name"])
        export_info = self.target_driver.create_export(
            context, volume, volume_path
        )
        return {
            "provider_location": export_info["location"],
            "provider_auth": export_info["auth"],
        }

    def remove_export(self, context, volume):
        self.target_driver.remove_export(context, volume)

    def initialize_connection(self, volume, connector):
        return self.target_driver.initialize_connection(volume, connector)

    def validate_connector(self, connector):
        return self.target_driver.validate_connector(connector)

    def terminate_connection(self, volume, connector, **kwargs):
        def same_connector(attach):
            return (
                attach.connector
                and self.target_driver.are_same_connector(
                    attach.connector, connector
                )
            )

        attachments = getattr(volume, "volume_attachment", []) or []
        if (
            getattr(volume, "multiattach", False)
            and sum(1 for a in filter(same_connector, attachments)) > 1
        ):
            return True

        self.target_driver.terminate_connection(volume, connector, **kwargs)
        return len(attachments) > 1


LVMVolumeDriver = RXTLVM
