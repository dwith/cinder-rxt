# Copyright 2024 Cloudnull <kevin@cloudnull.com>
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

import unittest
from unittest import mock

from oslo_utils import units

from cinder import exception
from cinder.volume import driver
from cinder.volume.targets import tgt

from cinder_rxt import encryption
from cinder_rxt import lvm
from cinder_rxt import rackspace
from cinder_rxt import snapshots
from cinder_rxt import targets

GI = units.Gi
MI = units.Mi


class FakeVolume(dict):
    """Dictionary supporting attribute access, matching Cinder Volume objects."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def enc_vol(size=40, key_id="key-123"):
    return FakeVolume(
        id="vol-enc-uuid",
        name="volume-vol-enc-uuid",
        size=size,
        encryption_key_id=key_id,
    )


class TestRXTTgtAdm(unittest.TestCase):
    """Tests for the RXTTgtAdm target driver (multipath and scsi_sn/scsi_id)."""

    def test_secondary_ip_support_enabled(self):
        self.assertTrue(targets.RXTTgtAdm.SECONDARY_IP_SUPPORT)

    @mock.patch.object(tgt.TgtAdm, "create_iscsi_target")
    def test_create_iscsi_target_injects_scsi_id_and_sn(self, mock_super_create):
        conf = mock.Mock()
        conf.iscsi_write_cache = "on"
        conf.target_prefix = "iqn.2010-10.org.openstack:"

        target_helper = targets.RXTTgtAdm(configuration=conf)
        target_name = "iqn.2010-10.org.openstack:vol-uuid-456"

        target_helper.create_iscsi_target(
            name=target_name,
            tid=1,
            lun=1,
            path="/dev/mapper/cinder--volumes-vol--uuid--456",
        )

        mock_super_create.assert_called_once_with(
            target_name,
            1,
            1,
            "/dev/mapper/cinder--volumes-vol--uuid--456",
            None,
        )

        # After execution, VOLUME_CONF must have been restored
        self.assertIn("scsi_sn", target_helper.VOLUME_CONF)


class TestEncryptionModule(unittest.TestCase):
    """Tests for LUKS header calculation, pre-flight checks, and key cloning."""

    def test_luks1_header_bytes(self):
        for p in ("luks", "LUKS", "os_brick.encryptors.luks.LuksEncryptor"):
            self.assertEqual(
                encryption._LUKS1_HEADER_BYTES,
                encryption.get_luks_header_bytes(p),
            )

    def test_luks2_header_bytes(self):
        for p in ("luks2", "LUKS2", "os_brick.encryptors.luks.Luks2Encryptor"):
            self.assertEqual(
                encryption._LUKS2_HEADER_BYTES,
                encryption.get_luks_header_bytes(p),
            )

    def test_unknown_provider_defaults_to_smallest_header(self):
        self.assertEqual(
            encryption._LUKS1_HEADER_BYTES,
            encryption.get_luks_header_bytes(None),
        )
        self.assertEqual(
            encryption._LUKS1_HEADER_BYTES,
            encryption.get_luks_header_bytes(""),
        )
        self.assertEqual(
            encryption._LUKS1_HEADER_BYTES,
            encryption.get_luks_header_bytes("plain"),
        )

    def test_check_image_fits_small_image_passes(self):
        img_svc = mock.Mock()
        img_svc.show.return_value = {"id": "img-1", "virtual_size": 4 * GI}
        vol = enc_vol(size=40)

        # Should not raise
        encryption.check_image_fits_encrypted_volume(
            mock.sentinel.ctx, vol, img_svc, "img-1"
        )

    def test_check_image_fits_exact_nominal_size_passes(self):
        """A full 40 GiB image passes because the driver pads the LV with header space."""
        img_svc = mock.Mock()
        img_svc.show.return_value = {
            "id": "img-1",
            "virtual_size": 40 * GI,
        }
        vol = enc_vol(size=40)

        # Should not raise
        encryption.check_image_fits_encrypted_volume(
            mock.sentinel.ctx, vol, img_svc, "img-1"
        )

    def test_check_image_truly_oversized_raises_image_too_big(self):
        """An image genuinely larger than the nominal volume size raises ImageTooBig."""
        img_svc = mock.Mock()
        img_svc.show.return_value = {
            "id": "img-oversized",
            "virtual_size": 41 * GI,
        }
        vol = enc_vol(size=40)

        with self.assertRaises(exception.ImageTooBig) as cm:
            encryption.check_image_fits_encrypted_volume(
                mock.sentinel.ctx, vol, img_svc, "img-oversized"
            )

        msg = str(cm.exception)
        self.assertIn("img-oversized", msg)
        self.assertIn("at least 41 GiB", msg)

    def test_get_luks_header_overhead_mib(self):
        self.assertEqual(16, encryption.get_luks_header_overhead_mib("luks2"))
        self.assertEqual(4, encryption.get_luks_header_overhead_mib("luks"))
        self.assertEqual(4, encryption.get_luks_header_overhead_mib("plain"))
        self.assertEqual(16, encryption.get_luks_header_overhead_mib(None))

    def test_get_volume_overhead_mib(self):
        raw_vol = {"id": "v-raw", "size": 40}
        self.assertEqual(0, encryption.get_volume_overhead_mib(raw_vol))

        luks2_vol = enc_vol(size=40)
        self.assertEqual(16, encryption.get_volume_overhead_mib(luks2_vol))

        db = mock.Mock()
        db.volume_encryption_metadata_get.return_value = {"provider": "luks"}
        luks1_vol = enc_vol(size=40)
        self.assertEqual(
            4,
            encryption.get_volume_overhead_mib(
                luks1_vol, context=mock.sentinel.ctx, db=db
            ),
        )

    def test_check_image_missing_virtual_size_skips(self):
        img_svc = mock.Mock()
        img_svc.show.return_value = {"id": "img-nosize"}
        vol = enc_vol(size=40)

        # Should return silently without raising
        encryption.check_image_fits_encrypted_volume(
            mock.sentinel.ctx, vol, img_svc, "img-nosize"
        )

    @mock.patch("cinder.volume.volume_utils.clone_encryption_key")
    def test_clone_encryption_key_success(self, mock_clone):
        mock_clone.return_value = "cloned-key-789"
        key_mgr = mock.Mock()
        src_vol = enc_vol(key_id="source-key-123")

        result = encryption.clone_encryption_key_if_needed(
            mock.sentinel.ctx, key_mgr, src_vol
        )

        self.assertEqual("cloned-key-789", result)
        mock_clone.assert_called_once_with(
            mock.sentinel.ctx, key_mgr, "source-key-123"
        )

    def test_clone_encryption_key_unencrypted_returns_none(self):
        plain_vol = FakeVolume(id="plain", name="plain", size=10)
        key_mgr = mock.Mock()

        result = encryption.clone_encryption_key_if_needed(
            mock.sentinel.ctx, key_mgr, plain_vol
        )

        self.assertIsNone(result)


class TestRXTLVMFork(unittest.TestCase):
    """Tests for the standalone RXTLVM driver."""

    def setUp(self):
        super().setUp()
        self.drv = lvm.RXTLVM.__new__(lvm.RXTLVM)
        self.drv.vg = mock.Mock()
        self.drv.vg.vg_name = "cinder-volumes"
        self.drv.db = mock.Mock()
        self.drv.db.volume_encryption_metadata_get.return_value = {
            "provider": "luks"
        }
        self.drv._execute = mock.Mock()
        self.drv.backend_name = "LVM"
        self.drv.hostname = "storage-node-1"
        self.drv._sparse_copy_volume = False

        class Conf:
            volume_group = "cinder-volumes"
            lvm_type = "default"
            lvm_mirrors = 0
            volume_dd_blocksize = "1M"
            volume_clear = "zero"
            volume_clear_size = 0
            volume_clear_ionice = None
            use_multipath_for_image_xfer = False
            enforce_multipath_for_image_xfer = False
            target_helper = "tgtadm"

        self.drv.configuration = Conf()
        self.drv.target_driver = mock.Mock()

    def test_driver_inherits_from_base_volume_driver(self):
        """Verify the driver is decoupled from upstream LVMVolumeDriver."""
        self.assertTrue(issubclass(lvm.RXTLVM, driver.VolumeDriver))

    def test_backward_compatibility_exports(self):
        """Verify rackspace.py exports the exact expected classes and symbols."""
        self.assertIs(rackspace.RXTLVM, lvm.RXTLVM)
        self.assertIs(rackspace.RXTTgtAdm, targets.RXTTgtAdm)
        self.assertEqual(rackspace._LUKS1_HEADER_BYTES, 2 * units.Mi)
        self.assertEqual(rackspace._LUKS2_HEADER_BYTES, 16 * units.Mi)

    def test_copy_image_to_encrypted_volume_prechecks_and_copies_without_resize(
        self,
    ):
        """Verify fail-fast check is called and LV is NEVER resized or shrunk."""
        img_svc = mock.Mock()
        img_svc.show.return_value = {"id": "img-1", "virtual_size": 4 * GI}
        vol = enc_vol(size=40)

        self.drv._copy_image_data_to_volume = mock.Mock()

        self.drv.copy_image_to_encrypted_volume(
            context=mock.sentinel.ctx,
            volume=vol,
            image_service=img_svc,
            image_id="img-1",
        )

        # Base copy was called with encrypted=True
        self.drv._copy_image_data_to_volume.assert_called_once_with(
            mock.sentinel.ctx,
            vol,
            img_svc,
            "img-1",
            encrypted=True,
            disable_sparse=False,
        )

        # Verification: extend_volume was NEVER called
        self.drv.vg.extend_volume.assert_not_called()

    def test_copy_image_to_encrypted_volume_exact_root_size_succeeds(self):
        """A 40 GiB image for a 40 GiB volume succeeds because the LV has header padding."""
        img_svc = mock.Mock()
        img_svc.show.return_value = {
            "id": "img-exact",
            "virtual_size": 40 * GI,
        }
        vol = enc_vol(size=40)

        self.drv._copy_image_data_to_volume = mock.Mock()

        self.drv.copy_image_to_encrypted_volume(
            context=mock.sentinel.ctx,
            volume=vol,
            image_service=img_svc,
            image_id="img-exact",
        )

        self.drv._copy_image_data_to_volume.assert_called_once_with(
            mock.sentinel.ctx,
            vol,
            img_svc,
            "img-exact",
            encrypted=True,
            disable_sparse=False,
        )

    def test_copy_image_to_encrypted_volume_rejects_oversized_before_copy(
        self,
    ):
        """A 41 GiB image for a 40 GiB volume is genuinely oversized and rejected."""
        img_svc = mock.Mock()
        img_svc.show.return_value = {
            "id": "img-bad",
            "virtual_size": 41 * GI,
        }
        vol = enc_vol(size=40)

        self.drv._copy_image_data_to_volume = mock.Mock()

        with self.assertRaises(exception.ImageTooBig):
            self.drv.copy_image_to_encrypted_volume(
                context=mock.sentinel.ctx,
                volume=vol,
                image_service=img_svc,
                image_id="img-bad",
            )

        self.drv._copy_image_data_to_volume.assert_not_called()

    def test_create_volume_unencrypted(self):
        volume = {"name": "vol-1", "size": 10}
        self.drv.create_volume(volume)
        self.drv.vg.create_volume.assert_called_once_with(
            "vol-1", "10g", "default", 0
        )

    def test_create_volume_encrypted_pads_header(self):
        vol = enc_vol(size=40)
        self.drv.create_volume(vol)
        # 40 * 1024 + 16 = 40976M
        self.drv.vg.create_volume.assert_called_once_with(
            "volume-vol-enc-uuid", "40976M", "default", 0
        )

    def test_extend_volume_unencrypted(self):
        volume = FakeVolume(id="v1", name="vol-extend", size=10)
        self.drv.extend_volume(volume, 20)

        self.drv.vg.extend_volume.assert_called_once_with("vol-extend", "20g")
        self.drv.target_driver.extend_target.assert_called_once_with(volume)

    def test_extend_volume_encrypted_pads_header(self):
        vol = enc_vol(size=40)
        self.drv.extend_volume(vol, 80)

        # 80 * 1024 + 16 = 81936M
        self.drv.vg.extend_volume.assert_called_once_with(
            "volume-vol-enc-uuid", "81936M"
        )
        self.drv.target_driver.extend_target.assert_called_once_with(vol)

    def test_delete_volume_unpresent_returns_true(self):
        self.drv.vg.get_volume.return_value = None
        self.assertTrue(self.drv.delete_volume({"name": "vol-ghost"}))


class TestSwiftSnapshots(unittest.TestCase):
    """Tests for the Swift-backed decoupled snapshot subsystem."""

    def setUp(self):
        super().setUp()
        self.drv = lvm.RXTLVM.__new__(lvm.RXTLVM)
        self.drv.vg = mock.Mock()
        self.drv.vg.vg_name = "cinder-volumes"
        self.drv._escape_snapshot = lambda s: s
        self.drv._sizestr = lambda sz: "%sg" % sz
        self.drv.local_path = lambda v: "/dev/mapper/cinder--volumes-%s" % v["name"]
        self.drv._create_volume = mock.Mock()
        self.drv._volume_not_present = mock.Mock(return_value=True)
        self.drv._execute = mock.Mock(return_value=('{"status":"ok"}', ""))


        class Conf:
            volume_group = "cinder-volumes"
            lvm_type = "default"
            lvm_mirrors = 0
            rxt_swift_snapshots_enabled = True
            rxt_swift_url = "http://swift.internal:8080/v1/AUTH_test"
            rxt_swift_container_prefix = "cinder-snapshots-"
            rxt_worker_path = "cinder-rxt-worker"
            rxt_snapshot_concurrency = 16
            rxt_snapshot_chunk_size_mib = 4

        self.drv.configuration = Conf()

    @mock.patch("cinder_rxt.snapshots.get_swift_credentials")
    @mock.patch("shutil.which")
    def test_create_swift_snapshot_creates_and_immediately_destroys_transient_lv(
        self, mock_which, mock_get_creds
    ):
        mock_which.return_value = "/usr/local/bin/cinder-rxt-worker"
        mock_get_creds.return_value = (
            "http://swift.internal:8080/v1/AUTH_test",
            "fake-token",
        )
        snapshot = {
            "id": "snap-uuid-1",
            "volume_id": "vol-uuid-1",
            "volume_name": "volume-vol-uuid-1",
            "volume_size": 20,
        }

        self.drv.create_snapshot(snapshot)

        # Transient snapshot was created and activated
        self.drv.vg.create_lv_snapshot.assert_called_once_with(
            "tmp-rxt-snap-snap-uuid-1", "volume-vol-uuid-1", "default"
        )
        self.drv.vg.activate_lv.assert_called_once_with(
            "tmp-rxt-snap-snap-uuid-1", is_snapshot=True
        )

        # Rust worker was invoked
        self.drv._execute.assert_called_once()
        args = self.drv._execute.call_args[0]
        self.assertEqual("/usr/local/bin/cinder-rxt-worker", args[0])
        self.assertEqual("sync-diff", args[1])
        self.assertIn("--source-dev", args)
        self.assertIn("--snapshot-id", args)
        self.assertIn("snap-uuid-1", args)

        # CRITICAL: Transient snapshot was deleted immediately after sync
        self.drv.vg.delete.assert_called_once_with("tmp-rxt-snap-snap-uuid-1")

    @mock.patch("cinder_rxt.snapshots.get_swift_credentials")
    @mock.patch("shutil.which")
    def test_create_swift_snapshot_with_custom_buffer_mib(
        self, mock_which, mock_get_creds
    ):
        mock_which.return_value = "/usr/local/bin/cinder-rxt-worker"
        mock_get_creds.return_value = (
            "http://swift.internal:8080/v1/AUTH_test",
            "fake-token",
        )
        self.drv.configuration.rxt_snapshot_buffer_mib = 1024
        snapshot = {
            "id": "snap-uuid-buf",
            "volume_id": "vol-uuid-buf",
            "volume_name": "volume-vol-uuid-buf",
            "volume_size": 20,
        }

        self.drv.create_snapshot(snapshot)

        self.drv._execute.assert_called_once()
        args = self.drv._execute.call_args[0]
        self.assertIn("--max-buffer-mib", args)
        idx = args.index("--max-buffer-mib")
        self.assertEqual("1024", args[idx + 1])

    @mock.patch("cinder_rxt.snapshots.get_swift_credentials")
    @mock.patch("shutil.which")
    def test_restore_swift_snapshot_to_volume(self, mock_which, mock_get_creds):
        mock_which.return_value = "/usr/local/bin/cinder-rxt-worker"
        mock_get_creds.return_value = (
            "http://swift.internal:8080/v1/AUTH_test",
            "fake-token",
        )
        volume = {"name": "volume-target-uuid", "size": 20}
        snapshot = {
            "id": "snap-uuid-1",
            "volume_id": "vol-uuid-1",
            "volume_name": "volume-vol-uuid-1",
            "volume_size": 20,
        }

        self.drv.create_volume_from_snapshot(volume, snapshot)

        # Destination volume created
        self.drv._create_volume.assert_called_once_with(
            "volume-target-uuid", "20g", "default", 0
        )

        # Rust worker invoked to restore chunks
        self.drv._execute.assert_called_once()
        args = self.drv._execute.call_args[0]
        self.assertEqual("restore", args[1])
        self.assertIn("--dest-dev", args)
        self.assertIn("/dev/mapper/cinder--volumes-volume-target-uuid", args)

    def test_disabled_swift_snapshots_uses_local_lvm_snapshot(self):
        self.drv.configuration.rxt_swift_snapshots_enabled = False
        snapshot = {
            "id": "snap-local",
            "volume_id": "vol-1",
            "name": "snapshot-snap-local",
            "volume_name": "vol-1",
            "volume_size": 10,
        }

        self.drv.create_snapshot(snapshot)

        self.drv.vg.create_lv_snapshot.assert_called_once_with(
            "snapshot-snap-local", "vol-1", "default"
        )
        # Transient delete was NOT called
        self.drv.vg.delete.assert_not_called()


class TestLegacySnapshotConversion(unittest.TestCase):
    """Tests for discovering and converting legacy local LVM snapshots into Swift."""

    def setUp(self):
        super().setUp()
        self.drv = lvm.RXTLVM.__new__(lvm.RXTLVM)
        self.drv.vg = mock.Mock()
        self.drv.vg.vg_name = "cinder-volumes"
        self.drv._escape_snapshot = lambda s: "_" + s if s.startswith("snapshot") else s
        self.drv._sizestr = lambda sz: "%sg" % sz
        self.drv.local_path = lambda v: f"/dev/mapper/cinder--volumes-{v['name']}"
        self.drv._create_volume = mock.Mock()
        self.drv._delete_volume = mock.Mock()
        self.drv._sparse_copy_volume = False
        self.drv._execute = mock.Mock(return_value=('{"status":"ok"}', ""))

        class Conf:
            volume_group = "cinder-volumes"
            lvm_type = "default"
            lvm_mirrors = 0
            volume_dd_blocksize = "1M"
            rxt_swift_snapshots_enabled = True
            rxt_auto_convert_legacy_snapshots = True
            rxt_swift_url = "http://swift.internal:8080/v1/AUTH_test"
            rxt_swift_container_prefix = "cinder-snapshots-"
            rxt_worker_path = "cinder-rxt-worker"
            rxt_snapshot_concurrency = 16
            rxt_snapshot_chunk_size_mib = 4

        self.drv.configuration = Conf()

    def test_discover_legacy_lvm_snapshots_via_vg(self):
        self.drv.vg.get_volumes.return_value = [
            {"name": "snapshot-uuid-1", "origin": "volume-uuid-1", "size": "20.00g", "attr": "swi-a-s---"},
            {"name": "snapshot-uuid-2", "origin": "volume-uuid-2", "size": "40.00g", "attr": "swi-a-s---"},
            {"name": "volume-uuid-1", "origin": "", "size": "20.00g", "attr": "wi-a-----"},
            {"name": "tmp-rxt-snap-uuid-3", "origin": "volume-uuid-1", "size": "20.00g", "attr": "swi-a-s---"},
        ]
        found = snapshots.discover_legacy_lvm_snapshots(self.drv.vg)
        self.assertEqual(2, len(found))
        self.assertEqual("snapshot-uuid-1", found[0]["lv_name"])
        self.assertEqual("uuid-1", found[0]["snapshot_id"])
        self.assertEqual("uuid-1", found[0]["volume_id"])
        self.assertEqual(20.0, found[0]["size_gib"])
        self.assertEqual("snapshot-uuid-2", found[1]["lv_name"])
        self.assertEqual("uuid-2", found[1]["snapshot_id"])

    def test_discover_legacy_lvm_snapshots_escaped_prefix_via_vg(self):
        self.drv.vg.get_volumes.return_value = [
            {"name": "_snapshot-uuid-1", "origin": "vol-uuid-1", "size": "40.00g", "attr": "swi-a-s---"},
            {"name": "vol-uuid-1", "origin": "", "size": "40.00g", "attr": "owi-aos---"},
        ]
        found = snapshots.discover_legacy_lvm_snapshots(self.drv.vg)
        self.assertEqual(1, len(found))
        self.assertEqual("_snapshot-uuid-1", found[0]["lv_name"])
        self.assertEqual("uuid-1", found[0]["snapshot_id"])
        self.assertEqual("vol-uuid-1", found[0]["volume_id"])
        self.assertEqual(40.0, found[0]["size_gib"])

    def test_discover_legacy_lvm_snapshots_via_lvs_cmd(self):
        mock_exec = mock.Mock(
            return_value=(
                "_snapshot-ca3c53dd;cinder-volumes-1;swi-a-s---;cac3b081;40.00g\n"
                "cac3b081;cinder-volumes-1;owi-aos---;;40.00g\n",
                "",
            )
        )
        found = snapshots.discover_legacy_lvm_snapshots("cinder-volumes-1", execute=mock_exec)
        self.assertEqual(1, len(found))
        self.assertEqual("_snapshot-ca3c53dd", found[0]["lv_name"])
        self.assertEqual("ca3c53dd", found[0]["snapshot_id"])
        self.assertEqual("cac3b081", found[0]["origin_lv_name"])
        self.assertEqual("cac3b081", found[0]["volume_id"])
        self.assertEqual(40.0, found[0]["size_gib"])

    def test_discover_legacy_lvm_snapshots_fallback_when_origin_missing(self):
        # Simulate Cinder LVM.get_lv_info which does not return origin
        self.drv.vg.get_volumes.return_value = [
            {"name": "_snapshot-ca3c53dd", "size": "40.00"},
            {"name": "cac3b081", "size": "40.00"},
        ]
        mock_exec = mock.Mock(
            return_value=(
                "_snapshot-ca3c53dd;cinder-volumes-1;swi-a-s---;cac3b081;40.00g\n"
                "cac3b081;cinder-volumes-1;owi-aos---;;40.00g\n",
                "",
            )
        )
        found = snapshots.discover_legacy_lvm_snapshots(self.drv.vg, execute=mock_exec)
        self.assertEqual(1, len(found))
        self.assertEqual("_snapshot-ca3c53dd", found[0]["lv_name"])
        self.assertEqual("ca3c53dd", found[0]["snapshot_id"])
        self.assertEqual("cac3b081", found[0]["volume_id"])
        mock_exec.assert_called_once()

    @mock.patch("cinder_rxt.snapshots.get_swift_credentials")
    @mock.patch("shutil.which")
    def test_convert_legacy_lvm_snapshot_success(self, mock_which, mock_get_creds):
        mock_which.return_value = "/usr/local/bin/cinder-rxt-worker"
        mock_get_creds.return_value = ("http://swift.internal:8080/v1/AUTH_test", "fake-token")
        snap_info = {
            "lv_name": "snapshot-uuid-1",
            "snapshot_id": "uuid-1",
            "origin_lv_name": "volume-uuid-1",
            "volume_id": "uuid-1",
            "size_gib": 20.0,
        }

        result = snapshots.convert_legacy_lvm_snapshot(self.drv, snap_info, execute=self.drv._execute)
        self.assertTrue(result)

        # 1. LV was activated
        self.drv.vg.activate_lv.assert_called_once_with("snapshot-uuid-1", is_snapshot=True)

        # 2. Worker sync-diff was executed
        self.drv._execute.assert_called_once()
        args = self.drv._execute.call_args[0]
        self.assertEqual("/usr/local/bin/cinder-rxt-worker", args[0])
        self.assertEqual("sync-diff", args[1])
        self.assertIn("--snapshot-id", args)
        self.assertIn("uuid-1", args)

        # 3. Local LVM snapshot was deleted
        self.drv.vg.delete.assert_called_once_with("_snapshot-uuid-1")

    @mock.patch("cinder_rxt.snapshots.get_swift_credentials")
    @mock.patch("shutil.which")
    def test_convert_legacy_lvm_snapshot_dry_run(self, mock_which, mock_get_creds):
        mock_which.return_value = "/usr/local/bin/cinder-rxt-worker"
        mock_get_creds.return_value = ("http://swift.internal:8080/v1/AUTH_test", "fake-token")
        snap_info = {
            "lv_name": "snapshot-uuid-1",
            "snapshot_id": "uuid-1",
            "origin_lv_name": "volume-uuid-1",
            "volume_id": "uuid-1",
            "size_gib": 20.0,
        }

        result = snapshots.convert_legacy_lvm_snapshot(
            self.drv, snap_info, execute=self.drv._execute, dry_run=True
        )
        self.assertTrue(result)
        self.drv._execute.assert_not_called()
        self.drv.vg.delete.assert_not_called()

    @mock.patch("cinder_rxt.snapshots.convert_legacy_lvm_snapshot")
    @mock.patch("cinder_rxt.snapshots.discover_legacy_lvm_snapshots")
    def test_convert_all_legacy_snapshots(self, mock_discover, mock_convert):
        mock_discover.return_value = [
            {"lv_name": "snapshot-1", "snapshot_id": "1"},
            {"lv_name": "snapshot-2", "snapshot_id": "2"},
        ]
        mock_convert.return_value = True

        res = snapshots.convert_all_legacy_snapshots(self.drv, execute=self.drv._execute)
        self.assertEqual(2, res["total"])
        self.assertEqual(2, res["converted"])
        self.assertEqual(0, res["failed"])
        self.assertEqual(2, mock_convert.call_count)

    @mock.patch("cinder_rxt.snapshots.restore_swift_snapshot_to_volume")
    @mock.patch("cinder_rxt.snapshots.convert_legacy_lvm_snapshot")
    def test_create_volume_from_unconverted_legacy_snapshot(self, mock_convert, mock_restore):
        self.drv._volume_not_present = mock.Mock(return_value=False)
        volume = {"name": "volume-target", "size": 20}
        snapshot = {
            "id": "uuid-legacy",
            "name": "snapshot-uuid-legacy",
            "volume_id": "orig-vol-id",
            "volume_name": "volume-orig-vol-id",
            "volume_size": 20,
        }

        self.drv.create_volume_from_snapshot(volume, snapshot)

        # Converted first
        mock_convert.assert_called_once()
        conv_info = mock_convert.call_args[0][1]
        self.assertEqual("uuid-legacy", conv_info["snapshot_id"])

        # Then restored
        mock_restore.assert_called_once_with(self.drv, volume, snapshot, context=None, execute=self.drv._execute)

    def test_delete_snapshot_removes_legacy_local_lv(self):
        # Local snapshot exists on the host
        self.drv._volume_not_present = mock.Mock(return_value=False)
        snapshot = {
            "id": "uuid-legacy",
            "name": "snapshot-uuid-legacy",
        }

        self.drv.delete_snapshot(snapshot)

        self.drv._delete_volume.assert_called_once_with(snapshot, is_snapshot=True)

    @mock.patch("cinder_rxt.snapshots.delete_swift_snapshot")
    def test_delete_snapshot_calls_swift_delete(self, mock_delete_swift):
        self.drv._volume_not_present = mock.Mock(return_value=True)
        snapshot = {
            "id": "uuid-snap-1",
            "name": "snapshot-uuid-snap-1",
            "volume_id": "uuid-vol-1",
        }

        self.drv.delete_snapshot(snapshot)

        mock_delete_swift.assert_called_once_with(
            self.drv,
            snapshot,
            context=None,
            execute=self.drv._execute,
        )

    @mock.patch("cinder_rxt.snapshots.delete_swift_volume_snapshots")
    def test_delete_volume_calls_swift_volume_delete(self, mock_delete_swift_vol):
        self.drv._volume_not_present = mock.Mock(return_value=False)
        self.drv.vg.lv_has_snapshot = mock.Mock(return_value=False)
        self.drv._delete_volume = mock.Mock()
        volume = {
            "id": "uuid-vol-1",
            "name": "volume-uuid-vol-1",
        }

        self.drv.delete_volume(volume)

        self.drv._delete_volume.assert_called_once_with(volume)
        mock_delete_swift_vol.assert_called_once_with(
            self.drv,
            volume,
            context=None,
            execute=self.drv._execute,
        )


    @mock.patch("cinder_rxt.snapshots.delete_swift_volume_snapshots")
    def test_delete_volume_skips_swift_purge_for_backup_vol(self, mock_delete_swift_vol):
        self.drv._volume_not_present = mock.Mock(return_value=False)
        self.drv.vg.lv_has_snapshot = mock.Mock(return_value=False)
        self.drv._delete_volume = mock.Mock()
        volume = {
            "id": "uuid-temp-backup",
            "name": "backup-vol-36baea48-4039",
        }

        self.drv.delete_volume(volume)

        self.drv._delete_volume.assert_called_once_with(volume)
        mock_delete_swift_vol.assert_not_called()

    @mock.patch("cinder.volume.volume_utils.copy_volume")
    @mock.patch("cinder_rxt.snapshots.copy_volume_accelerated", return_value=False)
    def test_create_cloned_volume_success(self, mock_accel, mock_copy):
        src_vref = {
            "id": "uuid-src",
            "name": "volume-uuid-src",
            "size": 20,
        }
        dest_volume = {
            "id": "uuid-dest",
            "name": "volume-uuid-dest",
            "size": 20,
        }
        self.drv._create_volume = mock.Mock()
        self.drv._create_local_lv_snapshot = mock.Mock()
        self.drv._delete_local_lv_snapshot = mock.Mock()
        self.drv.vg.activate_lv = mock.Mock()

        self.drv.create_cloned_volume(dest_volume, src_vref)

        self.drv._create_local_lv_snapshot.assert_called_once()
        temp_snap = self.drv._create_local_lv_snapshot.call_args[0][0]
        self.assertEqual("volume-uuid-src", temp_snap["volume_name"])
        self.assertEqual("uuid-src", temp_snap["volume_id"])

        self.drv._create_volume.assert_called_once()
        self.drv.vg.activate_lv.assert_called_once_with(temp_snap["name"], is_snapshot=True)
        mock_accel.assert_called_once()
        mock_copy.assert_called_once()
        self.drv._delete_local_lv_snapshot.assert_called_once_with(temp_snap)

    @mock.patch("cinder.volume.volume_utils.copy_volume")
    @mock.patch("cinder_rxt.snapshots.copy_volume_accelerated", return_value=True)
    def test_create_cloned_volume_accelerated_preferred(self, mock_accel, mock_copy):
        src_vref = {
            "id": "uuid-src",
            "name": "volume-uuid-src",
            "size": 20,
        }
        dest_volume = {
            "id": "uuid-dest",
            "name": "volume-uuid-dest",
            "size": 20,
        }
        self.drv._create_volume = mock.Mock()
        self.drv._create_local_lv_snapshot = mock.Mock()
        self.drv._delete_local_lv_snapshot = mock.Mock()
        self.drv.vg.activate_lv = mock.Mock()

        self.drv.create_cloned_volume(dest_volume, src_vref)

        mock_accel.assert_called_once()
        mock_copy.assert_not_called()
        self.drv._delete_local_lv_snapshot.assert_called_once()

    @mock.patch("cinder_rxt.snapshots._run_worker_stream", return_value=("", ""))
    @mock.patch("shutil.which", return_value="/usr/local/bin/cinder-rxt-worker")
    def test_copy_volume_accelerated_invokes_worker(self, mock_which, mock_worker):
        res = snapshots.copy_volume_accelerated(
            self.drv,
            "/dev/src",
            "/dev/dest",
            20 * 1024 * 1024 * 1024,
            concurrency=16,
            chunk_size_mib=4,
            sparse=True,
            execute=self.drv._execute,
        )
        self.assertTrue(res)
        mock_worker.assert_called_once()
        cmd = mock_worker.call_args[0][0]
        self.assertEqual("/usr/local/bin/cinder-rxt-worker", cmd[0])
        self.assertEqual("copy-volume", cmd[1])
        self.assertIn("--src", cmd)
        self.assertIn("/dev/src", cmd)
        self.assertIn("--dest", cmd)
        self.assertIn("/dev/dest", cmd)
        self.assertIn("--sparse", cmd)

    @mock.patch("cinder.volume.volume_utils.copy_volume")
    @mock.patch("cinder_rxt.snapshots.copy_volume_accelerated", return_value=False)
    @mock.patch("cinder_rxt.encryption.get_volume_overhead_mib", return_value=16)
    def test_create_cloned_volume_encrypted_copies_luks_overhead(self, mock_overhead, mock_accel, mock_copy):
        src_vref = {
            "id": "uuid-src-enc",
            "name": "volume-uuid-src-enc",
            "size": 20,
            "encryption_key_id": "key-uuid",
        }
        dest_volume = {
            "id": "uuid-dest-enc",
            "name": "volume-uuid-dest-enc",
            "size": 20,
            "encryption_key_id": "cloned-key-uuid",
        }
        self.drv._create_volume = mock.Mock()
        self.drv._create_local_lv_snapshot = mock.Mock()
        self.drv._delete_local_lv_snapshot = mock.Mock()
        self.drv.vg.activate_lv = mock.Mock()

        self.drv.create_cloned_volume(dest_volume, src_vref)

        # 20 GiB = 20480 MiB + 16 MiB LUKS overhead = 20496 MiB
        expected_mib = (20 * 1024) + 16
        mock_copy.assert_called_once()
        self.assertEqual(expected_mib, mock_copy.call_args[0][2])

    @mock.patch("cinder_rxt.snapshots._run_worker_stream", return_value=("", ""))
    @mock.patch("cinder_rxt.snapshots.get_swift_credentials", return_value=("http://swift", "token"))
    def test_create_swift_snapshot_handles_dict_missing_volume_id(self, mock_creds, mock_worker):
        self.drv.vg.create_lv_snapshot = mock.Mock()
        self.drv.vg.activate_lv = mock.Mock()
        self.drv.vg.delete = mock.Mock()
        snapshot = {
            "id": "uuid-snap",
            "volume_name": "volume-uuid-vol",
            "size": 20,
        }

        snapshots.create_swift_snapshot(self.drv, snapshot)

        self.drv.vg.create_lv_snapshot.assert_called_once()
        self.drv.vg.delete.assert_called_once()

    @mock.patch("cinder_rxt.snapshots.create_swift_snapshot")
    def test_create_snapshot_routes_temp_snapshot_to_local_lv(self, mock_swift_snap):
        self.drv._create_local_lv_snapshot = mock.Mock()
        temp_snap = {
            "name": "clone-snap-vol-1",
            "volume_name": "volume-vol-1",
        }
        self.drv.create_snapshot(temp_snap)
        mock_swift_snap.assert_not_called()
        self.drv._create_local_lv_snapshot.assert_called_once_with(temp_snap)

    @mock.patch("cinder_rxt.snapshots.delete_swift_snapshot")
    def test_delete_snapshot_routes_temp_snapshot_to_local_lv(self, mock_swift_delete):
        self.drv._delete_local_lv_snapshot = mock.Mock()
        temp_snap = {
            "name": "clone-snap-vol-1",
            "volume_name": "volume-vol-1",
        }
        self.drv.delete_snapshot(temp_snap)
        mock_swift_delete.assert_not_called()
        self.drv._delete_local_lv_snapshot.assert_called_once_with(temp_snap)

    def test_host_counting_semaphore(self):
        sem = snapshots.HostCountingSemaphore("test-unit", 2)
        with sem.acquire() as s1:
            self.assertEqual(0, s1)
            with sem.acquire() as s2:
                self.assertEqual(1, s2)

    @mock.patch("cinder_rxt.snapshots._run_worker_stream", return_value=("", ""))
    @mock.patch("shutil.which", return_value="/usr/local/bin/cinder-rxt-worker")
    def test_copy_volume_accelerated_passes_max_mbps(self, mock_which, mock_worker):
        self.drv.configuration.rxt_copy_max_mbps = 150
        snapshots.copy_volume_accelerated(
            self.drv,
            "/dev/src",
            "/dev/dest",
            10 * 1024 * 1024 * 1024,
            execute=self.drv._execute,
        )
        mock_worker.assert_called_once()
        cmd = mock_worker.call_args[0][0]
        self.assertIn("--max-mbps", cmd)
        self.assertIn("150", cmd)
        self.drv.configuration.rxt_copy_max_mbps = 0

    @mock.patch("cinder_rxt.snapshots._run_worker_stream", return_value=("", ""))
    @mock.patch("cinder_rxt.snapshots.get_swift_credentials", return_value=("http://swift", "token"))
    def test_create_swift_snapshot_passes_max_mbps(self, mock_creds, mock_worker):
        self.drv.vg.create_lv_snapshot = mock.Mock()
        self.drv.vg.activate_lv = mock.Mock()
        self.drv.vg.delete = mock.Mock()
        self.drv.configuration.rxt_snapshot_max_mbps = 200
        snapshot = {
            "id": "uuid-snap-throttle",
            "volume_name": "volume-uuid-vol",
            "size": 20,
        }

        snapshots.create_swift_snapshot(self.drv, snapshot)
        mock_worker.assert_called_once()
        cmd = mock_worker.call_args[0][0]
        self.assertIn("--max-mbps", cmd)
        self.assertIn("200", cmd)
        self.drv.configuration.rxt_snapshot_max_mbps = 0

    @mock.patch("subprocess.Popen")
    @mock.patch("shutil.which")
    def test_run_worker_stream_prepends_ionice_and_nice(self, mock_which, mock_popen):
        def which_side_effect(cmd):
            if cmd == "ionice":
                return "/usr/bin/ionice"
            if cmd == "nice":
                return "/usr/bin/nice"
            return cmd

        mock_which.side_effect = which_side_effect
        mock_proc = mock.Mock()
        mock_proc.stdout.readline.side_effect = ["output line", ""]
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        conf = mock.Mock(
            rxt_worker_ionice_class=2,
            rxt_worker_ionice_level=7,
            rxt_worker_nice_priority=10,
        )
        base_cmd = ["/usr/local/bin/cinder-rxt-worker", "test"]
        snapshots._run_worker_stream(base_cmd, conf=conf)

        mock_popen.assert_called_once()
        executed_cmd = mock_popen.call_args[0][0]
        self.assertEqual("/usr/bin/nice", executed_cmd[0])
        self.assertEqual("-n", executed_cmd[1])
        self.assertEqual("10", executed_cmd[2])
        self.assertEqual("/usr/bin/ionice", executed_cmd[3])
        self.assertEqual("-c", executed_cmd[4])
        self.assertEqual("2", executed_cmd[5])
        self.assertEqual("-n", executed_cmd[6])
        self.assertEqual("7", executed_cmd[7])
        self.assertEqual("/usr/local/bin/cinder-rxt-worker", executed_cmd[8])

    @mock.patch("subprocess.Popen")
    @mock.patch("shutil.which")
    def test_run_worker_stream_disabled_ionice(self, mock_which, mock_popen):
        mock_which.side_effect = lambda cmd: f"/usr/bin/{cmd}"
        mock_proc = mock.Mock()
        mock_proc.stdout.readline.side_effect = ["output", ""]
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        conf = mock.Mock(
            rxt_worker_ionice_class=0,
            rxt_worker_nice_priority=None,
        )
        base_cmd = ["/usr/local/bin/cinder-rxt-worker", "test"]
        snapshots._run_worker_stream(base_cmd, conf=conf)

        executed_cmd = mock_popen.call_args[0][0]
        self.assertNotIn("/usr/bin/ionice", executed_cmd)
        self.assertNotIn("/usr/bin/nice", executed_cmd)

    @mock.patch("subprocess.Popen")
    @mock.patch("shutil.which")
    def test_run_worker_stream_idle_ionice(self, mock_which, mock_popen):
        mock_which.side_effect = lambda cmd: f"/usr/bin/{cmd}"
        mock_proc = mock.Mock()
        mock_proc.stdout.readline.side_effect = ["output", ""]
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        conf = mock.Mock(
            rxt_worker_ionice_class=3,
            rxt_worker_ionice_level=7,
            rxt_worker_nice_priority=None,
        )
        base_cmd = ["/usr/local/bin/cinder-rxt-worker", "test"]
        snapshots._run_worker_stream(base_cmd, conf=conf)

        executed_cmd = mock_popen.call_args[0][0]
        self.assertEqual("/usr/bin/ionice", executed_cmd[0])
        self.assertEqual("-c", executed_cmd[1])
        self.assertEqual("3", executed_cmd[2])
        self.assertNotIn("-n", executed_cmd)

    def test_get_config_value_safe_get(self):
        class FakeConf:
            def safe_get(self, key):
                if key == "test_key":
                    return "safe_value"
                return None
        c = FakeConf()
        self.assertEqual("safe_value", snapshots.get_config_value(c, "test_key"))
        self.assertEqual("default", snapshots.get_config_value(c, "missing", "default"))


if __name__ == "__main__":
    unittest.main()

