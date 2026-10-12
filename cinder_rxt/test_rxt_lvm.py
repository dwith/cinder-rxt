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

    def test_check_image_fits_exact_usable_passes(self):
        img_svc = mock.Mock()
        # Exactly 40 GiB minus 2 MiB LUKS1 header
        img_svc.show.return_value = {
            "id": "img-1",
            "virtual_size": 40 * GI - 2 * MI,
        }
        vol = enc_vol(size=40)

        # Should not raise
        encryption.check_image_fits_encrypted_volume(
            mock.sentinel.ctx, vol, img_svc, "img-1"
        )

    def test_check_image_oversized_raises_image_too_big(self):
        img_svc = mock.Mock()
        # Full 40 GiB image for 40 GiB volume (missing LUKS header space)
        img_svc.show.return_value = {
            "id": "img-oversized",
            "virtual_size": 40 * GI,
        }
        vol = enc_vol(size=40)

        with self.assertRaises(exception.ImageTooBig) as cm:
            encryption.check_image_fits_encrypted_volume(
                mock.sentinel.ctx, vol, img_svc, "img-oversized"
            )

        msg = str(cm.exception)
        self.assertIn("img-oversized", msg)
        self.assertIn("2 MiB LUKS header", msg)
        self.assertIn("at least 41 GiB", msg)

    def test_check_image_luks2_provider_uses_16mib_header(self):
        img_svc = mock.Mock()
        # Fits LUKS1 (2 MiB) but exceeds LUKS2 (16 MiB)
        img_svc.show.return_value = {
            "id": "img-luks2",
            "virtual_size": 40 * GI - 8 * MI,
        }
        db = mock.Mock()
        db.volume_encryption_metadata_get.return_value = {"provider": "luks2"}
        vol = enc_vol(size=40)

        with self.assertRaises(exception.ImageTooBig) as cm:
            encryption.check_image_fits_encrypted_volume(
                mock.sentinel.ctx, vol, img_svc, "img-luks2", db=db
            )

        self.assertIn("16 MiB LUKS header", str(cm.exception))

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

    def test_copy_image_to_encrypted_volume_rejects_oversized_before_copy(
        self,
    ):
        img_svc = mock.Mock()
        img_svc.show.return_value = {
            "id": "img-bad",
            "virtual_size": 40 * GI,
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

    def test_create_volume(self):
        volume = {"name": "vol-1", "size": 10}
        self.drv.create_volume(volume)
        self.drv.vg.create_volume.assert_called_once_with(
            "vol-1", "10g", "default", 0
        )

    def test_extend_volume_calls_vg_and_target_driver(self):
        volume = FakeVolume(id="v1", name="vol-extend", size=10)
        self.drv.extend_volume(volume, 20)

        self.drv.vg.extend_volume.assert_called_once_with("vol-extend", "20g")
        self.drv.target_driver.extend_target.assert_called_once_with(volume)

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


if __name__ == "__main__":
    unittest.main()
