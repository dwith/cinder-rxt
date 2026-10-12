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

import math

from oslo_log import log as logging
from oslo_utils import units

from cinder import exception
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)

# LUKS headers live in-band at the front of the block device, so the
# decrypted dm-crypt payload presented to the guest is smaller than the
# volume by the header size.
# Provider "luks" (LUKS1) payload offset is 4096 sectors = 2 MiB.
# Provider "luks2" (LUKS2) default metadata + keyslot area is 16 MiB.
_LUKS1_HEADER_BYTES = 2 * units.Mi
_LUKS2_HEADER_BYTES = 16 * units.Mi


def get_luks_header_bytes(provider):
    """Return the LUKS header size in bytes for an encryption provider.

    Provider may be the short format name ('luks', 'luks2') or a
    legacy class path ('os_brick.encryptors.luks.Luks2Encryptor').
    Anything not recognisably LUKS2 is treated as LUKS1, the smaller
    header, so the pre-check never rejects an image that would fit.
    """
    if provider and "luks2" in str(provider).lower():
        return _LUKS2_HEADER_BYTES
    return _LUKS1_HEADER_BYTES


def get_luks_header_overhead_mib(provider=None):
    """Return physical LVM padding in MiB needed for the LUKS header.

    LUKS2 standard metadata+keyslots area is 16 MiB.
    LUKS1 payload offset is 2 MiB, which aligns to 4 MiB on standard LVM extents.
    Defaults to 16 MiB (LUKS2 standard).
    """
    if provider:
        p = str(provider).lower()
        if "luks2" in p:
            return 16
        elif "luks" in p or "plain" in p:
            return 4
    return 16


def is_encrypted_volume(volume, context=None, db=None):
    """Determine whether a volume is an encrypted volume."""
    key_id = (
        volume.get("encryption_key_id")
        if isinstance(volume, dict)
        else getattr(volume, "encryption_key_id", None)
    )
    if key_id:
        return True

    vol_id = (
        volume.get("id")
        if isinstance(volume, dict)
        else getattr(volume, "id", None)
    )
    if db and vol_id and context:
        try:
            encryption = db.volume_encryption_metadata_get(context, vol_id)
            if encryption and (
                encryption.get("encryption_key_id")
                or encryption.get("provider")
            ):
                return True
        except Exception:
            pass

    vol_type_id = (
        volume.get("volume_type_id")
        if isinstance(volume, dict)
        else getattr(volume, "volume_type_id", None)
    )
    if vol_type_id and context:
        try:
            from cinder.volume import volume_types

            if volume_types.is_encrypted(context, vol_type_id):
                return True
        except Exception:
            pass

    return False


def get_volume_overhead_mib(volume, context=None, db=None):
    """Return the extra physical overhead in MiB required for the volume.

    Returns 0 if the volume is unencrypted.
    Returns 16 MiB (or 4 MiB) for encrypted volumes so that the decrypted
    dm-crypt plaintext payload provides the full requested nominal capacity.
    """
    if not is_encrypted_volume(volume, context=context, db=db):
        return 0

    provider = None
    vol_id = (
        volume.get("id")
        if isinstance(volume, dict)
        else getattr(volume, "id", None)
    )
    if db and vol_id and context:
        try:
            encryption = db.volume_encryption_metadata_get(context, vol_id)
            if encryption:
                provider = encryption.get("provider")
        except Exception:
            pass

    return get_luks_header_overhead_mib(provider)


def check_image_fits_encrypted_volume(
    context, volume, image_service, image_id, db=None
):
    """Validate that an image fits the nominal volume size.

    Because the RXT LVM driver compensates for the LUKS header at LV allocation
    time (adding the header overhead to the physical LV), the decrypted dm-crypt
    device provides the full nominal volume size (volume['size'] * GiB) as usable
    payload.

    Therefore, an image whose virtual size equals the requested volume size
    (such as a 40 GiB image on a 40 GiB root volume) fits natively without
    truncation or ImageTooBig rejection. We only reject if the image actually
    exceeds the full nominal volume size.
    """
    try:
        image_meta = image_service.show(context, image_id)
        virtual_size = image_meta.get("virtual_size")
    except Exception:
        LOG.debug(
            "Unable to read virtual_size for image %s; skipping "
            "encrypted capacity pre-check.",
            image_id,
            exc_info=True,
        )
        return

    if not virtual_size:
        return
    virtual_size = int(virtual_size)

    volume_size_gib = (
        volume.get("size")
        if isinstance(volume, dict)
        else getattr(volume, "size", 0)
    )
    nominal_bytes = int(volume_size_gib) * units.Gi

    if virtual_size <= nominal_bytes:
        return

    min_gib = int(math.ceil(virtual_size / float(units.Gi)))
    vol_id = volume.get("id") if isinstance(volume, dict) else getattr(volume, "id", None)
    reason = (
        "Image virtual size %(vsize)d bytes exceeds the nominal capacity "
        "of encrypted volume %(vol)s (%(nominal)d GiB / %(nominal_bytes)d bytes). "
        "Request a volume of at least %(min_gib)d GiB for this image."
        % {
            "vsize": virtual_size,
            "vol": vol_id,
            "nominal": int(volume_size_gib),
            "nominal_bytes": nominal_bytes,
            "min_gib": min_gib,
        }
    )
    LOG.error(
        "Image %(image)s does not fit volume: %(reason)s",
        {"image": image_id, "reason": reason},
    )
    raise exception.ImageTooBig(image_id=image_id, reason=reason)


def clone_encryption_key_if_needed(context, key_manager, src_volume):
    """Clone Barbican encryption key for encrypted volume clone or snapshot.

    Returns the new cloned encryption_key_id, or None if the source volume
    is not encrypted or has no encryption_key_id.
    """
    key_id = src_volume.get("encryption_key_id") if isinstance(src_volume, dict) else getattr(src_volume, "encryption_key_id", None)
    if not key_id or not key_manager:
        return None

    try:
        new_key_id = volume_utils.clone_encryption_key(
            context, key_manager, key_id
        )
        LOG.info(
            "Cloned encryption key %(old_key)s to %(new_key)s for clone.",
            {"old_key": key_id, "new_key": new_key_id},
        )
        return new_key_id
    except Exception:
        LOG.exception(
            "Failed to clone encryption key %s for encrypted clone.", key_id
        )
        raise
