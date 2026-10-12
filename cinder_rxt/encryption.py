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


def check_image_fits_encrypted_volume(
    context, volume, image_service, image_id, db=None
):
    """Raise ImageTooBig if the image cannot fit the LUKS payload.

    Under upstream LVM sizing, the LUKS header is carved out of the requested
    size. If an image's virtual size exceeds the nominal volume size minus the
    LUKS header, qemu-img convert will fail late with "Cannot grow device files".
    More critically, attempting to resize/shrink the LV after writing truncates
    the end of full-sized images (corrupting GPT backup headers and root filesystems,
    causing boot failures; see OSPC-2358).

    This pre-check fails fast before downloading any image data, giving the user
    an actionable error specifying the minimum GiB needed.
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

    # An image uploaded from an encrypted volume is already a LUKS container;
    # its virtual size already includes the header, so nominal size check applies.
    if image_meta.get("cinder_encryption_key_id"):
        return

    provider = None
    vol_id = volume.get("id") if isinstance(volume, dict) else getattr(volume, "id", None)
    if db and vol_id:
        try:
            encryption = db.volume_encryption_metadata_get(context, vol_id)
            provider = (encryption or {}).get("provider")
        except Exception:
            LOG.debug(
                "Unable to read encryption metadata from DB for volume %s; "
                "assuming LUKS1 header.",
                vol_id,
                exc_info=True,
            )

    header = get_luks_header_bytes(provider)
    volume_size_gib = volume.get("size") if isinstance(volume, dict) else getattr(volume, "size", 0)
    nominal = int(volume_size_gib) * units.Gi
    usable = nominal - header

    if virtual_size <= usable:
        return

    min_gib = int(math.ceil((virtual_size + header) / float(units.Gi)))
    reason = (
        "image virtual size %(vsize)d bytes exceeds the usable capacity "
        "of encrypted volume %(vol)s (%(nominal)d GiB requested minus "
        "%(header)d MiB LUKS header = %(usable)d bytes). Encrypted "
        "volumes lose the LUKS header from the requested size; request "
        "a volume of at least %(min_gib)d GiB for this image."
        % {
            "vsize": virtual_size,
            "vol": vol_id,
            "nominal": int(volume_size_gib),
            "header": header // units.Mi,
            "usable": usable,
            "min_gib": min_gib,
        }
    )
    LOG.error(
        "Image %(image)s does not fit encrypted volume: %(reason)s",
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
