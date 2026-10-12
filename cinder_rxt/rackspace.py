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

"""
Backward compatibility module for cinder_rxt.

Exports RXTLVM and RXTTgtAdm to ensure existing OpenStack-Helm configurations
and cinder.conf settings continue to work transparently.
"""

from cinder_rxt.encryption import (
    _LUKS1_HEADER_BYTES,
    _LUKS2_HEADER_BYTES,
    check_image_fits_encrypted_volume,
    get_luks_header_bytes,
)
from cinder_rxt.lvm import RXTLVM, LVMVolumeDriver
from cinder_rxt.targets import RXTTgtAdm, RXT_VOLUME_CONF_TEMPLATE

# Maintain historical naming
_LUKS_HEADER_BYTES = _LUKS2_HEADER_BYTES

__all__ = [
    "RXTLVM",
    "LVMVolumeDriver",
    "RXTTgtAdm",
    "RXT_VOLUME_CONF_TEMPLATE",
    "_LUKS_HEADER_BYTES",
    "_LUKS1_HEADER_BYTES",
    "_LUKS2_HEADER_BYTES",
    "get_luks_header_bytes",
    "check_image_fits_encrypted_volume",
]
