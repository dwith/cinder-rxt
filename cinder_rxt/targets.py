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

import string
import textwrap

from oslo_log import log as logging

from cinder.volume.targets import tgt

LOG = logging.getLogger(__name__)

RXT_VOLUME_CONF_TEMPLATE = string.Template(
    """
<target %(name)s>
    backing-store %(path)s
    driver %(driver)s
    %(chap_auth)s
    %(target_flags)s
    scsi_sn $SCSI_SN
    scsi_id $SCSI_SN
    write-cache %(write_cache)s
</target>
"""
)


class RXTTgtAdm(tgt.TgtAdm):
    """Target object for block storage devices with SCSI serial number persistence.

    Controls TGT exports ensuring consistent scsi_id and scsi_sn are assigned
    based on the volume UUID. This is required for multipath-tools on compute
    nodes to properly assemble multipath devices across primary and secondary
    storage interfaces (portals).
    """

    SECONDARY_IP_SUPPORT = True

    def create_iscsi_target(
        self, name, tid, lun, path, chap_auth=None, **kwargs
    ):
        """Create a target for ISCSI and return target info.

        Sets the volume configuration template to ensure that SCSI_SN is defined
        for the return and reset afterwards.
        """
        vol_id = name.split(":")[1] if ":" in name else name
        volume_conf_copy = self.VOLUME_CONF
        try:
            self.VOLUME_CONF = textwrap.dedent(
                RXT_VOLUME_CONF_TEMPLATE.safe_substitute(
                    SCSI_SN=vol_id
                )
            )
            return super().create_iscsi_target(
                name, tid, lun, path, chap_auth, **kwargs
            )
        finally:
            self.VOLUME_CONF = volume_conf_copy
