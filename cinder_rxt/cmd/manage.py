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
cinder-rxt-manage: Operational management CLI for Cinder RXT storage nodes.

Provides tools to audit and convert legacy local LVM snapshots
into Swift differential snapshots and remove local copy-on-write snapshot dependencies.
"""

import argparse
import sys

from oslo_config import cfg
from oslo_log import log as logging

from cinder_rxt import lvm as rxt_lvm
from cinder_rxt import snapshots

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def setup_cinder_config(config_file=None):
    """Load Cinder configuration if available."""
    try:
        logging.register_options(CONF)
        rxt_lvm.register_opts(CONF)
        snapshots.register_opts(CONF)
        args = ["--config-file", config_file] if config_file else []
        CONF(args=args, project="cinder", default_config_files=[config_file] if config_file else None)
        logging.setup(CONF, "cinder-rxt-manage")
    except Exception as exc:
        print(f"Note: Running with default options (config loading: {exc})", file=sys.stderr)


def list_legacy_snapshots_cmd(args):
    """List all legacy local LVM snapshots on the storage node."""
    vg_name = args.vg or getattr(CONF, "volume_group", "cinder-volumes")
    print(f"Scanning Volume Group '{vg_name}' for legacy LVM snapshots...")

    legacy_snaps = snapshots.discover_legacy_lvm_snapshots(vg_name)
    if not legacy_snaps:
        print(f"No legacy LVM snapshots found in volume group '{vg_name}'.")
        return 0

    print(f"\nFound {len(legacy_snaps)} legacy LVM snapshot(s):")
    print("-" * 90)
    print(f"{'LV Name':<38} {'Origin Volume ID':<36} {'Size (GiB)':<10}")
    print("-" * 90)
    for snap in legacy_snaps:
        print(f"{snap['lv_name']:<38} {snap['volume_id']:<36} {snap['size_gib']:<10.1f}")
    print("-" * 90)
    print(f"Total: {len(legacy_snaps)} legacy snapshot(s) found.")
    return 0


def convert_legacy_snapshots_cmd(args):
    """Convert legacy local LVM snapshots to Swift and delete local LVs."""
    setup_cinder_config(args.config_file)
    vg_name = args.vg or getattr(CONF, "volume_group", "cinder-volumes")

    class SimpleDriverAdapter:
        def __init__(self, vg_name, conf):
            self.vg = None
            self.configuration = conf
            self.configuration.volume_group = vg_name
            self.configuration.rxt_swift_snapshots_enabled = True

        def _escape_snapshot(self, name):
            return "_" + name if name.startswith("snapshot") else name

        def local_path(self, volume):
            escaped_group = vg_name.replace("-", "--")
            escaped_name = self._escape_snapshot(volume["name"]).replace("-", "--")
            return f"/dev/mapper/{escaped_group}-{escaped_name}"

    driver = SimpleDriverAdapter(vg_name, CONF)

    if args.snapshot_id:
        all_snaps = snapshots.discover_legacy_lvm_snapshots(driver.vg or vg_name)
        matched = [s for s in all_snaps if s["snapshot_id"] == args.snapshot_id]
        if matched:
            snap_info = matched[0]
        else:
            snap_info = {
                "lv_name": f"_snapshot-{args.snapshot_id}",
                "snapshot_id": args.snapshot_id,
                "origin_lv_name": "",
                "volume_id": "",
                "size_gib": 0.0,
            }
        print(f"Converting single legacy snapshot {args.snapshot_id} (dry_run={args.dry_run})...")
        try:
            snapshots.convert_legacy_lvm_snapshot(driver, snap_info, dry_run=args.dry_run)
            print(f"Successfully converted snapshot {args.snapshot_id}.")
            return 0
        except Exception as exc:
            print(f"Error converting snapshot {args.snapshot_id}: {exc}", file=sys.stderr)
            return 1

    print(f"Starting conversion of legacy snapshots in VG '{vg_name}' (dry_run={args.dry_run})...")
    results = snapshots.convert_all_legacy_snapshots(driver, dry_run=args.dry_run)

    print("\nConversion Summary:")
    print("=" * 40)
    print(f"Total Found: {results['total']}")
    print(f"Converted & Destroyed: {results['converted']}")
    print(f"Failed: {results['failed']}")
    if results["errors"]:
        print("\nErrors encountered:")
        for err in results["errors"]:
            print(f"  - {err}", file=sys.stderr)

    return 0 if results["failed"] == 0 else 1


def main():
    parser = argparse.ArgumentParser(
        prog="cinder-rxt-manage",
        description="Administrative CLI for Cinder RXT storage node migration and legacy snapshots.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list-legacy-snapshots
    list_p = subparsers.add_parser(
        "list-legacy-snapshots",
        help="List legacy local LVM snapshots on the storage node.",
    )
    list_p.add_argument("--vg", default="cinder-volumes", help="Volume Group name (default: cinder-volumes)")
    list_p.add_argument("--config-file", default=None, help="Path to cinder.conf")
    list_p.set_defaults(func=list_legacy_snapshots_cmd)

    # convert-legacy-snapshots
    conv_p = subparsers.add_parser(
        "convert-legacy-snapshots",
        help="Convert legacy LVM snapshots to Swift and delete local LVs.",
    )
    conv_p.add_argument("--vg", default="cinder-volumes", help="Volume Group name (default: cinder-volumes)")
    conv_p.add_argument("--config-file", default=None, help="Path to cinder.conf")
    conv_p.add_argument("--dry-run", action="store_true", help="Simulate conversion without deleting local LVs")
    conv_p.add_argument("--snapshot-id", default=None, help="Convert only a specific snapshot UUID")
    conv_p.set_defaults(func=convert_legacy_snapshots_cmd)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
