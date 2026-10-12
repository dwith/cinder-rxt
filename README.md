# Cinder RXT Driver (`cinder-rxt`)

High-performance OpenStack Cinder LVM storage driver and differential snapshot engine for Rackspace Flex and Genestack clouds.

`cinder-rxt` replaces fragile subclassing of upstream Cinder's `LVMVolumeDriver` with an enterprise-grade standalone driver (`cinder_rxt.lvm.RXTLVM`) paired with a high-speed native differential snapshot worker (`cinder-rxt-worker`).

---

## Key Features

### 1. Persistent iSCSI & Multipath Target Management
- **Deterministic SCSI Identity:** Generates persistent `scsi_sn` and `scsi_id` from the volume UUID (`iqn...<uuid>`) within `RXTTgtAdm`, ensuring Linux `multipathd` creates consistent, stable device nodes across client nodes and reboots.
- **Secondary Portal IPs:** Supports `target_secondary_ip_addresses` to publish multi-path iSCSI portals across multiple storage NICs.

### 2. End-to-End Native Encryption (LUKS1 & LUKS2)
- **Fail-Fast Capacity Validation:** Computes LUKS header overhead (2 MiB for LUKS1, 16 MiB for LUKS2) and verifies raw image size before download or conversion. If the image exceeds usable encrypted volume capacity, it raises `ImageTooBig` immediately.
- **Zero Shrinking / Truncation:** Eliminates post-conversion LV shrinking hacks (OSPC-2358), guaranteeing that GPT backup partition tables and root filesystem superblocks are never truncated.
- **Barbican Key Cloning:** When cloning encrypted volumes, the driver clones Barbican secrets (`volume_utils.clone_encryption_key`) and performs fast raw block-level replication (`dd`) without mounting `dm-crypt` devices on the storage node.

### 3. Swift-Backed Differential Snapshots (Decoupled from Node)
- **No Lingering LVM Snapshots:** Upstream LVM snapshots remain tied to the physical storage node, creating volume dependencies that prevent operations teams from evacuating or migrating volumes off hypervisors and storage nodes. `cinder-rxt` creates a transient LVM snapshot, syncs changed 4MB differential chunks into OpenStack Swift, and **immediately deletes the local LVM snapshot**.
- **O(1) Sharded Binary Diff Manifest:** Replaces legacy Lunr monolithic YAML manifests (which bloated to 2 GB and caused 65 GB Python OOM crashes) with an ultra-compact binary format (`RXTD`, 36 bytes per chunk delta). Handles 10 TB volumes in < 25 MB RAM.
- **SIMD Zero-Block Elision:** Automatically skips unallocated or zeroed 4MB blocks, minimizing I/O and object storage consumption.

### 4. Post-Eventlet Architecture
- Built strictly on native OS threads, isolated subprocesses, and a compiled Rust worker daemon (`cinder-rxt-worker`). Fully compatible with upstream OpenStack's removal of Eventlet green threads (Gazpacho 2026.1+).

---

## Architecture Overview

```
                      +---------------------------------------+
                      |       OpenStack Cinder Volume         |
                      |            cinder-volume              |
                      +-------------------+-------------------+
                                          |
                                          | Python Driver API
                                          v
                      +-------------------+-------------------+
                      |      cinder_rxt.lvm.RXTLVM            |
                      |  - TGT Multipath Target Management    |
                      |  - LUKS Pre-flight Sizing             |
                      |  - Barbican Key Cloning               |
                      |  - Transient LVM Snapshot Lifecycle   |
                      +-------------------+-------------------+
                                          |
                                          | Subprocess Exec
                                          v
                      +-------------------+-------------------+
                      |         cinder-rxt-worker             |
                      |  (High-Performance Rust Engine)       |
                      |  - Direct Block I/O (O_DIRECT)        |
                      |  - SHA-256 SIMD Chunk Hashing         |
                      |  - Zero-Block Detection               |
                      |  - Binary Diff Manifest Generation    |
                      |  - Multi-threaded Async Swift Client  |
                      +---------+-------------------+---------+
                                |                   |
               Read/Write Block |                   | HTTP REST (PUT/GET)
                                v                   v
                     +----------+----+     +--------+--------+
                     | LVM / VG Host |     | OpenStack Swift |
                     | Block Storage |     | Object Storage  |
                     +---------------+     +-----------------+
```

---

## Configuration (`cinder.conf`)

Add or update the backend definition in `/etc/cinder/cinder.conf`:

```ini
[DEFAULT]
enabled_backends = rxt_lvm

[rxt_lvm]
# Driver & Target Configuration
volume_driver = cinder_rxt.lvm.RXTLVM
target_helper = cinder_rxt.targets.RXTTgtAdm
volume_group = cinder-volumes
volume_backend_name = rxt_lvm

# Multipath Secondary IPs (comma-separated list of secondary storage portal IPs)
target_secondary_ip_addresses = 10.240.10.12,10.240.10.13

# Swift Differential Snapshot Engine
rxt_swift_snapshots_enabled = True
rxt_auto_convert_legacy_snapshots = True
rxt_swift_url = http://swift.openstack.svc.cluster.local:8080/v1/AUTH_cinder_snapshots
rxt_swift_container = cinder_snapshots
rxt_swift_auth_token =
rxt_worker_path = /usr/local/bin/cinder-rxt-worker
rxt_worker_concurrency = 16
```

> **Backward Compatibility:** Existing deployments referencing `volume_driver = cinder_rxt.rackspace.RXTLVM` or `target_helper = cinder_rxt.rackspace.RXTTgtAdm` continue to work without modification.

---

## Legacy Snapshot Conversion

Under the legacy LVM snapshot paradigm, snapshots remain permanently on the physical storage host as LVM COW logical volumes (`snapshot-*`), creating local dependencies that prevent operations teams from evacuating or migrating volumes off hypervisors and storage nodes.

`cinder-rxt` provides three complementary mechanisms to convert legacy snapshots into Swift differential snapshots and remove local LVM copy-on-write volume dependencies:

1. **Automatic Startup Conversion:** When `rxt_auto_convert_legacy_snapshots = True` (default), the driver initiates an asynchronous background worker upon `cinder-volume` startup to discover all legacy snapshot LVs in the Volume Group, sync their 4MB differential chunks into Swift, and immediately destroy the local LVs.
2. **On-The-Fly Fallback:** If a tenant creates a volume from an old, unconverted snapshot or deletes it, `RXTLVM` automatically converts the legacy snapshot to Swift on-the-fly before volume provisioning, and cleans up the local LV.
3. **Operational CLI (`cinder-rxt-manage`):** Storage administrators can audit and execute conversions manually:
   ```bash
   # List all legacy snapshots currently on this node
   cinder-rxt-manage list-legacy-snapshots --vg cinder-volumes

   # Preview conversion without making any changes
   cinder-rxt-manage convert-legacy-snapshots --vg cinder-volumes --dry-run

   # Convert all legacy snapshots in the VG to Swift and purge local LVs
   cinder-rxt-manage convert-legacy-snapshots --vg cinder-volumes

   # Convert a single specific snapshot UUID
   cinder-rxt-manage convert-legacy-snapshots --vg cinder-volumes --snapshot-id <uuid>
   ```


---

## Differential Snapshot Manifest Specification

Snapshot deltas are stored in Swift using a compact, little-endian binary header:

```
+-------------------------------------------------------+
| Magic: "RXTD" (4 bytes)                               |
+-------------------------------------------------------+
| Version: u32 = 1 (4 bytes)                            |
+-------------------------------------------------------+
| Chunk Size: u32 = 4,194,304 (4 bytes, 4 MiB)         |
+-------------------------------------------------------+
| Total Chunks in Volume: u64 (8 bytes)                 |
+-------------------------------------------------------+
| Modified Chunk Count: u64 (8 bytes)                   |
+-------------------------------------------------------+
| Repeated per modified chunk:                          |
|   - chunk_index: u32 (4 bytes)                        |
|   - sha256_hash: [u8; 32] (32 bytes)                  |
| (Total per modified entry: 36 bytes)                  |
+-------------------------------------------------------+
```

### Storage Layout in Swift
- **Deltas:** `<container>/<volume_id>/<snapshot_id>.diff`
- **Payload Chunks:** `<container>/chunks/<sha256[0..2]>/<sha256>` (content-addressable deduplication across snapshots).

---

## Container Build (OpenStack-Helm)

The included multi-stage [`Dockerfile`](file:///Users/Dan.With/src.d/flex.d/WORKTREE/cinder-rxt.git/lvm-fork-encryption/Dockerfile) compiles the Rust worker and installs the Python driver directly into the Cinder container image:

```bash
docker build -t openstackhelm/cinder:master-ubuntu_jammy-rxt .
```

---

## Development & Testing

### Python Unit Tests
```bash
python -m unittest discover -s cinder_rxt
```

### Rust Engine Tests
```bash
cargo test
```

### Manual Release Build of Rust Worker
```bash
cargo build --release
# Output binary: target/release/cinder-rxt-worker
```
