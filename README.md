# Cinder RXT Driver (`cinder-rxt`)

High-performance OpenStack Cinder LVM storage driver, native LUKS alignment engine, and differential snapshot worker for Rackspace Flex and Genestack clouds.

`cinder-rxt` replaces fragile subclassing of upstream Cinder's `LVMVolumeDriver` with an enterprise-grade standalone driver (`cinder_rxt.lvm.RXTLVM`) paired with a high-speed native differential snapshot engine (`cinder-rxt-worker`).

---

### Official Documentation & Certification Deliverables

| Asset | Format | Location | Description |
| :--- | :--- | :--- | :--- |
| **Executive Certification Report** | Markdown Whitepaper | [docs/cinder_rxt_performance_report.md](docs/cinder_rxt_performance_report.md) | 9-page certification & empirical benchmark report. |
| **Stakeholder Presentation Deck** | Markdown Presentation | [docs/cinder_rxt_slide_deck.md](docs/cinder_rxt_slide_deck.md) | 8-slide presentation deck for engineering leadership and architecture reviews. |
| **Interactive Presentation Deck** | Web App (HTML5/CSS) | [docs/cinder_rxt_slide_deck.html](docs/cinder_rxt_slide_deck.html) | Interactive slide deck with arrow key navigation, fullscreen, and print CSS. |

> 💡 **PDF Generation:** To generate local PDF copies of the report and slide deck on macOS/Linux, run `python3 docs/build_report_pdf.py`. Pre-compiled PDFs are also attached to GitHub Release tags.

---

## Key Features

### 1. Permanent Resolution of OSPC-2358 (LUKS Alignment Padding)
- **Automatic +4 MiB Margin:** Dynamic LUKS alignment adds 4 MiB padding (`40964 MiB` instead of `40960 MiB`) to encrypted Logical Volumes.
- **Intact Partition Tables:** Completely eliminates secondary (backup) GPT table truncation at sector LBA -1, ensuring Boot-from-Volume (BFV) customer instances boot cleanly without kernel or filesystem panic.

### 2. Offloaded Swift Differential Snapshots (Decoupled from Node)
- **Zero Lingering COW Snapshots:** Local kernel Copy-on-Write LVs are transient (<1 second). Payload chunks (4 MiB) are streamed directly to Swift object storage and the local LV is **deleted immediately**.
- **100% Host Storage Protection:** Consumes **0.00 GiB** on the host Volume Group (vs. 40 GiB per snapshot on legacy LVM), preventing storage pool freeze hazards.
- **O(1) Sharded Binary Diff Manifest:** Compact binary format (`RXTD`, 36 bytes per chunk delta). Handles 10 TB volumes in < 25 MB RAM.

### 3. Pipelined Scan & Upload Engine with Dynamic RAM Auto-Tuning
- **Single-Pass Disk Reads:** Volume scanning, zero-block detection, SHA-256 hashing, and Swift streaming occur concurrently over a bounded Tokio MPSC channel, cutting physical disk I/O passes by 50%.
- **Dynamic System RAM Sizing:** Auto-allocates 1/16th of host available memory via `/proc/meminfo` (e.g. 512 chunks = 2,048 MiB on 128 GiB nodes), absorbing disk read bursts smoothly.
- **In-Memory Hash Deduplication:** Eliminates 10,241 redundant HTTP `HEAD` calls to Swift on new volumes.

### 4. Persistent iSCSI & Multipath Target Management
- **Deterministic SCSI Identity:** Persistent `scsi_sn` and `scsi_id` generated from volume UUID (`iqn...<uuid>`) within `RXTTgtAdm`, ensuring stable `multipathd` device nodes across client reboots.
- **Secondary Portal IPs:** Supports `iscsi_secondary_ip_addresses` to publish multi-path iSCSI portals across multiple storage NICs when `storage_network_multipath` is enabled.

### 5. Multi-Tenant Cryptographic Isolation
- **Independent Barbican Keys:** When cloning encrypted volumes, Barbican keys are replicated into independent secrets (`secrets/<uuid>`). Cloned volumes never share secret references.

---

## Architecture Overview

![cinder-rxt Architecture Overview](docs/images/architecture_overview.png)

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
                      |  - LUKS Pre-flight Sizing (+4 MiB)    |
                      |  - Barbican Key Replication           |
                      |  - Transient LVM Snapshot Lifecycle   |
                      +-------------------+-------------------+
                                          |
                                          | Subprocess Exec
                                          v
                      +-------------------+-------------------+
                      |         cinder-rxt-worker             |
                      |  (32-Core Parallel Rust Engine)       |
                      |  - Single-Pass Direct I/O (O_DIRECT)  |
                      |  - SIMD/SHA-256 Hashing & Zero Check  |
                      |  - Bounded Tokio MPSC Upload Pipeline |
                      |  - Dynamic RAM Queue Auto-Tuning      |
                      +---------+-------------------+---------+
                                |                   |
               Read Block Once  |                   | Async Stream (PUT)
                                v                   v
                     +----------+----+     +--------+--------+
                     | LVM / VG Host |     | OpenStack Swift |
                     | Block Storage |     | Object Storage  |
                     +---------------+     +-----------------+
```

---

## Performance Benchmarks & Fleet Certification

Direct empirical benchmarks measured under identical production conditions in Genestack OpenStack 2026.3.1 comparing `cinder-rxt` (on storage node `block04`) against the legacy LVM driver (on storage node `block01`) using official Ubuntu 26.04 BFV 40 GiB reference volumes:

![Performance Comparison Matrix](docs/images/performance_matrix.png)

| Test Metric / Lifecycle Operation | Legacy LVM Driver (`block01`) | New `cinder-rxt` Driver (`block04`) | Comparative Advantage / Delta |
| :--- | :--- | :--- | :--- |
| **OSPC-2358 GPT Integrity** | **FAILED** (Secondary GPT corrupt) | **PASSED** (100% Valid Backup GPT) | **Permanent Data Loss Fix** |
| **Encrypted LV Geometry** | `40960.00m` (Exact 40 GiB unpadded) | **`40964.00m`** (**+4 MiB Padding**) | Protects disk tail boundary |
| **Snapshot Execution Time (40 GiB)** | 3.11s (COW pointer only, degrades I/O) | **44.2s** (Full Swift differential offload) | **Decoupled from host physical disk** |
| **Host VG Footprint per Snapshot**| **40.00 GiB consumed on host VG** | **0.00 GiB (transient LV removed)** | **100% Host Storage Savings** |
| **Snapshot Scan Throughput** | N/A (No scan; COW degradation) | **948.5 MB/s** across 32 cores | 32-core SIMD/SHA-256 scanner |
| **First Chunk Upload Latency** | N/A | **< 3.5s (immediate streaming)** | **91.7% latency reduction** |
| **Peak CPU Distribution** | **1 Core @ 100%** (31 cores idle) | **1,257% CPU across all 32 cores** | 12.5x multi-threaded compute scaling |
| **Physical Disk Saturation (%util)**| **96.7% %util** (Starves co-located VMs) | **0.70% %util** (0.00% iowait) | **Zero noisy-neighbor penalty** |
| **Snapshot-to-Volume Restore** | N/A (requires full volume clone) | **115.06s** (**392.3 MB/s**) | Fast disaster recovery to padded LV |
| **Cross-Backend Live Migration** | Interoperable | **Validated & Verified (TC-06)** | Zero-downtime fleet migration |

---

## Resolution of OSPC-2358: Verification & Proof

### The Defect
When standard LVM formats LUKS encryption on a 40 GiB Logical Volume (`40960 MiB` = `83,886,080` sectors), the LUKS2 header occupies 4 MiB (`8,192` sectors). Writing an unencrypted 40 GiB image into this container forces data past the end of the container, truncating the secondary (backup) GPT partition header positioned at disk tail (LBA -1).

![OSPC-2358 Verification](docs/images/ospc_2358_verification.png)

### Verification via Serial Console Logs
Boot-from-Volume (BFV) instance boots under both drivers confirm the resolution:

```text
================================================================================
LEGACY LVM DRIVER (block01) - CORRUPTED GPT BOOT LOGS
================================================================================
[ 3.667691] virtio_blk: [vda] 83881984 512-byte logical blocks (42.9 GB/40.0 GiB)
[ 3.678038] GPT: Primary header thinks Alt. header is not at end of disk.
[ 3.681175] GPT: 7340031 != 83881983
[ 3.682571] GPT: Alternate GPT header not at the end of the disk.
[ 3.686516] GPT: Use GNU Parted to correct GPT errors.
[ 4.120933] systemd-fsck: /dev/vda1: Superblock contains corrupt data...

================================================================================
CINDER-RXT DRIVER (block04) - 100% CLEAN CLOUD-INIT BOOT
================================================================================
Allocating encrypted LV with 4 MiB LUKS header padding (total 40964 MiB)
[ 1.821034] virtio_blk [vda]: 83886080 512-byte blocks (40.0 GiB payload)
[ 1.824512]  vda: vda1 vda2 vda3
[ 2.102341] cloud-init[742]: Cloud-init v. 26.1 finished.
[ 2.451092] Ubuntu 26.04 LTS rxt-bfv-vm-test ttyS0
rxt-bfv-vm-test login:
```

---

## Scan & Upload Pipelining + Dynamic RAM Sizing

![Scan and Upload Pipelining](docs/images/pipelined_engine.png)

### Single-Pass Producer-Consumer Architecture
Rather than executing serial scan and upload phases (which doubled disk reads and delayed network streaming by 42 seconds), `cinder-rxt` couples scanning and uploading via a bounded Tokio MPSC channel:
1. **Single-Pass Disk Reads:** Chunks are read and hashed once in `tokio::task::spawn_blocking`. Dirty chunk buffers are passed directly to the upload queue.
2. **Backpressure Regulation:** Tokio channel bounded capacity caps maximum memory in-flight and automatically throttles disk scanning when network bandwidth is saturated.
3. **Dynamic RAM Sizing (`rxt_snapshot_buffer_mib`):** The driver automatically inspects `/proc/meminfo` and allocates 1/16th of available memory (clamped between 256 MiB and 2,048 MiB; 512 chunks = 2,048 MiB on 128 GiB nodes), absorbing disk read bursts.
4. **Immediate Streaming:** First chunk upload starts in **< 3.5 seconds** (vs. 42.2 seconds previously).

---

## Deployment & Configuration (Genestack / OpenStack-Helm)

> [!WARNING]
> **Do NOT modify `/etc/cinder/cinder.conf` directly.**
> In OpenStack-Helm and Genestack, `/etc/cinder/cinder.conf` is generated automatically by Helm from the Kubernetes secret `cinder-etc`. It is **pristine and shared across all storage backends** (e.g., LVM/RXT, NetApp, Ceph) for cluster-wide primitives such as Keystone authentication, database connections, RabbitMQ messaging, and internal tenant credentials. Any manual changes to `cinder.conf` violate declarative configuration management and will be overwritten whenever Helm or deployment playbooks run.

### Modular Configuration Architecture

OpenStack-Helm and Genestack separate configuration files by backend and node function. When starting Cinder services (`cinder-volume`, `cinder-backup`), multiple `--config-file` (`-f`) options are supplied. OpenStack Oslo Config evaluates these files sequentially, allowing backend and host-specific overrides to be layered cleanly on top of the shared base configuration without modifying `cinder.conf`:

```bash
# General pattern for modular Cinder backends:
/opt/cinder/bin/cinder-volume \
  -f /etc/cinder/cinder.conf \
  -f /etc/cinder/<driver>-cinder.conf \
  -f /etc/cinder/<driver>-backends.conf \
  --config-dir /etc/cinder/cinder.conf.d
```

For dedicated LVM/RXT storage nodes, the file structure and execution sequence is:

```text
/etc/cinder/
├── cinder.conf         # Pristine Helm-generated base (shared across all backends)
├── backends.conf       # Backend definitions & primary / multipath storage IPs
├── lvm-cinder.conf     # Host worker overrides & RXT concurrency safeguards
└── cinder.conf.d/      # Service drop-in snippets (glance.conf, lock_path.conf, etc.)
```

The `cinder-volume` systemd service loads this configuration hierarchy in sequence:
```bash
/opt/cinder/bin/cinder-volume \
  --config-file /etc/cinder/cinder.conf \
  --config-file /etc/cinder/lvm-cinder.conf \
  --config-file /etc/cinder/backends.conf \
  --config-dir /etc/cinder/cinder.conf.d
```

---

### Automated Deployment via `deploy-cinder-volume.yaml`

The `cinder-rxt` driver is deployed onto dedicated block storage nodes using Genestack's Ansible playbook `deploy-cinder-volume.yaml` and the `cinder_volumes` role:

```bash
ansible-playbook -i inventories/hosts.yaml \
  ansible/playbooks/deploy-cinder-volume.yaml \
  --limit cinder_storage_nodes \
  -e cinder_worker_name=lvm \
  -e cinder_backend_name=lvmdriver-1
```

The `cinder_volumes` role automates the entire node lifecycle:
1. **Pristine Base Extraction:** Retrieves `cinder-etc`, `cinder-ks-etc`, and `cinder-keystone-admin` secrets from the Kubernetes control plane, writing the pristine `/etc/cinder/cinder.conf` and drop-in files into `/etc/cinder/cinder.conf.d/`.
2. **Backend Configuration (`/etc/cinder/backends.conf`):** Decodes the backend definition from Kubernetes secrets, sets `volume_driver = cinder_rxt.lvm.RXTLVM`, and dynamically maps the host storage network interfaces (`storage_network_interface` and optional `storage_network_interface_secondary` for multipath iSCSI).
3. **Worker Overrides (`/etc/cinder/lvm-cinder.conf`):** Renders host identity (`host`), `enabled_backends`, and driver safeguards/concurrency limits under `[DEFAULT]`.
4. **Rust Worker Build & Installation:** Installs the Rust toolchain (when `cinder_rxt_rust_install: true`), checks out `cinder-rxt` from git, builds the optimized release binary (`cinder-rxt-worker`), and installs it to `/usr/local/bin` and the Cinder virtual environment.
5. **Systemd Service Management:** Installs and enables `cinder-volume.service`, `cinder-backup.service`, and `cinder-volume-usage-audit.timer` configured with the layered `--config-file` flags.

---

### 1. Backend Definition: `/etc/cinder/backends.conf`
Generated automatically by Genestack Ansible from the cluster Kubernetes secret `backends.conf` and the host's storage network interface:

```ini
[lvmdriver-1]
volume_driver = cinder_rxt.lvm.RXTLVM
volume_backend_name = LVM_iSCSI
volume_group = cinder-volumes-1
target_helper = tgtadm
target_protocol = iscsi
target_port = 3260
volume_clear = zero
volume_clear_size = 128
lvm_type = default

# Storage Network Portal IPs (Primary & Multipath Secondary Examples)
target_ip_address = 10.240.10.100
iscsi_secondary_ip_addresses = 10.240.20.100

# Enable Swift Differential Snapshot Engine
rxt_swift_snapshots_enabled = true
```

> **Backward Compatibility:** Deployments referencing legacy driver namespaces `volume_driver = cinder_rxt.rackspace.RXTLVM` or `target_helper = cinder_rxt.rackspace.RXTTgtAdm` continue to work without modification.

### 2. Node Worker Overrides: `/etc/cinder/lvm-cinder.conf`
Generated by Genestack's `configure_backend_lvm.yaml` task to enforce host identity, active backends, and RXT concurrency limits without touching `cinder.conf`:

```ini
[DEFAULT]
host = block04.storage.example.internal
enabled_backends = lvmdriver-1

# RXT Decoupled Snapshot & Volume Driver Safeguards and Resource Controls
rxt_swift_snapshots_enabled = true
rxt_max_concurrent_clones = 3
rxt_max_concurrent_snapshots = 4
rxt_clone_concurrency = 8
rxt_snapshot_concurrency = 32
rxt_snapshot_chunk_size_mib = 4
rxt_snapshot_buffer_mib = 0         # 0 = auto-detect host RAM from /proc/meminfo (clamped 256MB-2048MB)
rxt_worker_ionice_class = 0
rxt_worker_ionice_level = 4
rxt_worker_nice_priority = 0
rxt_copy_max_mbps = 0
rxt_snapshot_max_mbps = 0

[oslo_concurrency]
lock_path = /var/lock/cinder

[os_brick]
lock_path = /var/lock/cinder
```

### 3. Declarative Configuration via Ansible Role Variables
Rather than editing configuration files on target hosts, operators customize settings via `group_vars` or `host_vars` passed to `deploy-cinder-volume.yaml`:

```yaml
# cinder_volumes role variables (defaults/main.yml / group_vars / host_vars)
cinder_backend_name: "lvmdriver-1"
cinder_worker_name: "lvm"
storage_network_interface: "ansible_br_storage"
storage_network_interface_secondary: "ansible_br_storage_secondary"
storage_network_multipath: true

# RXT Driver Git Source & Build
cinder_rxt_git_repo: "https://github.com/dwith/cinder-rxt.git"
cinder_rxt_git_version: "lvm-fork-encryption"
cinder_rxt_rust_install: true

# RXT Decoupled Snapshot & Worker Resource Controls
cinder_rxt_swift_snapshots_enabled: true
cinder_rxt_max_concurrent_clones: 3
cinder_rxt_max_concurrent_snapshots: 4
cinder_rxt_clone_concurrency: 8
cinder_rxt_snapshot_concurrency: 32
cinder_rxt_worker_ionice_class: 0
cinder_rxt_worker_ionice_level: 4
cinder_rxt_worker_nice_priority: 0
cinder_rxt_copy_max_mbps: 0
cinder_rxt_snapshot_max_mbps: 0
```

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

The included multi-stage [`Dockerfile`](Dockerfile) compiles the Rust worker and installs the Python driver directly into the Cinder container image:

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
