// Copyright 2026 Rackspace US, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

mod block_io;
mod hash;
mod manifest;
mod swift;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::stream::{self, StreamExt};
use manifest::{SnapshotEntry, SnapshotManifest, VolumeManifestMeta};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use swift::SwiftClient;
use tracing::info;

#[allow(dead_code)]
struct UploadChunk {
    chunk_idx: u32,
    chunk_hash: [u8; 32],
    data: Vec<u8>,
    bytes: u64,
}

#[allow(dead_code)]
enum ScanChunk {
    Zero {
        chunk_idx: u32,
        bytes: u64,
    },
    Data {
        chunk_idx: u32,
        chunk_hash: [u8; 32],
        buf: Vec<u8>,
        bytes: u64,
    },
}

#[derive(Clone)]
struct ProgressReporter {
    start_time: Instant,
    total_chunks: u32,
    snapshot_id: String,
    last_log: Arc<Mutex<Instant>>,
    scanned_chunks: Arc<AtomicU32>,
    scanned_bytes: Arc<AtomicU64>,
    queued_chunks: Arc<AtomicU32>,
    uploaded_chunks: Arc<AtomicU32>,
    uploaded_bytes: Arc<AtomicU64>,
    scan_finished: Arc<AtomicBool>,
}

impl ProgressReporter {
    fn new(total_chunks: u32, snapshot_id: String) -> Self {
        Self {
            start_time: Instant::now(),
            total_chunks,
            snapshot_id,
            last_log: Arc::new(Mutex::new(Instant::now())),
            scanned_chunks: Arc::new(AtomicU32::new(0)),
            scanned_bytes: Arc::new(AtomicU64::new(0)),
            queued_chunks: Arc::new(AtomicU32::new(0)),
            uploaded_chunks: Arc::new(AtomicU32::new(0)),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
            scan_finished: Arc::new(AtomicBool::new(false)),
        }
    }

    fn report(&self, force: bool) {
        let mut last = self.last_log.lock().unwrap();
        if force || last.elapsed() >= Duration::from_secs(5) {
            *last = Instant::now();
            let elapsed = self.start_time.elapsed().as_secs_f64();
            let sc = self.scanned_chunks.load(Ordering::Relaxed);
            let sc_b = self.scanned_bytes.load(Ordering::Relaxed);
            let qc = self.queued_chunks.load(Ordering::Relaxed);
            let up = self.uploaded_chunks.load(Ordering::Relaxed);
            let up_b = self.uploaded_bytes.load(Ordering::Relaxed);
            let is_done = self.scan_finished.load(Ordering::Relaxed);

            let scan_pct = if self.total_chunks > 0 {
                (sc as f64 / self.total_chunks as f64) * 100.0
            } else {
                100.0
            };
            let scan_mbps = if elapsed > 0.0 {
                (sc_b as f64 / 1_048_576.0) / elapsed
            } else {
                0.0
            };
            let up_mbps = if elapsed > 0.0 {
                (up_b as f64 / 1_048_576.0) / elapsed
            } else {
                0.0
            };

            let overall_pct = if is_done {
                if qc == 0 {
                    100.0
                } else {
                    50.0 + (up as f64 / qc as f64) * 50.0
                }
            } else {
                let up_ratio = if qc > 0 {
                    (up as f64 / qc as f64).min(1.0)
                } else {
                    1.0
                };
                let scan_factor = if self.total_chunks > 0 {
                    sc as f64 / self.total_chunks as f64
                } else {
                    1.0
                };
                scan_factor * 50.0 + (up_ratio * scan_factor * 50.0)
            };

            info!(
                "Snapshot {}: scan {}/{} ({:.1}%, {:.1} MB/s) | upload {}/{} changed ({:.1} MB/s) | overall: {:.0}%",
                self.snapshot_id, sc, self.total_chunks, scan_pct, scan_mbps, up, qc, up_mbps, overall_pct
            );
            println!("PROGRESS: {:.0}%", overall_pct.floor());
            let _ = std::io::stdout().flush();
        }
    }
}

#[derive(Parser)]
#[command(name = "cinder-rxt-worker")]
#[command(about = "High-speed Swift differential snapshot & parallel chunk restore worker")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create differential snapshot and upload dirty 4MB chunks to Swift
    SyncDiff {
        #[arg(long)]
        source_dev: String,
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        swift_url: String,
        #[arg(long)]
        swift_token: String,
        #[arg(long, default_value = "cinder-snapshots-")]
        container_prefix: String,
        #[arg(long, default_value_t = 4)]
        chunk_size_mib: usize,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 0)]
        max_mbps: u64,
    },
    /// Reassemble snapshot chunks from Swift and stream directly to local block device
    Restore {
        #[arg(long)]
        dest_dev: String,
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        swift_url: String,
        #[arg(long)]
        swift_token: String,
        #[arg(long, default_value = "cinder-snapshots-")]
        container_prefix: String,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 0)]
        max_mbps: u64,
    },
    /// List snapshots in the volume's manifest
    ListSnapshots {
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        swift_url: String,
        #[arg(long)]
        swift_token: String,
        #[arg(long, default_value = "cinder-snapshots-")]
        container_prefix: String,
    },
    /// Delete a snapshot from Swift, removing its autonomous manifest and GCing unreferenced chunks
    DeleteSnapshot {
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        swift_url: String,
        #[arg(long)]
        swift_token: String,
        #[arg(long, default_value = "cinder-snapshots-")]
        container_prefix: String,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
    },
    /// Purge an entire volume's snapshot container and all its objects from Swift
    DeleteVolume {
        #[arg(long)]
        volume_id: String,
        #[arg(long)]
        swift_url: String,
        #[arg(long)]
        swift_token: String,
        #[arg(long, default_value = "cinder-snapshots-")]
        container_prefix: String,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
    },
    /// Accelerated multi-threaded direct block volume copy with sparse zero-block skipping
    CopyVolume {
        #[arg(long)]
        src: String,
        #[arg(long)]
        dest: String,
        #[arg(long)]
        size_bytes: u64,
        #[arg(long, default_value_t = 4)]
        chunk_size_mib: usize,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long, default_value_t = 0)]
        max_mbps: u64,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            num_args = 0..=1,
            default_missing_value = "true"
        )]
        sparse: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();

    match cli.command {
        Commands::SyncDiff {
            source_dev,
            volume_id,
            snapshot_id,
            swift_url,
            swift_token,
            container_prefix,
            chunk_size_mib,
            concurrency,
            max_mbps,
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));
            swift.ensure_container(&container).await?;

            let mut file = File::open(&source_dev)
                .with_context(|| format!("Failed to open source device {}", source_dev))?;
            let total_bytes = block_io::get_device_size(&mut file)?;
            if total_bytes == 0 {
                anyhow::bail!(
                    "Source device {} has reported size of 0 bytes. Cannot snapshot an empty or unreadable device.",
                    source_dev
                );
            }
            let file = Arc::new(file);
            let chunk_size = chunk_size_mib * 1024 * 1024;
            let total_chunks = ((total_bytes + chunk_size as u64 - 1) / chunk_size as u64) as u32;

            info!(
                "Starting sync-diff for volume {} (size: {} bytes, {} chunks of {} MiB, concurrency: {})",
                volume_id, total_bytes, total_chunks, chunk_size_mib, concurrency
            );

            // Fetch current manifest metadata if present
            let meta_obj = "manifest/meta.json";
            let mut meta: VolumeManifestMeta = match swift.get_object(&container, meta_obj).await {
                Ok(bytes) => serde_json::from_slice(&bytes)?,
                Err(_) => VolumeManifestMeta {
                    version: 1,
                    volume_id: volume_id.clone(),
                    volume_size_bytes: total_bytes,
                    chunk_size_bytes: chunk_size as u32,
                    snapshots: Vec::new(),
                },
            };

            // Fix volume_size_bytes if it was previously recorded as 0
            if meta.volume_size_bytes == 0 && total_bytes > 0 {
                meta.volume_size_bytes = total_bytes;
            }
            if meta.chunk_size_bytes == 0 {
                meta.chunk_size_bytes = chunk_size as u32;
            }

            // Load baseline manifest from most recent snapshot if available
            let base_map = if let Some(latest_entry) = meta.snapshots.iter().filter(|s| s.snapshot_id != snapshot_id).last() {
                match swift.get_object(&container, &latest_entry.manifest_file).await {
                    Ok(bytes) => {
                        match SnapshotManifest::deserialize(latest_entry.snapshot_id.clone(), volume_id.clone(), &bytes) {
                            Ok(m) => m.block_map,
                            Err(e) => {
                                info!("Could not deserialize baseline manifest {}: {}. Starting fresh baseline.", latest_entry.manifest_file, e);
                                HashMap::new()
                            }
                        }
                    }
                    Err(e) => {
                        info!("Could not fetch baseline manifest {}: {}. Starting fresh baseline.", latest_entry.manifest_file, e);
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };

            let concurrency = concurrency.max(1);
            let channel_capacity = (concurrency * 2).max(16);
            let reporter = ProgressReporter::new(total_chunks, snapshot_id.clone());

            let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadChunk>(channel_capacity);

            let reporter_scan = reporter.clone();
            let file_scan = Arc::clone(&file);
            let base_map = Arc::new(base_map);
            let base_map_scan = Arc::clone(&base_map);
            let snapshot_id_scan = snapshot_id.clone();
            let volume_id_scan = volume_id.clone();

            let scan_future = async move {
                let mut new_manifest = SnapshotManifest::new(snapshot_id_scan, volume_id_scan, total_chunks);
                let mut known_hashes: HashSet<[u8; 32]> = base_map_scan.values().copied().collect();

                let mut scan_stream = stream::iter(0..total_chunks)
                    .map(|chunk_idx| {
                        let file = Arc::clone(&file_scan);
                        async move {
                            tokio::task::spawn_blocking(move || -> Result<ScanChunk> {
                                let offset = chunk_idx as u64 * chunk_size as u64;
                                let bytes_to_read = std::cmp::min(chunk_size as u64, total_bytes - offset) as usize;
                                let mut buf = vec![0u8; bytes_to_read];
                                block_io::read_chunk_at(&file, offset, &mut buf)?;

                                if block_io::is_all_zeros(&buf) {
                                    Ok(ScanChunk::Zero { chunk_idx, bytes: bytes_to_read as u64 })
                                } else {
                                    let chunk_hash = hash::compute_chunk_hash(&buf);
                                    Ok(ScanChunk::Data { chunk_idx, chunk_hash, buf, bytes: bytes_to_read as u64 })
                                }
                            })
                            .await
                            .context("Scan task panicked")?
                        }
                    })
                    .buffer_unordered(concurrency);

                while let Some(scan_res) = scan_stream.next().await {
                    let scan_chunk = scan_res?;
                    reporter_scan.scanned_chunks.fetch_add(1, Ordering::Relaxed);

                    match scan_chunk {
                        ScanChunk::Zero { bytes, .. } => {
                            reporter_scan.scanned_bytes.fetch_add(bytes, Ordering::Relaxed);
                        }
                        ScanChunk::Data { chunk_idx, chunk_hash, buf, bytes } => {
                            reporter_scan.scanned_bytes.fetch_add(bytes, Ordering::Relaxed);
                            new_manifest.block_map.insert(chunk_idx, chunk_hash);

                            let prior_hash = base_map_scan.get(&chunk_idx);
                            if prior_hash == Some(&chunk_hash) {
                                // Content unchanged from baseline; skip upload
                            } else if known_hashes.insert(chunk_hash) {
                                // Brand new chunk content not yet uploaded; stream buffer directly into upload pipeline
                                reporter_scan.queued_chunks.fetch_add(1, Ordering::Relaxed);
                                upload_tx.send(UploadChunk {
                                    chunk_idx,
                                    chunk_hash,
                                    data: buf,
                                    bytes,
                                }).await.map_err(|_| anyhow::anyhow!("Upload consumer dropped prematurely"))?;
                            } else {
                                // Chunk changed from baseline for this block, but identical data exists in another block
                                // Swift CAS already stores this chunk, so upload is deduplicated!
                            }
                        }
                    }

                    reporter_scan.report(false);
                }

                reporter_scan.scan_finished.store(true, Ordering::SeqCst);
                let scan_elapsed = reporter_scan.start_time.elapsed().as_secs_f64();
                let scan_bytes = reporter_scan.scanned_bytes.load(Ordering::Relaxed);
                let scan_mbps = if scan_elapsed > 0.0 { (scan_bytes as f64 / 1_048_576.0) / scan_elapsed } else { 0.0 };
                let queued = reporter_scan.queued_chunks.load(Ordering::Relaxed);

                info!(
                    "Volume scan complete in {:.1}s ({:.1} MB/s): {} active chunks, {} changed chunks queued for upload",
                    scan_elapsed, scan_mbps, new_manifest.block_map.len(), queued
                );
                let _ = std::io::stdout().flush();

                drop(upload_tx);
                Ok::<SnapshotManifest, anyhow::Error>(new_manifest)
            };

            let reporter_upload = reporter.clone();
            let swift_upload = Arc::clone(&swift);
            let container_upload = container.clone();
            let need_head_check = meta.snapshots.len() > 1;
            let dispatched_bytes = Arc::new(AtomicU64::new(0));
            let rate_limiter_start = Instant::now();

            let upload_future = async move {
                let rx_stream = futures_util::stream::unfold(upload_rx, |mut rx| async move {
                    rx.recv().await.map(|chunk| (chunk, rx))
                });

                rx_stream
                    .map(|chunk| {
                        let swift = Arc::clone(&swift_upload);
                        let container = container_upload.clone();
                        let reporter = reporter_upload.clone();
                        let dispatched_bytes = Arc::clone(&dispatched_bytes);

                        async move {
                            let hex_name = format!("chunks/{}", hash::hash_to_hex(&chunk.chunk_hash));

                            if max_mbps > 0 {
                                let curr = dispatched_bytes.fetch_add(chunk.bytes, Ordering::SeqCst) + chunk.bytes;
                                let target_duration = Duration::from_secs_f64(curr as f64 / (max_mbps as f64 * 1_048_576.0));
                                let elapsed = rate_limiter_start.elapsed();
                                if target_duration > elapsed {
                                    tokio::time::sleep(target_duration - elapsed).await;
                                }
                            }

                            if need_head_check && swift.head_object(&container, &hex_name).await.unwrap_or(false) {
                                // Already in Swift from older snapshot
                            } else {
                                swift.put_object(&container, &hex_name, chunk.data).await?;
                            }

                            reporter.uploaded_chunks.fetch_add(1, Ordering::SeqCst);
                            reporter.uploaded_bytes.fetch_add(chunk.bytes, Ordering::SeqCst);
                            reporter.report(false);

                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .buffer_unordered(concurrency)
                    .collect::<Vec<Result<()>>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<()>>>()?;

                Ok::<(), anyhow::Error>(())
            };

            let (new_manifest, ()) = tokio::try_join!(scan_future, upload_future)?;
            reporter.report(true);

            let total_elapsed = reporter.start_time.elapsed().as_secs_f64();
            let up_chunks = reporter.uploaded_chunks.load(Ordering::Relaxed);
            let up_bytes = reporter.uploaded_bytes.load(Ordering::Relaxed);
            let total_up_mbps = if total_elapsed > 0.0 { (up_bytes as f64 / 1_048_576.0) / total_elapsed } else { 0.0 };

            if up_chunks == 0 {
                info!("No changed chunks to upload; snapshot is identical to baseline.");
            } else {
                info!(
                    "Pipelined sync complete in {:.1}s: {} chunks uploaded ({:.1} MB/s)",
                    total_elapsed, up_chunks, total_up_mbps
                );
            }
            println!("PROGRESS: 100%");
            let _ = std::io::stdout().flush();

            // Serialize & upload snapshot manifest file
            let manifest_filename = format!("manifest/snapshots/{}.manifest", snapshot_id);
            let manifest_bytes = new_manifest.serialize()?;
            swift.put_object(&container, &manifest_filename, manifest_bytes).await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;

            if let Some(existing) = meta.snapshots.iter_mut().find(|s| s.snapshot_id == snapshot_id) {
                existing.created_at = now;
                existing.manifest_file = manifest_filename;
                existing.total_chunks = total_chunks;
                existing.active_chunks = new_manifest.block_map.len() as u32;
            } else {
                meta.snapshots.push(SnapshotEntry {
                    snapshot_id: snapshot_id.clone(),
                    created_at: now,
                    manifest_file: manifest_filename,
                    total_chunks,
                    active_chunks: new_manifest.block_map.len() as u32,
                });
            }

            let meta_bytes = serde_json::to_vec_pretty(&meta)?;
            swift.put_object(&container, meta_obj, meta_bytes).await?;

            println!(
                "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"total_chunks\":{},\"active_chunks\":{}}}",
                volume_id, snapshot_id, total_chunks, new_manifest.block_map.len()
            );
        }

        Commands::Restore {
            dest_dev,
            volume_id,
            snapshot_id,
            swift_url,
            swift_token,
            container_prefix,
            concurrency,
            max_mbps,
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));

            let meta_bytes = swift.get_object(&container, "manifest/meta.json").await
                .context("Failed to fetch manifest/meta.json from Swift")?;
            let meta: VolumeManifestMeta = serde_json::from_slice(&meta_bytes)?;

            let entry = meta.snapshots.iter().find(|s| s.snapshot_id == snapshot_id)
                .with_context(|| format!("Snapshot {} not found in manifest", snapshot_id))?;

            let manifest_bytes = swift.get_object(&container, &entry.manifest_file).await
                .with_context(|| format!("Failed to fetch manifest file {}", entry.manifest_file))?;
            let manifest = SnapshotManifest::deserialize(snapshot_id.clone(), volume_id.clone(), &manifest_bytes)?;

            let chunk_size = meta.chunk_size_bytes as usize;
            let total_chunks = manifest.total_chunks;
            let total_bytes = meta.volume_size_bytes;

            info!(
                "Restoring snapshot {} ({} active non-empty chunks out of {} total chunks)",
                snapshot_id, manifest.block_map.len(), total_chunks
            );

            let dest_file = OpenOptions::new()
                .write(true)
                .open(&dest_dev)
                .with_context(|| format!("Failed to open destination device {}", dest_dev))?;
            let dest_file = Arc::new(dest_file);

            let mut restore_items: Vec<(u32, [u8; 32])> = manifest.block_map.into_iter().collect();
            restore_items.sort_unstable_by_key(|&(idx, _)| idx);
            let total_to_restore = restore_items.len();

            let restored_chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let restored_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let restore_start = std::time::Instant::now();
            let last_restore_log = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
            let dispatched_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let rate_limiter_start = std::time::Instant::now();

            let restore_stream = stream::iter(restore_items).map(|(idx, h)| {
                let swift = Arc::clone(&swift);
                let container = container.clone();
                let dest_file = Arc::clone(&dest_file);
                let restored_chunks = Arc::clone(&restored_chunks);
                let restored_bytes = Arc::clone(&restored_bytes);
                let last_restore_log = Arc::clone(&last_restore_log);
                let dispatched_bytes = Arc::clone(&dispatched_bytes);
                let snapshot_id = snapshot_id.clone();
                async move {
                    let offset = idx as u64 * chunk_size as u64;
                    let bytes_to_write = std::cmp::min(chunk_size as u64, total_bytes - offset) as usize;

                    if max_mbps > 0 {
                        let curr = dispatched_bytes.fetch_add(bytes_to_write as u64, std::sync::atomic::Ordering::SeqCst) + bytes_to_write as u64;
                        let target_duration = std::time::Duration::from_secs_f64(curr as f64 / (max_mbps as f64 * 1_048_576.0));
                        let elapsed = rate_limiter_start.elapsed();
                        if target_duration > elapsed {
                            tokio::time::sleep(target_duration - elapsed).await;
                        }
                    }
                    let hex_name = format!("chunks/{}", hash::hash_to_hex(&h));
                    let data = swift.get_object(&container, &hex_name).await?;
                    let slice = if data.len() > bytes_to_write {
                        &data[..bytes_to_write]
                    } else {
                        &data[..]
                    };
                    block_io::write_chunk_at(&dest_file, offset, slice)?;

                    let done = restored_chunks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    restored_bytes.fetch_add(bytes_to_write as u64, std::sync::atomic::Ordering::SeqCst);

                    let mut last = last_restore_log.lock().unwrap();
                    if last.elapsed() >= std::time::Duration::from_secs(5) || done == total_to_restore {
                        *last = std::time::Instant::now();
                        let elapsed = restore_start.elapsed().as_secs_f64();
                        let bytes = restored_bytes.load(std::sync::atomic::Ordering::SeqCst);
                        let rate_mb = if elapsed > 0.0 { (bytes as f64 / 1_048_576.0) / elapsed } else { 0.0 };
                        let pct = (done as f64 / total_to_restore as f64) * 100.0;
                        let eta_secs = if rate_mb > 0.0 {
                            let total_bytes_est = total_to_restore as f64 * chunk_size as f64;
                            let remaining_bytes = total_bytes_est - bytes as f64;
                            ((remaining_bytes / 1_048_576.0) / rate_mb).max(0.0) as u64
                        } else {
                            0
                        };
                        info!(
                            "Restoring snapshot {}: {}/{} chunks ({:.1}%) [{:.1} MB/s, elapsed: {:.0}s, ETA: {}s]",
                            snapshot_id, done, total_to_restore, pct, rate_mb, elapsed, eta_secs
                        );
                        println!("PROGRESS: {:.0}%", pct);
                        let _ = std::io::stdout().flush();
                    }

                    Ok::<(), anyhow::Error>(())
                }
            });

            restore_stream
                .buffer_unordered(concurrency)
                .collect::<Vec<Result<()>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<()>>>()?;

            println!(
                "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"restored_chunks\":{}}}",
                volume_id, snapshot_id, total_to_restore
            );
        }

        Commands::ListSnapshots {
            volume_id,
            swift_url,
            swift_token,
            container_prefix,
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = SwiftClient::new(swift_url, swift_token);
            let meta_bytes = swift.get_object(&container, "manifest/meta.json").await?;
            let meta: VolumeManifestMeta = serde_json::from_slice(&meta_bytes)?;
            println!("{}", serde_json::to_string_pretty(&meta.snapshots)?);
        }

        Commands::DeleteSnapshot {
            volume_id,
            snapshot_id,
            swift_url,
            swift_token,
            container_prefix,
            concurrency,
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));

            let meta_bytes = match swift.get_object(&container, "manifest/meta.json").await {
                Ok(bytes) => bytes,
                Err(e) => {
                    info!("No manifest found for container {}: {}", container, e);
                    println!(
                        "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"action\":\"manifest_not_found\"}}",
                        volume_id, snapshot_id
                    );
                    return Ok(());
                }
            };

            let mut meta: VolumeManifestMeta = serde_json::from_slice(&meta_bytes)?;
            let target_pos = meta.snapshots.iter().position(|s| s.snapshot_id == snapshot_id);

            let target_idx = match target_pos {
                Some(idx) => idx,
                None => {
                    info!("Snapshot {} not found in manifest for volume {}", snapshot_id, volume_id);
                    println!(
                        "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"action\":\"snapshot_not_found\"}}",
                        volume_id, snapshot_id
                    );
                    return Ok(());
                }
            };

            // Load target snapshot manifest
            let target_entry = meta.snapshots[target_idx].clone();
            let target_manifest = match swift.get_object(&container, &target_entry.manifest_file).await {
                Ok(bytes) => {
                    SnapshotManifest::deserialize(snapshot_id.clone(), volume_id.clone(), &bytes).ok()
                }
                Err(_) => None,
            };

            // Remove target manifest file from Swift
            let _ = swift.delete_object(&container, &target_entry.manifest_file).await;

            // Remove target from snapshot list
            meta.snapshots.remove(target_idx);

            // If no snapshots remain, purge the entire container!
            if meta.snapshots.is_empty() {
                info!("No snapshots remain for volume {}. Purging container {}", volume_id, container);
                swift.purge_container(&container, concurrency).await?;
                println!(
                    "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"action\":\"container_purged\"}}",
                    volume_id, snapshot_id
                );
                return Ok(());
            }

            // Otherwise, update manifest/meta.json
            let meta_bytes = serde_json::to_vec_pretty(&meta)?;
            swift.put_object(&container, "manifest/meta.json", meta_bytes).await?;

            // Garbage collection: delete chunks uniquely referenced by deleted manifest
            let mut gced_count = 0;
            if let Some(target_m) = target_manifest {
                let mut surviving_hashes = HashSet::new();
                for entry in &meta.snapshots {
                    if let Ok(bytes) = swift.get_object(&container, &entry.manifest_file).await {
                        if let Ok(m) = SnapshotManifest::deserialize(entry.snapshot_id.clone(), volume_id.clone(), &bytes) {
                            for (_, h) in m.block_map {
                                surviving_hashes.insert(h);
                            }
                        }
                    }
                }

                let mut orphaned = Vec::new();
                for (_, h) in target_m.block_map {
                    if !surviving_hashes.contains(&h) {
                        orphaned.push(h);
                    }
                }

                gced_count = orphaned.len();
                let stream = stream::iter(orphaned).map(|h| {
                    let swift = Arc::clone(&swift);
                    let container = container.clone();
                    async move {
                        let hex_name = format!("chunks/{}", hash::hash_to_hex(&h));
                        let _ = swift.delete_object(&container, &hex_name).await;
                        Ok::<(), anyhow::Error>(())
                    }
                });

                stream
                    .buffer_unordered(concurrency)
                    .collect::<Vec<Result<()>>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<()>>>()?;
            }

            info!(
                "Snapshot {} deleted from volume {}. GC'd {} orphaned chunks.",
                snapshot_id, volume_id, gced_count
            );
            println!(
                "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"action\":\"deleted\",\"gced_chunks\":{}}}",
                volume_id, snapshot_id, gced_count
            );
        }

        Commands::DeleteVolume {
            volume_id,
            swift_url,
            swift_token,
            container_prefix,
            concurrency,
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));
            swift.purge_container(&container, concurrency).await?;
            info!("Purged snapshot container {} for volume {}", container, volume_id);
            println!(
                "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"action\":\"volume_container_purged\"}}",
                volume_id
            );
        }

        Commands::CopyVolume {
            src,
            dest,
            mut size_bytes,
            chunk_size_mib,
            concurrency,
            max_mbps,
            sparse,
        } => {
            let src_file = OpenOptions::new()
                .read(true)
                .open(&src)
                .with_context(|| format!("Failed to open source device {}", src))?;
            let src_file = Arc::new(src_file);

            let dest_file = OpenOptions::new()
                .write(true)
                .open(&dest)
                .with_context(|| format!("Failed to open destination device {}", dest))?;
            let dest_file = Arc::new(dest_file);

            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let mut dev_size: u64 = 0;
                const BLKGETSIZE64: libc::c_ulong = 0x80081272;
                unsafe {
                    if libc::ioctl(dest_file.as_raw_fd(), BLKGETSIZE64, &mut dev_size) == 0 && dev_size > 0 {
                        if size_bytes > dev_size {
                            info!(
                                "Clamping copy size from {} to destination block device size {}",
                                size_bytes, dev_size
                            );
                            size_bytes = dev_size;
                        }
                    }
                    if libc::ioctl(src_file.as_raw_fd(), BLKGETSIZE64, &mut dev_size) == 0 && dev_size > 0 {
                        if size_bytes > dev_size {
                            info!(
                                "Clamping copy size from {} to source block device size {}",
                                size_bytes, dev_size
                            );
                            size_bytes = dev_size;
                        }
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            let _ = &mut size_bytes;

            let chunk_size = chunk_size_mib * 1024 * 1024;
            let total_chunks = ((size_bytes + chunk_size as u64 - 1) / chunk_size as u64) as u32;

            info!(
                "Starting accelerated block copy from {} to {} ({} bytes, {} MiB chunks, concurrency {}, max_mbps {}, sparse={})",
                src, dest, size_bytes, chunk_size_mib, concurrency, max_mbps, sparse
            );

            let copied_chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let copied_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let zero_chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let copy_start = std::time::Instant::now();
            let last_copy_log = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
            let dispatched_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let rate_limiter_start = std::time::Instant::now();

            let copy_stream = stream::iter(0..total_chunks).map(|chunk_idx| {
                let src_file = Arc::clone(&src_file);
                let dest_file = Arc::clone(&dest_file);
                let copied_chunks = Arc::clone(&copied_chunks);
                let copied_bytes = Arc::clone(&copied_bytes);
                let zero_chunks = Arc::clone(&zero_chunks);
                let last_copy_log = Arc::clone(&last_copy_log);
                let dispatched_bytes = Arc::clone(&dispatched_bytes);

                async move {
                    let offset = chunk_idx as u64 * chunk_size as u64;
                    let bytes_to_read = std::cmp::min(chunk_size as u64, size_bytes - offset) as usize;

                    if max_mbps > 0 {
                        let curr = dispatched_bytes.fetch_add(bytes_to_read as u64, std::sync::atomic::Ordering::SeqCst) + bytes_to_read as u64;
                        let target_duration = std::time::Duration::from_secs_f64(curr as f64 / (max_mbps as f64 * 1_048_576.0));
                        let elapsed = rate_limiter_start.elapsed();
                        if target_duration > elapsed {
                            tokio::time::sleep(target_duration - elapsed).await;
                        }
                    }

                    let mut buf = vec![0u8; bytes_to_read];

                    tokio::task::spawn_blocking(move || -> Result<()> {
                        block_io::read_chunk_at(&src_file, offset, &mut buf)?;

                        if sparse && block_io::is_all_zeros(&buf) {
                            zero_chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            block_io::write_chunk_at(&dest_file, offset, &buf)?;
                        }
                        Ok(())
                    })
                    .await??;

                    let done = copied_chunks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    copied_bytes.fetch_add(bytes_to_read as u64, std::sync::atomic::Ordering::SeqCst);

                    let mut last = last_copy_log.lock().unwrap();
                    if last.elapsed() >= std::time::Duration::from_secs(5) || done == total_chunks as usize {
                        *last = std::time::Instant::now();
                        let elapsed = copy_start.elapsed().as_secs_f64();
                        let bytes = copied_bytes.load(std::sync::atomic::Ordering::SeqCst);
                        let rate_mb = if elapsed > 0.0 { (bytes as f64 / 1_048_576.0) / elapsed } else { 0.0 };
                        let pct = (done as f64 / total_chunks as f64) * 100.0;
                        let eta_secs = if rate_mb > 0.0 {
                            let remaining_bytes = size_bytes as f64 - bytes as f64;
                            ((remaining_bytes / 1_048_576.0) / rate_mb).max(0.0) as u64
                        } else {
                            0
                        };
                        info!(
                            "Copying volume: {}/{} chunks ({:.1}%) [{:.1} MB/s, elapsed: {:.0}s, ETA: {}s]",
                            done, total_chunks, pct, rate_mb, elapsed, eta_secs
                        );
                        println!("PROGRESS: {:.0}%", pct);
                        let _ = std::io::stdout().flush();
                    }

                    Ok::<(), anyhow::Error>(())
                }
            });

            copy_stream
                .buffer_unordered(concurrency)
                .collect::<Vec<Result<()>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<()>>>()?;

            // Sync destination device
            let dest_file_sync = Arc::clone(&dest_file);
            tokio::task::spawn_blocking(move || -> Result<()> {
                dest_file_sync.sync_all().context("Failed to sync destination device")?;
                Ok(())
            })
            .await??;

            let elapsed = copy_start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 { (size_bytes as f64 / 1_048_576.0) / elapsed } else { 0.0 };
            let zeros = zero_chunks.load(std::sync::atomic::Ordering::SeqCst);

            info!(
                "Volume copy finished in {:.2}s: {} bytes copied ({:.1} MB/s), {} zero chunks skipped",
                elapsed, size_bytes, speed, zeros
            );
            println!(
                "{{\"status\":\"ok\",\"action\":\"volume_copied\",\"total_bytes\":{},\"total_chunks\":{},\"zero_chunks_skipped\":{},\"duration_secs\":{:.2},\"speed_mbps\":{:.2}}}",
                size_bytes, total_chunks, zeros, elapsed, speed
            );
        }
    }

    Ok(())
}
