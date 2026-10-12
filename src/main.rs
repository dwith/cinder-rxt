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
use manifest::{SnapshotDiff, SnapshotEntry, VolumeManifestMeta};
use std::fs::{File, OpenOptions};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use swift::SwiftClient;
use tracing::info;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
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
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));
            swift.ensure_container(&container).await?;

            let file = File::open(&source_dev)
                .with_context(|| format!("Failed to open source device {}", source_dev))?;
            let metadata = file.metadata()?;
            let total_bytes = metadata.len();
            let chunk_size = chunk_size_mib * 1024 * 1024;
            let total_chunks = ((total_bytes + chunk_size as u64 - 1) / chunk_size as u64) as u32;

            info!(
                "Starting sync-diff for volume {} (size: {} bytes, {} chunks of {} MiB)",
                volume_id, total_bytes, total_chunks, chunk_size_mib
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

            // Replay previous diffs to determine previous state
            let mut prior_diffs = Vec::new();
            for entry in &meta.snapshots {
                let diff_bytes = swift.get_object(&container, &entry.diff_file).await?;
                let diff = SnapshotDiff::deserialize(entry.snapshot_id.clone(), &diff_bytes)?;
                prior_diffs.push(diff);
            }
            let prior_map = manifest::replay_block_map(total_chunks, &prior_diffs);

            let mut new_diff = SnapshotDiff::new(snapshot_id.clone());
            let mut chunks_to_upload: Vec<(u32, [u8; 32], Vec<u8>)> = Vec::new();

            let mut buf = vec![0u8; chunk_size];
            for chunk_idx in 0..total_chunks {
                let offset = chunk_idx as u64 * chunk_size as u64;
                let bytes_to_read = std::cmp::min(chunk_size as u64, total_bytes - offset) as usize;
                let slice = &mut buf[..bytes_to_read];
                block_io::read_chunk_at(&file, offset, slice)?;

                let chunk_hash = hash::compute_chunk_hash(slice);
                let prior_hash = prior_map.get(&chunk_idx);

                if prior_hash != Some(&chunk_hash) {
                    new_diff.add_change(chunk_idx, chunk_hash);
                    if !block_io::is_all_zeros(slice) {
                        chunks_to_upload.push((chunk_idx, chunk_hash, slice.to_vec()));
                    }
                }
            }

            info!(
                "Detected {} changed chunks ({} to upload to Swift)",
                new_diff.changes.len(),
                chunks_to_upload.len()
            );

            // Parallel upload of changed chunks to Swift
            let upload_stream = stream::iter(chunks_to_upload).map(|(_idx, h, data)| {
                let swift = Arc::clone(&swift);
                let container = container.clone();
                async move {
                    let hex_name = format!("chunks/{}", hash::hash_to_hex(&h));
                    if !swift.head_object(&container, &hex_name).await.unwrap_or(false) {
                        swift.put_object(&container, &hex_name, data).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                }
            });

            upload_stream
                .buffer_unordered(concurrency)
                .collect::<Vec<Result<()>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<()>>>()?;

            // Serialize & upload snapshot diff file
            let diff_filename = format!("manifest/diffs/{}.diff", snapshot_id);
            let diff_bytes = new_diff.serialize()?;
            swift.put_object(&container, &diff_filename, diff_bytes).await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;

            meta.snapshots.push(SnapshotEntry {
                snapshot_id: snapshot_id.clone(),
                created_at: now,
                diff_file: diff_filename,
                changed_chunks: new_diff.changes.len() as u32,
            });

            let meta_bytes = serde_json::to_vec_pretty(&meta)?;
            swift.put_object(&container, meta_obj, meta_bytes).await?;

            println!(
                "{{\"status\":\"ok\",\"volume_id\":\"{}\",\"snapshot_id\":\"{}\",\"changed_chunks\":{}}}",
                volume_id, snapshot_id, new_diff.changes.len()
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
        } => {
            let container = format!("{}{}", container_prefix, volume_id);
            let swift = Arc::new(SwiftClient::new(swift_url, swift_token));

            let meta_bytes = swift.get_object(&container, "manifest/meta.json").await
                .context("Failed to fetch manifest/meta.json from Swift")?;
            let meta: VolumeManifestMeta = serde_json::from_slice(&meta_bytes)?;

            // Find target snapshot and replay diff chain
            let mut diffs_up_to_target = Vec::new();
            let mut found = false;
            for entry in &meta.snapshots {
                let diff_bytes = swift.get_object(&container, &entry.diff_file).await?;
                let diff = SnapshotDiff::deserialize(entry.snapshot_id.clone(), &diff_bytes)?;
                diffs_up_to_target.push(diff);
                if entry.snapshot_id == snapshot_id {
                    found = true;
                    break;
                }
            }

            if !found {
                anyhow::bail!("Snapshot {} not found in manifest", snapshot_id);
            }

            let chunk_size = meta.chunk_size_bytes as usize;
            let total_chunks = ((meta.volume_size_bytes + chunk_size as u64 - 1) / chunk_size as u64) as u32;
            let block_map = manifest::replay_block_map(total_chunks, &diffs_up_to_target);

            info!(
                "Replaying snapshot {} ({} non-empty blocks out of {} total chunks)",
                snapshot_id, block_map.len(), total_chunks
            );

            let dest_file = OpenOptions::new()
                .write(true)
                .open(&dest_dev)
                .with_context(|| format!("Failed to open destination device {}", dest_dev))?;
            let dest_file = Arc::new(dest_file);

            let restore_items: Vec<(u32, [u8; 32])> = block_map.into_iter().collect();
            let restore_count = restore_items.len();

            let restore_stream = stream::iter(restore_items).map(|(idx, h)| {
                let swift = Arc::clone(&swift);
                let container = container.clone();
                let dest_file = Arc::clone(&dest_file);
                async move {
                    let hex_name = format!("chunks/{}", hash::hash_to_hex(&h));
                    let data = swift.get_object(&container, &hex_name).await?;
                    let offset = idx as u64 * chunk_size as u64;
                    block_io::write_chunk_at(&dest_file, offset, &data)?;
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
                volume_id, snapshot_id, restore_count
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
    }

    Ok(())
}
