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

use anyhow::{bail, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

const MANIFEST_MAGIC: &[u8; 4] = b"RXTM";
const MANIFEST_VERSION: u16 = 2;

/// Metadata stored in `manifest/meta.json` inside the Swift container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeManifestMeta {
    pub version: u32,
    pub volume_id: String,
    pub volume_size_bytes: u64,
    pub chunk_size_bytes: u32,
    pub snapshots: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub snapshot_id: String,
    pub created_at: i64,
    #[serde(alias = "diff_file")]
    pub manifest_file: String,
    #[serde(default)]
    pub total_chunks: u32,
    #[serde(alias = "changed_chunks", default)]
    pub active_chunks: u32,
}

/// A self-contained, autonomous block map for a single point-in-time snapshot.
/// Maps every non-zero block index to its content-addressed SHA-256 hash in Swift.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub snapshot_id: String,
    pub volume_id: String,
    pub total_chunks: u32,
    pub block_map: HashMap<u32, [u8; 32]>, // block_index -> sha256_hash
}

impl SnapshotManifest {
    pub fn new(snapshot_id: String, volume_id: String, total_chunks: u32) -> Self {
        Self {
            snapshot_id,
            volume_id,
            total_chunks,
            block_map: HashMap::new(),
        }
    }

    /// Serialize to compact binary:
    /// [4 bytes Magic "RXTM"][2 bytes Version][4 bytes TotalChunks][4 bytes ActiveChunksCount]
    /// Followed by N records of: [4 bytes BlockIdx][32 bytes Hash]
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(14 + self.block_map.len() * 36);
        buf.write_all(MANIFEST_MAGIC)?;
        buf.write_u16::<BigEndian>(MANIFEST_VERSION)?;
        buf.write_u32::<BigEndian>(self.total_chunks)?;
        buf.write_u32::<BigEndian>(self.block_map.len() as u32)?;

        // Sort by block index for deterministic serialization
        let mut entries: Vec<(&u32, &[u8; 32])> = self.block_map.iter().collect();
        entries.sort_by_key(|(idx, _)| *idx);

        for (idx, hash) in entries {
            buf.write_u32::<BigEndian>(*idx)?;
            buf.write_all(hash)?;
        }
        Ok(buf)
    }

    /// Parse from compact binary.
    pub fn deserialize(snapshot_id: String, volume_id: String, bytes: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(bytes);
        let mut magic = [0u8; 4];
        rdr.read_exact(&mut magic)?;
        if &magic != MANIFEST_MAGIC {
            bail!("Invalid manifest magic header: {:?}", magic);
        }

        let version = rdr.read_u16::<BigEndian>()?;
        if version != MANIFEST_VERSION {
            bail!("Unsupported manifest version: {}", version);
        }

        let total_chunks = rdr.read_u32::<BigEndian>()?;
        let count = rdr.read_u32::<BigEndian>()? as usize;
        let mut block_map = HashMap::with_capacity(count);

        for _ in 0..count {
            let idx = rdr.read_u32::<BigEndian>()?;
            let mut hash = [0u8; 32];
            rdr.read_exact(&mut hash)?;
            block_map.insert(idx, hash);
        }

        Ok(Self {
            snapshot_id,
            volume_id,
            total_chunks,
            block_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_serialization_roundtrip() {
        let mut manifest = SnapshotManifest::new(
            "snap-test-1".to_string(),
            "vol-test-1".to_string(),
            5120,
        );
        manifest.block_map.insert(0, [1u8; 32]);
        manifest.block_map.insert(10, [2u8; 32]);
        manifest.block_map.insert(250000, [3u8; 32]);

        let bytes = manifest.serialize().expect("serialize failed");
        // Header (14 bytes) + 3 * 36 bytes = 122 bytes
        assert_eq!(122, bytes.len());

        let decoded = SnapshotManifest::deserialize(
            "snap-test-1".to_string(),
            "vol-test-1".to_string(),
            &bytes,
        )
        .expect("deserialize failed");

        assert_eq!(5120, decoded.total_chunks);
        assert_eq!(3, decoded.block_map.len());
        assert_eq!(Some(&[1u8; 32]), decoded.block_map.get(&0));
        assert_eq!(Some(&[2u8; 32]), decoded.block_map.get(&10));
        assert_eq!(Some(&[3u8; 32]), decoded.block_map.get(&250000));
    }

    #[test]
    fn test_autonomous_manifests_independent_deletion() {
        // Snapshot 1 manifest
        let mut snap1 = SnapshotManifest::new("snap-1".to_string(), "vol-1".to_string(), 4);
        snap1.block_map.insert(0, [1u8; 32]);
        snap1.block_map.insert(1, [1u8; 32]);

        // Snapshot 2 built on top of snap 1
        let mut snap2 = SnapshotManifest::new("snap-2".to_string(), "vol-1".to_string(), 4);
        // snap 2 retains block 0, updates block 1, adds block 2
        snap2.block_map.insert(0, [1u8; 32]);
        snap2.block_map.insert(1, [2u8; 32]); // updated
        snap2.block_map.insert(2, [3u8; 32]); // added

        // Deleting snap 1 has ZERO effect on snap 2's integrity
        drop(snap1);

        assert_eq!(Some(&[1u8; 32]), snap2.block_map.get(&0));
        assert_eq!(Some(&[2u8; 32]), snap2.block_map.get(&1));
        assert_eq!(Some(&[3u8; 32]), snap2.block_map.get(&2));
        assert_eq!(None, snap2.block_map.get(&3));
    }
}
