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

const DIFF_MAGIC: &[u8; 4] = b"RXTD";
const DIFF_VERSION: u16 = 1;

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
    pub diff_file: String,
    pub changed_chunks: u32,
}

/// A compact binary delta representing changed chunks for a single snapshot.
#[derive(Debug, Clone, Default)]
pub struct SnapshotDiff {
    #[allow(dead_code)]
    pub snapshot_id: String,
    pub changes: Vec<(u32, [u8; 32])>, // (block_index, sha256_hash)
}

impl SnapshotDiff {
    pub fn new(snapshot_id: String) -> Self {
        Self {
            snapshot_id,
            changes: Vec::new(),
        }
    }

    pub fn add_change(&mut self, block_index: u32, hash: [u8; 32]) {
        self.changes.push((block_index, hash));
    }

    /// Serialize to compact binary:
    /// [4 bytes Magic][2 bytes Version][4 bytes Count]
    /// Followed by N records of: [4 bytes BlockIdx][32 bytes Hash]
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(10 + self.changes.len() * 36);
        buf.write_all(DIFF_MAGIC)?;
        buf.write_u16::<BigEndian>(DIFF_VERSION)?;
        buf.write_u32::<BigEndian>(self.changes.len() as u32)?;

        for (idx, hash) in &self.changes {
            buf.write_u32::<BigEndian>(*idx)?;
            buf.write_all(hash)?;
        }
        Ok(buf)
    }

    /// Parse from compact binary.
    pub fn deserialize(snapshot_id: String, bytes: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(bytes);
        let mut magic = [0u8; 4];
        rdr.read_exact(&mut magic)?;
        if &magic != DIFF_MAGIC {
            bail!("Invalid diff magic header: {:?}", magic);
        }

        let version = rdr.read_u16::<BigEndian>()?;
        if version != DIFF_VERSION {
            bail!("Unsupported diff version: {}", version);
        }

        let count = rdr.read_u32::<BigEndian>()? as usize;
        let mut changes = Vec::with_capacity(count);

        for _ in 0..count {
            let idx = rdr.read_u32::<BigEndian>()?;
            let mut hash = [0u8; 32];
            rdr.read_exact(&mut hash)?;
            changes.push((idx, hash));
        }

        Ok(Self {
            snapshot_id,
            changes,
        })
    }
}

/// Resolves a full block map by replaying a sequence of diffs up to the target snapshot.
pub fn replay_block_map(
    total_blocks: u32,
    diffs_in_order: &[SnapshotDiff],
) -> HashMap<u32, [u8; 32]> {
    let mut map = HashMap::with_capacity(total_blocks as usize);
    for diff in diffs_in_order {
        for (idx, hash) in &diff.changes {
            map.insert(*idx, *hash);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_serialization_roundtrip() {
        let mut diff = SnapshotDiff::new("snap-test-1".to_string());
        diff.add_change(0, [1u8; 32]);
        diff.add_change(10, [2u8; 32]);
        diff.add_change(250000, [3u8; 32]);

        let bytes = diff.serialize().expect("serialize failed");
        // Header (10 bytes) + 3 * 36 bytes = 118 bytes
        assert_eq!(118, bytes.len());

        let decoded = SnapshotDiff::deserialize("snap-test-1".to_string(), &bytes)
            .expect("deserialize failed");
        assert_eq!(3, decoded.changes.len());
        assert_eq!((0, [1u8; 32]), decoded.changes[0]);
        assert_eq!((10, [2u8; 32]), decoded.changes[1]);
        assert_eq!((250000, [3u8; 32]), decoded.changes[2]);
    }

    #[test]
    fn test_replay_block_map() {
        let mut diff1 = SnapshotDiff::new("snap-1".to_string());
        diff1.add_change(0, [1u8; 32]);
        diff1.add_change(1, [1u8; 32]);

        let mut diff2 = SnapshotDiff::new("snap-2".to_string());
        diff2.add_change(1, [2u8; 32]); // block 1 modified
        diff2.add_change(2, [3u8; 32]); // block 2 added

        let map = replay_block_map(3, &[diff1, diff2]);
        assert_eq!(Some(&[1u8; 32]), map.get(&0));
        assert_eq!(Some(&[2u8; 32]), map.get(&1));
        assert_eq!(Some(&[3u8; 32]), map.get(&2));
    }
}
