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

use anyhow::{Context, Result};
use std::fs::File;
use std::os::unix::fs::FileExt;

#[allow(dead_code)]
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

/// Fast check if a buffer contains exclusively zero bytes.
pub fn is_all_zeros(buffer: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { buffer.align_to::<u128>() };
    prefix.iter().all(|&b| b == 0)
        && chunks.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

/// Reads a chunk from a file / block device at the specified offset.
pub fn read_chunk_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<usize> {
    file.read_at(buf, offset)
        .with_context(|| format!("Failed to read chunk at offset {}", offset))
}

/// Writes a chunk to a file / block device at the specified offset.
pub fn write_chunk_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    file.write_all_at(data, offset)
        .with_context(|| format!("Failed to write chunk at offset {}", offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_all_zeros() {
        let zeros = vec![0u8; 4096];
        assert!(is_all_zeros(&zeros));

        let mut dirty = vec![0u8; 4096];
        dirty[2048] = 1;
        assert!(!is_all_zeros(&dirty));
    }
}
