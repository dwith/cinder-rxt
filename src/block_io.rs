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
use std::io::{Seek, SeekFrom};
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
pub fn read_chunk_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
    file.read_exact_at(buf, offset)
        .with_context(|| format!("Failed to read chunk at offset {}", offset))
}

/// Writes a chunk to a file / block device at the specified offset.
pub fn write_chunk_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    file.write_all_at(data, offset)
        .with_context(|| format!("Failed to write chunk at offset {}", offset))
}

/// Determines the exact size in bytes of a regular file or block device.
pub fn get_device_size(file: &mut File) -> Result<u64> {
    let metadata = file
        .metadata()
        .context("Failed to query file metadata")?;

    // If it's a regular file and has non-zero size, return it
    if metadata.file_type().is_file() && metadata.len() > 0 {
        return Ok(metadata.len());
    }

    // For block devices or character devices (where st_size is 0), seek to end
    let end_offset = file
        .seek(SeekFrom::End(0))
        .context("Failed to seek to end of device to determine size")?;
    file.seek(SeekFrom::Start(0))
        .context("Failed to reset device seek position")?;

    if end_offset > 0 {
        return Ok(end_offset);
    }

    // Fallback for Linux block devices via ioctl BLKGETSIZE64
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let mut size: u64 = 0;
        let ret = unsafe { libc::ioctl(fd, 0x80081272, &mut size) };
        if ret == 0 && size > 0 {
            return Ok(size);
        }
    }

    Ok(metadata.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_is_all_zeros() {
        let zeros = vec![0u8; 4096];
        assert!(is_all_zeros(&zeros));

        let mut dirty = vec![0u8; 4096];
        dirty[2048] = 1;
        assert!(!is_all_zeros(&dirty));
    }

    #[test]
    fn test_get_device_size_regular_file() {
        let test_path = std::env::temp_dir().join("cinder_rxt_size_test.bin");
        {
            let mut f = File::create(&test_path).unwrap();
            f.write_all(&[42u8; 2048]).unwrap();
        }
        let mut f = File::open(&test_path).unwrap();
        let sz = get_device_size(&mut f).unwrap();
        let _ = std::fs::remove_file(&test_path);
        assert_eq!(sz, 2048);
    }

    #[test]
    fn test_concurrent_read_chunk_at() {
        use std::sync::Arc;
        use std::thread;

        let test_path = std::env::temp_dir().join("cinder_rxt_concurrent_read_test.bin");
        {
            let mut f = File::create(&test_path).unwrap();
            let mut data = Vec::with_capacity(4096 * 4);
            for i in 0..4 {
                data.resize(data.len() + 4096, i as u8);
            }
            f.write_all(&data).unwrap();
        }

        let f = Arc::new(File::open(&test_path).unwrap());
        let mut handles = Vec::new();
        for i in 0..4 {
            let f = Arc::clone(&f);
            handles.push(thread::spawn(move || {
                let mut buf = vec![0u8; 4096];
                read_chunk_at(&f, i as u64 * 4096, &mut buf).unwrap();
                assert!(buf.iter().all(|&b| b == i as u8));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let _ = std::fs::remove_file(&test_path);
    }
}

