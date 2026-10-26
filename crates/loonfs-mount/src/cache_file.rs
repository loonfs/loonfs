//! One open cache file, which read handles read and fetches write without
//! going through the control task.

use crate::error::{MountError, Result};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// One open cache file, which read handles read and fetches write.
#[derive(Debug)]
pub(crate) struct CacheFile {
    file: File,
    /// The bytes below this offset are present and don't change, so a read
    /// handle reads them without asking the control task. The control task
    /// sets it after the fetch that wrote the bytes reported them.
    present_prefix: AtomicU64,
}

impl CacheFile {
    pub(crate) fn new(file: File, present_prefix: u64) -> Arc<Self> {
        Arc::new(Self {
            file,
            present_prefix: AtomicU64::new(present_prefix),
        })
    }

    /// Returns the end of the bytes that are present from offset zero.
    pub(crate) fn present_prefix(&self) -> u64 {
        // Acquire pairs with the release in `set_present_prefix`, so the
        // bytes that a fetch wrote before the control task published the
        // prefix are visible to the reader.
        self.present_prefix.load(Ordering::Acquire)
    }

    pub(crate) fn set_present_prefix(&self, end: u64) {
        self.present_prefix.store(end, Ordering::Release);
    }

    /// Reads the bytes of `range`, which have to be present.
    pub(crate) fn read(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        let length =
            usize::try_from(range.end - range.start).expect("a read's length should fit in memory");
        let mut buffer = vec![0; length];
        self.file.read_exact_at(&mut buffer, range.start)?;
        Ok(Bytes::from(buffer))
    }

    /// Writes fetched bytes at `offset`.
    pub(crate) fn write(&self, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all_at(bytes, offset)
    }

    /// Returns the open file, for verification.
    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}

/// Creates an empty file of `size_bytes` that occupies no blocks until
/// fetches write it.
pub(crate) fn create_sparse(path: &Path, size_bytes: u64) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|error| MountError::io(path, &error))?;
    file.set_len(size_bytes)
        .map_err(|error| MountError::io(path, &error))?;
    Ok(file)
}
