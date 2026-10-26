//! The files that the kernel has open, by the file handle that `open`
//! returned.

use fuser::FileHandle;
use loonfs_mount::ReadHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One open file.
#[derive(Debug, Clone)]
pub(crate) enum OpenFile {
    /// A file of the engine, read through the engine's handle. A read in
    /// flight keeps the handle open after `release`.
    Content(Arc<ReadHandle>),
    /// A file of the control directory, with the bytes it had when it was
    /// opened.
    Control(Arc<[u8]>),
}

/// The open files of one mount.
///
/// The kernel sends one `release` for each `open`, so the table has one
/// entry for each file that the kernel has open, which the kernel's limit on
/// open files bounds.
#[derive(Debug, Default)]
pub(crate) struct OpenFiles {
    next_handle: AtomicU64,
    files: Mutex<HashMap<u64, OpenFile>>,
}

impl OpenFiles {
    fn files(&self) -> std::sync::MutexGuard<'_, HashMap<u64, OpenFile>> {
        self.files
            .lock()
            .expect("the open file lock should not be poisoned")
    }

    /// Adds an open file, and returns its handle.
    pub(crate) fn insert(&self, file: OpenFile) -> FileHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.files().insert(handle, file);
        FileHandle(handle)
    }

    /// Returns the open file with `handle`.
    pub(crate) fn find(&self, handle: FileHandle) -> Option<OpenFile> {
        self.files().get(&handle.0).cloned()
    }

    /// Removes the open file with `handle`.
    pub(crate) fn remove(&self, handle: FileHandle) -> Option<OpenFile> {
        self.files().remove(&handle.0)
    }
}

/// Returns the bytes of a control file that a read of `length` bytes at
/// `offset` returns.
pub(crate) fn control_read(bytes: &[u8], offset: u64, length: u32) -> &[u8] {
    let size = bytes.len() as u64;
    let start = offset.min(size);
    let end = offset.saturating_add(u64::from(length)).min(size);
    let start = usize::try_from(start).expect("an offset within a control file fits in usize");
    let end = usize::try_from(end).expect("an offset within a control file fits in usize");
    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that a read of a control file returns the bytes in range, and
    /// nothing at or past the end.
    #[test]
    fn a_control_read_returns_the_bytes_in_range() {
        let bytes = b"31\n";
        assert_eq!(control_read(bytes, 0, 4096), b"31\n");
        assert_eq!(control_read(bytes, 1, 1), b"1");
        assert_eq!(control_read(bytes, 3, 10), b"");
        assert_eq!(control_read(bytes, u64::MAX, u32::MAX), b"");
    }

    /// Checks that each open gets its own handle, and that a release removes
    /// only its own file.
    #[test]
    fn each_open_gets_its_own_handle() {
        let files = OpenFiles::default();
        let first = files.insert(OpenFile::Control(Arc::from(&b"a"[..])));
        let second = files.insert(OpenFile::Control(Arc::from(&b"b"[..])));
        assert_ne!(first.0, second.0);
        assert!(files.remove(first).is_some());
        assert!(files.find(first).is_none());
        assert!(matches!(
            files.find(second),
            Some(OpenFile::Control(bytes)) if &bytes[..] == b"b"
        ));
    }
}
