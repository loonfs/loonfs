//! Read handles: one per open descriptor of a file, pinned to the revision
//! it opened.

use crate::cache_file::CacheFile;
use crate::control::{ControlMessage, ControlSender};
use crate::error::{MountError, Result};
use crate::item::ItemId;
use crate::window::{FetchSizes, ReadWindow};
use bytes::Bytes;
use loonfs_types::{ContentRef, InodeId, RevisionNo};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// The identity of one open read handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HandleId(pub(crate) u64);

/// The revision that a read handle reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PinnedRevision {
    pub(crate) inode_id: InodeId,
    pub(crate) revision_no: RevisionNo,
    pub(crate) content_ref: ContentRef,
}

/// What a read handle reads, which the control task changes when an append
/// extends the revision or the cache replaces the file.
#[derive(Debug)]
pub(crate) struct HandleView {
    pub(crate) revision: PinnedRevision,
    /// The cache file that stores the revision's bytes, once there is one.
    pub(crate) file: Option<Arc<CacheFile>>,
}

/// Where a handle's revision is on the host, as "Content states" in the
/// design note describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentState {
    /// The bytes are on the server only.
    Absent,
    /// Some of the bytes are in a partial cache file.
    Partial,
    /// A cache file has every byte of the revision.
    Complete,
}

/// The parts of a new handle that the control task returns.
#[derive(Debug)]
pub(crate) struct OpenedRead {
    pub(crate) handle: HandleId,
    pub(crate) item: ItemId,
    pub(crate) view: Arc<Mutex<HandleView>>,
    pub(crate) sizes: FetchSizes,
}

/// One open descriptor of a file, which reads the revision that was current
/// when it opened, and follows the file as appends extend that revision.
///
/// Dropping the handle closes it.
#[derive(Debug)]
pub struct ReadHandle {
    handle: HandleId,
    item: ItemId,
    view: Arc<Mutex<HandleView>>,
    window: Mutex<ReadWindow>,
    control: ControlSender,
}

impl ReadHandle {
    pub(crate) fn new(opened: OpenedRead, control: ControlSender) -> Self {
        Self {
            handle: opened.handle,
            item: opened.item,
            view: opened.view,
            window: Mutex::new(ReadWindow::new(opened.sizes)),
            control,
        }
    }

    fn view(&self) -> std::sync::MutexGuard<'_, HandleView> {
        self.view
            .lock()
            .expect("a read handle's view lock should not be poisoned")
    }

    /// Returns the item that the handle reads.
    pub fn item(&self) -> ItemId {
        self.item
    }

    /// Returns the size of the revision that the handle reads.
    pub fn size_bytes(&self) -> u64 {
        self.view().revision.content_ref.size_bytes
    }

    /// Returns where the handle's revision is on the host. The adapter sets
    /// the kernel's keep-cache flag on an open of a complete file.
    pub fn content_state(&self) -> ContentState {
        let view = self.view();
        let size_bytes = view.revision.content_ref.size_bytes;
        match &view.file {
            _ if size_bytes == 0 => ContentState::Complete,
            Some(file) if file.present_prefix() >= size_bytes => ContentState::Complete,
            Some(_) => ContentState::Partial,
            None => ContentState::Absent,
        }
    }

    /// Reads up to `length` bytes from `offset`, and returns fewer only at the
    /// end of the revision.
    ///
    /// A read of bytes that the cache has returns at once. Otherwise it
    /// waits for the fetch of the window that contains it.
    pub async fn read(&self, offset: u64, length: u64) -> Result<Bytes> {
        let (size_bytes, file) = {
            let view = self.view();
            (view.revision.content_ref.size_bytes, view.file.clone())
        };
        let end = offset.saturating_add(length).min(size_bytes);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let needed = offset..end;
        let plan = self
            .window
            .lock()
            .expect("a read handle's window lock should not be poisoned")
            .plan(needed.clone(), size_bytes);
        if let Some(ahead) = plan.ahead {
            self.send(ControlMessage::ReadAhead {
                handle: self.handle,
                range: ahead,
            })?;
        }
        if let Some(file) = file.filter(|file| file.present_prefix() >= end) {
            return read_cache_file(&file, needed);
        }
        let (reply, replied) = oneshot::channel();
        self.send(ControlMessage::Read {
            handle: self.handle,
            demand: plan.demand,
            needed: needed.clone(),
            reply,
        })?;
        let file = replied.await.map_err(|_| MountError::Stopped)??;
        read_cache_file(&file, needed)
    }

    fn send(&self, message: ControlMessage) -> Result<()> {
        self.control.send(message).map_err(|_| MountError::Stopped)
    }
}

fn read_cache_file(file: &CacheFile, range: Range<u64>) -> Result<Bytes> {
    file.read(range).map_err(|error| MountError::Io {
        path: "cache".to_owned(),
        message: error.to_string(),
    })
}

impl Drop for ReadHandle {
    fn drop(&mut self) {
        // A stopped engine has nothing to close.
        let _ = self.control.send(ControlMessage::CloseRead {
            handle: self.handle,
        });
    }
}
