//! [`MountFilesystem`], which serves the kernel's requests for one mount from
//! its engine.
//!
//! Metadata requests read the engine's in-memory tree and reply at once. An
//! open and a read wait for the engine, so they run on the daemon's runtime
//! and reply from there, and the session's thread moves on to the next
//! request. The `Filesystem` implementation is in `requests`.

mod requests;

use crate::attributes::{cache_lifetime, file_attr, file_type, Owner, BLOCK_BYTES, GENERATION};
use crate::control_directory::ControlItem;
use crate::directory::{list, listed_kind, Listed};
use crate::errors::{errno, Refusal};
use crate::open_files::{control_read, OpenFile, OpenFiles};
use fuser::{
    Errno, FileAttr, FileHandle, FopenFlags, INodeNo, OpenAccMode, OpenFlags, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyOpen, ReplyStatfs,
};
use loonfs_mount::{Attr, ContentState, Engine, ItemId, LocalChanges, MountAccess};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;

/// The longest name that the mount accepts, as LoonFS limits a display name.
const MAX_NAME_BYTES: u32 = 255;

/// The kernel's view of one mount.
pub(crate) struct MountFilesystem {
    engine: Arc<Engine>,
    owner: Owner,
    access: MountAccess,
    runtime: tokio::runtime::Handle,
    open_files: Arc<OpenFiles>,
}

impl MountFilesystem {
    pub(crate) fn new(engine: Arc<Engine>, runtime: tokio::runtime::Handle) -> Self {
        let config = engine.config();
        Self {
            owner: Owner::of(config),
            access: config.access,
            engine,
            runtime,
            open_files: Arc::default(),
        }
    }

    /// Returns an item's attributes and how long the kernel caches them.
    fn attributes(&self, item: ItemId) -> Option<(FileAttr, Duration)> {
        let (attr, lifetime) = match ControlItem::of(item) {
            Some(control) => (self.control_attr(control), Duration::ZERO),
            None => {
                let attr = self.engine.getattr(item)?;
                let lifetime = cache_lifetime(attr.changes);
                (attr, lifetime)
            }
        };
        Some((file_attr(item, &attr, self.owner), lifetime))
    }

    fn control_attr(&self, control: ControlItem) -> Attr {
        let root_mtime_ms = self
            .engine
            .getattr(ItemId::ROOT)
            .map_or(0, |root| root.mtime_ms);
        control.attr(&self.engine.status(), root_mtime_ms)
    }

    /// Looks up `name` in `parent`, and counts a kernel lookup of an engine
    /// item, which `forget` subtracts.
    fn lookup_item(&self, parent: ItemId, name: &OsStr) -> Option<(FileAttr, Duration)> {
        if let Some(control) = ControlItem::lookup(parent, name) {
            return self.attributes(control.item());
        }
        if ControlItem::of(parent).is_some() {
            return None;
        }
        let entry = self.engine.lookup(parent, name)?;
        Some((
            file_attr(entry.item, &entry.attr, self.owner),
            cache_lifetime(entry.attr.changes),
        ))
    }

    /// Subtracts the kernel's lookups of an item. The control directory's
    /// items need no count, because they never leave the mount.
    fn forget_item(&self, item: ItemId, lookups: u64) {
        if ControlItem::of(item).is_none() {
            self.engine.forget(item, lookups);
        }
    }

    fn read_directory(&self, directory: ItemId, offset: u64, mut reply: ReplyDirectory) {
        for entry in list(&self.engine, directory, offset) {
            let (item, name) = match &entry {
                Listed::Dot { name, .. } => (directory, OsStr::new(name)),
                Listed::Control { control, .. } => (control.item(), OsStr::new(control.name())),
                Listed::Engine(entry) => (entry.item, entry.name.as_os_str()),
            };
            let kind = file_type(listed_kind(&entry));
            if reply.add(INodeNo(item.0), entry.offset(), kind, name) {
                break;
            }
        }
        reply.ok();
    }

    /// Lists a directory with each entry's attributes. Unlike `readdir`, the
    /// kernel counts a lookup of every entry but `.` and `..`, so the
    /// listing looks each engine entry up.
    fn read_directory_plus(&self, directory: ItemId, offset: u64, mut reply: ReplyDirectoryPlus) {
        for entry in list(&self.engine, directory, offset) {
            if self.add_plus(directory, &entry, &mut reply) {
                break;
            }
        }
        reply.ok();
    }

    /// Adds one entry to a readdirplus reply, and returns whether the reply
    /// is full. A full reply leaves the entry out, so its lookup is undone.
    fn add_plus(&self, directory: ItemId, entry: &Listed, reply: &mut ReplyDirectoryPlus) -> bool {
        let (name, attributes) = match entry {
            Listed::Dot { name, .. } => (OsStr::new(name), self.attributes(directory)),
            Listed::Control { control, .. } => {
                (OsStr::new(control.name()), self.attributes(control.item()))
            }
            Listed::Engine(entry) => (
                entry.name.as_os_str(),
                self.lookup_item(directory, &entry.name),
            ),
        };
        let Some((attr, lifetime)) = attributes else {
            // The entry left the directory after the listing read it.
            return false;
        };
        let full = reply.add(attr.ino, entry.offset(), name, &lifetime, &attr, GENERATION);
        if full && matches!(entry, Listed::Engine(_)) {
            self.forget_item(ItemId(attr.ino.0), 1);
        }
        full
    }

    fn open_file(&self, item: ItemId, flags: OpenFlags, reply: ReplyOpen) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Refusal::Change.errno());
            return;
        }
        if let Some(control) = ControlItem::of(item) {
            self.open_control(control, reply);
            return;
        }
        let engine = self.engine.clone();
        let open_files = self.open_files.clone();
        self.runtime.spawn(async move {
            match engine.open_read(item).await {
                Ok(handle) => {
                    let clean = engine
                        .getattr(item)
                        .is_some_and(|attr| attr.changes == LocalChanges::None);
                    // The kernel's cached pages of a clean, complete file are
                    // the revision's bytes, and the engine drops them when
                    // the feed changes the file.
                    let flags = if clean && handle.content_state() == ContentState::Complete {
                        FopenFlags::FOPEN_KEEP_CACHE
                    } else {
                        FopenFlags::empty()
                    };
                    let handle = open_files.insert(OpenFile::Content(Arc::new(handle)));
                    reply.opened(handle, flags);
                }
                Err(error) => reply.error(errno(&error)),
            }
        });
    }

    fn open_control(&self, control: ControlItem, reply: ReplyOpen) {
        match control.contents(&self.engine.status()) {
            Some(bytes) => {
                let handle = self.open_files.insert(OpenFile::Control(bytes.into()));
                // Direct I/O makes every read ask the adapter, so a read
                // returns the snapshot that the open took, whatever size the
                // kernel cached.
                reply.opened(handle, FopenFlags::FOPEN_DIRECT_IO);
            }
            None => reply.error(Errno::EISDIR),
        }
    }

    fn read_file(&self, handle: FileHandle, offset: u64, length: u32, reply: ReplyData) {
        match self.open_files.find(handle) {
            Some(OpenFile::Control(bytes)) => reply.data(control_read(&bytes, offset, length)),
            Some(OpenFile::Content(handle)) => {
                self.runtime.spawn(async move {
                    match handle.read(offset, u64::from(length)).await {
                        Ok(bytes) => reply.data(&bytes),
                        Err(error) => reply.error(errno(&error)),
                    }
                });
            }
            None => reply.error(Errno::EBADF),
        }
    }

    /// Replies to `fsync` of a file or a directory.
    fn sync(&self, reply: ReplyEmpty) {
        match self.access {
            // A read-only mount has nothing to publish.
            MountAccess::ReadOnly => reply.ok(),
            MountAccess::Writable => reply.error(Refusal::Change.errno()),
        }
    }

    /// Reports the cache's limit as the filesystem's size, its unused part
    /// as the free space, and the entry budget as the number of inodes.
    fn report_statfs(&self, reply: ReplyStatfs) {
        let status = self.engine.status();
        let block_bytes = u64::from(BLOCK_BYTES);
        let blocks = status.cache.max_cache_bytes / block_bytes;
        let used_blocks =
            (status.cache.evictable_bytes + status.cache.pinned_bytes).div_ceil(block_bytes);
        let free_blocks = blocks.saturating_sub(used_blocks);
        reply.statfs(
            blocks,
            free_blocks,
            free_blocks,
            status.max_entries,
            status.max_entries.saturating_sub(status.entries),
            BLOCK_BYTES,
            MAX_NAME_BYTES,
            BLOCK_BYTES,
        );
    }
}
