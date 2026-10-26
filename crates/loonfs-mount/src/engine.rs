//! [`Engine`], the interface of one mount's engine, which the FUSE adapter
//! and the daemon use.

use crate::admission::admit;
use crate::clock;
use crate::config::MountConfig;
use crate::control::{ControlMessage, ControlSender, ControlTask, Environment, SharedState};
use crate::database::{self, Database};
use crate::error::{MountError, Result};
use crate::host::{check_host, DiskSpace, HostDiskSpace};
use crate::invalidation::Invalidation;
use crate::item::{Attr, DirEntry, Entry, ItemId};
use crate::read_handle::ReadHandle;
use crate::status::{MountMethod, Status};
use crate::tree::Tree;
use crate::workers::ClientWorkers;
use crate::write::{Mkdir, OpenWrite, Rename, SetAttr, Symlink, WriteHandle};
use loonfs_client::Client;
use loonfs_types::{ChangeSeq, MonotonicTimer, StdMonotonicTimer};
use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};

/// The entries that one `readdir` batch reads under the tree's read lock.
const READ_DIRECTORY_BATCH_ENTRIES: usize = 256;

/// One mount's engine: its trees, its content cache, and its position in the
/// namespace's change feed.
///
/// Metadata reads (`lookup`, `getattr`, `readdir`, and `readlink`) read the
/// in-memory tree under a read lock. Everything else goes through the
/// mount's control task. Dropping the engine stops the control task.
pub struct Engine {
    shared: SharedState,
    control: ControlSender,
    mount_method: Mutex<Option<MountMethod>>,
    control_thread: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Engine").finish_non_exhaustive()
    }
}

impl Engine {
    /// Admits a mount and starts following the namespace's change feed, or
    /// resumes the state that a mount of the same namespace and path left in
    /// the state directory.
    ///
    /// A mount that the host or the namespace can't serve fails with the
    /// reason (see "Admitting a mount" in the design note). A resumed mount
    /// keeps the settings it was admitted with. It must be called from a
    /// Tokio runtime, which runs the engine's requests.
    pub async fn mount(client: Client, config: MountConfig) -> Result<Self> {
        let disk: Arc<dyn DiskSpace> = Arc::new(HostDiskSpace);
        let timer: Arc<dyn MonotonicTimer> = Arc::new(StdMonotonicTimer::default());
        let (database, reflinks) = match resume(&config, disk.as_ref())? {
            Some(resumed) => resumed,
            None => {
                let admission = admit(&client, &config, disk.as_ref(), timer.as_ref()).await?;
                (admission.database, admission.reflinks)
            }
        };
        let environment = Environment {
            timer: timer.clone(),
            disk,
            reflinks,
        };
        let (sender, receiver) = mpsc::unbounded_channel();
        let runtime = tokio::runtime::Handle::current();
        let workers = ClientWorkers::new(
            client,
            config.namespace_id.clone(),
            sender.clone(),
            timer,
            runtime.clone(),
        );
        let (task, shared) = ControlTask::load(database, Box::new(workers), environment)?;
        let poll_interval_ms = task.poll_interval_ms();
        let control_thread = std::thread::Builder::new()
            .name("loonfs-mount-control".to_owned())
            .spawn(move || task.run(receiver))
            .map_err(|error| MountError::Io {
                path: config.state_directory.display().to_string(),
                message: format!("starting the control thread failed: {error}"),
            })?;
        runtime.spawn(tick(sender.clone(), shared.clone(), poll_interval_ms));
        Ok(Self {
            shared,
            control: sender,
            mount_method: Mutex::new(None),
            control_thread: Some(control_thread),
        })
    }

    fn tree(&self) -> std::sync::RwLockReadGuard<'_, Tree> {
        self.shared
            .tree
            .read()
            .expect("the tree lock should not be poisoned")
    }

    /// Returns the item that `name` in `parent` refers to, and counts a
    /// kernel lookup of it, which [`Self::forget`] subtracts.
    pub fn lookup(&self, parent: ItemId, name: &OsStr) -> Option<Entry> {
        self.tree().lookup(parent, name)
    }

    /// Returns an item's attributes.
    pub fn getattr(&self, item: ItemId) -> Option<Attr> {
        self.tree().getattr(item)
    }

    /// Lists a directory's entries after `offset`, which is zero for the
    /// first entry or an earlier entry's offset.
    ///
    /// An offset stays valid while other entries are added and removed. The
    /// iterator reads the tree in batches, so the directory can change
    /// between them. A listing doesn't count kernel lookups, so an adapter
    /// that serves readdirplus calls [`Self::lookup`] for each entry it
    /// returns.
    pub fn readdir(&self, item: ItemId, offset: u64) -> impl Iterator<Item = DirEntry> {
        ReadDirectory {
            tree: self.shared.tree.clone(),
            item,
            offset,
            batch: VecDeque::new(),
            done: false,
        }
    }

    /// Returns a symlink's target.
    pub fn readlink(&self, item: ItemId) -> Option<OsString> {
        self.tree().readlink(item)
    }

    /// Opens a file for reading, pinned to the revision that it shows now.
    pub async fn open_read(&self, item: ItemId) -> Result<ReadHandle> {
        let (reply, replied) = oneshot::channel();
        self.send(ControlMessage::OpenRead { item, reply })?;
        let opened = replied.await.map_err(|_| MountError::Stopped)??;
        Ok(ReadHandle::new(opened, self.control.clone()))
    }

    /// Opens a file for writing.
    pub async fn open_write(&self, _request: OpenWrite) -> Result<WriteHandle> {
        Err(read_only("open_write"))
    }

    /// Waits until every write to `items` is published, and returns the
    /// sequence of the last commit that it waited for.
    pub async fn barrier(&self, _items: &[ItemId]) -> Result<ChangeSeq> {
        Err(read_only("barrier"))
    }

    /// Creates a directory.
    pub async fn mkdir(&self, _request: Mkdir) -> Result<Entry> {
        Err(read_only("mkdir"))
    }

    /// Creates a symlink.
    pub async fn symlink(&self, _request: Symlink) -> Result<Entry> {
        Err(read_only("symlink"))
    }

    /// Moves or renames an item.
    pub async fn rename(&self, _request: Rename) -> Result<()> {
        Err(read_only("rename"))
    }

    /// Removes a file's name.
    pub async fn unlink(&self, _parent: ItemId, _name: &OsStr) -> Result<()> {
        Err(read_only("unlink"))
    }

    /// Removes an empty directory.
    pub async fn rmdir(&self, _parent: ItemId, _name: &OsStr) -> Result<()> {
        Err(read_only("rmdir"))
    }

    /// Changes an item's size, mode, or modification time.
    pub async fn setattr(&self, _request: SetAttr) -> Result<Attr> {
        Err(read_only("setattr"))
    }

    /// Subtracts `lookups` from the kernel's lookup count of an item. An item
    /// that left the tree is removed once the count reaches zero and no
    /// handle has it open.
    pub fn forget(&self, item: ItemId, lookups: u64) {
        if self.tree().forget(item, lookups) {
            // A stopped engine has nothing to remove.
            let _ = self.control.send(ControlMessage::Sweep);
        }
    }

    /// Waits until every change in the mount is published, and returns the
    /// sequence of the last commit that it waited for.
    pub async fn sync(&self) -> Result<ChangeSeq> {
        Err(read_only("sync"))
    }

    /// Reads the change feed now, and waits until the mount has applied the
    /// commit at `seq` or a later one.
    pub async fn refresh_at_least(&self, seq: ChangeSeq) -> Result<()> {
        let (reply, replied) = oneshot::channel();
        self.send(ControlMessage::RefreshAtLeast { seq, reply })?;
        replied.await.map_err(|_| MountError::Stopped)?
    }

    /// Returns the changes that the kernel's caches have to drop, sent once
    /// per feed page from now on.
    pub fn subscribe_invalidations(&self) -> mpsc::Receiver<Invalidation> {
        self.shared.subscribers.subscribe()
    }

    /// Returns the mount's state, as `loonfs status` reports it.
    pub fn status(&self) -> Status {
        let mut status = self
            .shared
            .status
            .lock()
            .expect("the status lock should not be poisoned")
            .clone();
        status.mount_method = *self
            .mount_method
            .lock()
            .expect("the mount method lock should not be poisoned");
        status
    }

    /// Records how the adapter mounted the directory, which status reports.
    pub fn record_mount_method(&self, method: MountMethod) {
        *self
            .mount_method
            .lock()
            .expect("the mount method lock should not be poisoned") = Some(method);
    }

    fn send(&self, message: ControlMessage) -> Result<()> {
        self.control.send(message).map_err(|_| MountError::Stopped)
    }
}

/// Refuses a change to the tree. The engine's write path isn't built yet,
/// so every mount refuses changes the way a read-only mount does.
fn read_only(operation: &'static str) -> MountError {
    MountError::ReadOnly { operation }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.control.send(ControlMessage::Shutdown);
        if let Some(thread) = self.control_thread.take() {
            if thread.join().is_err() {
                tracing::error!("the mount's control thread panicked");
            }
        }
    }
}

/// Opens the state that a mount of the same namespace and path left, after
/// checking the host again. State of another mount is replaced when it has no
/// unpublished changes.
fn resume(
    config: &MountConfig,
    disk: &dyn DiskSpace,
) -> Result<Option<(Database, crate::status::ReflinkSupport)>> {
    let path = config.database_path();
    let Some(database) = Database::open(&path)? else {
        return Ok(None);
    };
    let stored = database.mount()?;
    if stored.config.namespace_id != config.namespace_id || stored.config.path != config.path {
        let dirty: i64 = database.connection().query_row(
            "SELECT COUNT(*) FROM items WHERE dirty_groups != 0",
            [],
            |row| row.get(0),
        )?;
        if dirty > 0 {
            return Err(MountError::StateMismatch {
                path: config.state_directory.display().to_string(),
                namespace_id: stored.config.namespace_id,
                mount_path: stored.config.path.to_string(),
            });
        }
        drop(database);
        database::remove_files(&path)?;
        return Ok(None);
    }
    let host = check_host(&stored.config, disk)?;
    Ok(Some((database, host.reflinks)))
}

/// Sends the control task a tick every poll interval, with at most one tick
/// waiting at a time, until the control task stops.
async fn tick(control: ControlSender, shared: SharedState, poll_interval_ms: u64) {
    loop {
        clock::delay(poll_interval_ms).await;
        if control.is_closed() {
            return;
        }
        if !shared.tick_pending.swap(true, Ordering::SeqCst)
            && control.send(ControlMessage::Tick).is_err()
        {
            return;
        }
    }
}

/// A directory listing that reads the tree in batches.
struct ReadDirectory {
    tree: Arc<RwLock<Tree>>,
    item: ItemId,
    offset: u64,
    batch: VecDeque<DirEntry>,
    done: bool,
}

impl Iterator for ReadDirectory {
    type Item = DirEntry;

    fn next(&mut self) -> Option<DirEntry> {
        if self.batch.is_empty() && !self.done {
            let batch = self
                .tree
                .read()
                .expect("the tree lock should not be poisoned")
                .read_directory(self.item, self.offset, READ_DIRECTORY_BATCH_ENTRIES);
            self.done = batch.len() < READ_DIRECTORY_BATCH_ENTRIES;
            if let Some(last) = batch.last() {
                self.offset = last.offset;
            }
            self.batch = batch.into();
        }
        self.batch.pop_front()
    }
}
