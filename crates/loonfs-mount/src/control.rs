//! The control task: the one owner of a mount's trees, cache state, and
//! position in the change feed.
//!
//! Every change arrives as a message, from the engine's API or from a
//! worker, and the task handles one message at a time on a thread of its
//! own, so database writes never block the async runtime. With its workers
//! replaced by a recorder, the task is a deterministic state machine over
//! its messages.

mod cache_files;
mod changes;
mod effects;
mod entering;
mod events;
mod feed;
mod outbox;
mod reads;
mod rebuild;
#[cfg(test)]
mod tests;

use crate::cache::Cache;
use crate::cache_file::CacheFile;
use crate::config::MountConfig;
use crate::database::{self, Ancestor, Database};
use crate::error::Result;
use crate::fetch::FetchQueue;
use crate::host::{DiskSpace, Owner};
use crate::invalidation::{Invalidation, Subscribers};
use crate::item::ItemId;
use crate::item_table::for_each_item;
use crate::read_handle::{HandleId, OpenedRead};
use crate::status::{
    CacheStatus, PublicationStop, Readiness, ReflinkSupport, Status, Transports, Warning,
};
use crate::tree::{ItemView, Tree};
use crate::workers::{WorkOutcome, Workers};
use loonfs_types::{AbsolutePath, ChangeSeq, ContentId, InodeId, MonotonicTimer};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};

pub(crate) use changes::write_listing;

use entering::EnteringState;
use feed::{FeedState, RefreshWaiter};
use outbox::Outbox;
use reads::{OpenHandle, ReadWaiter};

/// Sends messages to a control task.
///
/// The channel is unbounded, and its senders bound what's in it: the
/// adapter's requests by the kernel's requests in flight, close messages by
/// the open handles, worker outcomes by the work in flight, and ticks by one
/// pending tick at a time.
pub(crate) type ControlSender = mpsc::UnboundedSender<ControlMessage>;

/// One message to a control task.
#[derive(Debug)]
pub(crate) enum ControlMessage {
    OpenRead {
        item: ItemId,
        reply: oneshot::Sender<Result<OpenedRead>>,
    },
    Read {
        handle: HandleId,
        /// The window that the read fetches.
        demand: Range<u64>,
        /// The bytes that the read returns.
        needed: Range<u64>,
        reply: oneshot::Sender<Result<Arc<CacheFile>>>,
    },
    ReadAhead {
        handle: HandleId,
        range: Range<u64>,
    },
    CloseRead {
        handle: HandleId,
    },
    RefreshAtLeast {
        seq: ChangeSeq,
        reply: oneshot::Sender<Result<()>>,
    },
    /// The poll interval passed.
    Tick,
    /// The kernel forgot an item that left the tree.
    Sweep,
    /// A worker's outcome, boxed because some outcomes include entries.
    WorkDone(Box<WorkOutcome>),
    Shutdown,
}

/// Where the mount's root is in the namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RootLocation {
    pub(crate) inode_id: InodeId,
    pub(crate) path: AbsolutePath,
    /// From the root's parent up to the namespace's root.
    pub(crate) ancestors: Vec<Ancestor>,
}

impl RootLocation {
    /// Returns whether `inode_id` is the root or one of its ancestors.
    pub(crate) fn contains(&self, inode_id: InodeId) -> bool {
        inode_id == self.inode_id
            || self
                .ancestors
                .iter()
                .any(|ancestor| ancestor.inode_id == inode_id)
    }
}

/// What a control task shares with its engine.
#[derive(Clone)]
pub(crate) struct SharedState {
    pub(crate) tree: Arc<RwLock<Tree>>,
    pub(crate) status: Arc<Mutex<Status>>,
    pub(crate) subscribers: Subscribers,
    /// Set while a tick waits for the control task, so the ticker sends at
    /// most one.
    pub(crate) tick_pending: Arc<AtomicBool>,
}

/// The timer and filesystem that a control task reads.
#[derive(Clone)]
pub(crate) struct Environment {
    pub(crate) timer: Arc<dyn MonotonicTimer>,
    pub(crate) disk: Arc<dyn DiskSpace>,
    pub(crate) reflinks: ReflinkSupport,
}

/// The state that one mount's control task owns.
pub(crate) struct ControlTask {
    database: Database,
    config: MountConfig,
    shared: SharedState,
    workers: Box<dyn Workers>,
    environment: Environment,
    transports: Transports,
    inline_limit_bytes: u64,
    entries: u64,

    applied_seq: ChangeSeq,
    root: RootLocation,
    feed: FeedState,
    poll_requested: bool,
    started_polls: u64,
    completed_polls: u64,
    /// The namespace's head that the current poll's last page read.
    polled_head_seq: Option<ChangeSeq>,
    refresh_waiters: Vec<RefreshWaiter>,
    invalidations: Vec<Invalidation>,
    entering: Vec<EnteringState>,
    publication_stop: Option<PublicationStop>,
    reserved_name: Option<InodeId>,
    /// The owner that every item shows, which status reports.
    owner: Owner,
    feed_error: Option<String>,
    dropped_invalidations: u64,

    cache: Cache,
    fetches: FetchQueue,
    handles: HashMap<HandleId, OpenHandle>,
    /// The open handles that read each content, which keep its cache file.
    pins: HashMap<ContentId, usize>,
    read_waiters: Vec<ReadWaiter>,
    next_handle_id: u64,
    mismatched_files: u64,

    outbox: Outbox,
}

impl ControlTask {
    /// Loads a mount's state from its database, and returns the task with
    /// the state that it shares with the engine.
    pub(crate) fn load(
        mut database: Database,
        workers: Box<dyn Workers>,
        environment: Environment,
    ) -> Result<(Self, SharedState)> {
        let mount = database.mount()?;
        let root = RootLocation {
            inode_id: mount.root_inode_id,
            path: mount.root_path.clone(),
            ancestors: database::read_ancestors(database.connection())?,
        };
        let entering = entering::load(&mut database)?;
        let owner = Owner::of(&mount.config);
        let tree = load_tree(&database)?;
        let rows = database::read_cache_files(database.connection())?;
        let (cache, missing) = Cache::load(&mount.config.cache_directory(), rows)?;
        for content_id in missing {
            database::delete_cache_file(database.connection(), &content_id)?;
        }
        let entries = database::count_items(database.connection())?;
        let shared = SharedState {
            tree: Arc::new(RwLock::new(tree)),
            status: Arc::new(Mutex::new(initial_status(
                &mount.config,
                owner,
                &environment,
            ))),
            subscribers: Subscribers::default(),
            tick_pending: Arc::new(AtomicBool::new(false)),
        };
        let mut task = Self {
            database,
            config: mount.config,
            shared: shared.clone(),
            workers,
            environment,
            transports: mount.transports,
            inline_limit_bytes: mount.inline_limit_bytes,
            entries,
            applied_seq: mount.applied_seq,
            root,
            feed: FeedState::Idle,
            poll_requested: true,
            started_polls: 0,
            completed_polls: 0,
            polled_head_seq: None,
            refresh_waiters: Vec::new(),
            invalidations: Vec::new(),
            entering,
            publication_stop: mount.publication_stop,
            reserved_name: None,
            owner,
            feed_error: None,
            dropped_invalidations: 0,
            cache,
            fetches: FetchQueue::default(),
            handles: HashMap::new(),
            pins: HashMap::new(),
            read_waiters: Vec::new(),
            next_handle_id: 0,
            mismatched_files: 0,
            outbox: Outbox::default(),
        };
        task.publish_status();
        Ok((task, shared))
    }

    /// Handles messages until the engine shuts down or drops every sender.
    pub(crate) fn run(mut self, mut receiver: mpsc::UnboundedReceiver<ControlMessage>) {
        self.start();
        while let Some(message) = receiver.blocking_recv() {
            if matches!(message, ControlMessage::Shutdown) {
                break;
            }
            self.handle(message);
        }
    }

    /// Returns how often the engine reads the change feed.
    pub(crate) fn poll_interval_ms(&self) -> u64 {
        self.config.poll_interval_ms
    }

    /// Starts following the feed. The task's first message would do the
    /// same, and a test calls this instead of `run`.
    pub(crate) fn start(&mut self) {
        // A restart between a commit and the transaction that shows the
        // directories it reached leaves them to show now.
        if let Err(error) = self.reveal_entering(None) {
            tracing::error!(%error, "showing directories that entered the mount failed");
        }
        self.on_tick();
        self.publish_and_deliver();
    }

    /// Handles one message.
    pub(crate) fn handle(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::OpenRead { item, reply } => {
                let opened = self.open_read(item);
                self.outbox.reply(reply, opened);
            }
            ControlMessage::Read {
                handle,
                demand,
                needed,
                reply,
            } => self.on_read(handle, demand, needed, reply),
            ControlMessage::ReadAhead { handle, range } => self.on_read_ahead(handle, range),
            ControlMessage::CloseRead { handle } => self.on_close_read(handle),
            ControlMessage::RefreshAtLeast { seq, reply } => self.on_refresh(seq, reply),
            ControlMessage::Tick => {
                self.shared.tick_pending.store(false, Ordering::SeqCst);
                self.on_tick();
            }
            ControlMessage::Sweep => self.sweep_tree(),
            ControlMessage::WorkDone(outcome) => self.on_work_done(*outcome),
            ControlMessage::Shutdown => {}
        }
        self.publish_and_deliver();
    }

    /// Publishes the status, and then sends the replies and invalidations
    /// that the message produced.
    fn publish_and_deliver(&mut self) {
        self.publish_status();
        let subscribers = &self.shared.subscribers;
        let dropped = self.outbox.deliver(|batch| subscribers.send(batch));
        if dropped > 0 {
            self.dropped_invalidations += dropped;
            self.publish_status();
        }
    }

    fn on_work_done(&mut self, outcome: WorkOutcome) {
        match outcome {
            WorkOutcome::FeedPage(page) => self.on_feed_page(page),
            WorkOutcome::EnteringItemRead { inode_id, result } => {
                self.on_entering_item_read(inode_id, result);
            }
            WorkOutcome::EnteringDirectory {
                inode_id,
                entry,
                snapshot,
                created,
            } => self.on_entering_directory(inode_id, &entry, snapshot, created),
            WorkOutcome::AncestorsRead(result) => self.on_ancestors_read(result),
            WorkOutcome::ListingPage {
                listing,
                entries,
                processed,
            } => {
                self.on_listing_page(listing, &entries);
                let _ = processed.send(());
            }
            WorkOutcome::RebuildDone(result) => self.on_rebuild_done(result),
            WorkOutcome::Fetched { fetch_id, result } => self.on_fetched(fetch_id, result),
            WorkOutcome::Verified {
                content_id,
                file,
                target,
                result,
            } => self.on_verified(&content_id, &file, &target, result),
        }
    }

    fn on_tick(&mut self) {
        self.request_poll();
        self.keep_entering_alive();
        self.sweep_tree();
    }

    fn tree(&self) -> std::sync::RwLockWriteGuard<'_, Tree> {
        self.shared
            .tree
            .write()
            .expect("the tree lock should not be poisoned")
    }

    /// Removes the items that left the tree once the kernel and every
    /// descriptor are done with them.
    fn sweep_tree(&mut self) {
        let open: std::collections::HashSet<ItemId> =
            self.handles.values().map(|handle| handle.item).collect();
        self.tree().sweep(|item| open.contains(&item));
    }

    /// Queues the invalidations of the last page, which the task sends once
    /// it has published its status.
    fn flush_invalidations(&mut self) {
        let invalidations = std::mem::take(&mut self.invalidations);
        self.outbox.invalidations(invalidations);
    }

    fn publish_status(&mut self) {
        let pinned =
            |content_id: &ContentId| self.pins.get(content_id).is_some_and(|&pins| pins > 0);
        let mut warnings = Vec::new();
        if self.entries > self.config.max_entries.saturating_mul(2) {
            warnings.push(Warning::EntriesOverBudget {
                entries: self.entries,
                max_entries: self.config.max_entries,
            });
        }
        if let Some(inode_id) = self.reserved_name {
            warnings.push(Warning::ReservedName { inode_id });
        }
        let status = Status {
            namespace_id: self.config.namespace_id.clone(),
            path: self.config.path.clone(),
            access: self.config.access,
            uid: self.owner.uid,
            gid: self.owner.gid,
            state_directory: self.config.state_directory.clone(),
            available_bytes: None,
            readiness: if self.feed.is_rebuilding() {
                Readiness::Rebuilding
            } else {
                Readiness::Ready
            },
            mount_method: None,
            applied_seq: self.applied_seq,
            dirty_items: 0,
            oldest_unpublished_change_age_ms: None,
            pending_batch: None,
            uploads_in_flight: 0,
            cache: CacheStatus {
                evictable_bytes: self.cache.evictable_bytes(pinned),
                pinned_bytes: self.cache.pinned_bytes(pinned),
                max_cache_bytes: self.config.max_cache_bytes,
                max_pinned_bytes: self.config.max_pinned_bytes,
                mismatched_files: self.mismatched_files,
            },
            entries: self.entries,
            max_entries: self.config.max_entries,
            reflinks: self.environment.reflinks,
            inline_limit_bytes: self.inline_limit_bytes,
            transports: self.transports,
            conflicts: Vec::new(),
            refused_publications: Vec::new(),
            publication_stop: self.publication_stop.clone(),
            warnings,
            feed_error: self.feed_error.clone(),
            dropped_invalidations: self.dropped_invalidations,
        };
        *self
            .shared
            .status
            .lock()
            .expect("the status lock should not be poisoned") = status;
    }
}

fn initial_status(config: &MountConfig, owner: Owner, environment: &Environment) -> Status {
    Status {
        namespace_id: config.namespace_id.clone(),
        path: config.path.clone(),
        access: config.access,
        uid: owner.uid,
        gid: owner.gid,
        state_directory: config.state_directory.clone(),
        available_bytes: None,
        readiness: Readiness::Ready,
        mount_method: None,
        applied_seq: ChangeSeq(0),
        dirty_items: 0,
        oldest_unpublished_change_age_ms: None,
        pending_batch: None,
        uploads_in_flight: 0,
        cache: CacheStatus::default(),
        entries: 0,
        max_entries: config.max_entries,
        reflinks: environment.reflinks,
        inline_limit_bytes: 0,
        transports: Transports::default(),
        conflicts: Vec::new(),
        refused_publications: Vec::new(),
        publication_stop: None,
        warnings: Vec::new(),
        feed_error: None,
        dropped_invalidations: 0,
    }
}

/// Loads the Local tree from the database: every item first, then every
/// binding, so a parent is in the tree before any child binds under it.
fn load_tree(database: &Database) -> Result<Tree> {
    let mut views = Vec::new();
    for_each_item(database.connection(), |record| {
        views.push(ItemView::of(&record));
    })?;
    let mut tree = Tree::default();
    for view in &views {
        tree.show(ItemView {
            binding: None,
            ..view.clone()
        });
    }
    for view in views {
        tree.show(view);
    }
    Ok(tree)
}
