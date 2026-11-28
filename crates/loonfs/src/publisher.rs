//! Runtime publication service for namespace mutations.
//!
//! Each namespace writer session has two serial queues: the commit queue,
//! which sends its WAL puts, and the publication lane, which sends its
//! manifest puts. Concurrent commits may share a WAL object and head update.
//! Duplicate commit IDs join in flight, and conflicting reuse is rejected. A
//! namespace deletion and a retention advance run in their caller's task,
//! and pause the commit queue only for the fold of what arrived and their
//! last manifest: the tombstone, or the advance's manifest.
//! Every manifest publication of the session other than a claim (a fold, a
//! compaction unit, an extension entry, a window change, a retention
//! advance's manifest, a deletion's manifests, and a snapshot's record put
//! with its verification) takes its turn in the lane, and a claim keeps the
//! lane and the commit queue while it runs. A commit of a session that has
//! its writer epoch never waits for a manifest put.
//!
//! A new commit that reaches the write stop waits at the front of the queue
//! for the session's fold, for at most the publication policy's write-stop
//! wait. The worker releases its publication slot while the commit waits.
//!
//! Admitted work continues if its caller is cancelled. Shutdown closes
//! admission and drains the queues through
//! [`LoonFs::shutdown`](crate::LoonFs::shutdown).
//! [`LoonFs::drain`](crate::LoonFs::drain) drains them without closing
//! admission.
//!
//! The registry keeps one table of live sessions, for two reasons: an open
//! returns the session a handle already has, and shutdown drains every
//! session. The host sets how long a session stays open by keeping its
//! writable [`Namespace`](crate::Namespace).

mod admission;
mod compaction;
mod deletion;
mod folds;
mod maintenance;
pub(crate) mod metrics;
mod position;
mod queue;
mod registry;
mod retention_advance;
mod session_tasks;
mod two_folds;
mod worker;
mod write_stop;

use crate::runtime_core::{ReadPositionSlot, RuntimeCore};
use crate::{CoreError, SharedObjectStore, WriterState};
use admission::PublicationAdmission;
#[cfg(test)]
pub(crate) use compaction::MAX_COMPACTION_UNITS_PER_CALL;
use folds::NamespaceTurns;
pub(crate) use folds::{fold_due, FoldDue, FoldWhen, NamespaceFold, WalTailState};
use loonfs_core::publish::{
    LaneGuard, NamespaceCommitEngine, PublicationLane, UnitStart, WriterSessionStatus,
    WriterSessionStatusGuard, WriterSessionStatusView,
};
use loonfs_core::time::Observation;
use loonfs_core::MaintenanceCancellation;
use loonfs_types::{CommitId, CommitSeq, NamespaceId, WriterId};
pub(crate) use maintenance::AdmittedMaintenance;
use queue::{InFlightRequest, OpenBatch};
use registry::RegistryShared;
pub use registry::{CloseNamespaceReport, NamespaceSessionStatus};
pub(crate) use registry::{MergeReservation, NamespaceSession, PublisherRegistry};
use session_tasks::{SessionTask, WorkerHandle};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::sync::Mutex as AsyncMutex;
use worker::elapsed_ms_from;

/// What every namespace publisher of one runtime shares: the runtime core,
/// the writer and its ID, the Tokio runtime that runs the publisher tasks,
/// and the publication admission.
struct PublisherContext {
    /// Strong: the runtime core owns neither the registry nor the writer, so
    /// keeping it here cannot cycle. Publications read through its caches,
    /// and the core keeps only a weak reference back (see
    /// [`PublisherRegistry::head_positions`]).
    runtime_core: RuntimeCore,
    /// Weak: the state belongs to the writer, and a publication is the
    /// writer's work. Publish work upgrades per unit and reports
    /// `shutting_down` once the writer is gone, so dropping the writer stops
    /// new work without ever leaving the caches or store dangling.
    writer: Weak<WriterState>,
    /// The writer ID that each publisher's engine commits as.
    writer_id: WriterId,
    runtime: Handle,
    admission: Arc<PublicationAdmission>,
}

/// The commit queue, the publication lane and the publication worker for one
/// namespace's writer session.
///
/// The session's locks are taken in one order: the namespace's fold lock,
/// then the publication lane, then the engine, which is the commit queue. No
/// task takes the lane while it has the engine, so a claim, which keeps the
/// lane and the engine, can't wait for an item of the lane that waits for the
/// engine.
#[derive(Clone)]
struct NamespacePublisher {
    context: Arc<PublisherContext>,
    namespace_id: NamespaceId,
    state: Arc<Mutex<NamespacePublisherState>>,
    /// Admission keeps this lock locked until it reserves bytes against the observed
    /// tail, so a publication cannot change that tail before the reservation.
    /// Lock it through [`Self::lock_engine`].
    ///
    /// The publisher builds the engine when the publisher is created and
    /// keeps it for its whole life, because the engine's writer session
    /// status (the acquired epoch and terminal fencing state) describes this
    /// process and can't be reconstructed from object storage. Invalidation
    /// drops only the engine's position and the position's WAL tail.
    engine: Arc<AsyncMutex<NamespaceCommitEngine<SharedObjectStore>>>,
    /// The session's publication lane, through which every manifest
    /// publication of the session goes, from the engine (see
    /// [`PublicationLane`]). A unit of the commit queue never waits for it,
    /// so a commit never waits for a manifest put. Lock it through
    /// [`Self::lock_lane`].
    lane: PublicationLane,
    /// The engine's position as of the last release of the engine, brought
    /// up to the newest manifest that this runtime published for the
    /// namespace, which reads in this runtime start from. A publication
    /// records its manifest here without locking the engine.
    read_position: Arc<ReadPositionSlot>,
    /// A read-only view of the engine's writer session status, which
    /// admission reads while another task has the engine locked.
    session: WriterSessionStatusView,
    /// Weak: the session table owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    /// The session's stop, set once the session stops admitting work, is
    /// fenced, or finds its namespace deleted. A session never compacts again
    /// after that, and a host's fold, unit or snapshot that waits for a lock
    /// or a permit stops waiting.
    stop: MaintenanceCancellation,

    /// Set by the first release of the engine or of the lane after the
    /// session's status ended it (see [`Self::record_session_end`]), so the
    /// session records its end once.
    session_end_recorded: Arc<AtomicBool>,
    /// Signals each fold of this session's namespace that publishes, so a
    /// worker waiting at the write stop wakes once that fold frees room,
    /// even when the fold task goes on to fold again.
    fold_published: watch::Sender<()>,
    /// The namespace's fold and compaction locks. Strong: the registry's
    /// table of locks keeps a namespace's entry while a caller has it, so
    /// the session's folds and every other fold of the namespace share one
    /// lock for the session's whole life.
    turns: Arc<NamespaceTurns>,
}

/// The engine of one publisher, locked through
/// [`NamespacePublisher::lock_engine`].
///
/// Releasing it stores the engine's position where reads start from it (see
/// [`NamespacePublisher::release_engine`]). Every task that locks the engine
/// goes through this guard, so the stored position matches the engine as of
/// its last release.
struct LockedEngine<'a> {
    publisher: &'a NamespacePublisher,
    /// `None` only while `drop` runs.
    guard: Option<tokio::sync::MutexGuard<'a, NamespaceCommitEngine<SharedObjectStore>>>,
}

impl std::ops::Deref for LockedEngine<'_> {
    type Target = NamespaceCommitEngine<SharedObjectStore>;

    fn deref(&self) -> &NamespaceCommitEngine<SharedObjectStore> {
        self.guard
            .as_ref()
            .expect("a locked engine keeps its guard")
    }
}

impl std::ops::DerefMut for LockedEngine<'_> {
    fn deref_mut(&mut self) -> &mut NamespaceCommitEngine<SharedObjectStore> {
        self.guard
            .as_mut()
            .expect("a locked engine keeps its guard")
    }
}

impl Drop for LockedEngine<'_> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            self.publisher.release_engine(guard);
        }
    }
}

/// The publication lane of one publisher, kept through
/// [`NamespacePublisher::lock_lane`].
///
/// Releasing it finishes the lane's turn (see
/// [`NamespacePublisher::release_lane`]).
pub(crate) struct KeptLane<'a> {
    publisher: &'a NamespacePublisher,
    /// `None` only while `drop` runs.
    guard: Option<LaneGuard<'a>>,
}

impl<'a> std::ops::Deref for KeptLane<'a> {
    type Target = LaneGuard<'a>;

    fn deref(&self) -> &LaneGuard<'a> {
        self.guard.as_ref().expect("a kept lane keeps its guard")
    }
}

impl Drop for KeptLane<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.publisher.release_lane();
    }
}

/// Admission state for a namespace publisher. A session whose writer status
/// ended it (a fence, or a deleted namespace) refuses admission whatever this
/// state is (see [`NamespacePublisher::admitted_session`]).
enum PublisherAdmissionStatus {
    Open,
    /// Set by the registry's admission close. Later admissions fail with
    /// `shutting_down`; everything already queued keeps publishing.
    Closed,
    /// Set by [`NamespaceSession::close`]. Later admissions fail with
    /// `writer_session_closed`; everything already queued keeps publishing.
    SessionClosed,
}

struct NamespacePublisherState {
    /// Admitted commits in admission order, which coalesce into the tail
    /// batch.
    queue: VecDeque<OpenBatch>,
    in_flight: HashMap<CommitId, InFlightRequest>,
    admission: PublisherAdmissionStatus,
    /// The worker draining `queue`, while one is running. A live entry is
    /// what makes the loop single-flight: a worker installs itself under
    /// the admission lock and releases the slot under the same lock that
    /// finds the queue empty. That single flight is what makes the queue's
    /// admission order deterministic.
    worker: Option<WorkerHandle>,
    fold: Option<SessionTask>,
    /// One metadata compaction at a time, started when a fold publishes.
    compaction: Option<SessionTask>,
    /// The number of maintenance calls that hosts run on the session now
    /// (see [`maintenance::AdmittedMaintenance`]), which the session's close
    /// and the runtime's drain wait to reach zero. It's at most the number
    /// of the session's handles' calls in flight.
    maintenance: watch::Sender<usize>,
    next_task_id: u64,
    /// The last reserved WAL put. `None` is an idle namespace: nothing was
    /// queued when its last batch settled, so the next request publishes
    /// immediately.
    last_publish: Option<Observation>,
    /// The seq of the newest commit this session published, and `None`
    /// before its first.
    last_published_seq: Option<CommitSeq>,
    /// The candidates this namespace last added to the runtime's count of
    /// queued candidates, which [`NamespacePublisher::count_queued`] keeps
    /// equal to the candidates in `queue`.
    counted_candidates: usize,
}

impl NamespacePublisherState {
    /// Whether the session is over and nothing it admitted can still
    /// publish. A session is over once no handle that the caller counts in
    /// `held` refers to it, or once it was closed.
    fn has_ended(&self, held: bool) -> bool {
        let over = !held || matches!(self.admission, PublisherAdmissionStatus::SessionClosed);
        let worker_running = self.worker.as_ref().is_some_and(WorkerHandle::is_running);
        let fold_running = self.fold.as_ref().is_some_and(SessionTask::is_running);
        let compaction_running = self
            .compaction
            .as_ref()
            .is_some_and(SessionTask::is_running);
        let maintenance_running = *self.maintenance.borrow() > 0;
        over && !worker_running && !fold_running && !compaction_running && !maintenance_running
    }
}

impl NamespacePublisher {
    pub(super) fn new(
        namespace_id: NamespaceId,
        shared: &Arc<RegistryShared>,
        turns: Arc<NamespaceTurns>,
    ) -> Self {
        let engine = NamespaceCommitEngine::new(
            shared.context.runtime_core.namespace_store(&namespace_id),
            shared.context.writer_id.clone(),
        );
        let read_position = Arc::new(ReadPositionSlot::new(
            shared.context.runtime_core.head_and_wal_tail_cache(),
            engine.published_manifest(),
        ));
        let session = engine.session_status();
        let lane = engine.publication_lane();
        Self {
            context: Arc::clone(&shared.context),
            namespace_id,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                admission: PublisherAdmissionStatus::Open,
                worker: None,
                fold: None,
                compaction: None,
                maintenance: watch::Sender::new(0),
                next_task_id: 0,
                last_publish: None,
                last_published_seq: None,
                counted_candidates: 0,
            })),
            engine: Arc::new(AsyncMutex::new(engine)),
            lane,
            read_position,
            session,
            shared: Arc::downgrade(shared),
            stop: MaintenanceCancellation::default(),
            session_end_recorded: Arc::new(AtomicBool::new(false)),
            fold_published: watch::Sender::new(()),
            turns,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, NamespacePublisherState> {
        lock_state(&self.state)
    }

    /// Open becomes closed, and the session's compaction is cancelled.
    pub(super) fn close_admission(&self) {
        let mut state = self.lock_state();
        if matches!(state.admission, PublisherAdmissionStatus::Open) {
            state.admission = PublisherAdmissionStatus::Closed;
        }
        self.stop.cancel();
    }

    /// Closes admission for every handle that shares this session and
    /// returns how many commits were admitted before the close, or `None`
    /// when the session no longer admitted work. Fails with `shutting_down`
    /// after shutdown begins.
    pub(super) fn close_session(&self) -> std::result::Result<Option<usize>, CoreError> {
        let shared = self.shared.upgrade();
        let registry = shared.as_ref().map(|shared| shared.lock_state());
        if registry.as_ref().is_some_and(|registry| registry.closed) {
            return Err(CoreError::ShuttingDown);
        }
        let mut state = self.lock_state();
        if !matches!(state.admission, PublisherAdmissionStatus::Open) {
            return Ok(None);
        }
        state.admission = PublisherAdmissionStatus::SessionClosed;
        Ok(Some(state.in_flight.len()))
    }

    pub(super) fn session_closed(&self) -> bool {
        matches!(
            self.lock_state().admission,
            PublisherAdmissionStatus::SessionClosed
        )
    }

    pub(super) fn session_status(&self) -> NamespaceSessionStatus {
        if !matches!(self.lock_state().admission, PublisherAdmissionStatus::Open) {
            return NamespaceSessionStatus::Closed;
        }
        match *self.session.lock() {
            WriterSessionStatus::Fenced(_) => NamespaceSessionStatus::Fenced,
            WriterSessionStatus::NamespaceGone => NamespaceSessionStatus::Closed,
            WriterSessionStatus::Unacquired | WriterSessionStatus::Acquired { .. } => {
                NamespaceSessionStatus::Open
            }
        }
    }

    /// Removes this session from the table once it has ended and nothing it
    /// admitted is still running. Dropping the last handle, the worker's
    /// exit, the fold's exit, a close, and the end of a maintenance call each
    /// call this, so whichever of them comes last removes the session. A
    /// session whose writer status ended it (a fence, or a deleted
    /// namespace) ends even while handles still exist.
    /// A session the registry has removed never starts a task and never
    /// has queued work. Admission and task starts keep the writer-session
    /// lock through their decision, so fencing prevents both before removal.
    pub(super) fn forget_if_ended(&self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let mut registry = shared.lock_state();
        let held = match registry.sessions.get(&self.namespace_id) {
            Some(live) if Arc::ptr_eq(&live.publisher.state, &self.state) => {
                live.session.strong_count() > 0
            }
            _ => return,
        };
        let held = held && self.live_session().is_ok();
        if !self.lock_state().has_ended(held) {
            return;
        }
        // Reads stop finding the session once it's removed, so its position
        // ends here and becomes the namespace's cached head.
        self.read_position.clear();
        registry.sessions.remove(&self.namespace_id);
        let sessions = registry.sessions.len();
        drop(registry);
        self.context
            .runtime_core
            .instruments()
            .publisher
            .sessions(sessions);
    }

    pub(super) fn admitted_session(
        &self,
        state: &NamespacePublisherState,
    ) -> std::result::Result<WriterSessionStatusGuard<'_>, CoreError> {
        // A deleted namespace refuses before a closed admission does, so a
        // caller learns that the namespace is gone.
        match (&state.admission, self.live_session()) {
            (_, Err(deleted @ CoreError::NamespaceDeleted { .. })) => Err(deleted),
            (PublisherAdmissionStatus::Open, session) => session,
            (PublisherAdmissionStatus::Closed, _) => Err(CoreError::ShuttingDown),
            (PublisherAdmissionStatus::SessionClosed, _) => Err(CoreError::WriterSessionClosed {
                namespace_id: self.namespace_id.clone(),
            }),
        }
    }

    /// Returns the session's status, locked so that the status can't end the
    /// session while the caller decides on admission or a task start, or
    /// the refusal of a session that its status ended: `writer_fenced` for a
    /// fenced session, and `namespace_deleted` for a session whose namespace
    /// is gone.
    pub(super) fn live_session(
        &self,
    ) -> std::result::Result<WriterSessionStatusGuard<'_>, CoreError> {
        let session = self.session.lock();
        match &*session {
            WriterSessionStatus::Fenced(fence) => Err(CoreError::WriterFenced(fence.clone())),
            WriterSessionStatus::NamespaceGone => Err(self.namespace_deleted()),
            WriterSessionStatus::Unacquired | WriterSessionStatus::Acquired { .. } => Ok(session),
        }
    }

    pub(super) fn session_is_fenced(&self) -> bool {
        matches!(*self.session.lock(), WriterSessionStatus::Fenced(_))
    }

    /// Locks the engine for one unit of the commit queue, a commit batch, or
    /// for a load of the session's position outside
    /// the publication lane (see [`Self::writer_position`]), and returns
    /// where the unit or the load starts.
    ///
    /// A session that has its writer epoch starts from its position, or from
    /// the writer's load, with the engine alone. A session without its epoch
    /// claims it first, and a claim keeps the publication lane and the
    /// engine, so the worker releases the engine, takes the lane, and takes
    /// the engine again, in the order of the session's locks. The claim then
    /// waits for the publication of the lane in flight, and the worker
    /// releases the lane before the unit's put. A fenced session returns
    /// `writer_fenced`.
    pub(super) async fn lock_engine_for_unit(
        &self,
    ) -> std::result::Result<(LockedEngine<'_>, UnitStart), CoreError> {
        let engine = self.lock_engine().await;
        if let Some(start) = engine.kept_start()? {
            return Ok((engine, start));
        }
        drop(engine);
        let lane = self.lock_lane().await;
        let mut engine = self.lock_engine().await;
        if let Some(start) = engine.kept_start()? {
            return Ok((engine, start));
        }
        let claim = engine.claim(&lane).await?;
        Ok((engine, UnitStart::Claimed(claim)))
    }

    pub(super) async fn lock_engine(&self) -> LockedEngine<'_> {
        LockedEngine {
            publisher: self,
            guard: Some(self.engine.lock().await),
        }
    }

    /// Waits for the session's publication lane, and keeps it until the
    /// caller drops the guard, whose drop finishes the turn (see
    /// [`Self::release_lane`]). Every task that keeps the lane goes through
    /// this guard.
    pub(super) async fn lock_lane(&self) -> KeptLane<'_> {
        KeptLane {
            publisher: self,
            guard: Some(self.lane.lock().await),
        }
    }

    /// Finishes a turn of the session's publication lane once the lane is
    /// released: records the session's end when the turn found a fence or a
    /// deleted namespace, or published the tombstone (see
    /// [`Self::record_session_end`]), and brings the session's
    /// positions and the namespace's cached head up to the session's newest
    /// published manifest.
    ///
    /// Every publication of the lane passes here. A publication of the lane
    /// runs without the engine, so a fence or a tombstone that it finds is
    /// recorded here. The engine's release records one that a unit of the
    /// commit queue found. The positions follow the newest
    /// published manifest at once when no task has the engine (see
    /// [`Self::follow_published`]), and the cached head stays when it's
    /// against that manifest or a later one and is dropped otherwise (see
    /// [`HeadAndWalTailCache::confirm_or_drop_head`](loonfs_core::cache::HeadAndWalTailCache::confirm_or_drop_head)).
    fn release_lane(&self) {
        self.record_session_end();
        self.follow_published();
        let published = match &*self.session.lock() {
            WriterSessionStatus::Acquired { published, .. } => published.clone(),
            _ => return,
        };
        self.context
            .runtime_core
            .head_and_wal_tail_cache()
            .confirm_or_drop_head(&self.namespace_id, &published);
    }

    /// Stores the engine's position for reads, and releases the engine. The
    /// first release after the engine found the session fenced or its
    /// namespace deleted records the session's end (see
    /// [`Self::record_session_end`]). A publication of the lane records what
    /// it finds when it releases the lane (see [`Self::release_lane`]).
    pub(super) fn release_engine(
        &self,
        mut engine: tokio::sync::MutexGuard<'_, NamespaceCommitEngine<SharedObjectStore>>,
    ) {
        self.record_session_end();
        // A panic can stop any holder of the engine, in any task, partway
        // through its work, after a put whose outcome the engine hasn't
        // recorded (a commit's WAL put or a deletion's tombstone put).
        if std::thread::panicking() {
            self.drop_view_after_panic(&mut engine);
            return;
        }
        let position = self.read_position.prepare(engine.writer_position());
        match position {
            Some(position) => self.read_position.store(position),
            None => {
                // When the slot had a position, the engine found a change it
                // can't follow, such as another writer's claim, so the position
                // that just became the cached head may be out of date. (A put
                // of either queue that ends while the session has no epoch
                // drops the cached head itself before its caller hears the
                // outcome, whether or not the slot had a position, see
                // `SessionEpoch` in core.)
                if self.read_position.clear() {
                    self.context
                        .runtime_core
                        .invalidate_namespace_read_cache(&self.namespace_id);
                }
            }
        }
        // The engine is unlocked with the published manifest locked, after
        // both positions follow it (see `Self::follow_published`).
        let recorded = self.read_position.published().lock();
        let replaced = engine.follow_published(&recorded);
        let followed = self.read_position.follow_stored(&recorded);
        drop(engine);
        drop(recorded);
        drop(replaced);
        self.read_position.finish_follow(followed);
    }

    /// Ends the session the first time this finds that its status ended it,
    /// whichever unit found out: cancels the session's stop, and so its
    /// compaction, and records a fence in the `loonfs.publisher.displacements`
    /// metric, or drops the namespace's cached reads for a deleted
    /// namespace.
    ///
    /// A fenced session, and one whose namespace is gone, publishes nothing
    /// more and refuses new work (see [`Self::live_session`]). Its drain ends
    /// once its admitted work and a running fold end, instead of waiting for
    /// a compaction whose result it can't publish, and every unit still
    /// queued returns the session's refusal at its start without a request
    /// to the store.
    fn record_session_end(&self) {
        let fenced = match self.live_session() {
            Ok(_) => return,
            Err(error) => matches!(error, CoreError::WriterFenced(_)),
        };
        if self.session_end_recorded.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stop.cancel();
        if fenced {
            self.context
                .runtime_core
                .instruments()
                .publisher
                .displacement();
        } else {
            self.context
                .runtime_core
                .invalidate_namespace_read_cache(&self.namespace_id);
        }
    }

    /// Brings the engine's position and the position that reads start from
    /// up to the session's newest published manifest, which an item of the
    /// publication lane recorded, without waiting for the engine. When no
    /// task has the engine locked, both positions move onto the manifest at
    /// once, and otherwise the engine's release moves both.
    ///
    /// Either way neither position keeps the tail that the manifest folded,
    /// whatever the order of events. The record came before this call, and
    /// the attempt to lock the engine runs with the published manifest
    /// locked, and so does the release's move of both positions and its
    /// unlock of the engine. So either the record comes before the release's
    /// move, which then follows the manifest, or the release comes before
    /// this attempt, which then finds the engine free, or locked by a later
    /// task, whose release comes after the record.
    ///
    /// Under the published manifest's lock run only the two follows with
    /// their stores, and the unlock of the engine. Dropping the positions
    /// that the follows replaced, which can free a large WAL tail, and
    /// handing a removed read position's tail to the read cache run after the
    /// unlock.
    pub(super) fn follow_published(&self) {
        let recorded = self.read_position.published().lock();
        let Ok(mut engine) = self.engine.try_lock() else {
            return;
        };
        let replaced = engine.follow_published(&recorded);
        let followed = self.read_position.follow_stored(&recorded);
        drop(engine);
        drop(recorded);
        drop(replaced);
        self.read_position.finish_follow(followed);
    }

    /// Drops the session's view of the namespace after a panic: the engine's
    /// position, the position that reads start from, and the namespace's
    /// cached head, even when the slot had no position.
    ///
    /// The work that panicked may have stopped after a put whose outcome the
    /// session hasn't recorded, so the put may have committed. After this,
    /// the session's next unit of work loads the namespace from the store,
    /// and the next read loads it by discovery. The engine's release calls
    /// this for a panic in any holder of the engine, and
    /// [`Self::contain_panic`] calls it for a panic in publisher work that
    /// puts without the engine locked, which is a fold or a compaction.
    fn drop_view_after_panic(&self, engine: &mut NamespaceCommitEngine<SharedObjectStore>) {
        engine.invalidate_projection();
        self.read_position.clear();
        self.context
            .runtime_core
            .invalidate_namespace_read_cache(&self.namespace_id);
    }

    pub(super) fn namespace_deleted(&self) -> CoreError {
        CoreError::NamespaceDeleted {
            namespace_id: self.namespace_id.clone(),
        }
    }

    /// Records a panic that the publisher or the core contained, in the log
    /// at error level, in the count of panicked tasks that a drain reports,
    /// and in the `loonfs.publisher.panics` metric. `unit` identifies the work that
    /// panicked, and `panic` is the panic's message.
    pub(super) fn record_panic(&self, unit: &'static str, panic: &str) {
        tracing::error!(
            namespace_id = %self.namespace_id,
            unit,
            panic,
            "publisher task panicked"
        );
        if let Some(shared) = self.shared.upgrade() {
            shared.panicked_units.fetch_add(1, Ordering::SeqCst);
        }
        self.context.runtime_core.instruments().publisher.panic();
    }

    /// Brings this namespace's share of the runtime's count of queued
    /// candidates up to the candidates in its queue. Call it with the state
    /// locked after a change to the queue, and call
    /// [`Self::report_queue_depth`] once the lock is released.
    pub(super) fn count_queued(&self, state: &mut NamespacePublisherState) {
        let queued = queue::queued_candidates(state);
        self.context
            .runtime_core
            .instruments()
            .publisher
            .move_queued_candidates(state.counted_candidates, queued);
        state.counted_candidates = queued;
    }

    /// Reports the candidates queued at every namespace of the runtime.
    pub(super) fn report_queue_depth(&self) {
        self.context
            .runtime_core
            .instruments()
            .publisher
            .queue_depth();
    }

    pub(super) fn elapsed_ms_since(&self, started_at_ms: u64) -> u64 {
        elapsed_ms_from(
            started_at_ms,
            self.context.runtime_core.timer().monotonic_now_ms(),
        )
    }
}

/// Locks registry and publisher state, recovering a lock that a contained
/// panic poisoned.
///
/// The publisher contains panics by design: after a publication, fold,
/// or compaction panics, the worker, the table, the other sessions,
/// and the drain must keep working. Every critical section under these locks
/// only updates fields.
fn lock_state<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
