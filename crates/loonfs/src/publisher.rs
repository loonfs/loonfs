//! Runtime publication service for namespace mutations.
//!
//! Each namespace writer session has one queue. Concurrent commits may share
//! a WAL object and head update. Duplicate commit IDs join in flight,
//! conflicting reuse is rejected, and namespace deletion and retention
//! advances are ordered with other mutations.
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
mod delete_barrier;
mod folds;
pub(crate) mod metrics;
mod queue;
mod registry;
mod retention_advance;
mod session_tasks;
mod worker;
mod write_stop;

use crate::runtime_core::{ReadPositionSlot, RuntimeCore};
use crate::{CoreError, SharedObjectStore, WriterState};
use admission::PublicationAdmission;
#[cfg(test)]
pub(crate) use compaction::MAX_COMPACTION_UNITS_PER_CALL;
use folds::NamespaceTurns;
pub(crate) use folds::{fold_due, FoldDue, FoldWhen, NamespaceFold, WalTailState};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::{
    NamespaceCommitEngine, WriterSessionStatus, WriterSessionStatusGuard, WriterSessionStatusView,
};
use loonfs_core::time::Observation;
use loonfs_core::MetadataCompactionCancellation;
use loonfs_types::{CommitId, CommitSeq, NamespaceId, WriterId};
use queue::{InFlightRequest, WorkItem};
use registry::RegistryShared;
pub use registry::{CloseNamespaceReport, NamespaceSessionStatus};
pub(crate) use registry::{MergeReservation, NamespaceChange, NamespaceSession, PublisherRegistry};
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

/// The commit queue and publication worker for one namespace's writer
/// session.
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
    /// Set once the session stops admitting work or is fenced. A session
    /// never compacts again after that.
    compaction_cancellation: MetadataCompactionCancellation,
    /// Set by the first release of the engine after the engine found the
    /// session fenced, so the session records its displacement once.
    displacement_recorded: Arc<AtomicBool>,
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

/// Admission state for a namespace publisher.
///
/// A single enum preserves precedence: after deletion succeeds, the
/// publisher remains `Deleted` even if registry shutdown closes admission.
enum PublisherAdmissionStatus {
    Open,
    /// Set by the registry's admission close. Later admissions fail with
    /// `shutting_down`; everything already queued keeps publishing.
    Closed,
    /// Set by [`NamespaceSession::close`]. Later admissions fail with
    /// `writer_session_closed`; everything already queued keeps publishing.
    SessionClosed,
    /// Terminal: set once a delete succeeds. Admissions fail fast from then
    /// on without touching the store.
    Deleted,
}

struct NamespacePublisherState {
    /// Admitted work in admission order. Commits coalesce into the tail
    /// batch, so a delete queued between them keeps its barrier position.
    queue: VecDeque<WorkItem>,
    in_flight: HashMap<CommitId, InFlightRequest>,
    admission: PublisherAdmissionStatus,
    /// The worker draining `queue`, while one is running. A live entry is
    /// what makes the loop single-flight: a worker installs itself under
    /// the admission lock and releases the slot under the same lock that
    /// finds the queue empty. That single flight is what makes the delete
    /// barrier's admission order deterministic.
    worker: Option<WorkerHandle>,
    fold: Option<SessionTask>,
    /// One metadata compaction at a time, started when a fold publishes.
    compaction: Option<SessionTask>,
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
    /// publish. A session is over once no handle refers to it, or once it was
    /// closed or its namespace deleted.
    fn has_ended(&self, held: bool) -> bool {
        let deleted = matches!(self.admission, PublisherAdmissionStatus::Deleted);
        let over =
            !held || deleted || matches!(self.admission, PublisherAdmissionStatus::SessionClosed);
        // A deleted session's worker takes nothing more from its queue.
        let worker_running = !deleted && self.worker.as_ref().is_some_and(WorkerHandle::is_running);
        let fold_running = self.fold.as_ref().is_some_and(SessionTask::is_running);
        let compaction_running = self
            .compaction
            .as_ref()
            .is_some_and(SessionTask::is_running);
        over && !worker_running && !fold_running && !compaction_running
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
                next_task_id: 0,
                last_publish: None,
                last_published_seq: None,
                counted_candidates: 0,
            })),
            engine: Arc::new(AsyncMutex::new(engine)),
            read_position,
            session,
            shared: Arc::downgrade(shared),
            compaction_cancellation: MetadataCompactionCancellation::default(),
            displacement_recorded: Arc::new(AtomicBool::new(false)),
            fold_published: watch::Sender::new(()),
            turns,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, NamespacePublisherState> {
        lock_state(&self.state)
    }

    /// Open becomes closed; a delete that was already published is terminal and
    /// stays terminal. Either way the session's compaction is cancelled.
    pub(super) fn close_admission(&self) {
        let mut state = self.lock_state();
        if matches!(state.admission, PublisherAdmissionStatus::Open) {
            state.admission = PublisherAdmissionStatus::Closed;
        }
        self.compaction_cancellation.cancel();
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
            WriterSessionStatus::Unacquired | WriterSessionStatus::Acquired(_) => {
                NamespaceSessionStatus::Open
            }
        }
    }

    /// Removes this session from the table once it has ended and nothing it
    /// admitted is still running. Dropping the last handle, the worker's
    /// exit, the fold's exit, a close, and a published delete each call this,
    /// so whichever of them comes last removes the session. Fenced sessions
    /// end even while handles still exist.
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
        let held = held && !self.session_is_fenced();
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
        match state.admission {
            PublisherAdmissionStatus::Open => self.unfenced_session(),
            PublisherAdmissionStatus::Closed => Err(CoreError::ShuttingDown),
            PublisherAdmissionStatus::SessionClosed => Err(CoreError::WriterSessionClosed {
                namespace_id: self.namespace_id.clone(),
            }),
            PublisherAdmissionStatus::Deleted => Err(self.namespace_deleted()),
        }
    }

    /// Keeps fencing from racing with admission or a task start until the
    /// caller drops the guard.
    pub(super) fn unfenced_session(
        &self,
    ) -> std::result::Result<WriterSessionStatusGuard<'_>, CoreError> {
        let session = self.session.lock();
        if let WriterSessionStatus::Fenced(fence) = &*session {
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        Ok(session)
    }

    pub(super) fn session_is_fenced(&self) -> bool {
        matches!(*self.session.lock(), WriterSessionStatus::Fenced(_))
    }

    /// Drops the engine's position once no other task has the engine
    /// locked.
    ///
    /// Until then, reads keep the position they have. A position on a
    /// manifest that this runtime has replaced still reads correctly,
    /// because garbage collection keeps that manifest's objects for at least
    /// one grace.
    pub(super) async fn drop_position(&self) {
        self.lock_engine().await.invalidate_projection();
    }

    /// Moves the engine's position onto `newest`, a manifest above it under
    /// the session's writer epoch that a read's check found, or drops the
    /// position when it can't move there, once no other task has the engine
    /// locked (see [`Self::drop_position`]). A manifest that this runtime
    /// publishes doesn't wait for the engine: the publication records it in
    /// `read_position`.
    pub(super) async fn move_position(&self, newest: Held<LoadedManifest>) {
        self.lock_engine().await.move_position_onto(newest);
    }

    pub(super) async fn lock_engine(&self) -> LockedEngine<'_> {
        LockedEngine {
            publisher: self,
            guard: Some(self.engine.lock().await),
        }
    }

    /// Stores the engine's position for reads, and releases the engine. The
    /// first release after the engine found the session fenced records the
    /// displacement in the `loonfs.publisher.displacements` metric and
    /// cancels the session's compaction.
    ///
    /// Every unit that can find a fence runs with the engine locked, so this
    /// release is the one place that sees each fence. A fenced session
    /// publishes nothing more, and its drain ends once its admitted work and
    /// a running fold end instead of waiting for a compaction whose result
    /// it can't publish.
    pub(super) fn release_engine(
        &self,
        mut engine: tokio::sync::MutexGuard<'_, NamespaceCommitEngine<SharedObjectStore>>,
    ) {
        if self.session_is_fenced() && !self.displacement_recorded.swap(true, Ordering::SeqCst) {
            self.compaction_cancellation.cancel();
            self.context
                .runtime_core
                .instruments()
                .publisher
                .displacement();
        }
        // A panic can stop any holder of the engine, in any task, partway
        // through its work, after a put whose outcome the engine hasn't
        // recorded (a commit's WAL put or a deletion's tombstone put).
        if std::thread::panicking() {
            self.drop_view_after_panic(&mut engine);
            return;
        }
        let position = self.read_position.prepare(engine.read_position());
        match position {
            Some(position) => self.read_position.store(position),
            None => {
                // When the slot had a position, the engine found a change it
                // can't follow, such as another writer's claim, so the position
                // that just became the cached head may be out of date. (After
                // a commit's WAL put whose outcome the session didn't learn,
                // the publish path drops the head itself.)
                if self.read_position.clear() {
                    self.context
                        .runtime_core
                        .invalidate_namespace_read_cache(&self.namespace_id);
                }
            }
        }
        // The engine is unlocked with the published manifest locked, after
        // both positions follow it (see `Self::record_published`).
        let recorded = self.read_position.published().lock();
        let replaced = engine.follow_published(&recorded);
        let followed = self.read_position.follow_stored(&recorded);
        drop(engine);
        drop(recorded);
        drop(replaced);
        self.read_position.finish_follow(followed);
    }

    /// Records `published`, a manifest that this runtime published or found
    /// current, as the session's newest published manifest, without waiting
    /// for the engine. When no task has the engine locked, the engine's
    /// position and the position that reads start from move onto the
    /// manifest at once, and otherwise the engine's release moves both.
    ///
    /// Either way neither position keeps the tail that the manifest folded,
    /// whatever the order of events. The record and the attempt to lock the
    /// engine run with the published manifest locked, and so does the
    /// release's move of both positions and its unlock of the engine. So
    /// either the record comes before the release's move, which then follows
    /// the manifest, or it comes after the unlock, and the attempt finds the
    /// engine free, or locked by a later task, whose release comes after the
    /// record. A unit that stores its position before the record and
    /// releases the engine after it is the first case.
    ///
    /// Under the published manifest's lock run only the record, the two
    /// follows with their stores, and the unlock of the engine. Dropping the
    /// positions that the follows replaced, which can free a large WAL tail,
    /// and handing a removed read position's tail to the read cache run
    /// after the unlock.
    pub(super) fn record_published(&self, published: &Held<LoadedManifest>) {
        let mut recorded = self.read_position.published().lock();
        recorded.record(published);
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
/// compaction, or delete panics, the worker, the table, the other sessions,
/// and the drain must keep working. Every critical section under these locks
/// only updates fields.
fn lock_state<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
