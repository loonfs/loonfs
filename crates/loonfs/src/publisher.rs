//! Runtime publication service for namespace mutations.
//!
//! Each namespace writer session has one queue. Concurrent commits may share
//! a WAL object and head update. Duplicate commit IDs join in flight,
//! conflicting reuse is rejected, and namespace deletion is ordered with
//! other mutations.
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
mod delete_barrier;
mod fold_trigger;
mod inline_content;
pub(crate) mod metrics;
mod queue;
mod registry;
mod session_tasks;
mod worker;
mod write_stop;

use crate::runtime_core::{ReadPositionSlot, RuntimeCore};
use crate::{CoreError, MetadataMaintenanceOutcome, SharedObjectStore, WriterState};
use admission::PublicationAdmission;
pub(crate) use fold_trigger::FoldTrigger;
use loonfs_core::publish::{NamespaceCommitEngine, SharedWriterSessionStatus, WriterSessionStatus};
use loonfs_core::time::Observation;
use loonfs_core::MetadataCompactionCancellation;
use loonfs_types::{ChangeSeq, CommitId, ManifestNo, NamespaceId};
use queue::{InFlightRequest, WorkItem};
pub use registry::{CloseNamespaceReport, NamespaceSessionStatus};
pub(crate) use registry::{NamespaceChange, NamespaceSession, PublisherRegistry};
use registry::{PositionChange, RegistryShared};
use session_tasks::{SessionTask, WorkerHandle};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::sync::Mutex as AsyncMutex;
use worker::elapsed_ms_from;

/// What every namespace publisher of one runtime shares: the runtime core,
/// the writer, the Tokio runtime that runs the publisher tasks, and the
/// publication admission.
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
    engine: Arc<AsyncMutex<EngineSlot>>,
    /// The engine's position as of the last release of the engine, which
    /// reads in this runtime start from.
    read_position: Arc<ReadPositionSlot>,
    session: SharedWriterSessionStatus,
    /// Weak: the session table owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    /// Set once the session stops admitting work. A session never compacts
    /// again after that.
    compaction_cancellation: MetadataCompactionCancellation,
    /// Signals each fold of this session that publishes, so a worker waiting
    /// at the write stop wakes once that fold frees room, even when the fold
    /// task goes on to fold again.
    fold_published: watch::Sender<()>,
}

/// Commit engine and writer session retained by one namespace publisher.
///
/// The session stores the acquired epoch and terminal fencing state. Those
/// values describe this process and can't be reconstructed from object
/// storage, so the publisher keeps them for its whole life. The engine keeps
/// its WAL tail with its position until the position is dropped.
struct EngineSlot {
    /// Built on the first unit of work and kept for the publisher's life.
    /// Invalidation drops only its position and the position's WAL tail.
    engine: Option<NamespaceCommitEngine<SharedObjectStore>>,
    /// Never dropped or rebuilt while the publisher lives.
    session: SharedWriterSessionStatus,
}

impl EngineSlot {
    fn wal_tail_inline_bytes(&self) -> Option<usize> {
        self.engine
            .as_ref()
            .and_then(NamespaceCommitEngine::wal_tail_inline_bytes)
    }
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
    slot: Option<tokio::sync::MutexGuard<'a, EngineSlot>>,
}

impl std::ops::Deref for LockedEngine<'_> {
    type Target = EngineSlot;

    fn deref(&self) -> &EngineSlot {
        self.slot.as_ref().expect("a locked engine keeps its guard")
    }
}

impl std::ops::DerefMut for LockedEngine<'_> {
    fn deref_mut(&mut self) -> &mut EngineSlot {
        self.slot.as_mut().expect("a locked engine keeps its guard")
    }
}

impl Drop for LockedEngine<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.publisher.release_engine(slot);
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
    last_published: Option<LastPublish>,
    metadata_caught_up: bool,
    /// The newest manifest that the engine published itself (a claim or a
    /// tombstone) and that the worker hasn't raised the hint to yet.
    unraised_manifest_no: Option<ManifestNo>,
}

/// The newest commit this session published, and when it published it.
#[derive(Debug, Clone, Copy)]
struct LastPublish {
    seq: ChangeSeq,
    at_ms: u64,
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
    pub(super) fn new(namespace_id: NamespaceId, shared: &Arc<RegistryShared>) -> Self {
        let session = SharedWriterSessionStatus::default();
        let read_position = Arc::new(ReadPositionSlot::new(
            shared.context.runtime_core.head_and_wal_tail_cache(),
        ));
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
                last_published: None,
                metadata_caught_up: true,
                unraised_manifest_no: None,
            })),
            engine: Arc::new(AsyncMutex::new(EngineSlot {
                engine: None,
                session: Arc::clone(&session),
            })),
            read_position,
            session,
            shared: Arc::downgrade(shared),
            compaction_cancellation: MetadataCompactionCancellation::default(),
            fold_published: watch::Sender::new(()),
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
            NamespaceSessionStatus::Closed
        } else if self.session_is_fenced() {
            NamespaceSessionStatus::Fenced
        } else {
            NamespaceSessionStatus::Open
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
    ) -> std::result::Result<std::sync::MutexGuard<'_, WriterSessionStatus>, CoreError> {
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
    ) -> std::result::Result<std::sync::MutexGuard<'_, WriterSessionStatus>, CoreError> {
        let session = lock_state(&self.session);
        if let WriterSessionStatus::Fenced(fence) = &*session {
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        Ok(session)
    }

    pub(super) fn session_is_fenced(&self) -> bool {
        matches!(*lock_state(&self.session), WriterSessionStatus::Fenced(_))
    }

    /// Applies `change` to the engine's position once no other task has the
    /// engine locked.
    ///
    /// Until then, reads keep the position they have. A position on a
    /// manifest that this runtime has replaced still reads correctly,
    /// because garbage collection keeps that manifest's objects for at least
    /// one grace.
    pub(super) async fn change_position(&self, change: PositionChange) {
        let mut slot = self.lock_engine().await;
        if let Some(engine) = slot.engine.as_mut() {
            change.apply(engine);
        }
    }

    pub(super) async fn lock_engine(&self) -> LockedEngine<'_> {
        LockedEngine {
            publisher: self,
            slot: Some(self.engine.lock().await),
        }
    }

    /// Stores the engine's position for reads, records a manifest that the
    /// engine published for the worker to raise the hint to, and releases
    /// the engine.
    ///
    /// The engine publishes a manifest when it acquires the session's writer
    /// epoch and when it deletes the namespace.
    pub(super) fn release_engine(&self, mut slot: tokio::sync::MutexGuard<'_, EngineSlot>) {
        if std::thread::panicking() {
            // The engine may be partway through a unit of work. Reads start
            // from the namespace's cached head until the next release.
            self.read_position.clear();
            return;
        }
        let position = slot
            .engine
            .as_ref()
            .and_then(NamespaceCommitEngine::read_position);
        match self.read_position.prepare(position) {
            Some(position) => self.read_position.store(position),
            None => {
                if self.read_position.clear() {
                    // The engine dropped its position because it found a
                    // change it can't follow, such as another writer's claim,
                    // so the position that just became the cached head may
                    // be out of date.
                    self.context
                        .runtime_core
                        .invalidate_namespace_read_cache(&self.namespace_id);
                }
            }
        }
        let published = slot
            .engine
            .as_mut()
            .and_then(NamespaceCommitEngine::take_published_manifest_no);
        drop(slot);
        if published.is_some() {
            let mut state = self.lock_state();
            state.unraised_manifest_no = state.unraised_manifest_no.max(published);
        }
    }

    /// Raises the namespace's hint to the newest manifest that the engine
    /// published itself since the last call, after the unit of work that
    /// published it has delivered its results.
    pub(super) async fn raise_hint_to_engine_publication(&self) {
        let published = self.lock_state().unraised_manifest_no.take();
        if let Some((manifest_no, shared)) = published.zip(self.shared.upgrade()) {
            shared.raise_hint(&self.namespace_id, manifest_no).await;
        }
    }

    pub(super) fn record_metadata_maintenance(
        &self,
        expected_seq: Option<ChangeSeq>,
        outcome: MetadataMaintenanceOutcome,
    ) {
        let mut state = self.lock_state();
        if state.last_published.map(|last| last.seq) == expected_seq {
            state.metadata_caught_up = outcome == MetadataMaintenanceOutcome::CaughtUp;
        }
    }

    pub(super) fn namespace_deleted(&self) -> CoreError {
        CoreError::NamespaceDeleted {
            namespace_id: self.namespace_id.clone(),
        }
    }

    pub(super) fn record_panic(&self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.panicked_units.fetch_add(1, Ordering::SeqCst);
        }
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
