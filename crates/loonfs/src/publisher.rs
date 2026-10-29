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
//! returns the session a handle already holds, and shutdown drains every
//! session. The host decides how long a session lives by holding its
//! writable [`Namespace`](crate::Namespace).

mod admission;
mod fold_trigger;
mod inline_content;

use crate::cache::{HeadPositions, ReadPositionSlot};
use crate::fs::{RuntimeCore, WriterBits};
use crate::metrics::{PublishOutcome, RESULT_OK};
use crate::publish::CommitCandidate;
use crate::trace::{phase_event, phase_span};
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, FoldWalOutcome,
    MaintenanceCancellation, MetadataMaintenanceOutcome,
};
use admission::{AdmissionPermit, AdmittedWaiter, PublicationAdmission};
pub(crate) use fold_trigger::FoldTrigger;
use futures::future::BoxFuture;
use futures::FutureExt;
use loonfs_core::commit::CommitFingerprint;
use loonfs_core::control::raise_namespace_hint;
use loonfs_core::limits::{
    FOLD_AT_WAL_OBJECTS, HINT_RAISE_INTERVAL_MS, MAX_CONTENTION_ATTEMPTS, MAX_UNFOLDED_WAL_OBJECTS,
};
use loonfs_core::publish::{
    is_retryable_wal_publish, settle_publish_attempt, NamespaceCommitEngine,
    SharedWriterSessionStatus, TailSource, WalFoldInput, WalPublishError, WriterSessionStatus,
};
use loonfs_core::time::{Deadline, Observation};
use loonfs_core::{PublicationReport, PublishedManifest};
use loonfs_types::api::v0::Commit;
use loonfs_types::format::wal::{MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use loonfs_types::MonotonicTimer;
use loonfs_types::{ChangeSeq, CommitId, ManifestNo, NamespaceId};
use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::Instrument;

type CommitResult = Result<Commit, Error>;
type DeleteResult = Result<DeleteNamespaceResponse, Error>;

/// Result of closing one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseNamespaceReport {
    /// Commits admitted before the close and published during the drain, or
    /// `None` when the session had already stopped admitting work.
    pub drained_commits: Option<usize>,
    /// Whether the closed session had been fenced.
    pub fenced: bool,
}

/// Current state of one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSessionStatus {
    /// The session admits work.
    Open,
    /// Another writer acquired a later epoch. Mutations fail with
    /// `writer_fenced` for the rest of the session.
    Fenced,
    /// The session admits no more work. It was closed, its namespace was
    /// deleted, or the runtime is shutting down.
    Closed,
}

/// The live writer sessions of one writer, and the publication admission
/// they share.
///
/// Clones share the same sessions and worker tasks. The table holds a
/// session while a writable [`Namespace`](crate::Namespace) holds it or
/// while work it admitted is still running. A fenced session leaves once
/// its work ends, even while handles still hold it. Nothing here decides how many
/// sessions exist or how long they live.
#[derive(Clone)]
pub(crate) struct PublisherRegistry {
    shared: Arc<RegistryShared>,
    /// Strong: the runtime core owns neither this registry nor the writer, so
    /// holding it here cannot cycle. Publications read through its caches,
    /// and the core keeps only a weak reference back (see
    /// [`Self::head_positions`]).
    runtime_core: RuntimeCore,
    /// Weak: the writer owns its bits, and a publication is the writer's
    /// work. Publish work upgrades per unit and reports `shutting_down`
    /// once the writer is gone, so dropping the writer stops new work
    /// without ever leaving the caches or store dangling.
    writer: Weak<WriterBits>,
    runtime: Handle,
    timer: Arc<dyn MonotonicTimer>,
    min_publish_interval: Duration,
}

/// State shared by all publishers in this registry: admission status, the
/// session table, contained panic count, and the hint raises that this
/// runtime's manifest publications started.
struct RegistryShared {
    admission: Arc<PublicationAdmission>,
    state: Mutex<RegistryState>,
    /// Publication and deletion units whose panic a task survived. Tasks
    /// contain panics to keep the registry usable, so this — not a task
    /// join error — is what a drain reports.
    panicked_units: AtomicUsize,
    hint_raises: HintRaises,
}

/// Raises each namespace's hint after this runtime's manifest publications,
/// at most once per `HINT_RAISE_INTERVAL_MS` for each namespace.
struct HintRaises {
    store: crate::SharedObjectStore,
    timer: Arc<dyn MonotonicTimer>,
    /// When each raise that started less than `HINT_RAISE_INTERVAL_MS` ago
    /// started, on the monotonic timer. Each raise removes the older
    /// entries, so the map lists at most the namespaces whose hints this
    /// runtime raised in the last interval.
    started_at_ms: Mutex<HashMap<NamespaceId, u64>>,
}

impl HintRaises {
    /// Raises the namespace's hint to `manifest_no`, a manifest that this
    /// runtime published, with one compare-and-swap, unless this runtime
    /// started a raise of the namespace's hint less than
    /// `HINT_RAISE_INTERVAL_MS` ago (format spec, "The current manifest and
    /// WAL tip").
    ///
    /// A skipped raise returns at once, and a failed raise is logged and
    /// ignored. Either leaves the hint behind the newest manifest until a
    /// later publication raises it, and discovery probes past a hint that
    /// lags.
    async fn raise(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        let now_ms = self.timer.monotonic_now_ms();
        {
            let mut started_at_ms = lock_state(&self.started_at_ms);
            started_at_ms.retain(|_, at_ms| now_ms < at_ms.saturating_add(HINT_RAISE_INTERVAL_MS));
            match started_at_ms.entry(namespace_id.clone()) {
                std::collections::hash_map::Entry::Occupied(_) => return,
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(now_ms),
            };
        }
        if let Err(error) =
            raise_namespace_hint(self.store.as_ref(), namespace_id, manifest_no, None).await
        {
            tracing::debug!(
                namespace_id = %namespace_id,
                manifest_no = %manifest_no,
                error = %error,
                "the namespace hint raise failed, and a later publication raises the hint"
            );
        }
    }
}

struct RegistryState {
    closed: bool,
    sessions: HashMap<NamespaceId, LiveSession>,
}

/// One entry in the table of live sessions.
struct LiveSession {
    publisher: NamespacePublisher,
    /// Weak: the handles own the session, so the table must not keep it
    /// alive.
    session: Weak<NamespaceSession>,
}

/// One namespace's writer session, shared by every clone of its writable
/// [`Namespace`](crate::Namespace).
///
/// Dropping the last one ends the session. Work it already admitted still
/// publishes, and the table forgets the session once that work finishes.
pub(crate) struct NamespaceSession {
    publisher: NamespacePublisher,
}

impl Drop for NamespaceSession {
    fn drop(&mut self) {
        self.publisher.forget_if_ended();
    }
}

impl NamespaceSession {
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        &self.publisher.namespace_id
    }

    /// Submits one already-classified candidate. Admitted work continues if
    /// the caller is cancelled.
    pub(crate) async fn submit_candidate(&self, candidate: CommitCandidate) -> CommitResult {
        self.publisher.submit_candidate(candidate).await
    }

    /// Submits a namespace deletion, sequenced as a barrier: mutations
    /// admitted before it publish first, and mutations admitted after it
    /// fail once the delete succeeds.
    pub(crate) async fn submit_delete(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        self.publisher.submit_delete(options).await
    }

    pub(crate) fn state(&self) -> NamespaceSessionStatus {
        self.publisher.session_status()
    }

    pub(crate) fn last_published_seq(&self) -> Option<ChangeSeq> {
        self.publisher
            .lock_state()
            .last_published
            .map(|last| last.seq)
    }

    pub(crate) fn last_published_at_ms(&self) -> Option<u64> {
        self.publisher
            .lock_state()
            .last_published
            .map(|last| last.at_ms)
    }

    pub(crate) fn metadata_caught_up(&self) -> bool {
        self.publisher.lock_state().metadata_caught_up
    }

    pub(crate) fn idle_fold_due_at_ms(&self) -> Option<u64> {
        let published_at_ms = self.last_published_at_ms()?;
        self.publisher
            .fold_trigger
            .idle_fold_due_at_ms(published_at_ms)
    }

    pub(crate) fn record_metadata_maintenance(
        &self,
        expected_seq: Option<ChangeSeq>,
        outcome: MetadataMaintenanceOutcome,
    ) {
        self.publisher
            .record_metadata_maintenance(expected_seq, outcome);
    }

    pub(crate) async fn wait_for_fold(&self) -> Result<(), Error> {
        self.publisher.wait_for_fold().await
    }

    /// Refuses new work from every handle that shares this session, cancels
    /// its compaction, waits for admitted work, the running fold, and the
    /// compaction, and forgets the session.
    ///
    /// The worker, the fold task, and the compaction task forget the session
    /// too when they exit, so a cancelled close still leaves the table clean.
    pub(crate) async fn close(&self) -> Result<CloseNamespaceReport, CoreError> {
        let publisher = &self.publisher;
        let drained_commits = publisher.close_session()?;
        publisher.compaction_cancellation.cancel();
        publisher.forget_if_ended();
        publisher.wait_for_worker().await;
        if let Err(error) = publisher.wait_for_fold().await {
            phase_event!(
                publisher.runtime_core,
                "wal_fold",
                publisher.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "wal fold failed while the namespace session closed"
            );
        }
        if let Err(error) = publisher.wait_for_compaction().await {
            phase_event!(
                publisher.runtime_core,
                "metadata_compaction",
                publisher.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "metadata compaction failed while the namespace session closed"
            );
        }
        publisher.forget_if_ended();
        Ok(CloseNamespaceReport {
            drained_commits,
            fenced: publisher.session_is_fenced(),
        })
    }
}

impl RegistryShared {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        lock_state(&self.state)
    }

    fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| live.publisher.clone())
    }
}

impl HeadPositions for RegistryShared {
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| Arc::clone(&live.publisher.read_position))
    }

    fn drop_position<'a>(&'a self, namespace_id: &'a NamespaceId) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(publisher) = self.live_publisher(namespace_id) {
                publisher.change_position(PositionChange::Drop).await;
            }
        })
    }

    fn move_position<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
        published: PublishedManifest,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(publisher) = self.live_publisher(namespace_id) {
                publisher
                    .change_position(PositionChange::Move(published))
                    .await;
            }
        })
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

impl PublisherRegistry {
    /// Creates the registry a writer owns. Batches publish through each
    /// publisher's own commit engine and writer session.
    pub(crate) fn new(
        runtime_core: RuntimeCore,
        writer: Weak<WriterBits>,
        runtime: Handle,
        min_publish_interval: Duration,
        publication_policy: crate::PublicationPolicy,
        execution_budget: crate::ExecutionBudget,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                admission: Arc::new(PublicationAdmission::new(
                    publication_policy,
                    execution_budget,
                )),
                state: Mutex::new(RegistryState {
                    closed: false,
                    sessions: HashMap::new(),
                }),
                panicked_units: AtomicUsize::new(0),
                hint_raises: HintRaises {
                    store: runtime_core.shared_store(),
                    timer: Arc::clone(&runtime_core.inner.timer),
                    started_at_ms: Mutex::default(),
                },
            }),
            timer: Arc::clone(&runtime_core.inner.timer),
            runtime_core,
            writer,
            runtime,
            min_publish_interval,
        }
    }

    /// Returns the session for `namespace_id`, starting one when the table
    /// has none.
    ///
    /// A session whose last handle dropped stays in the table until its
    /// admitted work finishes, and an open before then continues it: a
    /// second session would acquire a new epoch and fence that work. A fenced
    /// session cannot continue: an open fails while its work ends, then starts
    /// a new session even while old handles are held.
    pub(crate) fn open_session(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Arc<NamespaceSession>, CoreError> {
        let mut state = self.shared.lock_state();
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        if let Some(live) = state.sessions.get(namespace_id) {
            if live.publisher.session_is_fenced() {
                if !live.publisher.lock_state().has_ended(false) {
                    return Err(CoreError::WriterSessionClosed {
                        namespace_id: namespace_id.clone(),
                    });
                }
                live.publisher.read_position.clear();
                state.sessions.remove(namespace_id);
            }
        }
        if let Some(live) = state.sessions.get_mut(namespace_id) {
            if live.publisher.session_closed() {
                return Err(CoreError::WriterSessionClosed {
                    namespace_id: namespace_id.clone(),
                });
            }
            if let Some(session) = live.session.upgrade() {
                return Ok(session);
            }
            let session = Arc::new(NamespaceSession {
                publisher: live.publisher.clone(),
            });
            live.session = Arc::downgrade(&session);
            return Ok(session);
        }
        let publisher = NamespacePublisher::new(namespace_id.clone(), self);
        let session = Arc::new(NamespaceSession {
            publisher: publisher.clone(),
        });
        state.sessions.insert(
            namespace_id.clone(),
            LiveSession {
                publisher,
                session: Arc::downgrade(&session),
            },
        );
        self.runtime_core
            .instruments()
            .publisher_sessions(state.sessions.len());
        Ok(session)
    }

    /// Tells the namespace's publisher how a mutation outside the publisher
    /// changed the namespace, and drops the namespace's cached head when the
    /// mutation may have changed the namespace in ways this runtime doesn't
    /// know.
    pub(crate) async fn invalidate_namespace(
        &self,
        namespace_id: &NamespaceId,
        change: NamespaceChange,
    ) {
        match change {
            NamespaceChange::Published(published) => {
                self.record_manifest_published(namespace_id, published)
                    .await;
            }
            NamespaceChange::Unchanged => {}
            NamespaceChange::Unknown => {
                self.runtime_core
                    .invalidate_namespace_read_cache(namespace_id);
                if let Some(publisher) = self.live_publisher(namespace_id) {
                    publisher.change_position(PositionChange::Drop).await;
                }
            }
        }
    }

    /// Runs [`Self::invalidate_namespace`] after a mutation's result when
    /// [`should_invalidate_after_result`](crate::fs::should_invalidate_after_result)
    /// selects it. `change` describes a successful result. A failed result
    /// may have changed the namespace in ways this runtime doesn't know.
    pub(crate) async fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T, Error>,
        change: impl FnOnce(&T) -> NamespaceChange,
    ) -> Result<T, Error> {
        if crate::fs::should_invalidate_after_result(&result) {
            let change = result.as_ref().map_or(NamespaceChange::Unknown, change);
            self.invalidate_namespace(namespace_id, change).await;
        }
        result
    }

    /// Moves the namespace's publisher to a manifest that this runtime
    /// published, or drops the publisher's position when it can't move
    /// there, so that later commits and reads in this runtime start from the
    /// newest manifest, and then raises the namespace's hint to the manifest
    /// (see [`Self::raise_hint`]).
    ///
    /// The hint of a namespace without a live session is raised too, because
    /// a maintenance call or a deletion can publish a manifest in that
    /// namespace.
    pub(crate) async fn record_manifest_published(
        &self,
        namespace_id: &NamespaceId,
        published: PublishedManifest,
    ) {
        let manifest_no = published.manifest.manifest_no;
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher
                .change_position(PositionChange::Move(published))
                .await;
        }
        self.raise_hint(namespace_id, manifest_no).await;
    }

    fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.shared.live_publisher(namespace_id)
    }

    /// Returns the writer sessions that this runtime's reads start from.
    pub(crate) fn head_positions(&self) -> Weak<dyn HeadPositions> {
        Arc::downgrade(&self.shared) as Weak<dyn HeadPositions>
    }

    pub(crate) async fn wal_tail_inline_bytes(&self, namespace_id: &NamespaceId) -> Option<usize> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.wal_tail_inline_bytes()
    }

    /// Returns the namespace's writer position that reads in this runtime
    /// start from, as a fold input, or `None` when this runtime has no
    /// position for the namespace or the namespace's session is fenced.
    ///
    /// Unlike [`Self::wal_fold_input`], this function doesn't wait for a
    /// publication that the session's engine is running, so the position
    /// doesn't include that publication's commits.
    pub(crate) fn read_position_fold_input(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<WalFoldInput> {
        let position = self.shared.read_position_slot(namespace_id)?.current()?;
        Some(position.wal_fold_input())
    }

    /// Returns the namespace's writer position as a fold input: the manifest
    /// it plans against and the unfolded WAL tail above that manifest. Returns
    /// `None` when this runtime has no position for the namespace.
    pub(crate) async fn wal_fold_input(&self, namespace_id: &NamespaceId) -> Option<WalFoldInput> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.engine
            .as_ref()
            .and_then(NamespaceCommitEngine::wal_fold_input)
    }

    /// Whether [`Self::close_admission`] has run: later submissions fail
    /// with `shutting_down`. Readiness probes report this state.
    pub(crate) fn is_admission_closed(&self) -> bool {
        self.shared.lock_state().closed
    }

    /// Stops accepting new submissions while allowing admitted work to finish.
    ///
    /// Later submissions fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub(crate) fn close_admission(&self) {
        let publishers: Vec<NamespacePublisher> = {
            let mut state = self.shared.lock_state();
            state.closed = true;
            state
                .sessions
                .values()
                .map(|live| live.publisher.clone())
                .collect()
        };
        for publisher in publishers {
            publisher.close_admission();
        }
    }

    /// Waits for all current publisher workers, folds, and compactions to
    /// finish, and with them the hint raises that their publications started.
    ///
    /// Folds are waited for before compactions, because a fold starts its
    /// session's compaction.
    ///
    /// Returns an error if any publication, deletion, fold, or compaction
    /// panicked and its task contained the panic.
    pub(crate) async fn drain(&self) -> Result<(), Error> {
        let publishers: Vec<NamespacePublisher> = self
            .shared
            .lock_state()
            .sessions
            .values()
            .map(|live| live.publisher.clone())
            .collect();
        for publisher in &publishers {
            publisher.wait_for_worker().await;
        }
        let mut task_error = None;
        for publisher in &publishers {
            if let Err(error) = publisher.wait_for_fold().await {
                task_error.get_or_insert(error);
            }
        }
        for publisher in &publishers {
            if let Err(error) = publisher.wait_for_compaction().await {
                task_error.get_or_insert(error);
            }
        }
        let panicked = self.shared.panicked_units.load(Ordering::SeqCst);
        if panicked > 0 {
            return Err(Error::RuntimeTask(format!(
                "{panicked} publisher task(s) panicked"
            )));
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(())
    }

    /// Raises the namespace's hint to `manifest_no`, a manifest that this
    /// runtime published, after the publication has returned, unless this
    /// runtime started a raise of that hint less than
    /// `HINT_RAISE_INTERVAL_MS` ago.
    pub(crate) async fn raise_hint(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        self.shared
            .hint_raises
            .raise(namespace_id, manifest_no)
            .await;
    }
}

/// How a mutation outside the publisher, such as a maintenance call, changed
/// a namespace, as the namespace's publisher needs to know it.
#[derive(Debug, Clone)]
pub(crate) enum NamespaceChange {
    /// The mutation published this manifest. The publisher moves to it, or
    /// drops its position when it can't move there. The cached head stays,
    /// because the manifest is at the head that the namespace already had.
    Published(PublishedManifest),
    /// The mutation published no manifest, such as a garbage collection pass
    /// or a change to a pin. The publisher keeps its position, and the
    /// cached head stays confirmed, because the head didn't move.
    Unchanged,
    /// The mutation may have changed the namespace in ways this runtime
    /// doesn't know, such as creating it or failing with a stale head. The
    /// publisher drops its position, and the runtime drops its cached head.
    Unknown,
}

impl NamespaceChange {
    /// Returns the change that a maintenance call's report describes.
    pub(crate) fn of_report<T>(report: &PublicationReport<T>) -> Self {
        report
            .published
            .clone()
            .map_or(Self::Unchanged, Self::Published)
    }
}

/// A change to the engine's position that a mutation outside the publisher
/// asked for.
#[derive(Debug)]
enum PositionChange {
    /// Moves the position onto a manifest above it under the session's
    /// writer epoch, or drops the position when it can't move there.
    Move(PublishedManifest),
    /// Drops the position.
    Drop,
}

impl PositionChange {
    fn apply(self, engine: &mut NamespaceCommitEngine) {
        match self {
            Self::Move(published) => engine.record_manifest_published(published),
            Self::Drop => engine.invalidate_projection(),
        }
    }
}

/// The commit queue and publication worker for one namespace's writer
/// session.
#[derive(Clone)]
struct NamespacePublisher {
    admission: Arc<PublicationAdmission>,
    namespace_id: NamespaceId,
    runtime_core: RuntimeCore,
    /// Weak for the same reason as the registry's reference: a publication
    /// is the owning writer's work, and it stops when that writer is gone.
    writer: Weak<WriterBits>,
    state: Arc<Mutex<NamespacePublisherState>>,
    /// Admission holds this lock until it reserves bytes against the observed
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
    runtime: Handle,
    timer: Arc<dyn MonotonicTimer>,
    min_publish_interval: Duration,
    inline_content: crate::InlineContentPolicy,
    fold_trigger: FoldTrigger,
    /// Set once the session stops admitting work. A session never compacts
    /// again after that.
    compaction_cancellation: MaintenanceCancellation,
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
    engine: Option<NamespaceCommitEngine>,
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
    /// publish. A session is over once no handle holds it, or once it was
    /// closed or its namespace deleted.
    fn has_ended(&self, held: bool) -> bool {
        let deleted = matches!(self.admission, PublisherAdmissionStatus::Deleted);
        let over =
            !held || deleted || matches!(self.admission, PublisherAdmissionStatus::SessionClosed);
        // A deleted session's worker takes nothing more from its queue.
        let worker_running = !deleted
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| !*worker.liveness.borrow());
        let fold_running = self.fold.as_ref().is_some_and(SessionTask::is_running);
        let compaction_running = self
            .compaction
            .as_ref()
            .is_some_and(SessionTask::is_running);
        over && !worker_running && !fold_running && !compaction_running
    }
}

struct WorkerHandle {
    _task: JoinHandle<()>,
    liveness: watch::Receiver<bool>,
}

/// A fold or a compaction that a session runs beside its queue.
struct SessionTask {
    id: u64,
    task: JoinHandle<()>,
    liveness: watch::Receiver<bool>,
}

impl SessionTask {
    fn is_running(&self) -> bool {
        !*self.liveness.borrow()
    }
}

/// Marks a worker or session task as exited when the task ends, even when
/// it panics.
struct TaskExit(watch::Sender<bool>);

impl Drop for TaskExit {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

struct PendingDelete {
    options: DeleteNamespaceOptions,
    waiters: Vec<AdmittedWaiter<DeleteResult>>,
}

enum WorkItem {
    Batch(OpenBatch),
    Delete(PendingDelete),
}

struct OpenBatch {
    candidates: Vec<BatchCandidate>,
    wal_record_bytes_upper_bound: usize,
    inline_content_bytes: usize,
}

struct PreparedCandidate {
    candidate: CommitCandidate,
    estimated_retained_bytes: usize,
    wal_record_bytes_upper_bound: usize,
    inline_content_bytes: usize,
}

impl PreparedCandidate {
    fn new(candidate: CommitCandidate) -> Result<Self, CoreError> {
        Ok(Self {
            estimated_retained_bytes: candidate.estimated_retained_bytes()?,
            wal_record_bytes_upper_bound: candidate.wal_record_bytes_upper_bound(),
            inline_content_bytes: candidate.inline_content_bytes(),
            candidate,
        })
    }

    fn with_inline_placement(
        candidate: CommitCandidate,
        namespace_id: &NamespaceId,
        retained_inline_content: &[loonfs_core::publish::InlineContent],
        staged_inline_content: &[loonfs_core::publish::InlineContent],
    ) -> Result<Self, CoreError> {
        Ok(Self {
            estimated_retained_bytes: candidate.estimated_retained_bytes_after_inline_staging(
                namespace_id,
                retained_inline_content,
                staged_inline_content,
            )?,
            wal_record_bytes_upper_bound: candidate
                .wal_record_bytes_upper_bound_with_inline_content(retained_inline_content),
            inline_content_bytes: retained_inline_content
                .iter()
                .fold(candidate.appended_bytes(), |total, value| {
                    total.saturating_add(value.bytes().len())
                }),
            candidate,
        })
    }
}

#[derive(Clone)]
struct BatchCandidate {
    permit: Arc<AdmissionPermit>,
    commit_id: CommitId,
    candidate: CommitCandidate,
    enqueued_at: u64,
    /// Whether this commit was queued when a wait at the write stop ended, so
    /// `write_stop_waits` records it once.
    write_stop_wait_recorded: bool,
    /// Whether the engine refused this commit at the write stop when the
    /// worker last published it.
    ///
    /// Only such a commit can reach the end of the write-stop wait without
    /// another publish, because the engine returns a retry's result from its
    /// receipt.
    refused_by_write_stop: bool,
}

struct InFlightRequest {
    semantic_identity: CommitFingerprint,
    waiters: Vec<AdmittedWaiter<CommitResult>>,
}

enum SubmissionAdmission {
    /// This submission owns the published outcome, either as the primary or
    /// as an exact duplicate of it.
    OwnOutcome,
    /// A different claim currently owns the commit ID. Its successful outcome
    /// supplies the receipt evidence this submission needs for a useful
    /// conflict; if it fails, this submission gets another turn at admission.
    Contended {
        primary_identity: CommitFingerprint,
        candidate: Box<PreparedCandidate>,
        semantic_identity: CommitFingerprint,
    },
}

impl NamespacePublisher {
    fn new(namespace_id: NamespaceId, registry: &PublisherRegistry) -> Self {
        let session = SharedWriterSessionStatus::default();
        let read_position = Arc::new(ReadPositionSlot::new(
            registry.runtime_core.head_and_wal_tail_cache(),
        ));
        let (inline_content, fold_trigger) = registry
            .writer
            .upgrade()
            .map(|writer| (writer.inline_content.clone(), writer.fold_trigger))
            .unwrap_or_default();
        Self {
            namespace_id,
            runtime_core: registry.runtime_core.clone(),
            writer: registry.writer.clone(),
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
            shared: Arc::downgrade(&registry.shared),
            runtime: registry.runtime.clone(),
            timer: Arc::clone(&registry.timer),
            min_publish_interval: registry.min_publish_interval,
            admission: Arc::clone(&registry.shared.admission),
            inline_content,
            fold_trigger,
            compaction_cancellation: MaintenanceCancellation::new(),
            fold_published: watch::Sender::new(()),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, NamespacePublisherState> {
        lock_state(&self.state)
    }

    /// Open becomes closed; a delete that already landed is terminal and
    /// stays terminal. Either way the session's compaction is cancelled.
    fn close_admission(&self) {
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
    fn close_session(&self) -> Result<Option<usize>, CoreError> {
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

    fn session_closed(&self) -> bool {
        matches!(
            self.lock_state().admission,
            PublisherAdmissionStatus::SessionClosed
        )
    }

    fn session_status(&self) -> NamespaceSessionStatus {
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
    /// exit, the fold's exit, a close, and a landed delete each call this,
    /// so whichever of them comes last removes the session. Fenced sessions
    /// end even while handles are held.
    /// A session the registry has forgotten never starts a task and never
    /// holds queued work. Admission and task starts hold the writer-session
    /// lock through their decision, so fencing prevents both before removal.
    fn forget_if_ended(&self) {
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
        self.runtime_core
            .instruments()
            .publisher_sessions(registry.sessions.len());
    }

    fn admitted_session(
        &self,
        state: &NamespacePublisherState,
    ) -> Result<std::sync::MutexGuard<'_, WriterSessionStatus>, CoreError> {
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
    fn unfenced_session(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, WriterSessionStatus>, CoreError> {
        let session = lock_state(&self.session);
        if let WriterSessionStatus::Fenced(fence) = &*session {
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        Ok(session)
    }

    fn session_is_fenced(&self) -> bool {
        matches!(*lock_state(&self.session), WriterSessionStatus::Fenced(_))
    }

    /// Applies `change` to the engine's position once no other task has the
    /// engine locked.
    ///
    /// Until then, reads keep the position they have. A position on a
    /// manifest that this runtime has replaced still reads correctly,
    /// because garbage collection keeps that manifest's objects for at least
    /// one grace.
    async fn change_position(&self, change: PositionChange) {
        let mut slot = self.lock_engine().await;
        if let Some(engine) = slot.engine.as_mut() {
            change.apply(engine);
        }
    }

    async fn lock_engine(&self) -> LockedEngine<'_> {
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
    fn release_engine(&self, mut slot: tokio::sync::MutexGuard<'_, EngineSlot>) {
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
                    self.runtime_core
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
    async fn raise_hint_to_engine_publication(&self) {
        let published = self.lock_state().unraised_manifest_no.take();
        if let Some((manifest_no, shared)) = published.zip(self.shared.upgrade()) {
            shared
                .hint_raises
                .raise(&self.namespace_id, manifest_no)
                .await;
        }
    }

    /// Places the candidate's inline content, then admits it before awaiting
    /// its result.
    ///
    /// Once the candidate enters the queue, cancelling the caller only drops
    /// result delivery; the worker still owns and publishes the request.
    async fn submit_candidate(&self, candidate: CommitCandidate) -> CommitResult {
        drop(self.admitted_session(&self.lock_state())?);
        let plan = self.plan_inline_candidate(candidate)?;
        let permit = self.acquire_candidate(&plan.candidate)?;
        let candidate = self.stage_inline_candidate(plan, &permit).await?;
        PublicationAdmission::validate_candidate(&candidate)?;
        submit_with_admission(
            &self.namespace_id,
            candidate,
            self.timer.as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                self.admit(
                    commit_id,
                    candidate,
                    semantic_identity,
                    AdmittedWaiter::new(waiter, &permit),
                    enqueued_at,
                )
            },
        )
        .await
    }

    /// Admits the request before awaiting its result.
    ///
    /// The first poll either admits the candidate, waits behind an in-flight
    /// claim on its commit ID, or returns an error. Once the candidate enters
    /// the queue, cancelling the caller only drops result delivery; the worker
    /// still owns and publishes the request.
    #[cfg(test)]
    async fn submit(&self, candidate: CommitCandidate) -> CommitResult {
        let candidate = PreparedCandidate::new(candidate)?;
        let permit = self.acquire_candidate(&candidate)?;
        submit_with_admission(
            &self.namespace_id,
            candidate,
            self.timer.as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                self.admit(
                    commit_id,
                    candidate,
                    semantic_identity,
                    AdmittedWaiter::new(waiter, &permit),
                    enqueued_at,
                )
            },
        )
        .await
    }

    fn acquire_candidate(
        &self,
        candidate: &PreparedCandidate,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        let state = self.lock_state();
        let _session = self.admitted_session(&state)?;
        self.admission
            .acquire_candidate(&self.namespace_id, candidate)
    }

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: PreparedCandidate,
        semantic_identity: CommitFingerprint,
        waiter: AdmittedWaiter<CommitResult>,
        enqueued_at: u64,
    ) -> Result<SubmissionAdmission, CoreError> {
        let mut state = self.lock_state();
        let _session = self.admitted_session(&state)?;
        if let Some(existing) = state.in_flight.get_mut(&commit_id) {
            if existing.semantic_identity != semantic_identity {
                let primary_identity = existing.semantic_identity.clone();
                existing.waiters.push(waiter);
                self.trace_enqueue(queued_candidates(&state), "contended");
                return Ok(SubmissionAdmission::Contended {
                    primary_identity,
                    candidate: Box::new(candidate),
                    semantic_identity,
                });
            }
            existing.waiters.push(waiter);
            self.trace_enqueue(queued_candidates(&state), "duplicate");
            return Ok(SubmissionAdmission::OwnOutcome);
        }

        let queued = queued_candidates(&state);
        let wal_record_bytes_upper_bound = candidate.wal_record_bytes_upper_bound;
        let inline_content_bytes = candidate.inline_content_bytes;
        let candidate = BatchCandidate {
            permit: Arc::clone(&waiter.permit),
            commit_id: commit_id.clone(),
            candidate: candidate.candidate,
            enqueued_at,
            write_stop_wait_recorded: false,
            refused_by_write_stop: false,
        };
        match state.queue.back_mut() {
            // Coalesce with the tail batch while its bound stays under the
            // WAL object limit. A delete at the tail, or a full batch, opens a
            // batch behind it; work behind a delete publishes only if that
            // delete fails.
            Some(WorkItem::Batch(batch))
                if batch
                    .wal_record_bytes_upper_bound
                    .saturating_add(wal_record_bytes_upper_bound)
                    .saturating_add(WAL_OBJECT_OVERHEAD_BYTES)
                    <= MAX_WAL_OBJECT_BYTES
                    && batch
                        .inline_content_bytes
                        .saturating_add(inline_content_bytes)
                        <= self.inline_content.inline_content_wal_object_budget_bytes =>
            {
                batch.wal_record_bytes_upper_bound += wal_record_bytes_upper_bound;
                batch.inline_content_bytes += inline_content_bytes;
                batch.candidates.push(candidate);
            }
            _ => state.queue.push_back(WorkItem::Batch(OpenBatch {
                candidates: vec![candidate],
                wal_record_bytes_upper_bound,
                inline_content_bytes,
            })),
        }
        self.trace_enqueue(queued + 1, "new");
        state.in_flight.insert(
            commit_id,
            InFlightRequest {
                semantic_identity,
                waiters: vec![waiter],
            },
        );
        self.spawn_worker_if_idle(&mut state);
        Ok(SubmissionAdmission::OwnOutcome)
    }

    /// Enqueues the delete as a barrier: requests admitted before it
    /// publish first, and requests admitted after it fail with
    /// `namespace_deleted` once it succeeds. If the delete fails (for
    /// example a stale `expected_head_seq`), later requests publish
    /// normally — nothing is rejected for a delete that did not happen.
    async fn submit_delete(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        let receiver = self.admit_delete(options)?;
        receive_delete(receiver).await
    }

    fn admit_delete(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<oneshot::Receiver<DeleteResult>, CoreError> {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.lock_state();
            let _session = self.admitted_session(&state)?;
            let permit = self.admission.acquire(&self.namespace_id, 0)?;
            let sender = AdmittedWaiter::new(sender, &permit);
            match state.queue.back_mut() {
                // A delete queued with the same options is the same request:
                // both callers share its outcome. Different options ask for
                // different operations and settle separately, in order.
                Some(WorkItem::Delete(pending)) if pending.options == options => {
                    pending.waiters.push(sender);
                }
                _ => state.queue.push_back(WorkItem::Delete(PendingDelete {
                    options,
                    waiters: vec![sender],
                })),
            }
            self.spawn_worker_if_idle(&mut state);
        }
        Ok(receiver)
    }

    /// Makes sure a worker owns this publisher's queue.
    ///
    /// Callers hold the state lock, so admitting work and installing the
    /// task that owns it is atomic: no second worker takes the same queue,
    /// and a shutdown drain that finds no worker cannot miss work an
    /// admission is about to queue.
    fn spawn_worker_if_idle(&self, state: &mut NamespacePublisherState) {
        if state
            .worker
            .as_ref()
            .is_some_and(|worker| !*worker.liveness.borrow())
        {
            return;
        }
        let publisher = self.clone();
        let (task, liveness) = self.spawn_tracked(|exit| async move {
            let _exit = exit;
            publisher.run_worker().await;
        });
        state.worker = Some(WorkerHandle {
            _task: task,
            liveness,
        });
    }

    /// Drains the queue in admission order, then exits.
    async fn run_worker(self) {
        loop {
            let collect_started = self.timer.monotonic_now_ms();
            let queue_depth_start = queued_candidates(&self.lock_state());
            // Do not add a separate batching delay. A request for an idle namespace
            // publishes immediately; requests that queue during a publish or its
            // pacing interval form the next batch.
            self.await_publish_slot().await;
            // Queue ownership and admission remain intact while another
            // namespace uses the shared publication slots. No engine is held.
            let publication = self.admission.publication_permit().await;
            let Some(item) = self.take_next_item() else {
                self.forget_if_ended();
                return;
            };

            match item {
                WorkItem::Batch(batch) => {
                    // Subscribed before the publish, so a fold that publishes
                    // after the engine refuses commits at the write stop still
                    // wakes the wait below.
                    let fold_published = self.fold_published.subscribe();
                    self.runtime_core
                        .instruments()
                        .publisher_batch(batch.candidates.len());
                    phase_event!(
                        self.runtime_core,
                        "batch_collect",
                        self.namespace_id,
                        tracing::Level::INFO,
                        batch_size = usize_to_u64(batch.candidates.len()),
                        queue_depth_start = usize_to_u64(queue_depth_start),
                        queue_depth_end = usize_to_u64(batch.candidates.len()),
                        collect_ms = self.elapsed_ms_since(collect_started)
                    );
                    let OpenBatch {
                        candidates,
                        wal_record_bytes_upper_bound,
                        inline_content_bytes,
                    } = batch;
                    let waiting = self.publish_batch(candidates).await;
                    drop(publication);
                    self.raise_hint_to_engine_publication().await;
                    if !waiting.is_empty() {
                        let waiting = OpenBatch {
                            candidates: waiting,
                            wal_record_bytes_upper_bound,
                            inline_content_bytes,
                        };
                        self.wait_at_write_stop(waiting, fold_published).await;
                    }
                }
                WorkItem::Delete(pending) => {
                    // A delete is bounded by its fold permit alone. Holding a
                    // publication permit while it waits for one would stall
                    // other namespaces' publications behind folds.
                    drop(publication);
                    let deleted = self.execute_delete(pending).await;
                    self.raise_hint_to_engine_publication().await;
                    if deleted {
                        return;
                    }
                }
            }
        }
    }

    /// Takes the next unit of work, or releases the worker slot.
    ///
    /// Ownership is released under the same lock that finds the queue empty,
    /// so a racing admission either queued before this check and is taken
    /// here, or finds no worker and spawns one.
    fn take_next_item(&self) -> Option<WorkItem> {
        let mut state = self.lock_state();
        // Terminal: a successful delete emptied the queue and set this
        // before its worker returned, so nothing may be taken afterwards.
        if matches!(state.admission, PublisherAdmissionStatus::Deleted) || state.queue.is_empty() {
            state.worker = None;
            return None;
        }
        let item = state.queue.pop_front();
        self.reserve_next_publish_slot(&mut state);
        let queue_depth = queued_candidates(&state);
        drop(state);
        self.runtime_core
            .instruments()
            .publisher_queue_depth(queue_depth);
        item
    }

    /// Keeps the commits that the engine refused at the write stop at the
    /// front of the queue, and waits until a fold of this session publishes,
    /// the fold task exits, or the oldest of the commits reaches the
    /// write-stop wait.
    ///
    /// A fold that fails, or finds its tail already folded, publishes
    /// nothing, so the fold task's exit also ends the wait. The worker then
    /// publishes the queue again, and a refusal at the write stop starts the
    /// next fold.
    async fn wait_at_write_stop(
        &self,
        waiting: OpenBatch,
        mut fold_published: watch::Receiver<()>,
    ) {
        let oldest_enqueued_at = waiting.candidates[0].enqueued_at;
        let fold_task = {
            let mut state = self.lock_state();
            state.queue.push_front(WorkItem::Batch(waiting));
            state.fold.as_ref().map(|fold| fold.liveness.clone())
        };
        let started_at_ms = self.timer.monotonic_now_ms();
        let remaining_ms = oldest_enqueued_at
            .saturating_add(self.admission.write_stop_wait_budget_ms())
            .saturating_sub(started_at_ms);
        tokio::select! {
            _ = fold_published.changed() => {}
            () = wait_for_fold_task_exit(fold_task) => {}
            () = wait_for_write_stop_budget(Duration::from_millis(remaining_ms)) => {}
        }
        self.runtime_core
            .instruments()
            .publisher_write_stop_wait_duration(self.elapsed_ms_since(started_at_ms));
        self.end_write_stop_wait();
    }

    /// Records each queued commit that waited at the write stop for the first
    /// time, and returns `maintenance_required` to each commit that the
    /// engine refused at the write stop and that has waited for the whole
    /// write-stop wait, measured from when it entered the queue.
    ///
    /// A commit queued behind those publishes next, so the engine examines
    /// it before it can be refused.
    fn end_write_stop_wait(&self) {
        let budget_ms = self.admission.write_stop_wait_budget_ms();
        let now_ms = self.timer.monotonic_now_ms();
        let mut first_waits = 0;
        let mut refused = Vec::new();
        {
            let mut state = self.lock_state();
            for item in &mut state.queue {
                if let WorkItem::Batch(batch) = item {
                    batch.candidates.retain_mut(|candidate| {
                        if !candidate.write_stop_wait_recorded {
                            candidate.write_stop_wait_recorded = true;
                            first_waits += 1;
                        }
                        let past = candidate.refused_by_write_stop
                            && elapsed_ms_from(candidate.enqueued_at, now_ms) >= budget_ms;
                        if past {
                            refused.push(candidate.commit_id.clone());
                        }
                        !past
                    });
                }
            }
            state.queue.retain(
                |item| !matches!(item, WorkItem::Batch(batch) if batch.candidates.is_empty()),
            );
        }
        let instruments = self.runtime_core.instruments();
        instruments.publisher_write_stop_wait(usize_to_u64(first_waits));
        if refused.is_empty() {
            return;
        }
        instruments.publisher_write_stop_refusal(usize_to_u64(refused.len()));
        let error = Error::Core(
            crate::MetadataViewError::MaintenanceRequired {
                namespace_id: self.namespace_id.clone(),
                reason: format!(
                    "WAL tail stayed at {MAX_UNFOLDED_WAL_OBJECTS} objects through the write-stop wait of {budget_ms} ms, and publishes resume once a fold brings it back under {MAX_UNFOLDED_WAL_OBJECTS}"
                ),
            }
            .into(),
        );
        let results = refused.iter().map(|_| Err(error.clone())).collect();
        self.deliver_batch_results(refused, results, now_ms);
    }

    /// Publishes one batch while containing panics from the publication, and
    /// returns the commits that wait at the write stop.
    ///
    /// This worker is the namespace's only publication path. If publication
    /// panics, each request in the batch receives `commit_outcome_unknown`
    /// because the panic may have occurred before or after the WAL put. Callers
    /// can retry with the same commit ID and use the durable receipt to resolve
    /// the outcome. The worker then continues with queued work.
    async fn publish_batch(&self, candidates: Vec<BatchCandidate>) -> Vec<BatchCandidate> {
        let taken_commit_ids = candidates
            .iter()
            .map(|candidate| candidate.commit_id.clone())
            .collect::<Vec<_>>();
        if let Ok(waiting) = AssertUnwindSafe(self.publish_taken_batch(candidates))
            .catch_unwind()
            .await
        {
            return waiting;
        }
        self.record_panic();
        // A panic can follow the WAL put, so the position's tail may omit
        // committed bytes.
        if let Some(engine) = self.lock_engine().await.engine.as_mut() {
            engine.invalidate_projection();
        }
        let orphaned_waiters = {
            let mut state = self.lock_state();
            taken_commit_ids
                .into_iter()
                .filter_map(|commit_id| state.in_flight.remove(&commit_id))
                .flat_map(|request| request.waiters)
                .collect::<Vec<_>>()
        };
        for waiter in orphaned_waiters {
            let _ = waiter.send(Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
                "publish task aborted mid-batch".to_owned(),
            ))
            .into()));
        }
        Vec::new()
    }

    /// Publishes one batch and delivers each result, except to the new
    /// commits that the engine refused at the write stop.
    ///
    /// Returns those commits in arrival order, to wait in the queue for a
    /// fold.
    async fn publish_taken_batch(&self, candidates: Vec<BatchCandidate>) -> Vec<BatchCandidate> {
        let selected_at = self.timer.monotonic_now_ms();
        for candidate in &candidates {
            phase_event!(
                self.runtime_core,
                "wait_for_batch",
                self.namespace_id,
                tracing::Level::DEBUG,
                result = RESULT_OK,
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at)
            );
        }
        let permits: Vec<_> = candidates
            .iter()
            .map(|candidate| Arc::clone(&candidate.permit))
            .collect();
        let queued: Vec<_> = candidates
            .iter()
            .map(|candidate| (candidate.enqueued_at, candidate.write_stop_wait_recorded))
            .collect();
        // A copy of each candidate that the engine refused at the write stop,
        // so the commit can wait in the queue.
        let mut stopped = vec![None; candidates.len()];
        let (commit_ids, candidates): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .map(|candidate| (candidate.commit_id, candidate.candidate))
            .unzip();

        let publish_span = phase_span!(
            self.runtime_core,
            "batch_publish",
            self.namespace_id,
            batch_size = usize_to_u64(candidates.len()),
            result = tracing::field::Empty,
            retry_count = tracing::field::Empty,
        );
        let (results, retry_count) = async {
            let context = match self.writer.upgrade() {
                Some(writer) => self.runtime_core.mutation_context(&writer.identity),
                None => {
                    let error = Error::Core(CoreError::ShuttingDown);
                    return (candidates.iter().map(|_| Err(error.clone())).collect(), 0);
                }
            };
            // The wall timestamp and elapsed-time origin describe one batch.
            // Refreshing only the timestamp on retry counts prior waiting twice.
            let batch = Deadline::start(Arc::clone(&self.timer));
            let mut results = vec![None; candidates.len()];
            let mut pending: Vec<_> = candidates.into_iter().enumerate().collect();
            let mut retry_count = 0_u64;
            for attempt in 0..MAX_CONTENTION_ATTEMPTS {
                let (indices, candidates): (Vec<_>, Vec<_>) =
                    std::mem::take(&mut pending).into_iter().unzip();
                let attempt_permits: Vec<_> = indices
                    .iter()
                    .map(|index| Arc::clone(&permits[*index]))
                    .collect();
                let observed = match self.writer.upgrade() {
                    Some(_writer) => {
                        self.publish_through_engine(&candidates, &attempt_permits, &context, &batch)
                            .await
                    }
                    None => candidates
                        .iter()
                        .map(|_| Err(CoreError::ShuttingDown))
                        .collect(),
                };
                for ((index, candidate), result) in indices.iter().zip(&candidates).zip(&observed) {
                    if is_maintenance_required(result) {
                        stopped[*index] = Some(candidate.clone());
                    }
                }
                pending = settle_publish_attempt(
                    &mut results,
                    indices.into_iter().zip(candidates),
                    observed,
                );
                if pending.is_empty() || attempt + 1 == MAX_CONTENTION_ATTEMPTS {
                    break;
                }
                retry_count += 1;
                self.claim_publish_slot().await;
            }
            let results = results
                .into_iter()
                .map(|result| {
                    result
                        .expect("each candidate should receive a publication result")
                        .map_err(Error::Core)
                })
                .collect::<Vec<_>>();
            (results, retry_count)
        }
        .instrument(publish_span.clone())
        .await;
        publish_span.record("result", batch_result_label(&results).as_str());
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        // A commit whose earlier put had an unknown outcome keeps
        // `commit_outcome_unknown` under the settling rule, so only a commit
        // whose result is the refusal waits.
        let mut waiting = Vec::new();
        let (mut delivered_ids, mut delivered) = (Vec::new(), Vec::new());
        for (index, (commit_id, result)) in commit_ids.into_iter().zip(results).enumerate() {
            let refused = matches!(
                &result,
                Err(error) if error.code() == loonfs_core::ErrorCode::MaintenanceRequired
            );
            match stopped[index].take() {
                Some(candidate) if refused => {
                    let (enqueued_at, write_stop_wait_recorded) = queued[index];
                    waiting.push(BatchCandidate {
                        permit: Arc::clone(&permits[index]),
                        commit_id,
                        candidate,
                        enqueued_at,
                        write_stop_wait_recorded,
                        refused_by_write_stop: true,
                    });
                }
                _ => {
                    delivered_ids.push(commit_id);
                    delivered.push(result);
                }
            }
        }
        self.deliver_batch_results(delivered_ids, delivered, selected_at);
        waiting
    }

    /// Publishes through the publisher-owned engine: one namespace, one
    /// engine, one writer session, for the publisher's whole life.
    async fn publish_through_engine(
        &self,
        candidates: &[CommitCandidate],
        permits: &[Arc<AdmissionPermit>],
        context: &loonfs_core::MutationContext,
        batch: &Deadline,
    ) -> Vec<Result<Commit, CoreError>> {
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot);
        let store = self.runtime_core.store();
        // Boxing erases the engine's deeply nested publish future. Without
        // it, callers awaiting a put or commit (CLI, server, embedding
        // crates) exceed rustc's type-recursion depth.
        let publish = Box::pin(engine.publish_batch(&store, candidates, context, batch)).await;
        if !publish.results.iter().any(is_retryable_wal_publish) {
            for (permit, result) in permits.iter().zip(&publish.results) {
                // A commit refused at the write stop publishes later, so its
                // inline bytes stay reserved against the tail limit.
                if !is_maintenance_required(result) {
                    permit.release_inline();
                }
            }
        }
        let fold_start = self.record_publication(&publish);
        drop(slot);
        if let Some(start) = fold_start {
            let _ = start.send(());
        }
        publish.results
    }

    /// Records the newest commit that `publish` published, and starts a fold
    /// when the tail it left is over the fold trigger or reached the write
    /// stop. The caller sends the returned start once it releases the
    /// engine.
    fn record_publication(
        &self,
        publish: &loonfs_core::publish::NamespaceCommitEnginePublishResult,
    ) -> Option<oneshot::Sender<()>> {
        if publish
            .wal_tail
            .is_some_and(|tail| tail.source == TailSource::Store)
        {
            self.runtime_core.instruments().publisher_tail_replay();
        }
        let write_stopped = publish.results.iter().any(is_maintenance_required);
        let fold_start = if publish.wal_tail.is_some_and(|tail| {
            self.fold_trigger
                .fold_is_due(FOLD_AT_WAL_OBJECTS, tail.objects, tail.inline_bytes)
        }) || write_stopped
        {
            self.start_fold()
        } else {
            None
        };
        let committed_seq = publish
            .results
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .map(|commit| commit.committed_seq)
            .max();
        if let Some(committed_seq) = committed_seq {
            let mut state = self.lock_state();
            let seq = state
                .last_published
                .map_or(committed_seq, |last| last.seq.max(committed_seq));
            state.last_published = Some(LastPublish {
                seq,
                at_ms: self.timer.monotonic_now_ms(),
            });
            state.metadata_caught_up = false;
        }
        fold_start
    }

    /// Returns the publisher's lazily created commit engine.
    ///
    /// Each publish loads the namespace identity from the head, so construction
    /// only needs the runtime's cache views and the writer session.
    fn engine_for<'slot>(&self, slot: &'slot mut EngineSlot) -> &'slot mut NamespaceCommitEngine {
        slot.engine.get_or_insert_with(|| {
            NamespaceCommitEngine::new(
                self.namespace_id.clone(),
                self.runtime_core
                    .inner
                    .config
                    .execution_budget
                    .content_writes(),
            )
            .monotonic_timer(Arc::clone(&self.timer))
            .segment_cache(self.runtime_core.metadata_segment_cache())
            .writer_session(Arc::clone(&slot.session))
        })
    }

    /// Starts a fold task after its triggering publication releases the engine.
    /// A panicked fold is retried by the next publish over the threshold. The
    /// drain reports the contained panic. A fold that publishes starts the
    /// session's compaction after it releases its fold permit, and the task
    /// folds again while the tail that the fold left is over the trigger.
    fn start_fold(&self) -> Option<oneshot::Sender<()>> {
        let mut state = self.lock_state();
        let _session = self.unfenced_session().ok()?;
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| !fold.task.is_finished())
        {
            return None;
        }
        let (start, started) = oneshot::channel();
        let publisher = self.clone();
        let fold = self.spawn_session_task(&mut state, async move {
            if started.await.is_err() {
                return;
            }
            // A fold frees the tail up to where it began, and a publish while
            // it runs starts no fold, so the task checks the trigger again
            // after each fold that published. Each pass after the first folds
            // commits published during the pass before it.
            loop {
                match AssertUnwindSafe(publisher.run_fold()).catch_unwind().await {
                    Ok(true) => {
                        publisher.fold_published.send_replace(());
                        publisher.start_compaction();
                    }
                    Ok(false) => return,
                    Err(_) => {
                        publisher.record_panic();
                        return;
                    }
                }
                if !publisher.position_fold_is_due().await {
                    return;
                }
            }
        });
        state.fold = Some(fold);
        Some(start)
    }

    /// Whether the engine's position shows its tail over the fold trigger.
    /// Without a position the tail is unknown, and the next publish reads
    /// the tail and starts the fold.
    async fn position_fold_is_due(&self) -> bool {
        let slot = self.lock_engine().await;
        slot.engine.as_ref().is_some_and(|engine| {
            engine
                .wal_tail_objects()
                .zip(engine.wal_tail_inline_bytes())
                .is_some_and(|(objects, inline_bytes)| {
                    self.fold_trigger
                        .fold_is_due(FOLD_AT_WAL_OBJECTS, objects, inline_bytes)
                })
        })
    }

    /// Starts the session's metadata compaction unless one is running or the
    /// session no longer admits work. A panicked compaction may run again
    /// after the next fold. The drain reports the contained panic.
    fn start_compaction(&self) {
        let mut state = self.lock_state();
        let Ok(_session) = self.admitted_session(&state) else {
            return;
        };
        if state
            .compaction
            .as_ref()
            .is_some_and(SessionTask::is_running)
        {
            return;
        }
        let publisher = self.clone();
        let compaction = self.spawn_session_task(&mut state, async move {
            if AssertUnwindSafe(publisher.run_compaction())
                .catch_unwind()
                .await
                .is_err()
            {
                publisher.record_panic();
            }
        });
        state.compaction = Some(compaction);
    }

    /// Spawns `work` as a task whose exit the drain and close can wait for,
    /// and that forgets the session if it was the last thing running.
    fn spawn_session_task(
        &self,
        state: &mut NamespacePublisherState,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> SessionTask {
        let id = state.next_task_id;
        state.next_task_id = state.next_task_id.wrapping_add(1);
        let publisher = self.clone();
        let (task, liveness) = self.spawn_tracked(|exit| async move {
            work.await;
            drop(exit);
            publisher.forget_if_ended();
        });
        SessionTask { id, task, liveness }
    }

    /// Spawns the task that `work` builds around its exit marker, and returns
    /// the task's handle and its liveness.
    ///
    /// The marker exists before the spawn, so a task dropped before its first
    /// poll still reports that it exited.
    fn spawn_tracked<Task>(
        &self,
        work: impl FnOnce(TaskExit) -> Task,
    ) -> (JoinHandle<()>, watch::Receiver<bool>)
    where
        Task: std::future::Future<Output = ()> + Send + 'static,
    {
        let (exit, liveness) = watch::channel(false);
        let task = self.runtime.spawn(work(TaskExit(exit)));
        (task, liveness)
    }

    /// Runs the maintenance loop over this session's namespace without its
    /// fold step, so folding stays with [`Self::run_fold`].
    async fn run_compaction(&self) {
        let Some(maintenance) = self.maintenance() else {
            return;
        };
        let seq = self.lock_state().last_published.map(|last| last.seq);
        let result = maintenance
            .compact_metadata_while_due(&self.namespace_id, &self.compaction_cancellation)
            .await;
        let outcome = result
            .as_ref()
            .map_or(MetadataMaintenanceOutcome::Due, |response| response.outcome);
        self.record_metadata_maintenance(seq, outcome);
        if let Err(error) = result {
            phase_event!(
                self.runtime_core,
                "metadata_compaction",
                self.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "session metadata compaction failed; the next fold starts it again"
            );
        }
    }

    fn record_metadata_maintenance(
        &self,
        expected_seq: Option<ChangeSeq>,
        outcome: MetadataMaintenanceOutcome,
    ) {
        let mut state = self.lock_state();
        if state.last_published.map(|last| last.seq) == expected_seq {
            state.metadata_caught_up = outcome == MetadataMaintenanceOutcome::CaughtUp;
        }
    }

    /// The runtime's maintenance, acting as this session's writer. `None`
    /// once the writer or the session table is gone.
    fn maintenance(&self) -> Option<crate::Maintenance> {
        let writer = self.writer.upgrade()?;
        let registry = PublisherRegistry {
            shared: self.shared.upgrade()?,
            runtime_core: self.runtime_core.clone(),
            writer: self.writer.clone(),
            runtime: self.runtime.clone(),
            timer: Arc::clone(&self.timer),
            min_publish_interval: self.min_publish_interval,
        };
        let actor = writer.identity.clone();
        Some(crate::Maintenance::new(
            self.runtime_core.clone(),
            registry,
            writer,
            actor,
        ))
    }

    /// Folds the tail under a fold permit. Returns whether the fold published
    /// a manifest.
    async fn run_fold(&self) -> bool {
        let Some(writer) = self.writer.upgrade() else {
            return false;
        };
        let _permit = writer.execution_budget.fold_permit().await;
        let input = {
            let slot = self.lock_engine().await;
            if slot.engine.as_ref().is_some_and(|engine| {
                engine
                    .wal_tail_objects()
                    .zip(engine.wal_tail_inline_bytes())
                    .is_some_and(|(objects, inline_bytes)| {
                        !self
                            .fold_trigger
                            .fold_is_due(FOLD_AT_WAL_OBJECTS, objects, inline_bytes)
                    })
            }) {
                return false;
            }
            slot.engine
                .as_ref()
                .and_then(NamespaceCommitEngine::wal_fold_input)
        };
        let started_at_ms = self.timer.monotonic_now_ms();
        let segment_cache = self.runtime_core.metadata_segment_cache();
        let config = &self.runtime_core.inner.config;
        let result = loonfs_core::fold_wal_tail(
            self.runtime_core.store(),
            Some(segment_cache.as_ref()),
            &self.namespace_id,
            input,
            &Deadline::start(Arc::clone(&self.timer)),
            config.metadata_run_policy,
            &config.execution_budget.content_writes(),
        )
        .instrument(phase_span!(
            self.runtime_core,
            "wal_fold",
            self.namespace_id
        ))
        .await;
        self.runtime_core
            .instruments()
            .publisher_wal_fold_duration(self.elapsed_ms_since(started_at_ms));
        // The fold moves the position onto the manifest that covers its tail,
        // whether it published that manifest or found it current. A fold
        // that failed leaves the position where it was.
        if let Ok(folded) = &result {
            self.change_position(PositionChange::Move(folded.manifest.clone()))
                .await;
        }
        match result {
            Ok(folded) => {
                let published = folded.response.outcome == FoldWalOutcome::Published;
                if published {
                    self.runtime_core.instruments().publisher_wal_fold();
                    if let Some(shared) = self.shared.upgrade() {
                        shared
                            .hint_raises
                            .raise(&self.namespace_id, folded.response.manifest_no)
                            .await;
                    }
                }
                published
            }
            Err(error) => {
                let error = Error::Core(error);
                phase_event!(
                    self.runtime_core,
                    "wal_fold",
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "WAL-tail fold failed"
                );
                false
            }
        }
    }

    /// Runs the delete barrier. Returns true when the publisher is now
    /// terminal and its worker should exit.
    async fn execute_delete(&self, pending: PendingDelete) -> bool {
        let PendingDelete { options, waiters } = pending;
        let outcome = match AssertUnwindSafe(self.delete_through_engine(options))
            .catch_unwind()
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                // Contain deletion panics so the worker can continue. Deletion has no commit
                // receipt for reconciliation, so report an internal error to its callers.
                self.record_panic();
                Err(CoreError::Internal("delete task aborted mid-delete".to_owned()).into())
            }
        };
        let deleted = outcome
            .as_ref()
            .err()
            .is_none_or(|error| error.code() == loonfs_core::ErrorCode::NamespaceDeleted);
        if deleted {
            // Tombstone first, then fail everything that queued behind
            // the delete; admissions from here on fail fast.
            let queued = {
                let mut state = self.lock_state();
                state.admission = PublisherAdmissionStatus::Deleted;
                take_queued_waiters(&mut state)
            };
            self.compaction_cancellation.cancel();
            // A deleted session has ended. It leaves the table before the
            // delete's callers hear the outcome, so an open after that
            // starts a fresh session whose publish fails on the durable
            // tombstone. Handles to this session fail fast on `Deleted`.
            // A session stays in the table while its compaction runs (the
            // drain finds running compactions through the table), so the
            // delete waits for the cancelled compaction before it forgets
            // the session.
            if let Err(error) = self.wait_for_compaction().await {
                phase_event!(
                    self.runtime_core,
                    "metadata_compaction",
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "metadata compaction failed while the namespace was deleted"
                );
            }
            self.forget_if_ended();
            for waiter in queued.commits {
                let _ = waiter.send(Err(self.namespace_deleted().into()));
            }
            for waiter in queued.deletes {
                let _ = waiter.send(Err(self.namespace_deleted().into()));
            }
        }
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
        deleted
    }

    async fn delete_through_engine(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        self.wait_for_fold().await?;
        let Some(writer) = self.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let _permit = writer.execution_budget.fold_permit().await;
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot);
        crate::fs::delete_namespace_with_engine(
            &self.runtime_core,
            &writer,
            &self.namespace_id,
            engine,
            options,
        )
        .await
    }

    fn namespace_deleted(&self) -> CoreError {
        CoreError::NamespaceDeleted {
            namespace_id: self.namespace_id.clone(),
        }
    }

    fn record_panic(&self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.panicked_units.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Waits for the running worker, if any.
    ///
    async fn wait_for_worker(&self) {
        let mut liveness = self
            .lock_state()
            .worker
            .as_ref()
            .map(|worker| worker.liveness.clone());
        if let Some(liveness) = liveness.as_mut() {
            while !*liveness.borrow_and_update() {
                if liveness.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    async fn wait_for_fold(&self) -> Result<(), Error> {
        self.wait_for_session_task(|state| &mut state.fold)
            .await
            .map_err(|error| Error::RuntimeTask(format!("WAL-tail fold task failed: {error}")))
    }

    async fn wait_for_compaction(&self) -> Result<(), Error> {
        self.wait_for_session_task(|state| &mut state.compaction)
            .await
            .map_err(|error| {
                Error::RuntimeTask(format!("metadata compaction task failed: {error}"))
            })
    }

    /// Waits for the task in `slot`, if any, and reaps it unless a newer
    /// task has replaced it.
    async fn wait_for_session_task(
        &self,
        slot: fn(&mut NamespacePublisherState) -> &mut Option<SessionTask>,
    ) -> Result<(), tokio::task::JoinError> {
        let task = slot(&mut self.lock_state())
            .as_ref()
            .map(|task| (task.id, task.liveness.clone(), task.task.is_finished()));
        let Some((id, mut liveness, finished)) = task else {
            return Ok(());
        };
        if !finished {
            while !*liveness.borrow_and_update() {
                if liveness.changed().await.is_err() {
                    break;
                }
            }
        }
        let task = {
            let mut state = self.lock_state();
            let slot = slot(&mut state);
            if slot.as_ref().is_some_and(|task| task.id == id) {
                slot.take().map(|task| task.task)
            } else {
                None
            }
        };
        if let Some(task) = task {
            task.await?;
        }
        Ok(())
    }

    /// Waits until the namespace may start another WAL put.
    async fn await_publish_slot(&self) {
        loop {
            let Some(last_publish) = self.lock_state().last_publish.clone() else {
                break;
            };
            let remaining_ms = loonfs_types::saturating_duration_ms(self.min_publish_interval)
                .saturating_sub(last_publish.age_ms());
            if remaining_ms == 0 {
                break;
            }
            wait_for_publish_pacing(Duration::from_millis(remaining_ms)).await;
        }
    }

    async fn claim_publish_slot(&self) {
        self.await_publish_slot().await;
        self.reserve_next_publish_slot(&mut self.lock_state());
    }

    fn reserve_next_publish_slot(&self, state: &mut NamespacePublisherState) {
        state.last_publish = Some(Observation::now(Arc::clone(&self.timer)));
    }

    fn elapsed_ms_since(&self, started_at_ms: u64) -> u64 {
        elapsed_ms_from(started_at_ms, self.timer.monotonic_now_ms())
    }

    fn deliver_batch_results(
        &self,
        commit_ids: Vec<CommitId>,
        results: Vec<CommitResult>,
        selected_at: u64,
    ) {
        let mut deliveries = Vec::new();
        let mut wait_traces = Vec::new();
        {
            let mut state = self.lock_state();
            // Nothing queued behind this publish means no load to batch, and the
            // next request may answer these results: it publishes at once.
            if state.queue.is_empty() {
                state.last_publish = None;
            }
            assert_eq!(
                results.len(),
                commit_ids.len(),
                "a publisher batch should return one result per candidate"
            );
            for (commit_id, result) in commit_ids.into_iter().zip(results) {
                wait_traces.push((result_label(&result), self.elapsed_ms_since(selected_at)));
                if let Some(in_flight) = state.in_flight.remove(&commit_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
        }

        for (outcome, wait_ms) in wait_traces {
            self.runtime_core.instruments().publisher_publish(outcome);
            phase_event!(
                self.runtime_core,
                "wait_for_result",
                self.namespace_id,
                tracing::Level::DEBUG,
                result = outcome.as_str(),
                wait_ms
            );
        }

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }
    }

    fn trace_enqueue(&self, queue_depth: usize, reason: &'static str) {
        self.runtime_core
            .instruments()
            .publisher_queue_depth(queue_depth);
        phase_event!(
            self.runtime_core,
            "enqueue",
            self.namespace_id,
            tracing::Level::DEBUG,
            queue_depth = usize_to_u64(queue_depth),
            reason
        );
    }
}

async fn submit_with_admission<F>(
    namespace_id: &NamespaceId,
    candidate: PreparedCandidate,
    timer: &dyn MonotonicTimer,
    mut admit: F,
) -> CommitResult
where
    F: FnMut(
        CommitId,
        PreparedCandidate,
        CommitFingerprint,
        oneshot::Sender<CommitResult>,
        u64,
    ) -> Result<SubmissionAdmission, CoreError>,
{
    let commit_id = candidate.candidate.commit_id().clone();
    let enqueued_at = timer.monotonic_now_ms();
    let mut candidate = candidate;
    let mut semantic_identity = candidate.candidate.semantic_identity(namespace_id)?;
    for _ in 0..MAX_CONTENTION_ATTEMPTS {
        let (sender, receiver) = oneshot::channel();
        let admission = admit(
            commit_id.clone(),
            candidate,
            semantic_identity,
            sender,
            enqueued_at,
        )?;
        let result = receiver.await.map_err(|_| {
            CoreError::WalPublish(WalPublishError::OutcomeUnknown(
                "publisher task stopped before reporting an outcome".to_owned(),
            ))
        })?;
        match admission {
            SubmissionAdmission::OwnOutcome => return result,
            SubmissionAdmission::Contended {
                primary_identity,
                candidate: returned_candidate,
                semantic_identity: returned_identity,
            } => match result {
                Ok(response) => {
                    return Err(CoreError::CommitIdReuseConflict {
                        commit_id: commit_id.to_string(),
                        committed_seq: Some(response.committed_seq),
                        committed_fingerprint: Some(primary_identity),
                    }
                    .into())
                }
                Err(_) => {
                    candidate = *returned_candidate;
                    semantic_identity = returned_identity;
                }
            },
        }
    }
    Err(CoreError::CommitIdReuseConflict {
        commit_id: commit_id.to_string(),
        committed_seq: None,
        committed_fingerprint: None,
    }
    .into())
}

async fn receive_delete(receiver: oneshot::Receiver<DeleteResult>) -> DeleteResult {
    receiver.await.unwrap_or_else(|_| {
        Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            "publisher task stopped mid-delete".to_owned(),
        ))
        .into())
    })
}

/// Waiters a landed delete barrier leaves behind, one vector per result
/// type.
#[derive(Default)]
struct QueuedWaiters {
    commits: Vec<AdmittedWaiter<CommitResult>>,
    deletes: Vec<AdmittedWaiter<DeleteResult>>,
}

/// Empties the queue and hands back every waiter it held. Called once the
/// delete barrier lands: nothing queued behind a tombstone may publish.
fn take_queued_waiters(state: &mut NamespacePublisherState) -> QueuedWaiters {
    let mut waiters = QueuedWaiters::default();
    for item in std::mem::take(&mut state.queue) {
        match item {
            WorkItem::Batch(batch) => {
                for candidate in batch.candidates {
                    if let Some(request) = state.in_flight.remove(&candidate.commit_id) {
                        waiters.commits.extend(request.waiters);
                    }
                }
            }
            WorkItem::Delete(pending) => waiters.deletes.extend(pending.waiters),
        }
    }
    waiters
}

fn is_maintenance_required(result: &Result<Commit, CoreError>) -> bool {
    matches!(
        result,
        Err(error) if error.code() == loonfs_core::ErrorCode::MaintenanceRequired
    )
}

/// Candidates queued but not yet taken by the worker: the depth admission
/// bounds and traces.
fn queued_candidates(state: &NamespacePublisherState) -> usize {
    state
        .queue
        .iter()
        .map(|item| match item {
            WorkItem::Batch(batch) => batch.candidates.len(),
            WorkItem::Delete(_) => 0,
        })
        .sum()
}

fn result_label<T, E>(result: &Result<T, E>) -> PublishOutcome {
    if result.is_ok() {
        PublishOutcome::Ok
    } else {
        PublishOutcome::Error
    }
}

fn batch_result_label(results: &[CommitResult]) -> PublishOutcome {
    if results.iter().all(Result::is_ok) {
        PublishOutcome::Ok
    } else {
        PublishOutcome::Error
    }
}

fn elapsed_ms_from(start: u64, end: u64) -> u64 {
    end.saturating_sub(start)
}

#[allow(clippy::disallowed_methods)]
// The configured publication pacing delay does not affect publication validity.
async fn wait_for_publish_pacing(delay: Duration) {
    tokio::time::sleep(delay).await;
}

#[allow(
    clippy::disallowed_methods,
    reason = "the write-stop wait only decides when a waiting commit returns `maintenance_required`"
)]
async fn wait_for_write_stop_budget(delay: Duration) {
    tokio::time::sleep(delay).await;
}

/// Waits until the fold task that `liveness` reports on exits, and returns at
/// once when the session has no fold task.
async fn wait_for_fold_task_exit(liveness: Option<watch::Receiver<bool>>) {
    if let Some(mut liveness) = liveness {
        let _ = liveness.wait_for(|exited| *exited).await;
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "publisher/tests.rs"]
mod tests;
