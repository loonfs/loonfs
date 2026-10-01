//! Runtime publication service for namespace mutations.
//!
//! Each namespace writer session has one queue. Concurrent commits may share
//! a WAL object and head update. Duplicate commit IDs join in flight,
//! conflicting reuse is rejected, and namespace deletion is ordered with
//! other mutations.
//!
//! Admitted work continues if its caller is cancelled. Shutdown closes
//! admission and drains the queues through
//! [`LoonFs::shutdown`](crate::LoonFs::shutdown).
//!
//! The registry keeps one table of live sessions, for three reasons: an open
//! returns the session a handle already holds, the retained projection
//! budget reaches every publisher, and shutdown drains every session. The
//! host decides how long a session lives by holding its writable
//! [`Namespace`](crate::Namespace).

mod admission;
mod inline_content;

use crate::fs::{RuntimeCore, WriterBits};
use crate::metrics::{PublishOutcome, RESULT_OK};
use crate::publish::CommitCandidate;
use crate::trace::{phase_event, phase_span};
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, RuntimeCacheConfig, RuntimeError,
};
use admission::{AdmissionPermit, AdmittedWaiter, PublicationAdmission};
use futures::FutureExt;
use loonfs_api::v0::Commit;
use loonfs_api::wire::wal::{MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use loonfs_api::{ChangeSeq, CommitId, NamespaceId};
use loonfs_core::cache::Recency;
use loonfs_core::commit::{
    is_retryable_wal_publish, settle_publish_attempt, CommitFingerprint, WalPublishError,
};
use loonfs_core::limits::{CONTENTION_RETRY_LIMIT, FOLD_AT_WAL_OBJECTS};
use loonfs_core::publish::{
    NamespaceCommitEngine, PublishTailWeight, SharedWriterSessionState, WriterSessionState,
};
use loonfs_core::time::{Deadline, Observation};
use loonfs_objectstore::timing::MonotonicTimer;
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

type CommitResult = Result<Commit, RuntimeError>;
type DeleteResult = Result<DeleteNamespaceResponse, RuntimeError>;

/// A report that one namespace's durable mutation history advanced.
///
/// A namespace-advance hint is a wake-up, not history. Consumers read the
/// ordered change feed and keep their own durable cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceAdvanceHint {
    /// Namespace whose durable mutation history advanced.
    pub namespace_id: NamespaceId,
    /// The namespace is durably visible through at least this sequence.
    ///
    /// One publication batch may carry several commits, so this is a
    /// high-water mark and not the identity of one commit.
    pub through_seq: ChangeSeq,
}

/// A synchronous, best-effort notification handed one
/// [`NamespaceAdvanceHint`] after a publication batch durably advances a
/// namespace.
///
/// Register one with
/// [`LoonFsBuilder::namespace_advance_observer`](crate::LoonFsBuilder::namespace_advance_observer),
/// which documents what the callback may do.
pub type NamespaceAdvanceObserver = Arc<dyn Fn(NamespaceAdvanceHint) + Send + Sync + 'static>;

/// Result of closing one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseNamespaceReport {
    /// False when the session had already stopped admitting work.
    pub was_open: bool,
    /// Commits admitted before the close and published during the drain.
    pub drained_commits: usize,
    /// Whether the closed session had been fenced.
    pub fenced: bool,
}

/// Current state of one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSessionState {
    /// The session admits work.
    Open,
    /// Another writer acquired a later epoch. Mutations fail with
    /// `writer_fenced` for the rest of the session.
    Fenced,
    /// The session admits no more work. It was closed, its namespace was
    /// deleted, or the runtime is shutting down.
    Closed,
}

/// The live writer sessions of one writer, and the admission budgets they
/// share.
///
/// Clones share the same sessions and worker tasks. The table holds a
/// session while a writable [`Namespace`](crate::Namespace) holds it or
/// while work it admitted is still running. Nothing here decides how many
/// sessions exist or how long they live.
///
/// Shutdown closes admission and then drains admitted work. Prefer
/// [`LoonFs::shutdown`](crate::LoonFs::shutdown), which closes admission
/// before draining publication work.
#[derive(Clone)]
pub struct PublisherRegistry {
    shared: Arc<RegistryShared>,
    /// Strong: the runtime core owns neither this registry nor the writer, so
    /// holding it here cannot cycle. Publications read through its caches
    /// and seed them with what they produce.
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
/// session table, retained projections, and contained panic count.
struct RegistryShared {
    admission: Arc<PublicationAdmission>,
    state: Mutex<RegistryState>,
    /// Publication and deletion units whose panic a task survived. Tasks
    /// contain panics to keep the registry usable, so this — not a task
    /// join error — is what a drain reports.
    panicked_units: AtomicUsize,
}

struct RegistryState {
    closed: bool,
    sessions: HashMap<NamespaceId, LiveSession>,
    projections: RetainedProjections,
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

    pub(crate) fn state(&self) -> NamespaceSessionState {
        self.publisher.session_state()
    }

    pub(crate) async fn wait_for_fold(&self) -> Result<(), RuntimeError> {
        self.publisher.wait_for_fold().await
    }

    /// Refuses new work from every handle that shares this session, waits
    /// for admitted work and the running fold, and forgets the session.
    ///
    /// The worker and the fold task forget the session too when they exit,
    /// so a cancelled close still leaves the table clean.
    pub(crate) async fn close(&self) -> Result<CloseNamespaceReport, CoreError> {
        let publisher = &self.publisher;
        let drained_commits = publisher.close_session()?;
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
        publisher.forget_if_ended();
        Ok(CloseNamespaceReport {
            was_open: drained_commits.is_some(),
            drained_commits: drained_commits.unwrap_or(0),
            fenced: publisher.session_is_fenced(),
        })
    }
}

impl RegistryShared {
    // Recover the inner state after poisoning. These critical sections only
    // update fields, and worker panic containment is intended to keep the
    // registry usable after a publication panics.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records the namespace's retained projection and evicts projections until
    /// the writer is within its shared budget.
    ///
    /// Eviction removes only rebuildable WAL-tail projections, starting with the
    /// least recently published namespace. If a publication or delete currently
    /// holds an engine, that namespace is skipped and remains counted. The active
    /// operation reports its projection weight when it finishes.
    fn settle_projection(
        &self,
        namespace_id: &NamespaceId,
        weight: Option<PublishTailWeight>,
        budget: &RuntimeCacheConfig,
        instruments: &crate::metrics::RuntimeInstruments,
    ) -> RetainedProjectionTotals {
        let mut state = self.lock_state();
        state.projections.record(namespace_id, weight);
        let attempts = state.projections.len();
        for _ in 0..attempts {
            if !state.projections.is_over_budget(budget) {
                break;
            }
            let Some(victim) = state.projections.oldest() else {
                break;
            };
            if state
                .sessions
                .get(&victim)
                .is_none_or(|live| live.publisher.invalidate_projection())
            {
                state.projections.remove_entry(&victim);
                instruments.publisher_projection_evicted();
            } else {
                state.projections.retain(&victim);
            }
        }
        state.projections.totals()
    }
}

/// The WAL-tail projections this writer's publishers retain, and what they
/// weigh together.
///
/// The per-projection ceiling a publish already applies bounds one namespace;
/// this bounds the writer, which is what a process publishing to thousands of
/// namespaces actually holds.
#[derive(Debug, Default)]
struct RetainedProjections {
    entries: HashMap<NamespaceId, (PublishTailWeight, u64)>,
    order: Recency<NamespaceId>,
    rows: usize,
    decoded_bytes: usize,
}

/// What the writer retains right now, for the gauges and for tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RetainedProjectionTotals {
    projections: usize,
    rows: usize,
    decoded_bytes: usize,
}

impl RetainedProjections {
    /// Records what one namespace retains after a publish; `None` is a
    /// publish that kept nothing.
    fn record(&mut self, namespace_id: &NamespaceId, weight: Option<PublishTailWeight>) {
        self.remove_entry(namespace_id);
        let Some(weight) = weight else {
            return;
        };
        let last_touch = self.order.touch(namespace_id);
        self.rows = self.rows.saturating_add(weight.rows);
        self.decoded_bytes = self.decoded_bytes.saturating_add(weight.decoded_bytes);
        self.entries
            .insert(namespace_id.clone(), (weight, last_touch));
        self.compact_order();
    }

    fn forget(&mut self, namespace_id: &NamespaceId) {
        self.remove_entry(namespace_id);
    }

    fn retain(&mut self, namespace_id: &NamespaceId) {
        let last_touch = self.order.touch(namespace_id);
        if let Some((_, entry_last_touch)) = self.entries.get_mut(namespace_id) {
            *entry_last_touch = last_touch;
        }
        self.compact_order();
    }

    fn remove_entry(&mut self, namespace_id: &NamespaceId) {
        let Some((weight, _)) = self.entries.remove(namespace_id) else {
            return;
        };
        self.rows = self.rows.saturating_sub(weight.rows);
        self.decoded_bytes = self.decoded_bytes.saturating_sub(weight.decoded_bytes);
    }

    fn oldest(&mut self) -> Option<NamespaceId> {
        let entries = &self.entries;
        self.order
            .pop_oldest(|namespace_id, stamp| projection_is_live(entries, namespace_id, stamp))
    }

    fn compact_order(&mut self) {
        let entries = &self.entries;
        self.order.compact(entries.len(), |namespace_id, stamp| {
            projection_is_live(entries, namespace_id, stamp)
        });
    }

    fn is_over_budget(&self, budget: &RuntimeCacheConfig) -> bool {
        self.rows > budget.max_cached_wal_tail_projection_rows
            || self.decoded_bytes > budget.max_cached_wal_tail_projection_decoded_bytes
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn totals(&self) -> RetainedProjectionTotals {
        RetainedProjectionTotals {
            projections: self.entries.len(),
            rows: self.rows,
            decoded_bytes: self.decoded_bytes,
        }
    }
}

fn projection_is_live(
    entries: &HashMap<NamespaceId, (PublishTailWeight, u64)>,
    namespace_id: &NamespaceId,
    stamp: u64,
) -> bool {
    entries
        .get(namespace_id)
        .is_some_and(|(_, last_touch)| *last_touch == stamp)
}

impl PublisherRegistry {
    /// Creates the registry a writer owns. Batches publish through each
    /// publisher's own commit engine and writer session.
    pub(crate) fn new(
        runtime_core: RuntimeCore,
        writer: Weak<WriterBits>,
        runtime: Handle,
        min_publish_interval: Duration,
        publication_limits: crate::PublicationLimits,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                admission: Arc::new(PublicationAdmission::new(publication_limits)),
                state: Mutex::new(RegistryState {
                    closed: false,
                    sessions: HashMap::new(),
                    projections: RetainedProjections::default(),
                }),
                panicked_units: AtomicUsize::new(0),
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
    /// second session would acquire a new epoch and fence that work.
    pub(crate) fn open_session(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Arc<NamespaceSession>, CoreError> {
        let mut state = self.shared.lock_state();
        if state.closed {
            return Err(CoreError::ShuttingDown);
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

    /// Invalidates the namespace's rebuildable WAL-tail projection without
    /// changing its writer epoch or fencing state.
    ///
    /// If an operation currently holds the engine, invalidation is skipped. That
    /// operation validates the live head and reports its retained projection when
    /// it completes.
    pub(crate) fn invalidate_projection(&self, namespace_id: &NamespaceId) {
        let totals = {
            let mut state = self.shared.lock_state();
            let Some(live) = state.sessions.get(namespace_id) else {
                return;
            };
            if !live.publisher.invalidate_projection() {
                return;
            }
            state.projections.remove_entry(namespace_id);
            state.projections.totals()
        };
        self.runtime_core
            .instruments()
            .publisher_retained_projections(totals.projections, totals.decoded_bytes);
    }

    fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.shared
            .lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| live.publisher.clone())
    }

    pub(crate) async fn wal_tail_inline_bytes(&self, namespace_id: &NamespaceId) -> Option<usize> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.engine.lock().await;
        slot.wal_tail_inline_bytes()
    }

    pub(crate) async fn record_fold_outcome(&self, namespace_id: &NamespaceId) {
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher.engine.lock().await.record_fold_outcome(None);
        }
    }

    /// Whether [`Self::close_admission`] has run: later submissions fail
    /// with `shutting_down`. Readiness probes report this state.
    pub fn is_admission_closed(&self) -> bool {
        self.shared.lock_state().closed
    }

    /// Stops accepting new submissions while allowing admitted work to finish.
    ///
    /// Later submissions fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub fn close_admission(&self) {
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

    /// Waits for all current publisher workers and folds to finish.
    ///
    /// Returns an error if any publication or deletion panicked and the worker
    /// contained the panic. Call [`Self::close_admission`] first to prevent new
    /// work from being admitted during the drain.
    pub async fn drain(&self) -> Result<(), RuntimeError> {
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
        for publisher in publishers {
            if let Err(error) = publisher.wait_for_fold().await {
                task_error.get_or_insert(error);
            }
        }
        let panicked = self.shared.panicked_units.load(Ordering::SeqCst);
        if panicked > 0 {
            return Err(RuntimeError::RuntimeTask(format!(
                "{panicked} publisher task(s) panicked"
            )));
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(())
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
    engine: Arc<AsyncMutex<EngineSlot>>,
    session: SharedWriterSessionState,
    /// Weak: the session table owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    runtime: Handle,
    timer: Arc<dyn MonotonicTimer>,
    min_publish_interval: Duration,
    inline_content: crate::InlineContentOptions,
}

/// Commit engine and writer session retained by one namespace publisher.
///
/// The session stores the acquired epoch and terminal fencing state. Those
/// values describe this process and cannot be reconstructed from object
/// storage. They therefore live for the publisher's lifetime rather than in
/// an evictable cache. Only the engine's WAL-tail projection is invalidated.
struct EngineSlot {
    /// Built on the first unit of work and kept for the publisher's life.
    /// Invalidation drops only its rebuildable tail projection.
    engine: Option<NamespaceCommitEngine>,
    /// Never dropped or rebuilt while the publisher lives.
    session: SharedWriterSessionState,
    /// Only a publish that observed the tail replaces it. A fold leaves it
    /// as an overestimate, so admission stays conservative until then.
    last_known_wal_tail_inline_bytes: Option<usize>,
}

impl EngineSlot {
    fn wal_tail_inline_bytes(&self) -> Option<usize> {
        self.engine
            .as_ref()
            .and_then(NamespaceCommitEngine::wal_fold_input)
            .map(|input| input.wal_tail_inline_bytes)
            .or(self.last_known_wal_tail_inline_bytes)
    }

    fn record_fold_outcome(&mut self, folded: Option<&loonfs_core::FoldedWalTail>) {
        if let Some(engine) = self.engine.as_mut() {
            engine.record_wal_fold(folded);
        }
    }
}

/// Admission state for a namespace publisher.
///
/// A single enum preserves precedence: after deletion succeeds, the
/// publisher remains `Deleted` even if registry shutdown closes admission.
enum PublisherAdmissionState {
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
    admission: PublisherAdmissionState,
    /// The worker draining `queue`, while one is running. A live entry is
    /// what makes the loop single-flight: a worker installs itself under
    /// the admission lock and releases the slot under the same lock that
    /// finds the queue empty. That single flight is what makes the delete
    /// barrier's admission order deterministic.
    worker: Option<WorkerHandle>,
    fold: Option<FoldHandle>,
    next_fold_id: u64,
    /// The last reserved WAL put. `None` is an idle namespace: nothing was
    /// queued when its last batch settled, so the next request publishes
    /// immediately.
    last_publish: Option<Observation>,
}

impl NamespacePublisherState {
    /// Whether the session is over and nothing it admitted can still
    /// publish. A session is over once no handle holds it, or once it was
    /// closed or its namespace deleted.
    fn has_ended(&self, held: bool) -> bool {
        let deleted = matches!(self.admission, PublisherAdmissionState::Deleted);
        let over =
            !held || deleted || matches!(self.admission, PublisherAdmissionState::SessionClosed);
        // A deleted session's worker takes nothing more from its queue.
        let worker_running = !deleted
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| !*worker.liveness.borrow());
        let fold_running = self
            .fold
            .as_ref()
            .is_some_and(|fold| !*fold.liveness.borrow());
        over && !worker_running && !fold_running
    }
}

struct WorkerHandle {
    _task: JoinHandle<()>,
    liveness: watch::Receiver<bool>,
}

struct FoldHandle {
    fold_id: u64,
    task: JoinHandle<()>,
    liveness: watch::Receiver<bool>,
}

struct WorkerExit(watch::Sender<bool>);

impl Drop for WorkerExit {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

struct FoldExit(watch::Sender<bool>);

impl Drop for FoldExit {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// A fold counted as waiting for a writer permit.
struct WaitingFold<'a> {
    counter: &'a AtomicUsize,
    instruments: &'a crate::metrics::RuntimeInstruments,
}

impl<'a> WaitingFold<'a> {
    fn new(counter: &'a AtomicUsize, instruments: &'a crate::metrics::RuntimeInstruments) -> Self {
        let waiting_fold = Self {
            counter,
            instruments,
        };
        let waiting = counter.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        instruments.publisher_wal_folds_waiting(waiting);
        waiting_fold
    }
}

impl Drop for WaitingFold<'_> {
    fn drop(&mut self) {
        let waiting = self
            .counter
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        self.instruments.publisher_wal_folds_waiting(waiting);
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
            inline_content_bytes: retained_inline_content.iter().fold(0usize, |total, value| {
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
        let session = SharedWriterSessionState::default();
        Self {
            namespace_id,
            runtime_core: registry.runtime_core.clone(),
            writer: registry.writer.clone(),
            state: Arc::new(Mutex::new(NamespacePublisherState {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                admission: PublisherAdmissionState::Open,
                worker: None,
                fold: None,
                next_fold_id: 0,
                last_publish: None,
            })),
            engine: Arc::new(AsyncMutex::new(EngineSlot {
                engine: None,
                session: Arc::clone(&session),
                last_known_wal_tail_inline_bytes: None,
            })),
            session,
            shared: Arc::downgrade(&registry.shared),
            runtime: registry.runtime.clone(),
            timer: Arc::clone(&registry.timer),
            min_publish_interval: registry.min_publish_interval,
            admission: Arc::clone(&registry.shared.admission),
            inline_content: registry
                .writer
                .upgrade()
                .map(|writer| writer.inline_content.clone())
                .unwrap_or_default(),
        }
    }

    /// Recovers a poisoned lock rather than propagating, for the same reason
    /// as [`RegistryShared::lock_state`]: every critical section here is a
    /// plain field update, and one panicked publication must not leave the
    /// namespace permanently unwritable.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, NamespacePublisherState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open becomes closed; a delete that already landed is terminal and
    /// stays terminal.
    fn close_admission(&self) {
        let mut state = self.lock_state();
        if matches!(state.admission, PublisherAdmissionState::Open) {
            state.admission = PublisherAdmissionState::Closed;
        }
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
        if !matches!(state.admission, PublisherAdmissionState::Open) {
            return Ok(None);
        }
        state.admission = PublisherAdmissionState::SessionClosed;
        Ok(Some(state.in_flight.len()))
    }

    fn session_closed(&self) -> bool {
        matches!(
            self.lock_state().admission,
            PublisherAdmissionState::SessionClosed
        )
    }

    fn session_state(&self) -> NamespaceSessionState {
        if !matches!(self.lock_state().admission, PublisherAdmissionState::Open) {
            NamespaceSessionState::Closed
        } else if self.session_is_fenced() {
            NamespaceSessionState::Fenced
        } else {
            NamespaceSessionState::Open
        }
    }

    /// Removes this session from the table once it has ended and nothing it
    /// admitted is still running. Dropping the last handle, the worker's
    /// exit, the fold's exit, a close, and a landed delete each call this,
    /// so whichever of them comes last removes the session.
    fn forget_if_ended(&self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let totals = {
            let mut registry = shared.lock_state();
            let held = match registry.sessions.get(&self.namespace_id) {
                Some(live) if Arc::ptr_eq(&live.publisher.state, &self.state) => {
                    live.session.strong_count() > 0
                }
                _ => return,
            };
            if !self.lock_state().has_ended(held) {
                return;
            }
            registry.sessions.remove(&self.namespace_id);
            registry.projections.forget(&self.namespace_id);
            self.runtime_core
                .instruments()
                .publisher_sessions(registry.sessions.len());
            registry.projections.totals()
        };
        self.report_retained_projections(totals);
    }

    /// Returns the error for the current admission state, or succeeds when open.
    fn check_admission(&self, state: &NamespacePublisherState) -> Result<(), CoreError> {
        match state.admission {
            PublisherAdmissionState::Open => Ok(()),
            PublisherAdmissionState::Closed => Err(CoreError::ShuttingDown),
            PublisherAdmissionState::SessionClosed => Err(CoreError::WriterSessionClosed {
                namespace_id: self.namespace_id.clone(),
            }),
            PublisherAdmissionState::Deleted => Err(self.namespace_deleted()),
        }
    }

    fn session_is_fenced(&self) -> bool {
        matches!(
            *self
                .session
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            WriterSessionState::Fenced(_)
        )
    }

    /// Drops the engine's tail projection, reporting whether it took the
    /// engine to do so. A `false` return means a publication or delete holds
    /// the engine, and that unit's own settlement reports what it retains.
    fn invalidate_projection(&self) -> bool {
        let Ok(mut slot) = self.engine.try_lock() else {
            return false;
        };
        if let Some(engine) = slot.engine.as_mut() {
            engine.invalidate_projection();
        }
        true
    }

    /// Records the projection retained by the completed publish and enforces the
    /// writer's shared projection budget.
    ///
    /// The caller still holds the engine lock, so the recorded weight matches the
    /// projection in the engine. Eviction only tries engine locks, so it
    /// does not wait while holding the registry lock.
    fn settle_retained_projection(&self, weight: Option<PublishTailWeight>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let budget = self.runtime_core.runtime_cache_config();
        let totals = shared.settle_projection(
            &self.namespace_id,
            weight,
            budget,
            self.runtime_core.instruments(),
        );
        self.report_retained_projections(totals);
    }

    fn report_retained_projections(&self, totals: RetainedProjectionTotals) {
        self.runtime_core
            .instruments()
            .publisher_retained_projections(totals.projections, totals.decoded_bytes);
    }

    /// Places the candidate's inline content, then admits it before awaiting
    /// its result.
    ///
    /// Once the candidate enters the queue, cancelling the caller only drops
    /// result delivery; the worker still owns and publishes the request.
    async fn submit_candidate(&self, candidate: CommitCandidate) -> CommitResult {
        self.check_admission(&self.lock_state())?;
        let plan = self.plan_inline_candidate(candidate)?;
        let permit = self
            .admission
            .acquire_candidate(&self.namespace_id, &plan.candidate)?;
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
        self.check_admission(&self.lock_state())?;
        let permit = self
            .admission
            .acquire_candidate(&self.namespace_id, &candidate)?;
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

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: PreparedCandidate,
        semantic_identity: CommitFingerprint,
        waiter: AdmittedWaiter<CommitResult>,
        enqueued_at: u64,
    ) -> Result<SubmissionAdmission, CoreError> {
        let mut state = self.lock_state();
        self.check_admission(&state)?;
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
        self.ensure_worker(&mut state);
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
            self.check_admission(&state)?;
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
            self.ensure_worker(&mut state);
        }
        Ok(receiver)
    }

    /// Makes sure a worker owns this publisher's queue.
    ///
    /// Callers hold the state lock, so admitting work and installing the
    /// task that owns it is atomic: no second worker takes the same queue,
    /// and a shutdown drain that finds no worker cannot miss work an
    /// admission is about to queue.
    fn ensure_worker(&self, state: &mut NamespacePublisherState) {
        if state
            .worker
            .as_ref()
            .is_some_and(|worker| !*worker.liveness.borrow())
        {
            return;
        }
        let publisher = self.clone();
        let (exit, liveness) = watch::channel(false);
        let exit = WorkerExit(exit);
        let task = self.runtime.spawn(async move {
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
            let _publication = self
                .admission
                .publications
                .acquire()
                .await
                .expect("publication semaphore is never closed");
            let Some(item) = self.take_next_item() else {
                self.forget_if_ended();
                return;
            };

            match item {
                WorkItem::Batch(batch) => {
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
                    self.publish_batch(batch.candidates).await;
                }
                WorkItem::Delete(pending) => {
                    if self.execute_delete(pending).await {
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
        if matches!(state.admission, PublisherAdmissionState::Deleted) || state.queue.is_empty() {
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

    /// Publishes one batch while containing panics from the publication.
    ///
    /// This worker is the namespace's only publication path. If publication
    /// panics, each request in the batch receives `commit_outcome_unknown`
    /// because the panic may have occurred before or after the WAL put. Callers
    /// can retry with the same commit ID and use the durable receipt to resolve
    /// the outcome. The worker then continues with queued work.
    async fn publish_batch(&self, candidates: Vec<BatchCandidate>) {
        let taken_commit_ids = candidates
            .iter()
            .map(|candidate| candidate.commit_id.clone())
            .collect::<Vec<_>>();
        if AssertUnwindSafe(self.publish_taken_batch(candidates))
            .catch_unwind()
            .await
            .is_ok()
        {
            return;
        }
        self.record_panic();
        // A panic can follow the WAL put, so the cached tail may omit committed bytes.
        {
            let mut slot = self.engine.lock().await;
            if let Some(engine) = slot.engine.as_mut() {
                engine.invalidate_projection();
            }
            self.settle_retained_projection(None);
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
    }

    async fn publish_taken_batch(&self, candidates: Vec<BatchCandidate>) {
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
                None => Err(CoreError::ShuttingDown.into()),
            };
            let context = match context {
                Ok(context) => context,
                Err(error) => {
                    return (candidates.iter().map(|_| Err(error.clone())).collect(), 0);
                }
            };
            // The wall timestamp and elapsed-time origin describe one batch.
            // Refreshing only the timestamp on retry counts prior waiting twice.
            let batch = Deadline::start(Arc::clone(&self.timer));
            let mut results = vec![None; candidates.len()];
            let mut pending: Vec<_> = candidates.into_iter().enumerate().collect();
            let mut retry_count = 0_u64;
            for attempt in 0..CONTENTION_RETRY_LIMIT {
                let (indices, candidates): (Vec<_>, Vec<_>) =
                    std::mem::take(&mut pending).into_iter().unzip();
                let attempt_permits: Vec<_> = indices
                    .iter()
                    .map(|index| Arc::clone(&permits[*index]))
                    .collect();
                let observed = match self.writer.upgrade() {
                    Some(writer) => {
                        self.publish_through_engine(
                            &writer,
                            &candidates,
                            &attempt_permits,
                            &context,
                            &batch,
                        )
                        .await
                    }
                    None => candidates
                        .iter()
                        .map(|_| Err(CoreError::ShuttingDown))
                        .collect(),
                };
                pending = settle_publish_attempt(
                    &mut results,
                    indices.into_iter().zip(candidates),
                    observed,
                );
                if pending.is_empty() || attempt + 1 == CONTENTION_RETRY_LIMIT {
                    break;
                }
                retry_count += 1;
                self.claim_publish_slot().await;
            }
            let results = results
                .into_iter()
                .map(|result| {
                    result
                        .expect("each candidate received a publication result")
                        .map_err(RuntimeError::Core)
                })
                .collect::<Vec<_>>();
            (results, retry_count)
        }
        .instrument(publish_span.clone())
        .await;
        publish_span.record("result", batch_result_label(&results).as_str());
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        self.deliver_batch_results(commit_ids, results, selected_at);
    }

    /// Publishes through the publisher-owned engine: one namespace, one
    /// engine, one writer session, for the publisher's whole life.
    async fn publish_through_engine(
        &self,
        writer: &Arc<WriterBits>,
        candidates: &[CommitCandidate],
        permits: &[Arc<AdmissionPermit>],
        context: &loonfs_core::MutationContext,
        batch: &Deadline,
    ) -> Vec<Result<Commit, CoreError>> {
        let mut slot = self.engine.lock().await;
        let engine = self.engine_for(&mut slot);
        let publish = crate::fs::publish_batch_with_engine(
            &self.runtime_core,
            writer,
            &self.namespace_id,
            engine,
            candidates,
            context,
            batch,
        )
        .await;
        if publish.wal_tail_discovered {
            self.runtime_core.instruments().publisher_tail_replay();
        }
        if !publish.results.iter().any(is_retryable_wal_publish) {
            for permit in permits {
                permit.release_inline();
            }
        }
        let write_stopped = publish.results.iter().any(is_maintenance_required);
        if write_stopped {
            self.runtime_core
                .instruments()
                .publisher_write_stop_refusal();
        }
        let fold_start = if publish.wal_tail_objects >= FOLD_AT_WAL_OBJECTS
            || publish.wal_tail_inline_bytes >= self.inline_content.inline_content_fold_at_bytes
            || write_stopped
        {
            self.start_fold()
        } else {
            None
        };
        let retained_tail_weight = engine.retained_tail_weight();
        if publish.wal_tail_observed {
            slot.last_known_wal_tail_inline_bytes = Some(publish.wal_tail_inline_bytes);
        }
        self.settle_retained_projection(retained_tail_weight);
        drop(slot);
        if let Some(start) = fold_start {
            let _ = start.send(());
        }
        publish.results
    }

    /// Returns the publisher's lazily created commit engine.
    ///
    /// Each publish loads the namespace identity from the head, so construction
    /// only needs the shared segment cache and writer session.
    fn engine_for<'slot>(&self, slot: &'slot mut EngineSlot) -> &'slot mut NamespaceCommitEngine {
        slot.engine.get_or_insert_with(|| {
            NamespaceCommitEngine::new(self.namespace_id.clone())
                .monotonic_timer(Arc::clone(&self.timer))
                .segment_cache(self.runtime_core.metadata_segment_cache())
                .writer_session(Arc::clone(&slot.session))
        })
    }

    /// Starts a fold task after its triggering publication releases the engine.
    /// A panicked fold is retried by the next publish over the threshold. The
    /// drain reports the contained panic.
    fn start_fold(&self) -> Option<oneshot::Sender<()>> {
        let mut state = self.lock_state();
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| !fold.task.is_finished())
        {
            return None;
        }
        let fold_id = state.next_fold_id;
        state.next_fold_id = state.next_fold_id.wrapping_add(1);
        let (start, started) = oneshot::channel();
        let (exit, liveness) = watch::channel(false);
        let publisher = self.clone();
        let task = self.runtime.spawn(async move {
            let exit = FoldExit(exit);
            if started.await.is_ok()
                && AssertUnwindSafe(publisher.run_fold())
                    .catch_unwind()
                    .await
                    .is_err()
            {
                publisher.record_panic();
            }
            drop(exit);
            publisher.forget_if_ended();
        });
        state.fold = Some(FoldHandle {
            fold_id,
            task,
            liveness,
        });
        Some(start)
    }

    async fn run_fold(&self) {
        let Some(writer) = self.writer.upgrade() else {
            return;
        };
        let waiting = WaitingFold::new(&writer.wal_folds_waiting, self.runtime_core.instruments());
        let _permit = writer
            .wal_fold_permits
            .acquire()
            .await
            .expect("fold permit semaphore should remain open");
        drop(waiting);
        let input = {
            let mut slot = self.engine.lock().await;
            let input = slot
                .engine
                .as_ref()
                .and_then(NamespaceCommitEngine::wal_fold_input);
            if input.as_ref().is_some_and(|input| {
                input.wal_tail_objects < FOLD_AT_WAL_OBJECTS
                    && input.wal_tail_inline_bytes
                        < self.inline_content.inline_content_fold_at_bytes
            }) {
                return;
            }
            slot.engine
                .as_mut()
                .and_then(NamespaceCommitEngine::begin_wal_fold)
        };
        match self.runtime_core.now_ms() {
            Ok(_) => {}
            Err(error) => {
                phase_event!(
                    self.runtime_core,
                    "wal_fold",
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "WAL-tail fold failed"
                );
                return;
            }
        };
        let started_ms = self.timer.monotonic_now_ms();
        let segment_cache = self.runtime_core.metadata_segment_cache();
        let result = loonfs_core::fold_wal_tail(
            self.runtime_core.store(),
            Some(segment_cache.as_ref()),
            &self.namespace_id,
            input,
            &Deadline::start(Arc::clone(&self.timer)),
        )
        .instrument(phase_span!(
            self.runtime_core,
            "wal_fold",
            self.namespace_id
        ))
        .await;
        self.runtime_core
            .instruments()
            .publisher_wal_fold_duration(self.elapsed_ms_since(started_ms));
        self.engine
            .lock()
            .await
            .record_fold_outcome(result.as_ref().ok());
        match result {
            Ok(_) => {
                self.runtime_core.instruments().publisher_wal_fold();
            }
            Err(error) => {
                let error = RuntimeError::Core(error);
                phase_event!(
                    self.runtime_core,
                    "wal_fold",
                    self.namespace_id,
                    tracing::Level::WARN,
                    error = %error.public_message(),
                    "WAL-tail fold failed"
                );
            }
        }
        writer.notify_fold_finished(&self.namespace_id);
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
                state.admission = PublisherAdmissionState::Deleted;
                take_queued_waiters(&mut state)
            };
            // A deleted session has ended. It leaves the table before the
            // delete's callers hear the outcome, so an open after that
            // starts a fresh session whose publish fails on the durable
            // tombstone. Handles to this session fail fast on `Deleted`.
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
        let waiting = WaitingFold::new(&writer.wal_folds_waiting, self.runtime_core.instruments());
        let _permit = writer
            .wal_fold_permits
            .acquire()
            .await
            .expect("fold permit semaphore should remain open");
        drop(waiting);
        let mut slot = self.engine.lock().await;
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

    async fn wait_for_fold(&self) -> Result<(), RuntimeError> {
        let fold = self
            .lock_state()
            .fold
            .as_ref()
            .map(|fold| (fold.fold_id, fold.liveness.clone(), fold.task.is_finished()));
        let Some((fold_id, mut liveness, finished)) = fold else {
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
            if state
                .fold
                .as_ref()
                .is_some_and(|fold| fold.fold_id == fold_id)
            {
                state.fold.take().map(|fold| fold.task)
            } else {
                None
            }
        };
        if let Some(task) = task {
            task.await.map_err(|error| {
                RuntimeError::RuntimeTask(format!("WAL-tail fold task failed: {error}"))
            })?;
        }
        Ok(())
    }

    /// Waits until the namespace may start another WAL put.
    async fn await_publish_slot(&self) {
        loop {
            let Some(last_publish) = self.lock_state().last_publish.clone() else {
                break;
            };
            let remaining_ms =
                duration_ms(self.min_publish_interval).saturating_sub(last_publish.age_ms());
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
            // Positional pairing is meaningless once lengths differ, so a
            // count mismatch fails every candidate instead of delivering
            // misaligned results to the earlier ones.
            let count_mismatch = (results.len() != commit_ids.len()).then(|| {
                RuntimeError::Core(CoreError::Internal(format!(
                    "publisher batch returned {got} results for {want} candidates",
                    got = results.len(),
                    want = commit_ids.len(),
                )))
            });
            let mut results = results.into_iter();
            for commit_id in commit_ids {
                let result = match &count_mismatch {
                    Some(error) => Err(error.clone()),
                    None => results
                        .next()
                        .expect("equal-length batch should hold one result per candidate"),
                };
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
    for _ in 0..CONTENTION_RETRY_LIMIT {
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
                        committed_fingerprint: Some(primary_identity.as_str().to_owned()),
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

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[allow(clippy::disallowed_methods)]
// The configured publication pacing delay does not affect publication validity.
async fn wait_for_publish_pacing(delay: Duration) {
    tokio::time::sleep(delay).await;
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "publisher/tests.rs"]
mod tests;
