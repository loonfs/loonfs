//! Runtime publication service for namespace mutations.
//!
//! Each namespace has one publisher and one worker queue. The publisher:
//!
//! - batches concurrent commits into one WAL segment and one head CAS,
//! - joins duplicate in-flight commit IDs and rejects conflicting reuse,
//! - orders namespace deletion after earlier work and before later work,
//! - retries stale or unknown CAS outcomes with the same commit IDs, and
//! - enforces the configured minimum interval between head CAS attempts.
//!
//! A publisher keeps one commit engine and writer session for its lifetime.
//! The writer session is small and cannot be reconstructed after fencing.
//! The WAL-tail projection is rebuildable, so the registry evicts projections
//! when the shared cache budget is exceeded.
//!
//! The first request for an idle namespace publishes immediately. Requests
//! that arrive during a publish or its pacing interval join the next batch.
//! Cancelling a caller does not cancel admitted work.
//!
//! Shutdown first closes admission and then drains admitted work. Use
//! [`FsWriter::shutdown`](crate::FsWriter::shutdown), which also coordinates
//! shutdown with the maintenance runner.

use crate::fs::{ReadCore, WriterBits};
use crate::metrics::PublishOutcome;
use crate::publish::CommitCandidate;
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, RuntimeCacheConfig, RuntimeError,
};
use futures::FutureExt;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::{CommitId, NamespaceId};
use loonfs_core::commit::{CommitFingerprint, CommitHeadPublishError};
// Publisher head-CAS races use the core-wide bounded contention retry limit.
use loonfs_core::limits::CONTENTION_RETRY_LIMIT;
use loonfs_core::publish::{NamespaceCommitEngine, PublishTailWeight, SharedWriterSessionState};
use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::Instrument;

type CommitResult = Result<ApiCommitResponse, RuntimeError>;
type DeleteResult = Result<DeleteNamespaceResponse, RuntimeError>;

/// A synchronous notification after one mutation batch durably advances a
/// namespace.
///
/// The callback runs on the publication task after durability and before
/// results are delivered. It must not block; enqueue any follow-up work onto
/// a non-blocking channel. The sequence is the highest committed sequence in
/// that publication batch.
pub type PublishObserver = Arc<dyn Fn(&NamespaceId, loonfs_api::ChangeSeq) + Send + Sync + 'static>;

/// Maximum candidates queued for one namespace before admission reports
/// `commit_queue_full`.
const MAX_BATCH_CANDIDATES: usize = 1024;

/// Registry of per-namespace publishers owned by one writer.
///
/// Clones share the same publishers and worker tasks. Submit through the
/// registry returned by [`FsWriter::publisher`](crate::FsWriter::publisher).
///
/// Shutdown closes admission and then drains admitted work. Prefer
/// [`FsWriter::shutdown`](crate::FsWriter::shutdown), which also coordinates
/// the maintenance runner.
#[derive(Clone)]
pub struct PublisherRegistry {
    shared: Arc<RegistryShared>,
    /// Strong: the read core owns neither this registry nor the writer, so
    /// holding it here cannot cycle. Publications read through its caches
    /// and seed them with what they produce.
    read_core: ReadCore,
    /// Weak: the writer owns its bits, and a publication is the writer's
    /// work. Publish work upgrades per unit and reports `shutting_down`
    /// once the writer is gone, so dropping the writer stops new work
    /// without ever leaving the caches or store dangling.
    writer: Weak<WriterBits>,
    min_publish_interval: Duration,
    trace_mode: &'static str,
    trace_store_kind: &'static str,
}

/// State shared by all publishers in this registry: admission status,
/// publisher instances, retained projections, and contained panic count.
struct RegistryShared {
    state: Mutex<RegistryState>,
    /// Publications and deletes whose panic a worker survived. Workers
    /// contain panics to keep their namespace writable, so this — not a
    /// task join error — is what a drain reports.
    panicked_units: AtomicUsize,
}

struct RegistryState {
    closed: bool,
    publishers: HashMap<NamespaceId, NamespacePublisher>,
    projections: RetainedProjections,
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

    fn evict(&self, namespace_id: &NamespaceId) -> RetainedProjectionTotals {
        let mut state = self.lock_state();
        state.publishers.remove(namespace_id);
        state.projections.forget(namespace_id);
        state.projections.totals()
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
    ) -> RetainedProjectionTotals {
        let victims = {
            let mut state = self.lock_state();
            state.projections.record(namespace_id, weight);
            let selected = state.projections.over_budget_victims(budget);
            selected
                .into_iter()
                .map(|victim| {
                    let publisher = state.publishers.get(&victim.namespace_id).cloned();
                    (victim, publisher)
                })
                .collect::<Vec<_>>()
        };
        // Invalidate projections without holding the registry lock.
        // Publication may acquire the engine lock before the registry lock,
        // so eviction must not acquire them in the opposite order.
        let dropped = victims
            .into_iter()
            .filter(|(_, publisher)| {
                // No publisher means no engine, so nothing is retained under
                // that namespace either way.
                publisher
                    .as_ref()
                    .is_none_or(NamespacePublisher::invalidate_projection)
            })
            .map(|(victim, _)| victim)
            .collect::<Vec<_>>();

        let mut state = self.lock_state();
        for victim in dropped {
            state.projections.forget_recorded(&victim);
        }
        state.projections.totals()
    }

    /// Forgets one namespace's retained projection after something outside
    /// the publish path dropped it.
    fn forget_projection(&self, namespace_id: &NamespaceId) -> RetainedProjectionTotals {
        let mut state = self.lock_state();
        state.projections.forget(namespace_id);
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
    /// Least-recently-published first, one entry per namespace whose engine
    /// retains a projection.
    entries: VecDeque<RetainedProjection>,
    rows: usize,
    decoded_bytes: usize,
    next_stamp: u64,
}

#[derive(Debug, Clone)]
struct RetainedProjection {
    namespace_id: NamespaceId,
    weight: PublishTailWeight,
    /// Distinguishes the projection a sweep selected from a newer one the
    /// namespace published while that sweep ran outside the lock, so a
    /// completed eviction never forgets a live projection.
    stamp: u64,
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
        self.forget(namespace_id);
        let Some(weight) = weight else {
            return;
        };
        self.next_stamp += 1;
        self.rows = self.rows.saturating_add(weight.rows);
        self.decoded_bytes = self.decoded_bytes.saturating_add(weight.decoded_bytes);
        self.entries.push_back(RetainedProjection {
            namespace_id: namespace_id.clone(),
            weight,
            stamp: self.next_stamp,
        });
    }

    fn forget(&mut self, namespace_id: &NamespaceId) {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.namespace_id == *namespace_id);
        self.remove_entry(index);
    }

    /// Forgets exactly the projection an eviction dropped. A namespace that
    /// published again while the sweep ran outside the lock carries a newer
    /// stamp, so its live projection stays accounted.
    fn forget_recorded(&mut self, recorded: &RetainedProjection) {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.stamp == recorded.stamp);
        self.remove_entry(index);
    }

    /// Drops one entry and its weight from the totals. At most one entry per
    /// namespace exists, so callers locate it by whichever key they hold.
    fn remove_entry(&mut self, index: Option<usize>) {
        let Some(entry) = index.and_then(|index| self.entries.remove(index)) else {
            return;
        };
        self.rows = self.rows.saturating_sub(entry.weight.rows);
        self.decoded_bytes = self
            .decoded_bytes
            .saturating_sub(entry.weight.decoded_bytes);
    }

    /// Selects least-recently-published projections until the remaining totals
    /// fit the same three limits used by the read-side projection cache.
    ///
    /// Selection does not change accounting. Totals are updated only after a
    /// selected projection is actually invalidated.
    fn over_budget_victims(&self, budget: &RuntimeCacheConfig) -> Vec<RetainedProjection> {
        let mut projections = self.entries.len();
        let mut rows = self.rows;
        let mut decoded_bytes = self.decoded_bytes;
        let mut victims = Vec::new();
        for entry in &self.entries {
            if projections <= budget.max_cached_namespaces
                && rows <= budget.max_cached_wal_tail_projection_rows
                && decoded_bytes <= budget.max_cached_wal_tail_projection_decoded_bytes
            {
                break;
            }
            projections = projections.saturating_sub(1);
            rows = rows.saturating_sub(entry.weight.rows);
            decoded_bytes = decoded_bytes.saturating_sub(entry.weight.decoded_bytes);
            victims.push(entry.clone());
        }
        victims
    }

    fn totals(&self) -> RetainedProjectionTotals {
        RetainedProjectionTotals {
            projections: self.entries.len(),
            rows: self.rows,
            decoded_bytes: self.decoded_bytes,
        }
    }
}

impl PublisherRegistry {
    /// Creates the registry a writer owns. Batches publish through each
    /// publisher's own commit engine and writer session, and the writer's
    /// [`FsBackgroundWork`](crate::FsBackgroundWork) policy governs any
    /// post-publish maintenance.
    pub(crate) fn new(
        read_core: ReadCore,
        writer: Weak<WriterBits>,
        min_publish_interval: Duration,
        trace_mode: &'static str,
        trace_store_kind: &'static str,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                state: Mutex::new(RegistryState {
                    closed: false,
                    publishers: HashMap::new(),
                    projections: RetainedProjections::default(),
                }),
                panicked_units: AtomicUsize::new(0),
            }),
            read_core,
            writer,
            min_publish_interval,
            trace_mode,
            trace_store_kind,
        }
    }

    /// Submits a namespace deletion, sequenced as a barrier: mutations
    /// admitted before it publish first, and mutations admitted after it
    /// fail once the delete succeeds.
    pub async fn submit_delete(
        &self,
        namespace_id: NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> DeleteResult {
        let publisher = self.publisher_for(&namespace_id)?;
        publisher.submit_delete(options).await
    }

    /// Submits one already-classified candidate; the runtime's direct
    /// mutation paths funnel through this.
    pub async fn submit_candidate(
        &self,
        namespace_id: NamespaceId,
        candidate: CommitCandidate,
    ) -> CommitResult {
        let publisher = self.publisher_for(&namespace_id)?;
        publisher.submit(candidate).await
    }

    /// Invalidates the namespace's rebuildable WAL-tail projection without
    /// changing its writer epoch or fencing state.
    ///
    /// If an operation currently holds the engine, invalidation is skipped. That
    /// operation validates the live head and reports its retained projection when
    /// it completes.
    pub(crate) fn invalidate_projection(&self, namespace_id: &NamespaceId) {
        let publisher = self
            .shared
            .lock_state()
            .publishers
            .get(namespace_id)
            .cloned();
        let Some(publisher) = publisher else {
            return;
        };
        if publisher.invalidate_projection() {
            let totals = self.shared.forget_projection(namespace_id);
            self.read_core
                .instruments()
                .publisher_retained_projections(totals.projections, totals.decoded_bytes);
        }
    }

    /// Looks up or creates the namespace's publisher, refusing once
    /// admission is closed.
    fn publisher_for(&self, namespace_id: &NamespaceId) -> Result<NamespacePublisher, CoreError> {
        let mut state = self.shared.lock_state();
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        Ok(state
            .publishers
            .entry(namespace_id.clone())
            .or_insert_with(|| {
                NamespacePublisher::new(
                    namespace_id.clone(),
                    self.read_core.clone(),
                    self.writer.clone(),
                    Arc::downgrade(&self.shared),
                    self.min_publish_interval,
                    self.trace_mode,
                    self.trace_store_kind,
                )
            })
            .clone())
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
            state.publishers.values().cloned().collect()
        };
        // Close each publisher without holding the registry lock. Publisher
        // operations may acquire their own state before the registry, so
        // shutdown must not acquire those locks in the opposite order.
        for publisher in publishers {
            publisher.close_admission();
        }
    }

    /// Waits for all current publisher workers to finish.
    ///
    /// Returns an error if any publication or deletion panicked and the worker
    /// contained the panic. Call [`Self::close_admission`] first to prevent new
    /// work from being admitted during the drain.
    pub async fn drain(&self) -> Result<(), RuntimeError> {
        let publishers: Vec<NamespacePublisher> = self
            .shared
            .lock_state()
            .publishers
            .values()
            .cloned()
            .collect();
        // Awaited outside the registry lock, for the same nesting reason as
        // the admission sweep.
        for publisher in publishers {
            publisher.wait_for_worker().await;
        }
        let panicked = self.shared.panicked_units.load(Ordering::SeqCst);
        if panicked > 0 {
            return Err(RuntimeError::RuntimeTask(format!(
                "{panicked} publisher task(s) panicked"
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct NamespacePublisher {
    namespace_id: NamespaceId,
    read_core: ReadCore,
    /// Weak for the same reason as the registry's reference: a publication
    /// is the owning writer's work, and it stops when that writer is gone.
    writer: Weak<WriterBits>,
    state: Arc<Mutex<NamespacePublisherState>>,
    /// Locked only by the worker, across one publication or delete, and by
    /// [`Self::invalidate_projection`], which never waits for it.
    engine: Arc<AsyncMutex<EngineSlot>>,
    /// Weak: the registry map owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    min_publish_interval: Duration,
    trace_mode: &'static str,
    trace_store_kind: &'static str,
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
    /// Earliest instant the next head compare-and-swap may start. `None` is
    /// a cold namespace: it publishes immediately.
    next_allowed_cas_at: Option<Instant>,
}

/// One publisher's worker task, split so a drain can await it without
/// making the publisher look idle.
struct WorkerHandle {
    /// Taken by a drain, which awaits it. The task itself is unaffected.
    task: Option<JoinHandle<()>>,
    /// Answers admission's single-flight check, which must stay answerable
    /// while a drain holds `task`. Never used to abort.
    liveness: tokio::task::AbortHandle,
}

struct PendingDelete {
    options: DeleteNamespaceOptions,
    waiters: Vec<oneshot::Sender<DeleteResult>>,
}

enum WorkItem {
    Batch(OpenBatch),
    Delete(PendingDelete),
}

struct OpenBatch {
    candidates: Vec<BatchCandidate>,
}

#[derive(Clone)]
struct BatchCandidate {
    commit_id: CommitId,
    candidate: CommitCandidate,
    enqueued_at: Instant,
}

struct InFlightRequest {
    semantic_identity: CommitFingerprint,
    waiters: Vec<oneshot::Sender<CommitResult>>,
}

impl NamespacePublisher {
    fn new(
        namespace_id: NamespaceId,
        read_core: ReadCore,
        writer: Weak<WriterBits>,
        shared: Weak<RegistryShared>,
        min_publish_interval: Duration,
        trace_mode: &'static str,
        trace_store_kind: &'static str,
    ) -> Self {
        Self {
            namespace_id,
            read_core,
            writer,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                admission: PublisherAdmissionState::Open,
                worker: None,
                next_allowed_cas_at: None,
            })),
            engine: Arc::new(AsyncMutex::new(EngineSlot {
                engine: None,
                session: SharedWriterSessionState::default(),
            })),
            shared,
            min_publish_interval,
            trace_mode,
            trace_store_kind,
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

    /// Returns the error for the current admission state, or succeeds when open.
    fn check_admission(&self, state: &NamespacePublisherState) -> Result<(), CoreError> {
        match state.admission {
            PublisherAdmissionState::Open => Ok(()),
            PublisherAdmissionState::Closed => Err(CoreError::ShuttingDown),
            PublisherAdmissionState::Deleted => Err(self.namespace_deleted()),
        }
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
    /// projection in the engine. This path may acquire the registry lock while
    /// holding the engine lock. Eviction avoids deadlock by releasing the
    /// registry lock before attempting to lock any engine.
    fn settle_retained_projection(&self, weight: Option<PublishTailWeight>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let budget = self.read_core.runtime_cache_config();
        let totals = shared.settle_projection(&self.namespace_id, weight, budget);
        self.report_retained_projections(totals);
    }

    fn report_retained_projections(&self, totals: RetainedProjectionTotals) {
        self.read_core
            .instruments()
            .publisher_retained_projections(totals.projections, totals.decoded_bytes);
    }

    /// Admits the request before awaiting its result.
    ///
    /// The first poll either admits the candidate or returns an error. After
    /// admission, cancelling the caller only drops result delivery; the worker
    /// still owns and publishes the request.
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to record queue latency.
    async fn submit(&self, candidate: CommitCandidate) -> CommitResult {
        let commit_id = candidate.commit_id().clone();
        let enqueued_at = Instant::now();
        let semantic_identity = candidate.semantic_identity(&self.namespace_id)?;
        let (sender, receiver) = oneshot::channel();
        self.admit(commit_id, candidate, semantic_identity, sender, enqueued_at)?;
        receiver.await.unwrap_or_else(|_| {
            Err(
                CoreError::HeadPublish(CommitHeadPublishError::OutcomeUnknown(
                    "publisher task stopped before reporting an outcome".to_owned(),
                ))
                .into(),
            )
        })
    }

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: CommitCandidate,
        semantic_identity: CommitFingerprint,
        waiter: oneshot::Sender<CommitResult>,
        enqueued_at: Instant,
    ) -> Result<(), CoreError> {
        let mut state = self.lock_state();
        self.check_admission(&state)?;
        if let Some(existing) = state.in_flight.get_mut(&commit_id) {
            if existing.semantic_identity != semantic_identity {
                // Both claims are still in flight, so nothing has landed
                // under this id for the caller to read back.
                return Err(CoreError::CommitIdReuseConflict {
                    commit_id: commit_id.to_string(),
                    committed_seq: None,
                    committed_fingerprint: None,
                });
            }
            existing.waiters.push(waiter);
            self.trace_enqueue(queued_candidates(&state), "duplicate");
            return Ok(());
        }

        let queued = queued_candidates(&state);
        if queued >= MAX_BATCH_CANDIDATES {
            self.trace_enqueue(queued, "full");
            return Err(CoreError::CommitQueueFull);
        }
        let candidate = BatchCandidate {
            commit_id: commit_id.clone(),
            candidate,
            enqueued_at,
        };
        match state.queue.back_mut() {
            // Coalesce with the tail batch, unless a delete sits at the tail:
            // work admitted after a delete opens a batch behind it and
            // publishes only if that delete fails.
            Some(WorkItem::Batch(batch)) => batch.candidates.push(candidate),
            _ => state.queue.push_back(WorkItem::Batch(OpenBatch {
                candidates: vec![candidate],
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
        Ok(())
    }

    /// Enqueues the delete as a barrier: requests admitted before it
    /// publish first, and requests admitted after it fail with
    /// `namespace_deleted` once it succeeds. If the delete fails (for
    /// example a stale `expected_head_seq`), later requests publish
    /// normally — nothing is rejected for a delete that did not happen.
    async fn submit_delete(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.lock_state();
            self.check_admission(&state)?;
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
        receiver.await.unwrap_or_else(|_| {
            Err(
                CoreError::HeadPublish(CommitHeadPublishError::OutcomeUnknown(
                    "publisher task stopped mid-delete".to_owned(),
                ))
                .into(),
            )
        })
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
            .is_some_and(|worker| !worker.liveness.is_finished())
        {
            return;
        }
        let publisher = self.clone();
        let task = tokio::spawn(async move {
            publisher.run_worker().await;
        });
        state.worker = Some(WorkerHandle {
            liveness: task.abort_handle(),
            task: Some(task),
        });
    }

    /// Drains the queue in admission order, then exits.
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to record batch collection latency.
    async fn run_worker(self) {
        loop {
            let collect_started = Instant::now();
            let queue_depth_start = queued_candidates(&self.lock_state());
            // Do not add a separate batching delay. The first request for an idle
            // namespace publishes immediately; requests arriving during a publish or
            // pacing interval form the next batch.
            self.await_cas_slot(false).await;
            let Some(item) = self.take_next_item() else {
                return;
            };

            match item {
                WorkItem::Batch(batch) => {
                    self.read_core
                        .instruments()
                        .publisher_batch(batch.candidates.len());
                    tracing::info!(
                        phase = "batch_collect",
                        mode = self.trace_mode,
                        store_kind = self.trace_store_kind,
                        batch_size = usize_to_u64(batch.candidates.len()),
                        queue_depth_start = usize_to_u64(queue_depth_start),
                        queue_depth_end = usize_to_u64(batch.candidates.len()),
                        collect_ms = elapsed_ms_since(collect_started),
                        "publisher.batch_collect"
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
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to pace CAS attempts.
    fn take_next_item(&self) -> Option<WorkItem> {
        let mut state = self.lock_state();
        // Terminal: a successful delete emptied the queue and set this
        // before its worker returned, so nothing may be taken afterwards.
        if matches!(state.admission, PublisherAdmissionState::Deleted) || state.queue.is_empty() {
            state.worker = None;
            return None;
        }
        let item = state.queue.pop_front();
        state.next_allowed_cas_at = Some(Instant::now() + self.min_publish_interval);
        let queue_depth = queued_candidates(&state);
        drop(state);
        self.read_core
            .instruments()
            .publisher_queue_depth(queue_depth);
        item
    }

    /// Publishes one batch while containing panics from the publication.
    ///
    /// This worker is the namespace's only publication path. If publication
    /// panics, each request in the batch receives `commit_outcome_unknown`
    /// because the panic may have occurred before or after the head CAS. Callers
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
        let orphaned_waiters = {
            let mut state = self.lock_state();
            taken_commit_ids
                .into_iter()
                .filter_map(|commit_id| state.in_flight.remove(&commit_id))
                .flat_map(|request| request.waiters)
                .collect::<Vec<_>>()
        };
        for waiter in orphaned_waiters {
            let _ = waiter.send(Err(CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown("publish task aborted mid-batch".to_owned()),
            )
            .into()));
        }
    }

    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to record publication latency.
    async fn publish_taken_batch(&self, candidates: Vec<BatchCandidate>) {
        let selected_at = Instant::now();
        for candidate in &candidates {
            tracing::info!(
                phase = "wait_for_batch",
                mode = self.trace_mode,
                store_kind = self.trace_store_kind,
                result = "ok",
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at),
                "publisher.wait_for_batch"
            );
        }

        let publish_span = tracing::debug_span!(
            "loonfs.phase",
            phase = "batch_publish",
            mode = self.trace_mode,
            store_kind = self.trace_store_kind,
            batch_size = usize_to_u64(candidates.len()),
            result = tracing::field::Empty,
            retry_count = tracing::field::Empty
        );
        let (results, retry_count) = async {
            let mut results = Vec::new();
            let mut retry_count = 0_u64;
            for attempt in 0..CONTENTION_RETRY_LIMIT {
                let batch_candidates = candidates
                    .iter()
                    .map(|candidate| candidate.candidate.clone())
                    .collect::<Vec<_>>();
                let Some(writer) = self.writer.upgrade() else {
                    results = candidates
                        .iter()
                        .map(|_| Err(CoreError::ShuttingDown.into()))
                        .collect();
                    break;
                };
                results = self.publish_through_engine(&writer, batch_candidates).await;
                if !results.iter().any(is_retryable_head_publish) {
                    break;
                }
                if attempt + 1 == CONTENTION_RETRY_LIMIT {
                    break;
                }
                retry_count += 1;
                self.await_cas_slot(true).await;
            }
            (results, retry_count)
        }
        .instrument(publish_span.clone())
        .await;
        publish_span.record("result", batch_result_label(&results).as_str());
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        self.deliver_batch_results(candidates, results, selected_at);
    }

    /// Publishes through the publisher-owned engine: one namespace, one
    /// engine, one writer session, for the publisher's whole life.
    async fn publish_through_engine(
        &self,
        writer: &Arc<WriterBits>,
        candidates: Vec<CommitCandidate>,
    ) -> Vec<CommitResult> {
        let mut slot = self.engine.lock().await;
        let engine = self.engine_for(&mut slot);
        let results = crate::fs::publish_batch_with_engine(
            &self.read_core,
            writer,
            &self.namespace_id,
            engine,
            candidates,
        )
        .await;
        self.settle_retained_projection(engine.retained_tail_weight());
        results
    }

    /// Returns the publisher's lazily created commit engine.
    ///
    /// Each publish loads the namespace identity from the head, so construction
    /// only needs the shared table cache and writer session.
    fn engine_for<'slot>(&self, slot: &'slot mut EngineSlot) -> &'slot mut NamespaceCommitEngine {
        slot.engine.get_or_insert_with(|| {
            NamespaceCommitEngine::new(self.namespace_id.clone())
                .table_cache(self.read_core.metadata_table_cache())
                .writer_session(Arc::clone(&slot.session))
        })
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
        match outcome {
            Ok(response) => {
                // Tombstone first, then fail everything that queued behind
                // the delete; admissions from here on fail fast.
                let queued = {
                    let mut state = self.lock_state();
                    state.admission = PublisherAdmissionState::Deleted;
                    take_queued_waiters(&mut state)
                };
                // The publisher is terminal; drop it from the registry map
                // so the map stays bounded by live namespaces. Clones still
                // in flight fail fast on `Deleted`, and a later submission
                // gets a fresh publisher whose publish fails on the durable
                // tombstone.
                if let Some(shared) = self.shared.upgrade() {
                    self.report_retained_projections(shared.evict(&self.namespace_id));
                }
                for waiter in waiters {
                    let _ = waiter.send(Ok(response.clone()));
                }
                for waiter in queued.commits {
                    let _ = waiter.send(Err(self.namespace_deleted().into()));
                }
                for waiter in queued.deletes {
                    let _ = waiter.send(Err(self.namespace_deleted().into()));
                }
                true
            }
            Err(error) => {
                // The namespace was not deleted (stale precondition, fencing
                // conflict, ...). Report it and let queued work publish.
                for waiter in waiters {
                    let _ = waiter.send(Err(error.clone()));
                }
                false
            }
        }
    }

    async fn delete_through_engine(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        let Some(writer) = self.writer.upgrade() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let mut slot = self.engine.lock().await;
        let engine = self.engine_for(&mut slot);
        crate::fs::delete_namespace_with_engine(
            &self.read_core,
            &writer.identity,
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
    /// Only the join handle is taken; the liveness half stays in the slot,
    /// so an admission racing this drain still sees a live worker instead of
    /// spawning a second one for the same queue.
    async fn wait_for_worker(&self) {
        let task = self
            .lock_state()
            .worker
            .as_mut()
            .and_then(|worker| worker.task.take());
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Waits until the namespace may start another head CAS.
    ///
    /// When `claim` is true, this method also reserves the next slot by advancing
    /// the deadline one pacing interval. When false, it only waits; the caller
    /// reserves the slot when it removes work from the queue.
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to wait between CAS attempts.
    async fn await_cas_slot(&self, claim: bool) {
        loop {
            let Some(sleep_until) = self.lock_state().next_allowed_cas_at else {
                break;
            };
            if sleep_until <= Instant::now() {
                break;
            }
            tokio::time::sleep_until(sleep_until).await;
        }
        if claim {
            let mut state = self.lock_state();
            state.next_allowed_cas_at = Some(Instant::now() + self.min_publish_interval);
        }
    }

    fn deliver_batch_results(
        &self,
        candidates: Vec<BatchCandidate>,
        results: Vec<CommitResult>,
        selected_at: Instant,
    ) {
        let mut deliveries = Vec::new();
        let mut wait_traces = Vec::new();
        {
            let mut state = self.lock_state();
            // Positional pairing is meaningless once lengths differ, so a
            // count mismatch fails every candidate instead of delivering
            // misaligned results to the earlier ones.
            let count_mismatch = (results.len() != candidates.len()).then(|| {
                RuntimeError::Core(CoreError::Internal(format!(
                    "publisher batch returned {got} results for {want} candidates",
                    got = results.len(),
                    want = candidates.len(),
                )))
            });
            let mut results = results.into_iter();
            for candidate in candidates {
                let result = match &count_mismatch {
                    Some(error) => Err(error.clone()),
                    None => results
                        .next()
                        .expect("equal-length batch should hold one result per candidate"),
                };
                wait_traces.push((result_label(&result), elapsed_ms_since(selected_at)));
                if let Some(in_flight) = state.in_flight.remove(&candidate.commit_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
        }

        for (result, wait_ms) in wait_traces {
            self.read_core.instruments().publisher_publish(result);
            tracing::info!(
                phase = "wait_for_result",
                mode = self.trace_mode,
                store_kind = self.trace_store_kind,
                result = result.as_str(),
                wait_ms,
                "publisher.wait_for_result"
            );
        }

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }
    }

    fn trace_enqueue(&self, queue_depth: usize, reason: &'static str) {
        self.read_core
            .instruments()
            .publisher_queue_depth(queue_depth);
        tracing::info!(
            phase = "enqueue",
            mode = self.trace_mode,
            store_kind = self.trace_store_kind,
            queue_depth = usize_to_u64(queue_depth),
            reason,
            "publisher.enqueue"
        );
    }
}

/// Waiters a landed delete barrier leaves behind, one vector per result
/// type.
#[derive(Default)]
struct QueuedWaiters {
    commits: Vec<oneshot::Sender<CommitResult>>,
    deletes: Vec<oneshot::Sender<DeleteResult>>,
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

/// Returns whether publication should retry the batch.
///
/// Retrying with the same commit IDs is safe because committed candidates
/// replay their durable receipts. Both stale-head and unknown-outcome errors
/// are retried to obtain a definite result.
fn is_retryable_head_publish(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(RuntimeError::Core(CoreError::HeadPublish(
            CommitHeadPublishError::StaleHead | CommitHeadPublishError::OutcomeUnknown(_)
        )))
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

fn elapsed_ms_since(start: Instant) -> u64 {
    duration_ms(start.elapsed())
}

fn elapsed_ms_from(start: Instant, end: Instant) -> u64 {
    duration_ms(end.saturating_duration_since(start))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "publisher/tests.rs"]
mod tests;
