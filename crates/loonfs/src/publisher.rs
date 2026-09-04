//! Runtime publication service for namespace mutations.
//!
//! Each namespace has one queue. Concurrent commits may share a WAL segment
//! and head update. Duplicate commit IDs join in flight, conflicting reuse is
//! rejected, and namespace deletion is ordered with other mutations.
//!
//! Admitted work continues if its caller is cancelled. Shutdown closes
//! admission and drains the queues through
//! [`FsWriter::shutdown`](crate::FsWriter::shutdown).

use crate::fs::{ReadCore, WriterBits};
use crate::metrics::{PublishOutcome, RESULT_OK};
use crate::publish::CommitCandidate;
use crate::trace::{phase_event, phase_span};
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, NamespaceSessionPolicy,
    RuntimeCacheConfig, RuntimeError,
};
use futures::FutureExt;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::{ChangeSeq, CommitId, NamespaceId};
use loonfs_core::cache::Recency;
use loonfs_core::commit::{CommitFingerprint, CommitHeadPublishError};
use loonfs_core::limits::{CHECKPOINT_AT_WAL_SEGMENTS, CONTENTION_RETRY_LIMIT};
use loonfs_core::publish::{
    NamespaceCommitEngine, PublishTailWeight, SharedWriterSessionState, WriterSessionState,
};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::Instrument;

type CommitResult = Result<ApiCommitResponse, RuntimeError>;
type DeleteResult = Result<DeleteNamespaceResponse, RuntimeError>;
type CloseCompletion = watch::Receiver<Option<CloseNamespaceReport>>;

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
/// [`FsWriterBuilder::namespace_advance_observer`](crate::FsWriterBuilder::namespace_advance_observer),
/// which documents what the callback may do.
pub type NamespaceAdvanceObserver = Arc<dyn Fn(NamespaceAdvanceHint) + Send + Sync + 'static>;

/// Result of closing one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseNamespaceReport {
    /// False when no session was open.
    pub was_open: bool,
    /// Commits admitted before the close and published during the drain.
    pub drained_commits: usize,
    /// Whether the closed session had been fenced.
    pub fenced: bool,
}

/// Current state of one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSessionState {
    /// No session is open.
    ///
    /// Under [`NamespaceSessionPolicy::ExplicitOpen`], mutations fail with
    /// `writer_session_closed`.
    Closed,
    /// The session is admitting work.
    Open {
        /// Whether another writer superseded this session.
        fenced: bool,
        /// Commits waiting to be published.
        queued_commits: usize,
    },
    /// A close is draining admitted work.
    Closing,
}

/// Totals for the namespace writer sessions held by one writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterSessionStats {
    /// Sessions admitting work.
    pub open: usize,
    /// Sessions draining a close.
    pub closing: usize,
    /// Open sessions that have been fenced.
    pub fenced: usize,
    /// Maximum sessions this writer may hold at once.
    pub capacity: usize,
}

/// Maximum candidates queued for one namespace before admission reports
/// `commit_queue_full`.
const MAX_BATCH_CANDIDATES: usize = 1024;

/// Registry of per-namespace publishers owned by one writer.
///
/// Clones share the same publishers and worker tasks. Submit through the
/// registry returned by [`FsWriter::publisher`](crate::FsWriter::publisher).
///
/// Shutdown closes admission and then drains admitted work. Prefer
/// [`FsWriter::shutdown`](crate::FsWriter::shutdown), which closes admission
/// before draining publication work.
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
    runtime: Handle,
    timer: Arc<dyn MonotonicTimer>,
    min_publish_interval: Duration,
}

/// State shared by all publishers in this registry: admission status,
/// publisher instances, retained projections, and contained panic count.
struct RegistryShared {
    state: Mutex<RegistryState>,
    /// Publication, deletion, and namespace-close units whose panic a task
    /// survived. Tasks contain panics to keep the registry usable, so this —
    /// not a task join error — is what a drain reports.
    panicked_units: AtomicUsize,
}

struct RegistryState {
    closed: bool,
    policy: NamespaceSessionPolicy,
    capacity: NonZeroUsize,
    publishers: HashMap<NamespaceId, NamespacePublisher>,
    closing: HashMap<NamespaceId, CloseCompletion>,
    projections: RetainedProjections,
}

impl RegistryState {
    fn session_counts(&self) -> (usize, usize) {
        (
            self.publishers.len() - self.closing.len(),
            self.closing.len(),
        )
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

    fn evict(
        &self,
        namespace_id: &NamespaceId,
        instruments: &crate::metrics::RuntimeInstruments,
    ) -> RetainedProjectionTotals {
        let mut state = self.lock_state();
        if !state.closing.contains_key(namespace_id) {
            state.publishers.remove(namespace_id);
        }
        state.projections.forget(namespace_id);
        let (open, closing) = state.session_counts();
        instruments.publisher_sessions(open, closing);
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
                .publishers
                .get(&victim)
                .is_none_or(NamespacePublisher::invalidate_projection)
            {
                state.projections.remove_entry(&victim);
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
        self.entries.len() > budget.max_cached_namespaces
            || self.rows > budget.max_cached_wal_tail_projection_rows
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
        read_core: ReadCore,
        writer: Weak<WriterBits>,
        runtime: Handle,
        min_publish_interval: Duration,
        policy: NamespaceSessionPolicy,
        capacity: NonZeroUsize,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                state: Mutex::new(RegistryState {
                    closed: false,
                    policy,
                    capacity,
                    publishers: HashMap::new(),
                    closing: HashMap::new(),
                    projections: RetainedProjections::default(),
                }),
                panicked_units: AtomicUsize::new(0),
            }),
            read_core,
            writer,
            runtime,
            timer: Arc::new(StdMonotonicTimer::default()),
            min_publish_interval,
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
        let receiver = {
            let mut state = self.shared.lock_state();
            let publisher = self.publisher_for(&mut state, &namespace_id, false)?;
            publisher.admit_delete(options)?
        };
        receive_delete(receiver).await
    }

    /// Submits one already-classified candidate; the runtime's direct
    /// mutation paths funnel through this.
    pub async fn submit_candidate(
        &self,
        namespace_id: NamespaceId,
        candidate: CommitCandidate,
    ) -> CommitResult {
        submit_with_admission(
            &namespace_id,
            candidate,
            self.timer.as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                let mut state = self.shared.lock_state();
                let publisher = self.publisher_for(&mut state, &namespace_id, false)?;
                publisher.admit(commit_id, candidate, semantic_identity, waiter, enqueued_at)
            },
        )
        .await
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
            let Some(publisher) = state.publishers.get(namespace_id) else {
                return;
            };
            if !publisher.invalidate_projection() {
                return;
            }
            state.projections.remove_entry(namespace_id);
            state.projections.totals()
        };
        self.read_core
            .instruments()
            .publisher_retained_projections(totals.projections, totals.decoded_bytes);
    }

    #[cfg(test)]
    fn test_publisher_for(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespacePublisher, CoreError> {
        let mut state = self.shared.lock_state();
        self.publisher_for(&mut state, namespace_id, false)
    }

    fn publisher_for(
        &self,
        state: &mut RegistryState,
        namespace_id: &NamespaceId,
        explicit_open: bool,
    ) -> Result<NamespacePublisher, CoreError> {
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        if state.closing.contains_key(namespace_id) {
            return Err(CoreError::WriterSessionClosed {
                namespace_id: namespace_id.clone(),
            });
        }
        if let Some(publisher) = state.publishers.get(namespace_id) {
            return Ok(publisher.clone());
        }
        if !explicit_open && state.policy == NamespaceSessionPolicy::ExplicitOpen {
            return Err(CoreError::WriterSessionClosed {
                namespace_id: namespace_id.clone(),
            });
        }
        if state.publishers.len() >= state.capacity.get() {
            return Err(CoreError::WriterCapacityExceeded {
                max_open_namespaces: state.capacity.get(),
            });
        }
        let publisher = NamespacePublisher::new(
            namespace_id.clone(),
            self.read_core.clone(),
            self.writer.clone(),
            Arc::downgrade(&self.shared),
            self.runtime.clone(),
            Arc::clone(&self.timer),
            self.min_publish_interval,
        );
        state
            .publishers
            .insert(namespace_id.clone(), publisher.clone());
        let (open, closing) = state.session_counts();
        self.report_session_counts(open, closing);
        Ok(publisher)
    }

    pub(crate) fn open_namespace(&self, namespace_id: &NamespaceId) -> Result<(), CoreError> {
        let mut state = self.shared.lock_state();
        self.publisher_for(&mut state, namespace_id, true)?;
        Ok(())
    }

    pub(crate) async fn close_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CloseNamespaceReport, CoreError> {
        let (completion, close_in_progress) = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(CoreError::ShuttingDown);
            }
            let Some(publisher) = state.publishers.get(namespace_id).cloned() else {
                return Ok(CloseNamespaceReport {
                    was_open: false,
                    drained_commits: 0,
                    fenced: false,
                });
            };
            if let Some(completion) = state.closing.get(namespace_id) {
                (completion.clone(), true)
            } else {
                // Flipping admission while the registry lock is held is the close
                // linearization point.
                let drained_commits = publisher.close_session_admission();
                let (sender, completion) = watch::channel(None);
                state
                    .closing
                    .insert(namespace_id.clone(), completion.clone());
                let (open, closing) = state.session_counts();
                self.report_session_counts(open, closing);
                let shared = Arc::clone(&self.shared);
                let namespace_id = namespace_id.clone();
                self.runtime.spawn(async move {
                    finish_namespace_close(
                        shared,
                        publisher,
                        namespace_id,
                        drained_commits,
                        sender,
                    )
                    .await;
                });
                (completion, false)
            }
        };
        let mut report = wait_for_close(completion).await?;
        if close_in_progress {
            report.was_open = false;
            report.drained_commits = 0;
        }
        Ok(report)
    }

    pub(crate) fn namespace_session_state(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceSessionState {
        let publisher = {
            let state = self.shared.lock_state();
            if state.closing.contains_key(namespace_id) {
                return NamespaceSessionState::Closing;
            }
            state.publishers.get(namespace_id).cloned()
        };
        let Some(publisher) = publisher else {
            return NamespaceSessionState::Closed;
        };
        NamespaceSessionState::Open {
            fenced: publisher.session_is_fenced(),
            queued_commits: publisher.queued_commits(),
        }
    }

    pub(crate) fn writer_session_stats(&self) -> WriterSessionStats {
        let (publishers, open, closing, capacity) = {
            let state = self.shared.lock_state();
            let publishers = state
                .publishers
                .iter()
                .filter_map(|(namespace_id, publisher)| {
                    (!state.closing.contains_key(namespace_id)).then_some(publisher.clone())
                })
                .collect::<Vec<_>>();
            let (open, closing) = state.session_counts();
            (publishers, open, closing, state.capacity.get())
        };
        WriterSessionStats {
            open,
            closing,
            fenced: publishers
                .iter()
                .filter(|publisher| publisher.session_is_fenced())
                .count(),
            capacity,
        }
    }

    pub(crate) async fn wait_for_fold(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(), RuntimeError> {
        let publisher = self
            .shared
            .lock_state()
            .publishers
            .get(namespace_id)
            .cloned();
        if let Some(publisher) = publisher {
            publisher.wait_for_fold().await?;
        }
        Ok(())
    }

    fn report_session_counts(&self, open: usize, closing: usize) {
        self.read_core
            .instruments()
            .publisher_sessions(open, closing);
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
        for publisher in &publishers {
            publisher.wait_for_worker().await;
        }
        let mut task_error = None;
        for publisher in publishers {
            if let Err(error) = publisher.wait_for_fold().await {
                task_error.get_or_insert(error);
            }
        }
        let closes = self
            .shared
            .lock_state()
            .closing
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for completion in closes {
            let _ = wait_for_close(completion).await;
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

async fn wait_for_close(
    mut completion: CloseCompletion,
) -> Result<CloseNamespaceReport, CoreError> {
    loop {
        if let Some(report) = *completion.borrow_and_update() {
            return Ok(report);
        }
        // The sender drops without a report only when the runtime shut down
        // under the close task.
        if completion.changed().await.is_err() {
            return Err(CoreError::ShuttingDown);
        }
    }
}

async fn finish_namespace_close(
    shared: Arc<RegistryShared>,
    publisher: NamespacePublisher,
    namespace_id: NamespaceId,
    drained_commits: usize,
    sender: watch::Sender<Option<CloseNamespaceReport>>,
) {
    let fenced = match AssertUnwindSafe(async {
        publisher.wait_for_worker().await;
        if let Err(error) = publisher.wait_for_fold().await {
            tracing::info!(
                namespace_id = %publisher.namespace_id,
                error = %error,
                "wal fold failed while the namespace session closed"
            );
        }
        publisher.session_is_fenced()
    })
    .catch_unwind()
    .await
    {
        Ok(fenced) => fenced,
        Err(_) => {
            shared.panicked_units.fetch_add(1, Ordering::SeqCst);
            publisher.session_is_fenced()
        }
    };
    let totals = {
        let mut state = shared.lock_state();
        state.publishers.remove(&namespace_id);
        state.projections.forget(&namespace_id);
        state.closing.remove(&namespace_id);
        let (open, closing) = state.session_counts();
        publisher
            .read_core
            .instruments()
            .publisher_sessions(open, closing);
        state.projections.totals()
    };
    publisher.report_retained_projections(totals);
    let _ = sender.send(Some(CloseNamespaceReport {
        was_open: true,
        drained_commits,
        fenced,
    }));
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
    session: SharedWriterSessionState,
    /// Weak: the registry map owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    runtime: Handle,
    timer: Arc<dyn MonotonicTimer>,
    min_publish_interval: Duration,
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
    next_fold_generation: u64,
    /// Earliest instant the next head compare-and-swap may start. `None` is
    /// a cold namespace: it publishes immediately.
    next_allowed_cas_at: Option<u64>,
}

struct WorkerHandle {
    _task: JoinHandle<()>,
    liveness: watch::Receiver<bool>,
}

struct FoldHandle {
    generation: u64,
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
    enqueued_at: u64,
}

struct InFlightRequest {
    semantic_identity: CommitFingerprint,
    waiters: Vec<oneshot::Sender<CommitResult>>,
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
        candidate: CommitCandidate,
        semantic_identity: CommitFingerprint,
    },
}

impl NamespacePublisher {
    fn new(
        namespace_id: NamespaceId,
        read_core: ReadCore,
        writer: Weak<WriterBits>,
        shared: Weak<RegistryShared>,
        runtime: Handle,
        timer: Arc<dyn MonotonicTimer>,
        min_publish_interval: Duration,
    ) -> Self {
        let session = SharedWriterSessionState::default();
        Self {
            namespace_id,
            read_core,
            writer,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                admission: PublisherAdmissionState::Open,
                worker: None,
                fold: None,
                next_fold_generation: 0,
                next_allowed_cas_at: None,
            })),
            engine: Arc::new(AsyncMutex::new(EngineSlot {
                engine: None,
                session: Arc::clone(&session),
            })),
            session,
            shared,
            runtime,
            timer,
            min_publish_interval,
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

    fn close_session_admission(&self) -> usize {
        let mut state = self.lock_state();
        if matches!(state.admission, PublisherAdmissionState::Open) {
            state.admission = PublisherAdmissionState::SessionClosed;
        }
        state.in_flight.len()
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

    fn queued_commits(&self) -> usize {
        queued_candidates(&self.lock_state())
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
    /// The first poll either admits the candidate, waits behind an in-flight
    /// claim on its commit ID, or returns an error. Once the candidate enters
    /// the queue, cancelling the caller only drops result delivery; the worker
    /// still owns and publishes the request.
    #[cfg(test)]
    async fn submit(&self, candidate: CommitCandidate) -> CommitResult {
        submit_with_admission(
            &self.namespace_id,
            candidate,
            self.timer.as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                self.admit(commit_id, candidate, semantic_identity, waiter, enqueued_at)
            },
        )
        .await
    }

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: CommitCandidate,
        semantic_identity: CommitFingerprint,
        waiter: oneshot::Sender<CommitResult>,
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
                    candidate,
                    semantic_identity,
                });
            }
            existing.waiters.push(waiter);
            self.trace_enqueue(queued_candidates(&state), "duplicate");
            return Ok(SubmissionAdmission::OwnOutcome);
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
        Ok(SubmissionAdmission::OwnOutcome)
    }

    /// Enqueues the delete as a barrier: requests admitted before it
    /// publish first, and requests admitted after it fail with
    /// `namespace_deleted` once it succeeds. If the delete fails (for
    /// example a stale `expected_head_seq`), later requests publish
    /// normally — nothing is rejected for a delete that did not happen.
    #[cfg(test)]
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
            // Do not add a separate batching delay. The first request for an idle
            // namespace publishes immediately; requests arriving during a publish or
            // pacing interval form the next batch.
            self.await_cas_slot().await;
            let Some(item) = self.take_next_item() else {
                return;
            };

            match item {
                WorkItem::Batch(batch) => {
                    self.read_core
                        .instruments()
                        .publisher_batch(batch.candidates.len());
                    phase_event!(
                        self.read_core,
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
        self.reserve_next_cas_slot(&mut state);
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

    async fn publish_taken_batch(&self, candidates: Vec<BatchCandidate>) {
        let selected_at = self.timer.monotonic_now_ms();
        for candidate in &candidates {
            phase_event!(
                self.read_core,
                "wait_for_batch",
                self.namespace_id,
                tracing::Level::DEBUG,
                result = RESULT_OK,
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at)
            );
        }

        let publish_span = phase_span!(
            self.read_core,
            "batch_publish",
            self.namespace_id,
            batch_size = usize_to_u64(candidates.len()),
            result = tracing::field::Empty,
            retry_count = tracing::field::Empty,
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
                self.claim_cas_slot().await;
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
        let publish = crate::fs::publish_batch_with_engine(
            &self.read_core,
            writer,
            &self.namespace_id,
            engine,
            candidates,
        )
        .await;
        let write_stopped =
            !publish.results.is_empty() && publish.results.iter().all(is_maintenance_required);
        if write_stopped {
            self.read_core.instruments().publisher_write_stop_refusal();
        }
        let fold_start = if publish.wal_tail_segments >= CHECKPOINT_AT_WAL_SEGMENTS || write_stopped
        {
            self.start_fold()
        } else {
            None
        };
        if !self.read_core.control_cache_enabled() {
            engine.invalidate_projection();
        }
        self.settle_retained_projection(engine.retained_tail_weight());
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
                .segment_cache(self.read_core.metadata_segment_cache())
                .writer_session(Arc::clone(&slot.session))
        })
    }

    fn start_fold(&self) -> Option<oneshot::Sender<()>> {
        let mut state = self.lock_state();
        if state
            .fold
            .as_ref()
            .is_some_and(|fold| !fold.task.is_finished())
        {
            return None;
        }
        let generation = state.next_fold_generation;
        state.next_fold_generation = state.next_fold_generation.wrapping_add(1);
        let (start, started) = oneshot::channel();
        let (exit, liveness) = watch::channel(false);
        let publisher = self.clone();
        let task = self.runtime.spawn(async move {
            let _exit = FoldExit(exit);
            if started.await.is_err() {
                return;
            }
            if AssertUnwindSafe(publisher.run_fold())
                .catch_unwind()
                .await
                .is_err()
            {
                publisher.record_panic();
            }
        });
        state.fold = Some(FoldHandle {
            generation,
            task,
            liveness,
        });
        Some(start)
    }

    async fn run_fold(&self) {
        let Some(writer) = self.writer.upgrade() else {
            return;
        };
        let waiting = writer.wal_folds_waiting.fetch_add(1, Ordering::SeqCst) + 1;
        self.read_core
            .instruments()
            .publisher_wal_folds_waiting(waiting);
        let _permit = writer
            .wal_fold_permits
            .acquire()
            .await
            .expect("fold permit semaphore should remain open");
        let waiting = writer.wal_folds_waiting.fetch_sub(1, Ordering::SeqCst) - 1;
        self.read_core
            .instruments()
            .publisher_wal_folds_waiting(waiting);
        let snapshot = {
            let slot = self.engine.lock().await;
            let snapshot = slot
                .engine
                .as_ref()
                .and_then(NamespaceCommitEngine::wal_fold_snapshot);
            if snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.wal_tail_segments < CHECKPOINT_AT_WAL_SEGMENTS)
            {
                return;
            }
            snapshot
        };
        let context = match writer.identity.mutation_context() {
            Ok(context) => context,
            Err(error) => {
                tracing::info!(
                    namespace_id = %self.namespace_id,
                    error = %error,
                    "WAL-tail fold failed"
                );
                return;
            }
        };
        let started_ms = self.timer.monotonic_now_ms();
        let segment_cache = self.read_core.metadata_segment_cache();
        let result = loonfs_core::fold_wal_tail(
            self.read_core.store(),
            Some(segment_cache.as_ref()),
            &self.namespace_id,
            snapshot,
            &context,
            self.timer.as_ref(),
        )
        .instrument(phase_span!(self.read_core, "wal_fold", self.namespace_id))
        .await;
        self.read_core
            .instruments()
            .publisher_wal_fold_duration(self.elapsed_ms_since(started_ms));
        match result {
            Ok(_) => {
                self.read_core.instruments().publisher_wal_fold();
                writer.notify_after_fold(&self.namespace_id);
            }
            Err(error) => {
                tracing::info!(
                    namespace_id = %self.namespace_id,
                    error = %error.message(),
                    "WAL-tail fold failed"
                );
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
        match outcome {
            Ok(response) => {
                // Tombstone first, then fail everything that queued behind
                // the delete; admissions from here on fail fast.
                let queued = {
                    let mut state = self.lock_state();
                    state.admission = PublisherAdmissionState::Deleted;
                    take_queued_waiters(&mut state)
                };
                if let Err(error) = self.wait_for_fold().await {
                    tracing::info!(
                        namespace_id = %self.namespace_id,
                        error = %error,
                        "wal fold failed while the namespace was deleted"
                    );
                }
                // The publisher is terminal; drop it from the registry map
                // so the map stays bounded by live namespaces. Clones still
                // in flight fail fast on `Deleted`, and a later submission
                // gets a fresh publisher whose publish fails on the durable
                // tombstone.
                if let Some(shared) = self.shared.upgrade() {
                    let totals = shared.evict(&self.namespace_id, self.read_core.instruments());
                    self.report_retained_projections(totals);
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
        let fold = self.lock_state().fold.as_ref().map(|fold| {
            (
                fold.generation,
                fold.liveness.clone(),
                fold.task.is_finished(),
            )
        });
        let Some((generation, mut liveness, finished)) = fold else {
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
                .is_some_and(|fold| fold.generation == generation)
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

    /// Waits until the namespace may start another head CAS.
    async fn await_cas_slot(&self) {
        loop {
            let Some(sleep_until) = self.lock_state().next_allowed_cas_at else {
                break;
            };
            let now_ms = self.timer.monotonic_now_ms();
            if sleep_until <= now_ms {
                break;
            }
            wait_for_cas_pacing(Duration::from_millis(sleep_until - now_ms)).await;
        }
    }

    async fn claim_cas_slot(&self) {
        self.await_cas_slot().await;
        self.reserve_next_cas_slot(&mut self.lock_state());
    }

    fn reserve_next_cas_slot(&self, state: &mut NamespacePublisherState) {
        state.next_allowed_cas_at = Some(
            self.timer
                .monotonic_now_ms()
                .saturating_add(duration_ms(self.min_publish_interval)),
        );
    }

    fn elapsed_ms_since(&self, started_at_ms: u64) -> u64 {
        elapsed_ms_from(started_at_ms, self.timer.monotonic_now_ms())
    }

    fn deliver_batch_results(
        &self,
        candidates: Vec<BatchCandidate>,
        results: Vec<CommitResult>,
        selected_at: u64,
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
                wait_traces.push((result_label(&result), self.elapsed_ms_since(selected_at)));
                if let Some(in_flight) = state.in_flight.remove(&candidate.commit_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
        }

        for (outcome, wait_ms) in wait_traces {
            self.read_core.instruments().publisher_publish(outcome);
            phase_event!(
                self.read_core,
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
        self.read_core
            .instruments()
            .publisher_queue_depth(queue_depth);
        phase_event!(
            self.read_core,
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
    candidate: CommitCandidate,
    timer: &dyn MonotonicTimer,
    mut admit: F,
) -> CommitResult
where
    F: FnMut(
        CommitId,
        CommitCandidate,
        CommitFingerprint,
        oneshot::Sender<CommitResult>,
        u64,
    ) -> Result<SubmissionAdmission, CoreError>,
{
    let commit_id = candidate.commit_id().clone();
    let enqueued_at = timer.monotonic_now_ms();
    let mut candidate = candidate;
    let mut semantic_identity = candidate.semantic_identity(namespace_id)?;
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
            CoreError::HeadPublish(CommitHeadPublishError::OutcomeUnknown(
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
                    candidate = returned_candidate;
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
        Err(
            CoreError::HeadPublish(CommitHeadPublishError::OutcomeUnknown(
                "publisher task stopped mid-delete".to_owned(),
            ))
            .into(),
        )
    })
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

fn is_maintenance_required(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(RuntimeError::Core(error))
            if error.code() == loonfs_core::ErrorCode::MaintenanceRequired
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
// The configured CAS pacing delay does not affect publication validity.
async fn wait_for_cas_pacing(delay: Duration) {
    tokio::time::sleep(delay).await;
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "publisher/tests.rs"]
mod tests;
