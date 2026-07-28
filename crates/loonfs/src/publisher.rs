//! The runtime's publication service: every mutation publishes through
//! here.
//!
//! [`PublisherRegistry`] funnels every mutation for a namespace through one
//! per-namespace publisher that:
//!
//! - coalesces concurrent requests into a single batched publication (one
//!   WAL segment, one head compare-and-swap),
//! - deduplicates resubmissions by commit id — a duplicate joins the
//!   in-flight request, while reusing a commit id for semantically
//!   different contents is rejected,
//! - sequences namespace deletion as a barrier: work admitted before the
//!   delete publishes first, work admitted after it fails once the delete
//!   succeeds,
//! - retries stale-head and unknown-outcome compare-and-swap results with
//!   the same commit ids, so durable receipts replay instead of surfacing
//!   ambiguity, and
//! - paces successive head compare-and-swap attempts per namespace.
//!
//! A publisher owns its namespace's commit engine and writer session for its
//! whole life, so it is the single writer of head-advancing state: batches
//! and the delete barrier run through one engine, under one session epoch,
//! on one worker task draining one queue.
//!
//! Every runtime core owns one registry, and the direct
//! [`FsWriter`](crate::FsWriter) mutation methods, the reference server,
//! and any embedded host with many in-process writer agents all submit
//! through it — one publication implementation, one batching policy, one
//! delete barrier. [`FsWriter::publisher`](crate::FsWriter::publisher)
//! exposes the writer's registry for hosts that want to submit
//! already-classified candidates directly.
//!
//! Batching is adaptive, driven by one knob (the runtime's
//! `min_publish_interval_ms`): a submission to a cold namespace publishes
//! immediately, and while a publish is in flight or the namespace is
//! within the pacing interval of its last publication start, later
//! submissions coalesce into the next batch. A solo writer submitting
//! sequentially therefore pays no added latency, while sustained
//! concurrent load amortizes into batches at most one publication per
//! interval — larger batches and fewer WAL segments instead of head
//! compare-and-swap thrash. The trade sits on a cold burst: its first
//! submission publishes alone and the rest coalesce into the next paced
//! batch, so one extra segment buys the immediate first flush.
//!
//! Admitted work is owned by the publisher's worker task, never by the
//! caller futures awaiting results: a cancelled caller abandons only its
//! result delivery, and the publication still lands. At shutdown,
//! [`PublisherRegistry::close_admission`] refuses new submissions with
//! `shutting_down` and [`PublisherRegistry::drain`] settles everything
//! already admitted; the reference server runs both once its listener
//! drains, and
//! [`FsWriter::shutdown_background`](crate::FsWriter::shutdown_background)
//! drains without closing, so the handle stays usable.

use crate::fs::{FsCore, FsInner};
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent, PreparedContent};
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, RuntimeError};
use futures::FutureExt;
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::{CommitId, NamespaceId};
use loonfs_core::commit::{CommitHeadPublishError, SemanticMutationIdentity};
// Publisher head-CAS races use the core-wide bounded contention retry limit.
use loonfs_core::limits::CONTENTION_RETRY_LIMIT;
use loonfs_core::publish::{NamespaceCommitEngine, SharedWriterSessionState};
use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::Instrument;

type CommitResult = Result<ApiCommitResponse, CoreError>;
type DeleteResult = Result<DeleteNamespaceResponse, CoreError>;

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

/// Shared front door to the per-namespace publishers of one runtime core.
///
/// Cloning is cheap; clones share the same per-namespace publishers, so
/// every writer in the process should submit through clones of one
/// registry — [`FsWriter::publisher`](crate::FsWriter::publisher) hands
/// out exactly that.
///
/// The registry owns the worker tasks its publishers spawn. Shut it down
/// in two steps once the host stops accepting work:
/// [`Self::close_admission`], then [`Self::drain`].
#[derive(Clone)]
pub struct PublisherRegistry {
    shared: Arc<RegistryShared>,
    /// Weak: the runtime core owns this registry, so a strong reference
    /// back would cycle the runtime into a leak. Publish work upgrades per
    /// use and reports `shutting_down` once the core is gone.
    core: Weak<FsInner>,
    min_publish_interval: Duration,
    trace_mode: &'static str,
    trace_store_kind: &'static str,
}

/// Registry state every publisher reaches back into: admission gating, the
/// publisher map, and the panic tally a shutdown reports.
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
}

impl RegistryShared {
    // Recover a poisoned lock instead of `expect`: every critical section
    // over this state is a plain field update, and turning one panicked
    // publication into a permanently unusable registry is exactly the
    // failure the workers' panic containment exists to prevent.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn evict(&self, namespace_id: &NamespaceId) {
        self.lock_state().publishers.remove(namespace_id);
    }
}

impl PublisherRegistry {
    /// Creates the registry a runtime core owns. Batches publish through
    /// each publisher's own commit engine and writer session, and the
    /// core's [`FsBackgroundWork`](crate::FsBackgroundWork) policy governs
    /// any post-publish maintenance.
    pub(crate) fn from_core(
        core: Weak<FsInner>,
        min_publish_interval: Duration,
        trace_mode: &'static str,
        trace_store_kind: &'static str,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                state: Mutex::new(RegistryState {
                    closed: false,
                    publishers: HashMap::new(),
                }),
                panicked_units: AtomicUsize::new(0),
            }),
            core,
            min_publish_interval,
            trace_mode,
            trace_store_kind,
        }
    }

    /// Submits one explicit semantic commit request through the
    /// namespace's publisher.
    pub async fn submit_commit(
        &self,
        namespace_id: NamespaceId,
        request: ApiCommitRequest,
    ) -> CommitResult {
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::commit(request))
            .await
    }

    /// Submits one path-level mutation intent through the namespace's
    /// publisher.
    pub async fn submit_path_intent(
        &self,
        namespace_id: NamespaceId,
        intent: PathMutationIntent,
    ) -> CommitResult {
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::path(intent))
            .await
    }

    /// Submits a path-level mutation intent together with opaque proofs for
    /// its already-prepared content.
    pub async fn submit_path_intent_with_prepared_content(
        &self,
        namespace_id: NamespaceId,
        intent: PathMutationIntent,
        content: Vec<PreparedContent>,
    ) -> CommitResult {
        self.submit_candidate(
            namespace_id,
            NamespaceMutationCandidate::path_prepared(intent, content),
        )
        .await
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
        candidate: NamespaceMutationCandidate,
    ) -> CommitResult {
        let publisher = self.publisher_for(&namespace_id)?;
        publisher.submit(candidate).await
    }

    /// Drops the rebuildable half of the namespace's publish state (its WAL
    /// tail projection). The session's epoch and fencing are untouched:
    /// they are facts about this process, not a cache.
    ///
    /// A held engine means a publication or delete is in flight; that unit
    /// revalidates against the live head itself, so skipping it is safe.
    pub(crate) fn invalidate_engine(&self, namespace_id: &NamespaceId) {
        let publisher = self.shared.lock_state().publishers.get(namespace_id).cloned();
        if let Some(publisher) = publisher {
            publisher.invalidate_engine();
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
                    self.core.clone(),
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

    /// Closes the front door: every later submission fails with
    /// `shutting_down`, while already-admitted work keeps publishing.
    /// Idempotent.
    pub fn close_admission(&self) {
        let publishers: Vec<NamespacePublisher> = {
            let mut state = self.shared.lock_state();
            state.closed = true;
            state.publishers.values().cloned().collect()
        };
        // Swept outside the registry lock: locks nest publisher-then-
        // registry on the eviction path, never the other way around.
        for publisher in publishers {
            publisher.close_admission();
        }
    }

    /// Waits for every publisher's worker to settle the work it owns,
    /// surfacing publications whose panic the worker contained.
    ///
    /// Call [`Self::close_admission`] first for a terminal drain; without
    /// it this settles only the work admitted so far, and new submissions
    /// keep scheduling more.
    pub async fn drain(&self) -> Result<(), RuntimeError> {
        let publishers: Vec<NamespacePublisher> =
            self.shared.lock_state().publishers.values().cloned().collect();
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
    /// Weak for the same reason as the registry's reference: the core owns
    /// the registry that owns this publisher.
    core: Weak<FsInner>,
    state: Arc<Mutex<NamespacePublisherState>>,
    /// Locked only by the worker, across one publication or delete, and by
    /// [`Self::invalidate_engine`], which never waits for it.
    engine: Arc<AsyncMutex<EngineSlot>>,
    /// Weak: the registry map owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with an unowned worker.
    shared: Weak<RegistryShared>,
    min_publish_interval: Duration,
    trace_mode: &'static str,
    trace_store_kind: &'static str,
}

/// The publisher's commit engine and writer session for its namespace.
///
/// A writer session's acquired epoch and terminal fencing record are facts
/// about this process, not cached views of durable state: nothing in the
/// store can rebuild "this session was fenced". When they lived inside the
/// LRU control cache, eviction erased them (and cache-disabled runs never
/// kept them at all), so a fenced writer could silently bump the epoch back
/// and fence the legitimate writer instead. They live here instead, with the
/// publisher that owns every head-advancing write for the namespace: a few
/// dozen bytes per namespace this process has published to, released only
/// with the publisher itself, once the namespace is deleted and its id can
/// never rebind.
struct EngineSlot {
    /// Built on the first unit of work and kept for the publisher's life.
    /// Invalidation drops only its rebuildable tail projection.
    engine: Option<NamespaceCommitEngine>,
    /// Never dropped or rebuilt while the publisher lives.
    session: SharedWriterSessionState,
}

struct NamespacePublisherState {
    /// Admitted work in admission order. Mutations coalesce into the tail
    /// batch, so a delete queued between them keeps its barrier position.
    queue: VecDeque<WorkItem>,
    in_flight: HashMap<CommitId, InFlightRequest>,
    /// Terminal: set once a delete succeeds. Admissions fail fast from then
    /// on without touching the store.
    deleted: bool,
    /// Set by the registry's admission close. Later admissions fail with
    /// `shutting_down`; everything already queued keeps publishing.
    closed: bool,
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
    candidate: NamespaceMutationCandidate,
    operation_class: &'static str,
    enqueued_at: Instant,
}

struct InFlightRequest {
    semantic_identity: SemanticMutationIdentity,
    waiters: Vec<oneshot::Sender<CommitResult>>,
}

impl NamespacePublisher {
    fn new(
        namespace_id: NamespaceId,
        core: Weak<FsInner>,
        shared: Weak<RegistryShared>,
        min_publish_interval: Duration,
        trace_mode: &'static str,
        trace_store_kind: &'static str,
    ) -> Self {
        Self {
            namespace_id,
            core,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                queue: VecDeque::new(),
                in_flight: HashMap::new(),
                deleted: false,
                closed: false,
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

    /// The owning runtime core, while it is still alive. `None` means the
    /// core was dropped without draining; admitted work then settles as
    /// `shutting_down`.
    fn core(&self) -> Option<FsCore> {
        self.core.upgrade().map(|inner| FsCore { inner })
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

    fn close_admission(&self) {
        self.lock_state().closed = true;
    }

    fn invalidate_engine(&self) {
        if let Ok(mut slot) = self.engine.try_lock() {
            if let Some(engine) = slot.engine.as_mut() {
                engine.invalidate();
            }
        }
    }

    /// The path to `admit` is await-free: a submission future's first poll
    /// either admits the candidate or fails, and only then parks on the
    /// result channel. Cancellation tests rely on this — after one poll of
    /// a submission, the publication is admitted and owned by the worker.
    async fn submit(&self, candidate: NamespaceMutationCandidate) -> CommitResult {
        let commit_id = candidate.commit_id().clone();
        let enqueued_at = Instant::now();
        let semantic_identity = candidate.semantic_identity(&self.namespace_id)?;
        let operation_class = operation_class(&semantic_identity);
        let (sender, receiver) = oneshot::channel();
        self.admit(
            commit_id,
            candidate,
            semantic_identity,
            sender,
            operation_class,
            enqueued_at,
        )?;
        receiver.await.unwrap_or_else(|_| {
            Err(CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown(
                    "publisher task stopped before reporting an outcome".to_owned(),
                ),
            ))
        })
    }

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: NamespaceMutationCandidate,
        semantic_identity: SemanticMutationIdentity,
        waiter: oneshot::Sender<CommitResult>,
        operation_class: &'static str,
        enqueued_at: Instant,
    ) -> Result<(), CoreError> {
        let mut state = self.lock_state();
        if state.deleted {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: self.namespace_id.clone(),
            });
        }
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        if let Some(existing) = state.in_flight.get_mut(&commit_id) {
            if existing.semantic_identity != semantic_identity {
                return Err(CoreError::CommitIdReuseConflict(commit_id.to_string()));
            }
            existing.waiters.push(waiter);
            self.trace_enqueue(operation_class, queued_candidates(&state), "duplicate");
            return Ok(());
        }

        let queued = queued_candidates(&state);
        if queued >= MAX_BATCH_CANDIDATES {
            self.trace_enqueue(operation_class, queued, "full");
            return Err(CoreError::CommitQueueFull);
        }
        let candidate = BatchCandidate {
            commit_id: commit_id.clone(),
            candidate,
            operation_class,
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
        self.trace_enqueue(operation_class, queued + 1, "new");
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
            if state.deleted {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: self.namespace_id.clone(),
                });
            }
            if state.closed {
                return Err(CoreError::ShuttingDown);
            }
            match state.queue.back_mut() {
                // A delete already queued at the tail is the same barrier:
                // both callers get its outcome.
                Some(WorkItem::Delete(pending)) => pending.waiters.push(sender),
                _ => state.queue.push_back(WorkItem::Delete(PendingDelete {
                    options,
                    waiters: vec![sender],
                })),
            }
            self.ensure_worker(&mut state);
        }
        receiver.await.unwrap_or_else(|_| {
            Err(CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown(
                    "publisher task stopped mid-delete".to_owned(),
                ),
            ))
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
    async fn run_worker(self) {
        loop {
            let collect_started = Instant::now();
            let queue_depth_start = queued_candidates(&self.lock_state());
            // There is no fixed coalescing wait — batches form from what
            // arrives while a publication is in flight or while the pacing
            // interval since the last publication start runs out, so a cold
            // namespace publishes its first submission immediately.
            self.await_cas_slot(false).await;
            let Some(item) = self.take_next_item() else {
                return;
            };

            match item {
                WorkItem::Batch(batch) => {
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
    fn take_next_item(&self) -> Option<WorkItem> {
        let mut state = self.lock_state();
        // Terminal: a successful delete emptied the queue and set this
        // before its worker returned, so nothing may be taken afterwards.
        if state.deleted || state.queue.is_empty() {
            state.worker = None;
            return None;
        }
        let item = state.queue.pop_front();
        state.next_allowed_cas_at = Some(Instant::now() + self.min_publish_interval);
        item
    }

    /// Publishes one taken batch, containing a panic in the publication.
    ///
    /// Deliberate v0 scope, not defensive habit. This publisher is the only
    /// path by which its namespace accepts writes, so a panic that killed
    /// the worker would leave that namespace unwritable until the process
    /// restarts, with every taken waiter hanging. The taken requests instead
    /// settle as `commit_outcome_unknown` — the panic may have struck either
    /// side of the head compare-and-swap, and that is an answer callers
    /// already know how to resolve: retry with the same commit id and the
    /// durable receipt replays. The worker keeps its queue and moves on.
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
            )));
        }
    }

    async fn publish_taken_batch(&self, candidates: Vec<BatchCandidate>) {
        let selected_at = Instant::now();
        for candidate in &candidates {
            tracing::info!(
                phase = "wait_for_batch",
                mode = self.trace_mode,
                store_kind = self.trace_store_kind,
                operation_class = candidate.operation_class,
                result = "ok",
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at),
                "publisher.wait_for_batch"
            );
        }

        let publish_span = tracing::info_span!(
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
                let Some(core) = self.core() else {
                    results = candidates
                        .iter()
                        .map(|_| Err(CoreError::ShuttingDown))
                        .collect();
                    break;
                };
                results = self.publish_through_engine(&core, batch_candidates).await;
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
        publish_span.record("result", batch_result_label(&results));
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        self.deliver_batch_results(candidates, results, selected_at);
    }

    /// Publishes through the publisher-owned engine: one namespace, one
    /// engine, one writer session, for the publisher's whole life.
    async fn publish_through_engine(
        &self,
        core: &FsCore,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<CommitResult> {
        let mut slot = self.engine.lock().await;
        let engine = self.engine_for(&mut slot, core).await;
        crate::FsWriter::from_core(core.clone())
            .publish_batch_with_engine(&self.namespace_id, engine, candidates)
            .await
            .into_iter()
            .map(|result| result.map_err(runtime_error_to_core))
            .collect()
    }

    /// The publisher's engine, built on first use. A new engine starts from
    /// the namespace's immutable catalog pair when the control cache can
    /// supply it, so the first publication does not walk the descriptor
    /// chain twice.
    async fn engine_for<'slot>(
        &self,
        slot: &'slot mut EngineSlot,
        core: &FsCore,
    ) -> &'slot mut NamespaceCommitEngine {
        if slot.engine.is_none() {
            let catalog = core
                .load_namespace_catalog_cached(&self.namespace_id)
                .await
                .ok()
                .flatten();
            let mut engine = NamespaceCommitEngine::new(self.namespace_id.clone())
                .table_cache(core.metadata_table_cache())
                .writer_session(Arc::clone(&slot.session));
            if let Some(catalog) = catalog {
                engine = engine.catalog_entry(catalog);
            }
            slot.engine = Some(engine);
        }
        slot.engine
            .as_mut()
            .expect("engine is present once installed")
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
                // Contained like a panicked publication, but a delete has no
                // receipt to replay: the caller is told, and the worker keeps
                // serving the namespace the delete did not remove.
                self.record_panic();
                Err(CoreError::Internal(
                    "delete task aborted mid-delete".to_owned(),
                ))
            }
        };
        match outcome {
            Ok(response) => {
                // Tombstone first, then fail everything that queued behind
                // the delete; admissions from here on fail fast.
                let queued = {
                    let mut state = self.lock_state();
                    state.deleted = true;
                    take_queued_waiters(&mut state)
                };
                // The publisher is terminal; drop it from the registry map
                // so the map stays bounded by live namespaces. Clones still
                // in flight fail fast on `deleted`, and a later submission
                // gets a fresh publisher whose publish fails on the durable
                // tombstone.
                if let Some(shared) = self.shared.upgrade() {
                    shared.evict(&self.namespace_id);
                }
                for waiter in waiters {
                    let _ = waiter.send(Ok(response.clone()));
                }
                for waiter in queued.commits {
                    let _ = waiter.send(Err(self.namespace_deleted()));
                }
                for waiter in queued.deletes {
                    let _ = waiter.send(Err(self.namespace_deleted()));
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
        let Some(core) = self.core() else {
            return Err(CoreError::ShuttingDown);
        };
        let mut slot = self.engine.lock().await;
        let engine = self.engine_for(&mut slot, &core).await;
        core.delete_namespace_with_engine(&self.namespace_id, engine, options)
            .await
            .map_err(runtime_error_to_core)
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

    /// Sleeps until this namespace's next head compare-and-swap is allowed.
    ///
    /// `claim` decides what happens on arrival: a claiming waiter also
    /// reserves the slot by pushing the next allowed instant out by one
    /// pacing interval, so two waiters cannot both take the same slot. A
    /// non-claiming waiter only observes that the slot is open — the caller
    /// reserves it later, when it actually takes a work item.
    async fn await_cas_slot(&self, claim: bool) {
        loop {
            let sleep_until = self.lock_state().next_allowed_cas_at;
            let arrived = sleep_until.is_none_or(|instant| instant <= Instant::now());
            if arrived {
                if claim {
                    let mut state = self.lock_state();
                    state.next_allowed_cas_at = Some(Instant::now() + self.min_publish_interval);
                }
                return;
            }
            if let Some(sleep_until) = sleep_until {
                tokio::time::sleep_until(sleep_until).await;
            }
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
                CoreError::Internal(format!(
                    "publisher batch returned {got} results for {want} candidates",
                    got = results.len(),
                    want = candidates.len(),
                ))
            });
            let mut results = results.into_iter();
            for candidate in candidates {
                let result = match &count_mismatch {
                    Some(error) => Err(error.clone()),
                    None => results
                        .next()
                        .expect("equal-length batch should hold one result per candidate"),
                };
                wait_traces.push((
                    candidate.operation_class,
                    result_label(&result),
                    elapsed_ms_since(selected_at),
                ));
                if let Some(in_flight) = state.in_flight.remove(&candidate.commit_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
        }

        for (operation_class, result, wait_ms) in wait_traces {
            tracing::info!(
                phase = "wait_for_result",
                mode = self.trace_mode,
                store_kind = self.trace_store_kind,
                operation_class,
                result,
                wait_ms,
                "publisher.wait_for_result"
            );
        }

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }
    }

    fn trace_enqueue(
        &self,
        operation_class: &'static str,
        queue_depth: usize,
        reason: &'static str,
    ) {
        tracing::info!(
            phase = "enqueue",
            mode = self.trace_mode,
            store_kind = self.trace_store_kind,
            operation_class,
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

/// Retrying with the same commit ids is safe: candidates that actually
/// committed replay their durable receipts. So an unknown head outcome is
/// retried like a stale head, resolving it into a definite answer instead of
/// handing `commit_outcome_unknown` to every waiter.
fn is_retryable_head_publish(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(CoreError::HeadPublish(
            CommitHeadPublishError::StaleHead | CommitHeadPublishError::OutcomeUnknown(_)
        ))
    )
}

fn runtime_error_to_core(error: RuntimeError) -> CoreError {
    match error {
        RuntimeError::Core(error) => error,
        RuntimeError::Bootstrap(error) => CoreError::Internal(error.to_string()),
        RuntimeError::Grep(error) => CoreError::Internal(error.to_string()),
        RuntimeError::Config(message) => CoreError::Internal(message),
        RuntimeError::RuntimeTask(message) => CoreError::Internal(message),
    }
}

fn operation_class(semantic_identity: &SemanticMutationIdentity) -> &'static str {
    match semantic_identity {
        SemanticMutationIdentity::CoreCommit(_) => "explicit_commit",
        SemanticMutationIdentity::PathIntent(_) => "path_mutation",
    }
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

fn result_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "error"
    }
}

fn batch_result_label(results: &[CommitResult]) -> &'static str {
    if results.iter().all(Result::is_ok) {
        "ok"
    } else {
        "error"
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
