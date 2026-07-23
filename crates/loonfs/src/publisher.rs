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
//! Admitted work is owned by registry-spawned publish tasks, never by the
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
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::{CommitId, NamespaceId};
use loonfs_core::commit::{CommitHeadPublishError, SemanticMutationIdentity};
// Publisher head-CAS races use the core-wide bounded contention retry limit.
use loonfs_core::limits::CONTENTION_RETRY_LIMIT;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
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

/// Maximum candidates held by one pending batch before admission reports
/// `commit_queue_full`.
const MAX_BATCH_CANDIDATES: usize = 1024;

/// Shared front door to the per-namespace publishers of one runtime core.
///
/// Cloning is cheap; clones share the same per-namespace publishers, so
/// every writer in the process should submit through clones of one
/// registry — [`FsWriter::publisher`](crate::FsWriter::publisher) hands
/// out exactly that.
///
/// The registry owns the publish tasks its publishers spawn. Shut it down
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
/// publisher map, and the task registry a shutdown drains.
struct RegistryShared {
    state: Mutex<RegistryState>,
}

struct RegistryState {
    closed: bool,
    publishers: HashMap<NamespaceId, NamespacePublisher>,
    tasks: Vec<JoinHandle<()>>,
}

impl RegistryShared {
    // Recover a poisoned lock instead of `expect`: every critical section
    // over this state is a plain field update, and the publish abort guard
    // registers respawns from a drop that may run during a panic unwind,
    // where a second panic would abort the process.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Spawns a publish task and registers it for shutdown. Callers hold
    /// their publisher's state lock, so admitting work and registering the
    /// task that owns it is atomic: a shutdown drain either observes the
    /// task or the admission never happened.
    fn register_task(&self, future: impl Future<Output = ()> + Send + 'static) {
        let mut state = self.lock_state();
        state.tasks.retain(|task| !task.is_finished());
        state.tasks.push(tokio::spawn(future));
    }

    fn evict(&self, namespace_id: &NamespaceId) {
        self.lock_state().publishers.remove(namespace_id);
    }
}

impl PublisherRegistry {
    /// Creates the registry a runtime core owns. Batches publish through
    /// the core's writer session, and its
    /// [`FsBackgroundWork`](crate::FsBackgroundWork) policy governs any
    /// post-publish maintenance.
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
                    tasks: Vec::new(),
                }),
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
        // registry on the spawn path, never the other way around.
        for publisher in publishers {
            publisher.close_admission();
        }
    }

    /// Waits for every admitted publication to settle, surfacing panics.
    /// Loops because admitted work may respawn its publish task (panic
    /// recovery) while the drain waits.
    ///
    /// Call [`Self::close_admission`] first for a terminal drain; without
    /// it this settles only the work registered so far, and new submissions
    /// keep scheduling more.
    pub async fn drain(&self) -> Result<(), RuntimeError> {
        let mut panicked = 0usize;
        loop {
            let drained = std::mem::take(&mut self.shared.lock_state().tasks);
            if drained.is_empty() {
                break;
            }
            for task in drained {
                if let Err(error) = task.await {
                    if error.is_panic() {
                        panicked += 1;
                    }
                }
            }
        }
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
    /// Weak: the registry map owns its publishers, and a strong reference
    /// back would cycle the whole structure into a leak. A publisher whose
    /// registry is gone keeps serving, with unowned tasks.
    shared: Weak<RegistryShared>,
    min_publish_interval: Duration,
    trace_mode: &'static str,
    trace_store_kind: &'static str,
}

struct NamespacePublisherState {
    batch: Option<OpenBatch>,
    /// At most one delete waits here (later deletes join its waiters). It
    /// seals the batch that was open when it arrived: those requests publish
    /// first, the delete runs next, and anything admitted afterwards lands
    /// in a fresh `batch` that only publishes if the delete fails.
    pending_delete: Option<PendingDelete>,
    /// Terminal: set once a delete succeeds. Admissions fail fast from then
    /// on without touching the store.
    deleted: bool,
    /// Set by the registry's admission close. Later admissions fail with
    /// `shutting_down`; everything already batched keeps publishing.
    closed: bool,
    in_flight: HashMap<CommitId, InFlightRequest>,
    /// A publish task owns this publisher's work loop. Set by whoever
    /// spawns the task — not by the task's first unit-take — and cleared
    /// only when the task exits, so at most one task processes work units
    /// at a time. That single flight is what makes the delete barrier's
    /// admission order deterministic: a delete arriving inside a batch's
    /// coalescing window queues behind the sealed batch in the one live
    /// task instead of racing it from a second one.
    publishing: bool,
    next_allowed_cas_at: Instant,
}

struct PendingDelete {
    sealed_batch: Option<OpenBatch>,
    options: DeleteNamespaceOptions,
    waiters: Vec<oneshot::Sender<DeleteResult>>,
}

enum WorkUnit {
    Mutations(Vec<BatchCandidate>),
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
                batch: None,
                pending_delete: None,
                deleted: false,
                closed: false,
                in_flight: HashMap::new(),
                publishing: false,
                // In the past, so a cold namespace publishes immediately.
                next_allowed_cas_at: Instant::now(),
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

    fn close_admission(&self) {
        self.state
            .lock()
            .expect("namespace publisher mutex poisoned")
            .closed = true;
    }

    /// The path to `admit` is await-free: a submission future's first poll
    /// either admits the candidate or fails, and only then parks on the
    /// result channel. Cancellation tests rely on this — after one poll of
    /// a submission, the publication is admitted and owned by the publish
    /// task.
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
        let mut state = self
            .state
            .lock()
            .expect("namespace publisher mutex poisoned");
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
            self.trace_enqueue(operation_class, pending_queue_depth(&state), "duplicate");
            return Ok(());
        }

        if state.batch.is_none() {
            let should_spawn = !state.publishing;
            state.batch = Some(OpenBatch {
                candidates: Vec::new(),
            });
            if should_spawn {
                // Ownership is taken here, under the admission lock, so no
                // other caller spawns a second task for the same work.
                // Registered while this lock is held, so a shutdown drain
                // that finds no tasks cannot miss the work this admission
                // is about to queue; the task blocks on this same lock
                // until the batch below is populated.
                state.publishing = true;
                self.spawn_publish_task();
            }
        }

        let batch_len = {
            let batch = state.batch.as_mut().expect("open batch should exist");
            if batch.candidates.len() >= MAX_BATCH_CANDIDATES {
                self.trace_enqueue(operation_class, batch.candidates.len(), "full");
                return Err(CoreError::CommitQueueFull);
            }
            batch.candidates.push(BatchCandidate {
                commit_id: commit_id.clone(),
                candidate: candidate.clone(),
                operation_class,
                enqueued_at,
            });
            batch.candidates.len()
        };
        self.trace_enqueue(operation_class, batch_len, "new");
        state.in_flight.insert(
            commit_id,
            InFlightRequest {
                semantic_identity,
                waiters: vec![waiter],
            },
        );
        Ok(())
    }

    /// Enqueues the delete as a barrier: requests admitted before it
    /// publish first, the delete runs next, and requests admitted after it
    /// fail with `namespace_deleted` once it succeeds. If the delete fails
    /// (for example a stale `expected_head_seq`), later requests publish
    /// normally — nothing is rejected for a delete that did not happen.
    async fn submit_delete(&self, options: DeleteNamespaceOptions) -> DeleteResult {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            if state.deleted {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: self.namespace_id.clone(),
                });
            }
            if state.closed {
                return Err(CoreError::ShuttingDown);
            }
            if let Some(pending) = state.pending_delete.as_mut() {
                pending.waiters.push(sender);
            } else {
                state.pending_delete = Some(PendingDelete {
                    sealed_batch: state.batch.take(),
                    options,
                    waiters: vec![sender],
                });
                if !state.publishing {
                    // Ownership taken and the task registered under the
                    // lock, for the same single-flight and shutdown-drain
                    // atomicity as `admit`.
                    state.publishing = true;
                    self.spawn_publish_task();
                }
            }
        }
        receiver.await.unwrap_or_else(|_| {
            Err(CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown(
                    "publisher task stopped mid-delete".to_owned(),
                ),
            ))
        })
    }

    /// Spawns this namespace's publish loop, registered with the owning
    /// registry so a shutdown drain joins it. Callers hold the publisher
    /// state lock across this call. Without a registry (it was dropped, or
    /// the publisher was built standalone in tests) the task runs unowned.
    fn spawn_publish_task(&self) {
        let publisher = self.clone();
        let future = async move {
            publisher.publish_open_batch().await;
        };
        match self.shared.upgrade() {
            Some(shared) => shared.register_task(future),
            None => {
                tokio::spawn(future);
            }
        }
    }

    async fn publish_open_batch(self) {
        let mut abort_guard = PublishAbortGuard::new(self.clone());

        // Drain work units in admission order: the batch sealed by a pending
        // delete, then the delete itself, then whatever queued behind it.
        // There is no fixed coalescing wait — batches form from what arrives
        // while a publish is in flight or while the pacing interval since
        // the last publication start runs out, so a cold namespace
        // publishes its first submission immediately.
        loop {
            let collect_started = Instant::now();
            let queue_depth_start = {
                let state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                pending_queue_depth(&state)
            };
            self.wait_for_cas_pacing().await;

            let unit = {
                let mut state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                // `publishing` is already true: the spawner took loop
                // ownership before this task existed.
                let unit = if let Some(pending) = state.pending_delete.as_mut() {
                    if let Some(batch) = pending.sealed_batch.take() {
                        Some(WorkUnit::Mutations(batch.candidates))
                    } else {
                        state.pending_delete.take().map(WorkUnit::Delete)
                    }
                } else {
                    state
                        .batch
                        .take()
                        .map(|batch| WorkUnit::Mutations(batch.candidates))
                };
                match unit {
                    Some(unit) => {
                        state.next_allowed_cas_at = Instant::now() + self.min_publish_interval;
                        Some(unit)
                    }
                    None => {
                        // Ownership checked and released under one lock, so
                        // a racing admit either sees `publishing` already
                        // false and spawns its own task, or queued before
                        // this check and was taken.
                        state.publishing = false;
                        None
                    }
                }
            };
            let Some(unit) = unit else {
                abort_guard.disarm();
                return;
            };

            match unit {
                WorkUnit::Mutations(candidates) => {
                    if candidates.is_empty() {
                        continue;
                    }
                    tracing::info!(
                        phase = "batch_collect",
                        mode = self.trace_mode(),
                        store_kind = self.trace_store_kind(),
                        batch_size = usize_to_u64(candidates.len()),
                        queue_depth_start = usize_to_u64(queue_depth_start),
                        queue_depth_end = usize_to_u64(candidates.len()),
                        collect_ms = elapsed_ms_since(collect_started),
                        "publisher.batch_collect"
                    );
                    self.publish_mutation_run(&mut abort_guard, candidates)
                        .await;
                }
                WorkUnit::Delete(pending) => {
                    if self.execute_delete(pending).await {
                        abort_guard.disarm();
                        return;
                    }
                }
            }
        }
    }

    async fn publish_mutation_run(
        &self,
        abort_guard: &mut PublishAbortGuard,
        candidates: Vec<BatchCandidate>,
    ) {
        abort_guard.batch_taken(
            candidates
                .iter()
                .map(|candidate| candidate.commit_id.clone())
                .collect(),
        );
        let selected_at = Instant::now();
        for candidate in &candidates {
            tracing::info!(
                phase = "wait_for_batch",
                mode = self.trace_mode(),
                store_kind = self.trace_store_kind(),
                operation_class = candidate.operation_class,
                result = "ok",
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at),
                "publisher.wait_for_batch"
            );
        }

        let publish_span = tracing::info_span!(
            "loonfs.phase",
            phase = "batch_publish",
            mode = self.trace_mode(),
            store_kind = self.trace_store_kind(),
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
                results = core
                    .publish_namespace_mutations_batch(&self.namespace_id, batch_candidates)
                    .await
                    .into_iter()
                    .map(|result| result.map_err(runtime_error_to_core))
                    .collect();
                if !results.iter().any(is_retryable_head_publish) {
                    break;
                }
                if attempt + 1 == CONTENTION_RETRY_LIMIT {
                    break;
                }
                retry_count += 1;
                self.wait_for_next_cas_token().await;
            }
            (results, retry_count)
        }
        .instrument(publish_span.clone())
        .await;
        publish_span.record("result", batch_result_label(&results));
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        self.deliver_batch_results(candidates, results, selected_at);
        abort_guard.batch_taken(Vec::new());
    }

    /// Runs the delete barrier. Returns true when the publisher is now
    /// terminal and the task should exit.
    async fn execute_delete(&self, pending: PendingDelete) -> bool {
        let outcome = match self.core() {
            Some(core) => core
                .delete_namespace_unqueued(&self.namespace_id, pending.options)
                .await
                .map_err(runtime_error_to_core),
            None => Err(CoreError::ShuttingDown),
        };
        match outcome {
            Ok(response) => {
                // Tombstone first, then fail everything that queued behind
                // the delete; admissions from here on fail fast.
                let failed_waiters = {
                    let mut state = self
                        .state
                        .lock()
                        .expect("namespace publisher mutex poisoned");
                    state.deleted = true;
                    state.publishing = false;
                    let mut failed = Vec::new();
                    if let Some(batch) = state.batch.take() {
                        for candidate in batch.candidates {
                            if let Some(in_flight) = state.in_flight.remove(&candidate.commit_id) {
                                failed.extend(in_flight.waiters);
                            }
                        }
                    }
                    failed
                };
                // The publisher is terminal; drop it from the registry map
                // so the map stays bounded by live namespaces. Clones still
                // in flight fail fast on `deleted`, and a later submission
                // gets a fresh publisher whose publish fails on the durable
                // tombstone.
                if let Some(shared) = self.shared.upgrade() {
                    shared.evict(&self.namespace_id);
                }
                for waiter in pending.waiters {
                    let _ = waiter.send(Ok(response.clone()));
                }
                for waiter in failed_waiters {
                    let _ = waiter.send(Err(CoreError::NamespaceDeleted {
                        namespace_id: self.namespace_id.clone(),
                    }));
                }
                true
            }
            Err(error) => {
                // The namespace was not deleted (stale precondition, fencing
                // conflict, ...). Report it and let queued work publish.
                for waiter in pending.waiters {
                    let _ = waiter.send(Err(error.clone()));
                }
                false
            }
        }
    }

    /// Sleeps until the next head CAS is allowed, without claiming it.
    async fn wait_for_cas_pacing(&self) {
        loop {
            let sleep_until = {
                let state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at
            };
            let now = Instant::now();
            if sleep_until <= now {
                break;
            }
            tokio::time::sleep_until(sleep_until).await;
        }
    }

    async fn wait_for_next_cas_token(&self) {
        loop {
            let sleep_until = {
                let state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at
            };
            let now = Instant::now();
            if sleep_until <= now {
                let mut state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at = Instant::now() + self.min_publish_interval;
                break;
            }
            tokio::time::sleep_until(sleep_until).await;
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
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
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
                mode = self.trace_mode(),
                store_kind = self.trace_store_kind(),
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
            mode = self.trace_mode(),
            store_kind = self.trace_store_kind(),
            operation_class,
            queue_depth = usize_to_u64(queue_depth),
            reason,
            "publisher.enqueue"
        );
    }

    fn trace_mode(&self) -> &'static str {
        self.trace_mode
    }

    fn trace_store_kind(&self) -> &'static str {
        self.trace_store_kind
    }
}

/// Keeps a namespace publisher serviceable if its publish task dies.
///
/// The publish task owns the taken batch: if it panics mid-publish, the
/// taken requests' waiters would otherwise wait forever, and the stuck
/// `publishing` flag would stop every future submit from spawning a new
/// task. On abnormal exit this guard fails the taken waiters with an
/// unknown outcome (the panic may have struck before or after the head
/// compare-and-swap), clears the flag, and restarts publication for any
/// batch that queued up behind the dead task.
struct PublishAbortGuard {
    publisher: NamespacePublisher,
    taken_commit_ids: Vec<CommitId>,
    disarmed: bool,
}

impl PublishAbortGuard {
    fn new(publisher: NamespacePublisher) -> Self {
        Self {
            publisher,
            taken_commit_ids: Vec::new(),
            disarmed: false,
        }
    }

    fn batch_taken(&mut self, commit_ids: Vec<CommitId>) {
        self.taken_commit_ids = commit_ids;
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for PublishAbortGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let mut orphaned_waiters = Vec::new();
        {
            // Recover a poisoned lock instead of `expect`: panicking in this
            // drop during an unwind would abort the process, and every
            // critical section over this state is a plain field update.
            let mut state = self
                .publisher
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.publishing = false;
            for commit_id in self.taken_commit_ids.drain(..) {
                if let Some(in_flight) = state.in_flight.remove(&commit_id) {
                    orphaned_waiters.extend(in_flight.waiters);
                }
            }
            let should_spawn_next = state.pending_delete.is_some()
                || state
                    .batch
                    .as_ref()
                    .is_some_and(|batch| !batch.candidates.is_empty());
            if should_spawn_next {
                // The respawn re-takes loop ownership and is registered
                // under the lock, so a racing admit does not double-spawn
                // and a shutdown drain joins the respawn instead of
                // concluding while queued work remains.
                state.publishing = true;
                self.publisher.spawn_publish_task();
            }
        }

        for waiter in orphaned_waiters {
            let _ = waiter.send(Err(CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown("publish task aborted mid-batch".to_owned()),
            )));
        }
    }
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

fn pending_queue_depth(state: &NamespacePublisherState) -> usize {
    state
        .batch
        .as_ref()
        .map_or(0, |batch| batch.candidates.len())
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
