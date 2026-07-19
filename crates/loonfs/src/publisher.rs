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

use crate::content_tokens::ContentAdmission;
use crate::fs::{FsCore, FsInner};
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent};
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, RuntimeError};
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::{CommitId, NamespaceId};
use loonfs_core::commit::{CommitHeadPublishError, SemanticMutationIdentity};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::Instrument;

type CommitResult = Result<ApiCommitResponse, CoreError>;
type DeleteResult = Result<DeleteNamespaceResponse, CoreError>;

const MAX_BATCH_CANDIDATES: usize = 1024;
const HEAD_CAS_RETRY_LIMIT: usize = 8;

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
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::Commit(request))
            .await
    }

    /// Submits one path-level mutation intent through the namespace's
    /// publisher.
    pub async fn submit_path_intent(
        &self,
        namespace_id: NamespaceId,
        intent: PathMutationIntent,
    ) -> CommitResult {
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::Path(intent))
            .await
    }

    /// Submits a path-level mutation intent together with the content
    /// admissions vouching for its already-validated direct-put content.
    pub async fn submit_path_intent_with_content_admission(
        &self,
        namespace_id: NamespaceId,
        intent: PathMutationIntent,
        admissions: Vec<ContentAdmission>,
    ) -> CommitResult {
        self.submit_candidate(
            namespace_id,
            NamespaceMutationCandidate::PathWithContentAdmission { intent, admissions },
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
    pub(crate) async fn submit_candidate(
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

    async fn submit(&self, candidate: NamespaceMutationCandidate) -> CommitResult {
        let commit_id = candidate.commit_id().clone();
        let operation_class = operation_class(&candidate);
        let enqueued_at = Instant::now();
        let semantic_identity = candidate.semantic_identity(&self.namespace_id)?;
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
            "publisher.batch_publish",
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
            for attempt in 0..HEAD_CAS_RETRY_LIMIT {
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
                if attempt + 1 == HEAD_CAS_RETRY_LIMIT {
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
        RuntimeError::Config(message) => CoreError::Internal(message),
        RuntimeError::RuntimeTask(message) => CoreError::Internal(message),
    }
}

fn operation_class(candidate: &NamespaceMutationCandidate) -> &'static str {
    match candidate {
        NamespaceMutationCandidate::Commit(_) => "explicit_commit",
        NamespaceMutationCandidate::Path(_)
        | NamespaceMutationCandidate::PathWithContentAdmission { .. } => "path_mutation",
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
mod tests {
    #![allow(clippy::panic)]
    // Publisher tests use panic in async result helpers for precise diagnostics.

    use super::*;
    use crate::background::{BackgroundWork, FsBackgroundWork};
    use crate::config::FsConfig;
    use crate::{
        BeginUploadRequest, CreateNamespaceOptions, ErrorCode, RuntimeCacheConfig,
        SharedObjectStore as SharedStore, TraceMode, TraceStoreKind,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::v0::{CommitOp, CommitRequest};
    use loonfs_api::wire::wal::decode_wal_segment_envelope_zstd;
    use loonfs_api::{AbsolutePath, ChangeSeq, DestinationBehavior, InodeId};
    use loonfs_objectstore::keys::{wal_head, wal_segment_prefix};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Condvar;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct BlockingHeadCasStore {
        inner: LocalFsStore,
        head_key: String,
        gate: Arc<HeadCasGate>,
    }

    #[derive(Debug)]
    struct HeadCasGate {
        state: Mutex<HeadCasGateState>,
        cvar: Condvar,
    }

    #[derive(Debug)]
    struct HeadCasGateState {
        blocks_remaining: usize,
        entered: usize,
        released: bool,
    }

    impl BlockingHeadCasStore {
        fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
            Self {
                inner: LocalFsStore::new(root.as_ref()).expect("store"),
                head_key: wal_head(namespace_id.as_str()),
                gate: Arc::new(HeadCasGate {
                    state: Mutex::new(HeadCasGateState {
                        blocks_remaining: 0,
                        entered: 0,
                        released: false,
                    }),
                    cvar: Condvar::new(),
                }),
            }
        }

        fn arm_next_head_cas(&self) {
            let mut state = self.gate.state.lock().expect("head gate mutex poisoned");
            state.blocks_remaining = 1;
            state.released = false;
        }

        async fn wait_for_blocked_head_cas(&self) {
            let gate = self.gate.clone();
            tokio::task::spawn_blocking(move || {
                let mut state = gate.state.lock().expect("head gate mutex poisoned");
                while state.entered < 1 {
                    state = gate.cvar.wait(state).expect("head gate mutex poisoned");
                }
            })
            .await
            .expect("wait for blocked head CAS");
        }

        fn release_head_cas(&self) {
            let mut state = self.gate.state.lock().expect("head gate mutex poisoned");
            state.released = true;
            self.gate.cvar.notify_all();
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingHeadCasStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
                let gate = self.gate.clone();
                tokio::task::spawn_blocking(move || {
                    let mut state = gate.state.lock().expect("head gate mutex poisoned");
                    if state.blocks_remaining > 0 {
                        state.blocks_remaining -= 1;
                        state.entered += 1;
                        gate.cvar.notify_all();
                        while !state.released {
                            state = gate.cvar.wait(state).expect("head gate mutex poisoned");
                        }
                    }
                })
                .await
                .expect("head CAS gate wait task panicked");
            }
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    #[derive(Debug)]
    struct PanicHeadCasStore {
        inner: LocalFsStore,
        head_key: String,
        gate: Arc<PanicGate>,
    }

    #[derive(Debug)]
    struct PanicGate {
        state: Mutex<PanicGateState>,
        cvar: Condvar,
    }

    #[derive(Debug)]
    struct PanicGateState {
        armed: bool,
        entered: bool,
        released: bool,
    }

    impl PanicHeadCasStore {
        fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
            Self {
                inner: LocalFsStore::new(root.as_ref()).expect("store"),
                head_key: wal_head(namespace_id.as_str()),
                gate: Arc::new(PanicGate {
                    state: Mutex::new(PanicGateState {
                        armed: false,
                        entered: false,
                        released: false,
                    }),
                    cvar: Condvar::new(),
                }),
            }
        }

        fn arm_blocking_panic(&self) {
            let mut state = self.gate.lock_state();
            state.armed = true;
            state.entered = false;
            state.released = false;
        }

        async fn wait_for_blocked_head_cas(&self) {
            let gate = self.gate.clone();
            tokio::task::spawn_blocking(move || {
                let mut state = gate.lock_state();
                while !state.entered {
                    state = gate
                        .cvar
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            })
            .await
            .expect("wait for blocked head CAS");
        }

        fn release_into_panic(&self) {
            let mut state = self.gate.lock_state();
            state.released = true;
            self.gate.cvar.notify_all();
        }
    }

    impl PanicGate {
        // The injected panic poisons this mutex by design; later store calls
        // must keep working, so recover instead of unwrapping.
        fn lock_state(&self) -> std::sync::MutexGuard<'_, PanicGateState> {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    #[async_trait]
    impl ObjectStore for PanicHeadCasStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
                let gate = self.gate.clone();
                tokio::task::spawn_blocking(move || {
                    let mut state = gate.lock_state();
                    if state.armed {
                        state.armed = false;
                        state.entered = true;
                        gate.cvar.notify_all();
                        while !state.released {
                            state = gate
                                .cvar
                                .wait(state)
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                        }
                        panic!("injected publish task panic");
                    }
                })
                .await
                .expect("head CAS gate task");
            }
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    /// Applies head CAS writes but reports a transport failure for the next
    /// one: the commit lands durably while the acknowledgement is lost.
    #[derive(Debug)]
    struct LostHeadCasAckStore {
        inner: LocalFsStore,
        head_key: String,
        lose_next_head_cas_ack: AtomicBool,
    }

    impl LostHeadCasAckStore {
        fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
            Self {
                inner: LocalFsStore::new(root.as_ref()).expect("store"),
                head_key: wal_head(namespace_id.as_str()),
                lose_next_head_cas_ack: AtomicBool::new(false),
            }
        }

        fn lose_next_head_cas_ack(&self) {
            self.lose_next_head_cas_ack.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ObjectStore for LostHeadCasAckStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            let lose_ack = key == self.head_key
                && matches!(mode, PutMode::CompareAndSwap { .. })
                && self.lose_next_head_cas_ack.swap(false, Ordering::SeqCst);
            let metadata = self.inner.put(key, bytes, mode).await?;
            if lose_ack {
                return Err(ObjectStoreError::transport(
                    key,
                    "injected lost head CAS acknowledgement",
                ));
            }
            Ok(metadata)
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    fn test_fs(store: SharedStore) -> FsCore {
        FsCore::open_with_background(
            store,
            FsConfig {
                writer_id: "writer-a".to_owned(),
                writer_version: "test".to_owned(),
                // The publisher under test is itself the coalescer; direct
                // windows would only add latency to these tests.
                min_publish_interval_ms: 0,
                max_read_content_bytes: None,
                runtime_cache: RuntimeCacheConfig::default(),
                gram_index_build: crate::GramIndexBuildPolicy::default(),
                trace_mode: TraceMode::Remote,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
            BackgroundWork::new(
                FsBackgroundWork::ManualOnly,
                None,
                crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            ),
            None,
        )
        .expect("open runtime")
    }

    async fn test_writer(store: SharedStore) -> crate::FsWriter {
        test_writer_with_interval(store, crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS).await
    }

    async fn test_writer_with_interval(
        store: SharedStore,
        min_publish_interval_ms: u64,
    ) -> crate::FsWriter {
        crate::FsWriter::builder_with_store(store)
            .writer_id("writer-a")
            .writer_version("test")
            .min_publish_interval_ms(min_publish_interval_ms)
            .trace_mode(TraceMode::Remote)
            .trace_store_kind(TraceStoreKind::LocalFs)
            .build()
            .await
            .expect("build writer")
    }

    async fn create_namespace(fs: &FsCore, namespace_id: &NamespaceId) {
        fs.create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("bootstrap");
    }

    /// Pacing for standalone test publishers, long enough that
    /// `wait_past_cas_pacing` outlasting it is meaningful.
    const TEST_STANDALONE_PACING: Duration = Duration::from_secs(1);

    /// A publisher with no owning registry, exercising the unowned-task
    /// fallback the production paths reserve for a dropped registry. The
    /// caller keeps `fs` alive; the publisher holds it weakly.
    fn standalone_publisher(namespace_id: &NamespaceId, fs: &FsCore) -> NamespacePublisher {
        NamespacePublisher::new(
            namespace_id.clone(),
            Arc::downgrade(&fs.inner),
            Weak::new(),
            TEST_STANDALONE_PACING,
            fs.inner.config.trace_mode.as_str(),
            fs.inner.config.trace_store_kind.as_str(),
        )
    }

    #[allow(clippy::disallowed_methods)]
    async fn wait_past_cas_pacing() {
        // Deliberate wall-clock wait past the per-namespace CAS pacing
        // interval. A work loop that were not single-flight would let a
        // racing second task release a queued delete after exactly that
        // interval, so outlasting it proves the delete is ordered behind
        // the sealed batch, not merely paced behind it.
        tokio::time::sleep(TEST_STANDALONE_PACING + Duration::from_millis(300)).await;
    }

    fn create_directory_request(
        commit_id: impl Into<String>,
        display_name: impl Into<String>,
    ) -> CommitRequest {
        CommitRequest {
            commit_id: CommitId::parse(commit_id.into()).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: display_name.into(),
            }],
            message: None,
        }
    }

    fn admit_commit(
        publisher: &NamespacePublisher,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> oneshot::Receiver<CommitResult> {
        try_admit_commit(publisher, namespace_id, request).expect("admit commit")
    }

    fn try_admit_commit(
        publisher: &NamespacePublisher,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
        let commit_id = request.commit_id.clone();
        let candidate = NamespaceMutationCandidate::Commit(request);
        let operation_class = operation_class(&candidate);
        let semantic_identity = candidate.semantic_identity(namespace_id)?;
        let (sender, receiver) = oneshot::channel();
        publisher.admit(
            commit_id,
            candidate,
            semantic_identity,
            sender,
            operation_class,
            Instant::now(),
        )?;
        Ok(receiver)
    }

    async fn recv_commit(
        receiver: oneshot::Receiver<CommitResult>,
        label: &str,
    ) -> ApiCommitResponse {
        receiver
            .await
            .unwrap_or_else(|err| panic!("{label} receiver dropped: {err}"))
            .unwrap_or_else(|err| panic!("{label} failed: {err}"))
    }

    #[test]
    fn publisher_trace_labels_are_low_cardinality() {
        let commit = NamespaceMutationCandidate::Commit(create_directory_request(
            "commit-trace",
            "private-name",
        ));
        let path = NamespaceMutationCandidate::Path(PathMutationIntent::CreateDir {
            commit_id: CommitId::parse("path-trace").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/private/path").expect("path"),
            parents: false,
        });

        assert_eq!(operation_class(&commit), "explicit_commit");
        assert_eq!(operation_class(&path), "path_mutation");
        assert_eq!(result_label(&Ok::<_, CoreError>(())), "ok");
        assert_eq!(
            result_label(&Err::<(), _>(CoreError::Internal(
                "private error".to_owned()
            ))),
            "error"
        );
        assert_eq!(usize_to_u64(7), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_admits_pending_batch_while_active_publish_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let pending = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("pending", "pending"),
        );
        {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            assert!(state.publishing);
            assert_eq!(
                state
                    .batch
                    .as_ref()
                    .expect("pending batch")
                    .candidates
                    .len(),
                1
            );
        }

        store.release_head_cas();
        let active_response = recv_commit(active, "active").await;
        let pending_response = recv_commit(pending, "pending").await;
        assert_eq!(active_response.committed_seq, ChangeSeq(1));
        assert_eq!(pending_response.committed_seq, ChangeSeq(2));

        let wal_keys = shared
            .list_prefix(&wal_segment_prefix("demo"))
            .await
            .expect("list wal");
        assert_eq!(wal_keys.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_duplicate_active_request_joins_while_conflict_fails() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let duplicate = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("active", "active"),
        );
        let conflict = try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("active", "different-active"),
        );
        assert!(matches!(
            conflict,
            Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "active"
        ));

        store.release_head_cas();
        let active_response = recv_commit(active, "active").await;
        let duplicate_response = recv_commit(duplicate, "duplicate").await;
        assert_eq!(active_response.committed_seq, ChangeSeq(1));
        assert_eq!(duplicate_response.committed_seq, ChangeSeq(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_pending_batch_full_rejects_distinct_but_allows_duplicate() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let mut pending = Vec::with_capacity(MAX_BATCH_CANDIDATES);
        for index in 0..MAX_BATCH_CANDIDATES {
            pending.push(admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request(format!("pending-{index}"), format!("pending-{index}")),
            ));
        }

        let duplicate = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("pending-0", "pending-0"),
        );
        let conflict = try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("pending-0", "different-pending"),
        );
        assert!(matches!(
            conflict,
            Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "pending-0"
        ));

        let overflow = try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("overflow", "overflow"),
        );
        assert!(matches!(overflow, Err(CoreError::CommitQueueFull)));

        store.release_head_cas();
        assert_eq!(
            recv_commit(active, "active").await.committed_seq,
            ChangeSeq(1)
        );
        for (index, receiver) in pending.into_iter().enumerate() {
            assert_eq!(
                recv_commit(receiver, "pending").await.committed_seq,
                ChangeSeq(index as u64 + 2)
            );
        }
        assert_eq!(
            recv_commit(duplicate, "duplicate").await.committed_seq,
            ChangeSeq(2)
        );
    }

    /// A cold namespace takes whatever has batched — here a full batch
    /// admitted before the publish task first runs — immediately, with no
    /// coalescing wait in front of the first publication.
    #[tokio::test(flavor = "current_thread")]
    async fn publisher_takes_a_cold_full_batch_immediately() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        store.arm_next_head_cas();
        let mut receivers = Vec::with_capacity(MAX_BATCH_CANDIDATES);
        for index in 0..MAX_BATCH_CANDIDATES {
            receivers.push(admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request(format!("full-{index}"), format!("full-{index}")),
            ));
        }

        tokio::task::yield_now().await;
        {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            assert!(state.publishing);
            assert!(state.batch.is_none());
        }
        store.release_head_cas();
        for (index, receiver) in receivers.into_iter().enumerate() {
            assert_eq!(
                recv_commit(receiver, "full").await.committed_seq,
                ChangeSeq(index as u64 + 1)
            );
        }
    }

    /// A submission to a cold namespace is taken immediately: after one
    /// poll of the publish task there is no open batch parked behind a
    /// timer, so a lone candidate never waits out a coalescing window.
    #[tokio::test(flavor = "current_thread")]
    async fn cold_submission_publishes_without_a_coalescing_delay() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let fs = test_fs(store);
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        let receiver = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("cold", "cold"),
        );
        tokio::task::yield_now().await;
        {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            assert!(
                state.batch.is_none(),
                "a cold batch must be taken immediately, not held for a coalescing timer"
            );
        }
        let response = recv_commit(receiver, "cold").await;
        assert_eq!(response.committed_seq, ChangeSeq(1));
    }

    /// Follow-up submissions inside the pacing interval coalesce and
    /// publish no earlier than the interval boundary — the timer gives a
    /// deterministic lower bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hot_submissions_wait_out_the_pacing_interval() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = test_writer_with_interval(store.clone(), 400).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        let warmup_started = Instant::now();
        registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("warmup", "warmup"),
            )
            .await
            .expect("warmup commit");

        registry
            .submit_commit(namespace_id.clone(), create_directory_request("hot", "hot"))
            .await
            .expect("hot commit");
        let elapsed = warmup_started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(400),
            "a follow-up publication must wait out the pacing interval, took {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_resolves_unknown_head_outcome_by_replaying_receipt() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(LostHeadCasAckStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared);
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        // The commit lands but the CAS acknowledgement is lost. The publisher
        // retries with the same commit id and replays the durable receipt
        // instead of reporting `commit_outcome_unknown` to the waiter.
        store.lose_next_head_cas_ack();
        let response = recv_commit(
            admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request("unknown-ack", "unknown-ack"),
            ),
            "unknown-ack",
        )
        .await;
        assert_eq!(response.committed_seq, ChangeSeq(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_survives_publish_task_panic_and_keeps_serving() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        store.arm_blocking_panic();
        let doomed = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("doomed", "doomed"),
        );
        store.wait_for_blocked_head_cas().await;

        // Queued behind the in-flight batch: only the abort guard's respawn
        // can ever publish this one.
        let queued = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("queued", "queued"),
        );

        store.release_into_panic();

        // The panic may have struck either side of the head CAS, so the
        // taken request reports an unknown outcome, not definite failure.
        let doomed_error = doomed
            .await
            .expect("doomed waiter is answered, not abandoned")
            .expect_err("doomed commit did not complete");
        assert_eq!(doomed_error.code(), ErrorCode::CommitOutcomeUnknown);

        let queued_response = recv_commit(queued, "queued").await;
        assert_eq!(queued_response.committed_seq, ChangeSeq(1));

        // The publisher is fully serviceable after the panic.
        let after = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("after", "after"),
        );
        assert_eq!(
            recv_commit(after, "after").await.committed_seq,
            ChangeSeq(2)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_barrier_publishes_admitted_work_and_rejects_later_work() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let fs = test_fs(shared.clone());
        create_namespace(&fs, &namespace_id).await;
        let publisher = standalone_publisher(&namespace_id, &fs);

        // A publishes and blocks at its head CAS; B queues behind it.
        store.arm_next_head_cas();
        let before_a = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("before-a", "before-a"),
        );
        store.wait_for_blocked_head_cas().await;
        let before_b = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("before-b", "before-b"),
        );

        // The delete arrives: everything above was admitted before it,
        // everything below after it.
        let delete_task = {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                publisher
                    .submit_delete(DeleteNamespaceOptions::default())
                    .await
            })
        };
        // Deterministic: wait until the delete has sealed the open batch.
        loop {
            let sealed = {
                let state = publisher
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.pending_delete.is_some()
            };
            if sealed {
                break;
            }
            tokio::task::yield_now().await;
        }
        let after = admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("after", "after"),
        );

        store.release_head_cas();

        // Admitted-before work publishes; the delete lands after it.
        assert_eq!(
            recv_commit(before_a, "before-a").await.committed_seq,
            ChangeSeq(1)
        );
        assert_eq!(
            recv_commit(before_b, "before-b").await.committed_seq,
            ChangeSeq(2)
        );
        let response = delete_task
            .await
            .expect("delete task")
            .expect("delete succeeds");
        assert_eq!(response.head_seq, ChangeSeq(2));

        // Admitted-after work is rejected, and the tombstone fails new
        // admissions immediately.
        let after_error = after
            .await
            .expect("after waiter answered")
            .expect_err("admitted after the delete");
        assert_eq!(after_error.code(), ErrorCode::NamespaceDeleted);
        let fast_fail = try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("too-late", "too-late"),
        );
        assert!(matches!(fast_fail, Err(CoreError::NamespaceDeleted { .. })));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_batches_concurrent_distinct_commits_into_one_wal_segment() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = test_writer_with_interval(store.clone(), 400).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        // Warm the namespace: a cold one publishes its first submission
        // immediately, so truly concurrent submissions could split across
        // two publications. Inside the pacing interval the open batch
        // deterministically holds both.
        registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("warmup", "warmup"),
            )
            .await
            .expect("warmup commit");

        let request_a = CommitRequest {
            commit_id: CommitId::parse("req-a").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
        };
        let request_b = CommitRequest {
            commit_id: CommitId::parse("req-b").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: "beta".to_owned(),
            }],
            message: None,
        };

        let (response_a, response_b) = tokio::join!(
            registry.submit_commit(namespace_id.clone(), request_a),
            registry.submit_commit(namespace_id.clone(), request_b)
        );
        assert_eq!(response_a.expect("response a").committed_seq, ChangeSeq(2));
        assert_eq!(response_b.expect("response b").committed_seq, ChangeSeq(3));

        // The warmup published alone; the two concurrent submissions share
        // one segment.
        let wal_keys = store
            .list_prefix(&wal_segment_prefix("demo"))
            .await
            .expect("list wal");
        assert_eq!(wal_keys.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_batches_explicit_commit_and_path_intent_together() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = test_writer_with_interval(store.clone(), 400).await;
        create_namespace(writer.core(), &namespace_id).await;
        let upload = writer
            .begin_upload(
                &namespace_id,
                BeginUploadRequest {
                    mode: None,
                    content_ref: None,
                },
            )
            .await
            .expect("begin upload");
        let staged = writer
            .upload_content(&namespace_id, &upload.upload_id, b"hello")
            .await
            .expect("stage content");
        let registry = writer.publisher();

        // Warm the namespace so the pacing interval deterministically holds
        // the two concurrent submissions in one batch.
        registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("warmup", "warmup"),
            )
            .await
            .expect("warmup commit");

        let explicit = CommitRequest {
            commit_id: CommitId::parse("explicit-commit").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
        };
        let path_intent = PathMutationIntent::PutFile {
            commit_id: CommitId::parse("path-put").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/file.txt").expect("path"),
            content_ref: staged.content_ref,
            behavior: DestinationBehavior::NoReplace,
        };

        let (explicit_response, path_response) = tokio::join!(
            registry.submit_commit(namespace_id.clone(), explicit),
            registry.submit_path_intent(namespace_id.clone(), path_intent)
        );
        assert_eq!(
            explicit_response.expect("explicit response").committed_seq,
            ChangeSeq(2)
        );
        assert_eq!(
            path_response.expect("path response").committed_seq,
            ChangeSeq(3)
        );

        let wal_keys = store
            .list_prefix(&wal_segment_prefix("demo"))
            .await
            .expect("list wal");
        let mut record_counts = Vec::new();
        for key in &wal_keys {
            let wal_bytes = store
                .get(key, None)
                .await
                .expect("read wal")
                .expect("wal exists");
            let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode wal segment");
            record_counts.push(segment.payload.records.len());
        }
        record_counts.sort_unstable();
        // The warmup published alone; the concurrent pair shares a segment.
        assert_eq!(record_counts, vec![1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_close_admission_refuses_new_work_while_admitted_work_drains() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let writer = test_writer(shared.clone()).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        // An admitted publication blocks at its head CAS...
        store.arm_next_head_cas();
        let active = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_commit(namespace_id, create_directory_request("active", "active"))
                    .await
            })
        };
        store.wait_for_blocked_head_cas().await;

        // ...then admission closes. New work is refused at the front door.
        registry.close_admission();
        let refused = registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("refused", "refused"),
            )
            .await
            .expect_err("submission after close_admission");
        assert_eq!(refused.code(), ErrorCode::ShuttingDown);

        // A publisher clone that predates the sweep also refuses directly.
        let publisher = registry
            .shared
            .lock_state()
            .publishers
            .get(&namespace_id)
            .expect("active publisher exists")
            .clone();
        let direct = try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("direct", "direct"),
        );
        assert!(matches!(direct, Err(CoreError::ShuttingDown)));

        // The admitted publication still settles, and drain joins its task.
        store.release_head_cas();
        let response = active
            .await
            .expect("submit task")
            .expect("admitted commit publishes");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        registry.drain().await.expect("drain settles publish tasks");
        assert!(registry.shared.lock_state().tasks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_drain_surfaces_panics_and_settles_respawned_work() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let writer = test_writer(shared.clone()).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        store.arm_blocking_panic();
        let doomed = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_commit(namespace_id, create_directory_request("doomed", "doomed"))
                    .await
            })
        };
        store.wait_for_blocked_head_cas().await;

        // Queued behind the blocked batch: only the abort guard's respawn
        // publishes this one, and the drain must join that respawn.
        let queued = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_commit(namespace_id, create_directory_request("queued", "queued"))
                    .await
            })
        };
        let publisher = registry
            .shared
            .lock_state()
            .publishers
            .get(&namespace_id)
            .expect("publisher exists while blocked")
            .clone();
        loop {
            let queued_admitted = {
                let state = publisher
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state
                    .batch
                    .as_ref()
                    .is_some_and(|batch| !batch.candidates.is_empty())
            };
            if queued_admitted {
                break;
            }
            tokio::task::yield_now().await;
        }

        store.release_into_panic();
        registry.close_admission();

        let doomed_error = doomed
            .await
            .expect("doomed submit task")
            .expect_err("doomed commit did not complete");
        assert_eq!(doomed_error.code(), ErrorCode::CommitOutcomeUnknown);
        let queued_response = queued
            .await
            .expect("queued submit task")
            .expect("respawned task publishes queued work");
        assert_eq!(queued_response.committed_seq, ChangeSeq(1));

        let drain_error = registry
            .drain()
            .await
            .expect_err("drain surfaces the panicked task");
        assert!(
            drain_error.to_string().contains("panicked"),
            "drain reports panicked publisher tasks: {drain_error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_delete_evicts_the_namespace_publisher() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = test_writer(store.clone()).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("before", "before"),
            )
            .await
            .expect("commit before delete");
        assert_eq!(registry.shared.lock_state().publishers.len(), 1);

        registry
            .submit_delete(namespace_id.clone(), DeleteNamespaceOptions::default())
            .await
            .expect("delete namespace");
        assert!(
            registry.shared.lock_state().publishers.is_empty(),
            "a terminal publisher must not stay in the map"
        );

        // A later submission builds a fresh publisher and still fails, now
        // on the durable tombstone instead of the fast in-memory flag.
        let late = registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("late", "late"),
            )
            .await
            .expect_err("submission after delete");
        assert_eq!(late.code(), ErrorCode::NamespaceDeleted);
        registry.close_admission();
        registry.drain().await.expect("drain after delete");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_admission_refuses_without_creating_publishers() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = test_writer(store.clone()).await;
        let registry = writer.publisher();

        registry.close_admission();
        let refused = registry
            .submit_commit(
                namespace_id.clone(),
                create_directory_request("nope", "nope"),
            )
            .await
            .expect_err("closed registry refuses commits");
        assert_eq!(refused.code(), ErrorCode::ShuttingDown);
        let refused_delete = registry
            .submit_delete(namespace_id, DeleteNamespaceOptions::default())
            .await
            .expect_err("closed registry refuses deletes");
        assert_eq!(refused_delete.code(), ErrorCode::ShuttingDown);
        assert!(registry.shared.lock_state().publishers.is_empty());
        registry.drain().await.expect("nothing to drain");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_queued_mid_publish_waits_behind_the_sealed_batch() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let writer = test_writer(shared.clone()).await;
        create_namespace(writer.core(), &namespace_id).await;
        let registry = writer.publisher();

        // Park the first publication at its head CAS, batch a second commit
        // behind it, then queue the delete: the sealed batch must publish
        // before the delete runs, and the blocked CAS outlasts the pacing
        // interval — the interleaving where a racing second task could run
        // the delete first.
        store.arm_next_head_cas();
        let before = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_commit(namespace_id, create_directory_request("before", "before"))
                    .await
            })
        };
        store.wait_for_blocked_head_cas().await;
        let publisher = registry
            .shared
            .lock_state()
            .publishers
            .get(&namespace_id)
            .cloned()
            .expect("publisher exists once a publish is in flight");

        // The publish task is parked in the blocked CAS, so this admission
        // deterministically opens the next batch instead of being taken.
        let second = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_commit(namespace_id, create_directory_request("second", "second"))
                    .await
            })
        };
        loop {
            let batch_open = {
                let state = publisher
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state
                    .batch
                    .as_ref()
                    .is_some_and(|batch| !batch.candidates.is_empty())
            };
            if batch_open {
                break;
            }
            tokio::task::yield_now().await;
        }

        let delete = {
            let registry = registry.clone();
            let namespace_id = namespace_id.clone();
            tokio::spawn(async move {
                registry
                    .submit_delete(namespace_id, DeleteNamespaceOptions::default())
                    .await
            })
        };
        // Deterministic: the delete has sealed the open batch.
        loop {
            let sealed = {
                let state = publisher
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.pending_delete.is_some()
            };
            if sealed {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Snapshots are taken while the CAS is blocked but asserted only
        // after the gate is released: a regression then fails the test
        // instead of hanging runtime teardown on the never-released gate.
        let unfinished_tasks_while_blocked = registry
            .shared
            .lock_state()
            .tasks
            .iter()
            .filter(|task| !task.is_finished())
            .count();
        // With the sealed batch still blocked at its head CAS, outlast the
        // pacing interval: the delete must still not have run.
        wait_past_cas_pacing().await;
        let (deleted_while_blocked, delete_queued_while_blocked) = {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            (state.deleted, state.pending_delete.is_some())
        };

        // Released: the parked commit publishes, then the sealed batch, and
        // only then the delete.
        store.release_head_cas();
        assert_eq!(
            unfinished_tasks_while_blocked, 1,
            "a delete must not spawn a racing second publish task"
        );
        assert!(
            !deleted_while_blocked,
            "delete executed while the sealed batch was still publishing"
        );
        assert!(
            delete_queued_while_blocked,
            "delete must stay queued behind the sealed batch"
        );
        let before_response = before
            .await
            .expect("before submit task")
            .expect("parked commit publishes before the delete");
        assert_eq!(before_response.committed_seq, ChangeSeq(1));
        let second_response = second
            .await
            .expect("second submit task")
            .expect("sealed batch publishes before the delete");
        assert_eq!(second_response.committed_seq, ChangeSeq(2));
        let delete_response = delete
            .await
            .expect("delete task")
            .expect("delete succeeds after the sealed batch");
        assert_eq!(delete_response.head_seq, ChangeSeq(2));
        registry.close_admission();
        registry.drain().await.expect("drain settles both units");
    }
}
