//! The worker that drains a namespace's queue in admission order, publishes
//! each batch, delivers the results, and paces the publications.

use super::admission::AdmissionPermit;
use super::metrics::PublishOutcome;
use super::queue::{queued_candidates, BatchCandidate, OpenBatch, WorkItem};
use super::session_tasks::WorkerHandle;
use super::{
    EngineSlot, FoldTrigger, LastPublish, NamespacePublisher, NamespacePublisherState,
    PublisherAdmissionStatus,
};
use crate::metrics::RESULT_OK;
use crate::publish::CommitCandidate;
use crate::trace::{phase_event, phase_span};
use crate::{CoreError, Error, Result, SharedObjectStore};
use futures::FutureExt;
use loonfs_core::limits::{FOLD_AT_WAL_OBJECTS, MAX_CONTENTION_ATTEMPTS};
use loonfs_core::publish::{
    is_retryable_wal_publish, settle_publish_attempt, NamespaceCommitEngine, TailSource,
    WalPublishError,
};
use loonfs_core::time::OperationClock;
use loonfs_core::Phase;
use loonfs_types::Commit;
use loonfs_types::{u64_from_usize, CommitId, WriterId};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::Instrument;

impl NamespacePublisher {
    /// Makes sure a worker owns this publisher's queue.
    ///
    /// Callers keep the state lock locked, so admitting work and installing the
    /// task that owns it is atomic: no second worker takes the same queue,
    /// and a shutdown drain that finds no worker cannot miss work an
    /// admission is about to queue.
    pub(super) fn spawn_worker_if_idle(&self, state: &mut NamespacePublisherState) {
        if state.worker.as_ref().is_some_and(WorkerHandle::is_running) {
            return;
        }
        let publisher = self.clone();
        state.worker = Some(self.spawn_worker(async move {
            publisher.run_worker().await;
        }));
    }

    /// Drains the queue in admission order, then exits.
    pub(super) async fn run_worker(self) {
        loop {
            let collect_started = self.context.runtime_core.timer().monotonic_now_ms();
            let queue_depth_start = queued_candidates(&self.lock_state());
            // Do not add a separate batching delay. A request for an idle namespace
            // publishes immediately; requests that queue during a publish or its
            // pacing interval form the next batch.
            self.await_publish_slot().await;
            // Queue ownership and admission remain intact while another
            // namespace uses the shared publication slots. No engine is kept.
            let publication = self.context.admission.publication_permit().await;
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
                    self.context
                        .runtime_core
                        .instruments()
                        .publisher
                        .batch(batch.candidates.len());
                    phase_event!(
                        self.context.runtime_core,
                        Phase::BatchCollect,
                        self.namespace_id,
                        tracing::Level::INFO,
                        batch_size = u64_from_usize(batch.candidates.len()),
                        queue_depth_start = u64_from_usize(queue_depth_start),
                        queue_depth_end = u64_from_usize(batch.candidates.len()),
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
                    // A delete is bounded by its fold permit alone. Keeping a
                    // publication permit while it waits for one would stall
                    // other namespaces' publications behind folds.
                    drop(publication);
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
    pub(super) fn take_next_item(&self) -> Option<WorkItem> {
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
        self.context
            .runtime_core
            .instruments()
            .publisher
            .queue_depth(queue_depth);
        item
    }

    /// Publishes one batch while containing panics from the publication, and
    /// returns the commits that wait at the write stop.
    ///
    /// This worker is the namespace's only publication path. If publication
    /// panics, each request in the batch receives `commit_outcome_unknown`
    /// because the panic may have occurred before or after the WAL put. Callers
    /// can retry with the same commit ID and use the durable receipt to resolve
    /// the outcome. The worker then continues with queued work.
    pub(super) async fn publish_batch(
        &self,
        candidates: Vec<BatchCandidate>,
    ) -> Vec<BatchCandidate> {
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
    pub(super) async fn publish_taken_batch(
        &self,
        candidates: Vec<BatchCandidate>,
    ) -> Vec<BatchCandidate> {
        let selected_at = self.context.runtime_core.timer().monotonic_now_ms();
        for candidate in &candidates {
            phase_event!(
                self.context.runtime_core,
                Phase::WaitForBatch,
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
            self.context.runtime_core,
            Phase::BatchPublish,
            self.namespace_id,
            batch_size = u64_from_usize(candidates.len()),
            result = tracing::field::Empty,
            retry_count = tracing::field::Empty,
        );
        let (results, retry_count) = self
            .publish_attempts(candidates, &permits, &mut stopped)
            .instrument(publish_span.clone())
            .await;
        publish_span.record("result", batch_result_label(&results).as_str());
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        let (waiting, delivered_ids, delivered) =
            split_write_stop_waiters(commit_ids, results, stopped, &queued, &permits);
        self.deliver_batch_results(delivered_ids, delivered, selected_at);
        waiting
    }

    /// Publishes through the publisher-owned engine: one namespace, one
    /// engine, one writer session, for the publisher's whole life.
    pub(super) async fn publish_through_engine(
        &self,
        candidates: &[CommitCandidate],
        permits: &[Arc<AdmissionPermit>],
        writer_id: &WriterId,
        clock: &OperationClock,
    ) -> Vec<std::result::Result<Commit, CoreError>> {
        let mut slot = self.lock_engine().await;
        let engine = self.engine_for(&mut slot, writer_id);
        // Boxing erases the engine's deeply nested publish future. Without
        // it, callers awaiting a put or commit (CLI, server, embedding
        // crates) exceed rustc's type-recursion depth.
        let publish = Box::pin(engine.publish_batch(candidates, clock)).await;
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
    pub(super) fn record_publication(
        &self,
        publish: &loonfs_core::publish::NamespaceCommitEnginePublishResult,
    ) -> Option<oneshot::Sender<()>> {
        if publish
            .wal_tail
            .is_some_and(|tail| tail.source == TailSource::Store)
        {
            self.context
                .runtime_core
                .instruments()
                .publisher
                .tail_replay();
        }
        let write_stopped = publish.results.iter().any(is_maintenance_required);
        let fold_start = if publish.wal_tail.is_some_and(|tail| {
            FoldTrigger::new(self.context.runtime_core.config()).fold_is_due(
                FOLD_AT_WAL_OBJECTS.get(),
                tail.objects,
                tail.inline_bytes,
            )
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
                at_ms: self.context.runtime_core.timer().monotonic_now_ms(),
            });
            state.metadata_caught_up = false;
        }
        fold_start
    }

    /// Returns the publisher's lazily created commit engine, which commits as
    /// `writer_id`.
    ///
    /// Each publish loads the namespace identity from the head, so construction
    /// only needs the runtime's cache views and the writer session.
    pub(super) fn engine_for<'slot>(
        &self,
        slot: &'slot mut EngineSlot,
        writer_id: &WriterId,
    ) -> &'slot mut NamespaceCommitEngine<SharedObjectStore> {
        slot.engine.get_or_insert_with(|| {
            NamespaceCommitEngine::new(
                self.context
                    .runtime_core
                    .namespace_store(&self.namespace_id),
                writer_id.clone(),
            )
            .writer_session(Arc::clone(&slot.session))
        })
    }

    pub(super) fn deliver_batch_results(
        &self,
        commit_ids: Vec<CommitId>,
        results: Vec<Result<Commit>>,
        selected_at: u64,
    ) {
        let mut deliveries = Vec::new();
        let mut wait_traces = Vec::new();
        {
            let mut state = self.lock_state();
            // Nothing queued behind this publish means no load to batch, and the
            // next request may return these results: it publishes at once.
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
            self.context
                .runtime_core
                .instruments()
                .publisher
                .publish(outcome);
            phase_event!(
                self.context.runtime_core,
                Phase::WaitForResult,
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

    /// Waits until the namespace may start another WAL put.
    pub(super) async fn await_publish_slot(&self) {
        let interval_ms = self.context.runtime_core.config().min_publish_interval_ms;
        loop {
            let Some(last_publish) = self.lock_state().last_publish.clone() else {
                break;
            };
            let remaining_ms = interval_ms.saturating_sub(last_publish.age_ms());
            if remaining_ms == 0 {
                break;
            }
            sleep_ms(remaining_ms).await;
        }
    }

    pub(super) async fn claim_publish_slot(&self) {
        self.await_publish_slot().await;
        self.reserve_next_publish_slot(&mut self.lock_state());
    }

    pub(super) fn reserve_next_publish_slot(&self, state: &mut NamespacePublisherState) {
        state.last_publish = Some(self.context.runtime_core.observation());
    }

    /// Publishes `candidates` through the engine, and again for the ones
    /// whose result is retryable, for at most `MAX_CONTENTION_ATTEMPTS`
    /// attempts. Returns each candidate's result and the number of retries.
    ///
    /// Records in `stopped` a copy of each candidate that the engine refused
    /// at the write stop, so the commit can wait in the queue.
    async fn publish_attempts(
        &self,
        candidates: Vec<CommitCandidate>,
        permits: &[Arc<AdmissionPermit>],
        stopped: &mut [Option<CommitCandidate>],
    ) -> (Vec<Result<Commit>>, u64) {
        let writer_id = match self.context.writer.upgrade() {
            Some(writer) => writer.writer_id.clone(),
            None => {
                let error = Error::from(CoreError::ShuttingDown);
                return (candidates.iter().map(|_| Err(error.clone())).collect(), 0);
            }
        };
        // The batch's retries share one clock. A retry that read the wall
        // clock again would count the earlier waiting twice.
        let clock = OperationClock::start(
            self.context.runtime_core.now_ms(),
            Arc::clone(self.context.runtime_core.timer()),
        );
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
            let observed = match self.context.writer.upgrade() {
                Some(_writer) => {
                    self.publish_through_engine(&candidates, &attempt_permits, &writer_id, &clock)
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
            pending =
                settle_publish_attempt(&mut results, indices.into_iter().zip(candidates), observed);
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
                    .map_err(Error::from)
            })
            .collect::<Vec<_>>();
        (results, retry_count)
    }
}

/// Splits the results of a published batch into the commits that the engine
/// refused at the write stop, which wait in the queue for a fold, and the
/// commit IDs and results of the others, which go to their callers.
fn split_write_stop_waiters(
    commit_ids: Vec<CommitId>,
    results: Vec<Result<Commit>>,
    mut stopped: Vec<Option<CommitCandidate>>,
    queued: &[(u64, bool)],
    permits: &[Arc<AdmissionPermit>],
) -> (Vec<BatchCandidate>, Vec<CommitId>, Vec<Result<Commit>>) {
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
    (waiting, delivered_ids, delivered)
}

pub(super) fn is_maintenance_required(result: &std::result::Result<Commit, CoreError>) -> bool {
    matches!(
        result,
        Err(error) if error.code() == loonfs_core::ErrorCode::MaintenanceRequired
    )
}

pub(super) fn result_label<T, E>(result: &std::result::Result<T, E>) -> PublishOutcome {
    if result.is_ok() {
        PublishOutcome::Ok
    } else {
        PublishOutcome::Error
    }
}

fn batch_result_label(results: &[Result<Commit>]) -> PublishOutcome {
    if results.iter().all(Result::is_ok) {
        PublishOutcome::Ok
    } else {
        PublishOutcome::Error
    }
}

pub(super) fn elapsed_ms_from(start: u64, end: u64) -> u64 {
    end.saturating_sub(start)
}

#[allow(
    clippy::disallowed_methods,
    reason = "the publication pacing and the write-stop wait decide when a request publishes or \
              returns, not whether a publication is valid"
)]
pub(super) async fn sleep_ms(delay_ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
}
