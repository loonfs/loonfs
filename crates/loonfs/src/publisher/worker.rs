//! The worker that drains a namespace's queue in admission order, publishes
//! each batch, delivers the results, and paces the publications.

use super::admission::AdmissionPermit;
use super::folds::{fold_due, FoldDue, WalTailState};
use super::metrics::PublishOutcome;
use super::queue::{
    queued_candidates, BatchCandidate, OpenBatch, PendingDelete, PendingRetentionAdvance, WorkItem,
};
use super::session_tasks::WorkerHandle;
use super::{NamespacePublisher, NamespacePublisherState, PublisherAdmissionStatus};
use crate::metrics::RESULT_OK;
use crate::publish::CommitCandidate;
use crate::trace::{phase_event, phase_span};
use crate::{CoreError, Error, Result};
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::publish::{settle_publish_attempt, TailSource, WalPublishError};
use loonfs_core::time::OperationClock;
use loonfs_core::Phase;
use loonfs_types::Commit;
use loonfs_types::{u64_from_usize, CommitId};
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
    ///
    /// The worker contains a panic in each unit it takes (see
    /// [`Self::contain_panic`]), returns `outcome_unknown` to every caller of
    /// that unit, and goes on to the next unit.
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
                    let commit_ids = candidates
                        .iter()
                        .map(|candidate| candidate.commit_id.clone())
                        .collect::<Vec<_>>();
                    let waiting = match self
                        .contain_panic("publish", self.publish_taken_batch(candidates))
                        .await
                    {
                        Some(waiting) => waiting,
                        None => {
                            self.fail_panicked_commits(commit_ids);
                            Vec::new()
                        }
                    };
                    drop(publication);
                    if !waiting.is_empty() {
                        let waiting = OpenBatch {
                            candidates: waiting,
                            wal_record_bytes_upper_bound,
                            inline_content_bytes,
                        };
                        self.wait_at_write_stop(waiting, fold_published).await;
                    }
                }
                WorkItem::Delete(PendingDelete {
                    options,
                    administrator,
                    waiters,
                }) => {
                    // A delete is bounded by its fold permit alone. Keeping a
                    // publication permit while it waits for one would stall
                    // other namespaces' publications behind folds.
                    drop(publication);
                    let outcome = self
                        .contain_panic(
                            "delete",
                            self.delete_through_engine(&administrator, options),
                        )
                        .await
                        .unwrap_or_else(panicked_outcome);
                    if self.finish_delete(outcome, waiters).await {
                        return;
                    }
                }
                WorkItem::RetentionAdvance(PendingRetentionAdvance {
                    target,
                    planned,
                    waiters,
                }) => {
                    // An advance is one WAL put, so it's bounded by the
                    // publication permit, as a batch is.
                    let outcome = self
                        .contain_panic(
                            "retention_advance",
                            self.publish_floor_object(&planned, target),
                        )
                        .await
                        .unwrap_or_else(panicked_outcome);
                    drop(publication);
                    for waiter in waiters {
                        let _ = waiter.send(outcome.clone());
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
        self.count_queued(&mut state);
        drop(state);
        self.report_queue_depth();
        item
    }

    /// Returns `outcome_unknown` to every caller that still waits for one of
    /// `commit_ids`, the commits of a batch whose publication panicked.
    ///
    /// A caller can retry with the same commit ID, and LoonFS returns the
    /// original result from the durable receipt. (A panic in one commit's preparation doesn't
    /// reach the worker, because the core fails only that commit, before the
    /// WAL put.)
    fn fail_panicked_commits(&self, commit_ids: Vec<CommitId>) {
        let waiters = {
            let mut state = self.lock_state();
            commit_ids
                .into_iter()
                .filter_map(|commit_id| state.in_flight.remove(&commit_id))
                .flat_map(|request| request.waiters)
                .collect::<Vec<_>>()
        };
        for waiter in waiters {
            let _ = waiter.send(panicked_outcome());
        }
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
            .publish_attempts(candidates, &mut stopped)
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
        clock: &OperationClock,
    ) -> Vec<std::result::Result<Commit, CoreError>> {
        let (mut engine, start) = match self.lock_engine_for_unit().await {
            Ok(unit) => unit,
            Err(error) => return candidates.iter().map(|_| Err(error.clone())).collect(),
        };
        // Boxing erases the engine's deeply nested publish future. Without
        // it, callers awaiting a put or commit (CLI, server, embedding
        // crates) exceed rustc's type-recursion depth.
        let publish = Box::pin(engine.publish_batch(start, candidates, clock)).await;
        let fold_start = self.record_publication(&publish);
        drop(engine);
        // A WAL put whose outcome the session didn't learn may have
        // committed, so the cached head may be older than a commit that this
        // runtime made. A panic after the put drops the head when the engine
        // is released.
        if publish.results.iter().any(|result| {
            matches!(
                result,
                Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(_)))
            )
        }) {
            self.context
                .runtime_core
                .invalidate_namespace_read_cache(&self.namespace_id);
        }
        if let Some(start) = fold_start {
            let _ = start.send(());
        }
        publish.results
    }

    /// Records the newest commit that `publish` published, and starts a fold
    /// when a fold of the tail it left is due (see [`fold_due`]) or a commit
    /// reached the write stop. The caller sends the returned start once it
    /// releases the engine.
    pub(super) fn record_publication(
        &self,
        publish: &loonfs_core::publish::NamespaceCommitEnginePublishOutcome,
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
        let write_stopped = publish
            .results
            .iter()
            .any(|result| result.as_ref().err().is_some_and(is_maintenance_required));
        let core = &self.context.runtime_core;
        let fold_start = if publish.wal_tail.is_some_and(|tail| {
            fold_due(
                core,
                core.session_fold_at_wal_objects().get(),
                &WalTailState::from(&tail),
                core.now_ms(),
            ) == FoldDue::Now
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
            state.last_published_seq = Some(
                state
                    .last_published_seq
                    .map_or(committed_seq, |last_seq| last_seq.max(committed_seq)),
            );
        }
        fold_start
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
        stopped: &mut [Option<CommitCandidate>],
    ) -> (Vec<Result<Commit>>, u64) {
        if self.context.writer.upgrade().is_none() {
            let error = Error::from(CoreError::ShuttingDown);
            return (candidates.iter().map(|_| Err(error.clone())).collect(), 0);
        }
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
            let observed = match self.context.writer.upgrade() {
                Some(_writer) => self.publish_through_engine(&candidates, &clock).await,
                None => candidates
                    .iter()
                    .map(|_| Err(CoreError::ShuttingDown))
                    .collect(),
            };
            // The core contains a panic in one commit's preparation and
            // returns it as that commit's result, so each panic is recorded
            // here, in the attempt that hit it, whatever the caller gets.
            for result in &observed {
                if let Err(CoreError::CommitPreparationPanicked(panic)) = result {
                    self.record_panic("commit_preparation", panic);
                }
            }
            for ((index, candidate), result) in indices.iter().zip(&candidates).zip(&observed) {
                if result.as_ref().err().is_some_and(is_maintenance_required) {
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
    // `outcome_unknown` under the settling rule, so only a commit
    // whose result is the refusal waits.
    let mut waiting = Vec::new();
    let (mut delivered_ids, mut delivered) = (Vec::new(), Vec::new());
    for (index, (commit_id, result)) in commit_ids.into_iter().zip(results).enumerate() {
        let refused = match &result {
            Err(Error::Core(CoreError::MaintenanceRequired { reason, .. })) => Some(reason.clone()),
            _ => None,
        };
        match (stopped[index].take(), refused) {
            (Some(candidate), Some(reason)) => {
                let (enqueued_at, write_stop_wait_recorded) = queued[index];
                waiting.push(BatchCandidate {
                    permit: Arc::clone(&permits[index]),
                    commit_id,
                    candidate,
                    enqueued_at,
                    write_stop_wait_recorded,
                    write_stop_reason: Some(reason),
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

/// Returns the result that every caller of a unit of work receives when the
/// unit panics. The unit may have stopped after its put, so the outcome is
/// unknown.
fn panicked_outcome<T>() -> Result<T> {
    Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
        "the publisher's work panicked before it reported an outcome".to_owned(),
    ))
    .into())
}

/// Returns whether `error` is core's refusal of a commit at the write stop.
fn is_maintenance_required(error: &CoreError) -> bool {
    matches!(error, CoreError::MaintenanceRequired { .. })
}

fn result_label<T, E>(result: &std::result::Result<T, E>) -> PublishOutcome {
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
    reason = "the publication pacing and the write-stop wait decide when a request publishes \
              or returns, not whether a publication is valid"
)]
pub(crate) async fn sleep_ms(delay_ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
}
