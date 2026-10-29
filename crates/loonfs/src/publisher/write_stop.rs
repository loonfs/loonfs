//! The wait of a namespace's queue at the write stop, for the session's fold.

use super::queue::{OpenBatch, WorkItem};
use super::session_tasks::wait_for_exit;
use super::worker::{elapsed_ms_from, sleep_ms};
use super::NamespacePublisher;
use crate::Error;
use loonfs_core::limits::MAX_UNFOLDED_WAL_OBJECTS;
use loonfs_types::u64_from_usize;
use tokio::sync::watch;

impl NamespacePublisher {
    /// Keeps the commits that the engine refused at the write stop at the
    /// front of the queue, and waits until a fold of this session publishes,
    /// the fold task exits, or the oldest of the commits reaches the
    /// write-stop wait.
    ///
    /// A fold that fails, or finds its tail already folded, publishes
    /// nothing, so the fold task's exit also ends the wait. The worker then
    /// publishes the queue again, and a refusal at the write stop starts the
    /// next fold.
    pub(super) async fn wait_at_write_stop(
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
        let started_at_ms = self.context.runtime_core.timer().monotonic_now_ms();
        let remaining_ms = oldest_enqueued_at
            .saturating_add(self.context.admission.write_stop_wait_budget_ms())
            .saturating_sub(started_at_ms);
        tokio::select! {
            _ = fold_published.changed() => {}
            () = wait_for_fold_task_exit(fold_task) => {}
            () = sleep_ms(remaining_ms) => {}
        }
        self.context
            .runtime_core
            .instruments()
            .publisher
            .write_stop_wait_duration(self.elapsed_ms_since(started_at_ms));
        self.end_write_stop_wait();
    }

    /// Records each queued commit that waited at the write stop for the first
    /// time, and returns `maintenance_required` to each commit that the
    /// engine refused at the write stop and that has waited for the whole
    /// write-stop wait, measured from when it entered the queue.
    ///
    /// A commit queued behind those publishes next, so the engine examines
    /// it before it can be refused.
    pub(super) fn end_write_stop_wait(&self) {
        let budget_ms = self.context.admission.write_stop_wait_budget_ms();
        let now_ms = self.context.runtime_core.timer().monotonic_now_ms();
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
        let instruments = self.context.runtime_core.instruments();
        instruments
            .publisher
            .write_stop_wait(u64_from_usize(first_waits));
        if refused.is_empty() {
            return;
        }
        instruments
            .publisher
            .write_stop_refusal(u64_from_usize(refused.len()));
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
}

/// Waits until the fold task that `liveness` reports on exits, and returns at
/// once when the session has no fold task.
async fn wait_for_fold_task_exit(liveness: Option<watch::Receiver<bool>>) {
    if let Some(liveness) = liveness {
        wait_for_exit(liveness).await;
    }
}
