//! The metadata work that a caller asks a writer session to do on its own
//! namespace, which runs in the caller's task: the fold that a merge or a
//! rebase of the namespace needs, and a host's metadata step.

use super::session_tasks::FoldRule;
use super::NamespacePublisher;
use crate::maintenance::{Compactor, TailFold};
use crate::{
    CoreError, MaintainMetadataOptions, MetadataMaintenanceOutcome, MetadataMaintenanceResponse,
    Result,
};
use loonfs_core::MetadataCompactionCancellation;

impl NamespacePublisher {
    /// Folds any WAL tail of the session's namespace after any fold that the
    /// session is already running, and starts the session's compaction when
    /// the fold publishes, as the session's own folds do.
    pub(super) async fn fold_any_tail(&self) -> Result<()> {
        if self.fold(FoldRule::Any).await? {
            self.start_compaction();
        }
        Ok(())
    }

    /// Folds the session's WAL tail when a fold is due, and then runs
    /// compaction units while compaction is due, up to the bounds of
    /// [`Maintenance::maintain_metadata_while_due`](crate::Maintenance::maintain_metadata_while_due).
    ///
    /// The fold waits for any fold that the session is already running, and
    /// the compaction waits for any compaction that the session is already
    /// running. No fold of the session waits for the compaction. A session
    /// without a position doesn't know its tail, so the call loads
    /// the tail from the store, folds it when the fold trigger or the idle
    /// fold age makes it due, and reports what it found, as a maintenance
    /// call does.
    ///
    /// The call stops when `cancellation` is cancelled, and when the session
    /// stops admitting work, so a close or a delete of the session doesn't
    /// wait for more than the unit in progress. A wait ends at once, this
    /// call's own fold is dropped if it has started, and a running compaction
    /// unit stops at its next block. When the session no longer admits work
    /// as the call starts, the call returns the session's admission error.
    pub(super) async fn maintain_metadata_while_due(
        &self,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        self.admitted_session(&self.lock_state()).map(drop)?;
        let Some(compactor) = self.compactor() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let stop = MetadataCompactionCancellation::default();
        let work = self.metadata_work(&compactor, &stop);
        tokio::pin!(work);
        // The work goes on after `stop` is cancelled, so a running unit stops
        // at its next block instead of being dropped.
        tokio::select! {
            biased;
            () = async {
                tokio::select! {
                    () = cancellation.cancelled() => {}
                    () = self.compaction_cancellation.cancelled() => {}
                }
            } => stop.cancel(),
            maintained = &mut work => return maintained,
        }
        work.await
    }

    /// Runs the work of [`Self::maintain_metadata_while_due`] until `stop`
    /// is cancelled.
    ///
    /// The fold runs under the session's fold turn, and the compaction runs
    /// after it under the compaction turn alone, so a fold of the session
    /// never waits for this call's compaction units.
    async fn metadata_work(
        &self,
        compactor: &Compactor,
        stop: &MetadataCompactionCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        let due = || MetadataMaintenanceResponse {
            namespace_id: self.namespace_id.clone(),
            outcome: MetadataMaintenanceOutcome::Due,
            compaction_units: 0,
        };
        let tail_fold = if self.has_position().await {
            tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(due()),
                folded = self.fold(FoldRule::WhenDue) => {
                    folded?;
                }
            }
            TailFold::Never
        } else {
            // Without a position the session can't tell whether a fold is
            // due, so the compactor reads the tail from the store and decides.
            let _turn = tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(due()),
                turn = self.fold_turn.lock() => turn,
            };
            let tail_caught_up = tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(due()),
                folded = compactor.fold_when_due(&self.namespace_id) => folded?,
            };
            TailFold::Folded { tail_caught_up }
        };
        let _turn = tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(due()),
            turn = self.compaction_turn.lock() => turn,
        };
        // Boxing erases the loop's nested step futures, for the same reason
        // as in `Maintenance::maintain_metadata_while_due_with_options`
        // (rustc's type-recursion depth).
        Box::pin(compactor.metadata_while_due(
            &self.namespace_id,
            stop,
            tail_fold,
            MaintainMetadataOptions::default().compaction_policy,
        ))
        .await
    }
}
