//! The metadata work that a caller asks a writer session to do on its own
//! namespace, which runs in the caller's task: the fold that a merge or a
//! rebase of the namespace needs, a maintenance call's fold, the fold before
//! a pin, and the metadata work of a host's metadata step or a maintenance
//! call.

use super::registry::NamespaceChange;
use super::session_tasks::FoldRule;
use super::{CaughtUpAt, NamespacePublisher, PublisherRegistry};
use crate::maintenance::{Compactor, TailFold};
use crate::{
    CoreError, Error, FoldWalResponse, MaintainMetadataOptions, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, Result,
};
use loonfs_core::publish::WriterPosition;
use loonfs_core::{MetadataCompactionCancellation, PublicationReport};
use std::future::Future;

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

    /// Folds the session's WAL tail after any fold that the session is
    /// already running, and returns what
    /// [`Maintenance::fold_wal`](crate::Maintenance::fold_wal) returns: the
    /// manifest that the fold published, or the current manifest when the
    /// tail is already folded. The fold starts no compaction.
    pub(super) async fn fold_wal(&self) -> Result<FoldWalResponse> {
        let _turn = self.fold_turn.lock().await;
        let _permit = self.context.runtime_core.fold_permit().await;
        self.fold_tail(self.read_position_input()).await
    }

    /// Runs `pin` after any fold that the session is already running, and
    /// records the change that `change` returns before the session's next
    /// fold starts (see [`PublisherRegistry::finish_namespace_mutation`]).
    ///
    /// `pin` folds the session's WAL tail from the position that the call
    /// passes it, and then pins the manifest that covers the tail. A fold
    /// that published raises the namespace's hint and wakes the commits that
    /// wait at the write stop. The call starts no compaction, and neither
    /// does a pin's fold without a session.
    pub(super) async fn fold_and_pin<T, Pin>(
        &self,
        registry: &PublisherRegistry,
        change: impl FnOnce(&PublicationReport<T>) -> NamespaceChange,
        pin: impl FnOnce(Option<WriterPosition>) -> Pin,
    ) -> Result<PublicationReport<T>>
    where
        Pin: Future<Output = loonfs_core::Result<PublicationReport<T>>>,
    {
        let pinned = {
            let _turn = self.fold_turn.lock().await;
            let _permit = self.context.runtime_core.fold_permit().await;
            let pinned = pin(self.read_position_input()).await.map_err(Error::from);
            registry
                .finish_pin(&self.namespace_id, pinned, change)
                .await
        };
        if pinned
            .as_ref()
            .is_ok_and(|report| report.published.is_some())
        {
            self.fold_published.send_replace(());
        }
        pinned
    }

    /// Returns the position that reads in this runtime start from, as a
    /// fold input. It's the engine's position as of the engine's last
    /// release, so a fold from it doesn't wait for a publication that the
    /// engine is running, and that publication's commits stay in the tail
    /// above the fold's manifest.
    fn read_position_input(&self) -> Option<WriterPosition> {
        self.read_position
            .current()
            .map(|position| WriterPosition::clone(&position))
    }

    /// Folds the session's WAL tail when a fold is due under `options`, and
    /// then runs compaction units while compaction is due under `options`,
    /// up to the bounds of
    /// [`Maintenance::maintain_metadata_while_due`](crate::Maintenance::maintain_metadata_while_due).
    ///
    /// The fold waits for any fold that the session is already running, and
    /// the compaction waits for any compaction that the session is already
    /// running. No fold of the session waits for the compaction. A session
    /// whose position was checked within the revalidation bound uses the
    /// position, and reads the position's manifest through the segment
    /// cache. Any other session can't trust what it knows about its tail
    /// (it has no position, or another writer may have committed since the
    /// position's last check), so the call loads the tail from the store,
    /// folds it when the fold trigger or the idle fold age makes it due, and
    /// reports what it found, as a maintenance call does. When such a call
    /// with the default options finds metadata caught up, the session keeps
    /// that answer, and later calls with the default options return it
    /// without reading the store until the session attempts a batch, its
    /// position is dropped, or the revalidation bound passes (see
    /// [`CaughtUpAt`]).
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
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        self.admitted_session(&self.lock_state()).map(drop)?;
        let Some(compactor) = self.compactor() else {
            return Err(CoreError::ShuttingDown.into());
        };
        let current = self.has_current_position().await;
        // Metadata that's caught up under one set of options can be due
        // under another, so only calls with the default options keep a
        // caught-up answer, and only those calls return it.
        let keeps_answer = !current && *options == MaintainMetadataOptions::default();
        let observed = self.context.runtime_core.observation();
        let started = {
            let state = self.lock_state();
            if keeps_answer && state.caught_up_answer_applies() {
                return Ok(MetadataMaintenanceResponse {
                    namespace_id: self.namespace_id.clone(),
                    outcome: MetadataMaintenanceOutcome::CaughtUp,
                    compaction_units: 0,
                });
            }
            CaughtUpAt {
                position_changes: state.position_changes,
                observed,
            }
        };
        let maintained = self
            .metadata_work_until_stopped(&compactor, current, cancellation, options)
            .await?;
        if keeps_answer && maintained.outcome == MetadataMaintenanceOutcome::CaughtUp {
            self.lock_state().caught_up_at = Some(started);
        }
        Ok(maintained)
    }

    /// Runs [`Self::metadata_work`], and stops it when `cancellation` is
    /// cancelled or the session stops admitting work.
    async fn metadata_work_until_stopped(
        &self,
        compactor: &Compactor,
        current: bool,
        cancellation: &MetadataCompactionCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        let stop = MetadataCompactionCancellation::default();
        let work = self.metadata_work(compactor, current, &stop, options);
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
    /// is cancelled, from the session's position when `current` says that
    /// the position was checked within the revalidation bound, and from the
    /// store otherwise.
    ///
    /// The fold runs under the session's fold turn, and the compaction runs
    /// after it under the compaction turn alone, so a fold of the session
    /// never waits for this call's compaction units.
    async fn metadata_work(
        &self,
        compactor: &Compactor,
        current: bool,
        stop: &MetadataCompactionCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        let max_wal_tail_objects = options.max_wal_tail_objects.get();
        let due = || MetadataMaintenanceResponse {
            namespace_id: self.namespace_id.clone(),
            outcome: MetadataMaintenanceOutcome::Due,
            compaction_units: 0,
        };
        let tail_fold = if current {
            tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(due()),
                folded = self.fold(FoldRule::WhenDue { max_wal_tail_objects }) => {
                    folded?;
                }
            }
            TailFold::Never
        } else {
            // Without a current position the session can't tell whether a
            // fold is due, so the compactor reads the tail from the store and
            // folds it when a fold is due. The compactor's fold runs through
            // this session, under the session's fold turn.
            let tail_caught_up = tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(due()),
                folded = compactor.fold_when_due(&self.namespace_id, max_wal_tail_objects) => folded?,
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
            options.compaction_policy,
        ))
        .await
    }
}
