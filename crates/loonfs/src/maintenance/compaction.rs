//! The compaction loop that [`Maintenance`] and each writer session run over
//! a namespace: compaction units under the namespace's compaction lock.

use super::{Maintenance, MetadataCompactionOutcome};
use crate::publisher::NamespaceChange;
use crate::trace::phase_span;
use crate::{Error, Result};
use crate::{
    MetadataMaintenanceOutcome, MetadataMaintenanceResponse, NamespaceId, SharedObjectStore,
};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::WriterPosition;
use loonfs_core::{MetadataCompactionCancellation, Phase};
use std::ops::ControlFlow;
use tracing::Instrument;

/// Abandoned compaction units in a row after which
/// [`Maintenance::compact_namespace`] stops, so a loop whose units
/// keep finding their inputs replaced, or keep losing every publication
/// attempt, can't spin.
const MAX_LOST_COMPACTION_RACES: usize = 3;

/// Compaction units that one call of [`Maintenance::compact_namespace`]
/// publishes before it returns,
/// so a namespace with a backlog cannot keep its caller waiting for long.
pub(super) const MAX_COMPACTION_UNITS_PER_CALL: usize = 16;

/// Converts what one compaction unit did into its wire form.
fn compaction_outcome(outcome: loonfs_core::CompactionUnitOutcome) -> MetadataCompactionOutcome {
    match outcome {
        loonfs_core::CompactionUnitOutcome::NotNeeded => MetadataCompactionOutcome::NotNeeded,
        loonfs_core::CompactionUnitOutcome::Published {
            manifest,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            skipped,
            ..
        } => MetadataCompactionOutcome::Published {
            manifest_no: manifest.manifest_no(),
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments: loonfs_types::u64_from_usize(output_segments),
            skipped,
        },
        loonfs_core::CompactionUnitOutcome::Cancelled => MetadataCompactionOutcome::Cancelled,
        loonfs_core::CompactionUnitOutcome::Abandoned => MetadataCompactionOutcome::Abandoned,
    }
}

impl Maintenance {
    /// Returns a mutating engine that commits as this value's writer ID, with
    /// no subject, because maintenance acts in the service context whatever
    /// subject the runtime has.
    pub(super) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = loonfs_core::NamespaceWriterEngine::writer(
            self.core.namespace_store(namespace_id),
            self.writer_id.clone(),
        );
        #[cfg(test)]
        let engine = match self.segment_row_budget {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    /// Returns the manifest of this runtime's writer position as a basis,
    /// loaded through the segment cache, or `None` when the runtime has no
    /// position for the namespace or the position's manifest is missing from
    /// the store and at or below the manifest boundary (see
    /// [`Self::position_basis`]).
    pub(super) async fn load_position_basis(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Held<LoadedManifest>>> {
        match self.publisher.current_position(namespace_id) {
            Some(position) => self.position_basis(namespace_id, &position).await,
            None => Ok(None),
        }
    }

    /// Returns the manifest of `position`, this runtime's writer position, as
    /// a basis, or `None` when the manifest is missing from the store and at
    /// or below the manifest boundary. In that case the call drops this
    /// runtime's view of the namespace, as a check of the position does (see
    /// [`RuntimeCore::forget_namespace_view`]), so the next use starts from
    /// the newest manifest.
    pub(super) async fn position_basis(
        &self,
        namespace_id: &NamespaceId,
        position: &WriterPosition,
    ) -> Result<Option<Held<LoadedManifest>>> {
        let basis = self
            .core
            .namespace_store(namespace_id)
            .load_position_basis(position)
            .await?;
        if basis.is_none() {
            self.core.forget_namespace_view(namespace_id).await;
        }
        Ok(basis)
    }

    /// Runs size-tiered compaction units on `namespace_id` while a unit is
    /// due, and reports how many units the call published.
    ///
    /// This is the namespace's one compaction loop, which a writer session runs
    /// after its folds and a host's metadata calls run after their folds. Each
    /// unit compacts the whole size-tiered window that the namespace's run
    /// policy selects. The first unit plans from `observed`, the current
    /// manifest that the caller loaded from the store, when it's given. The
    /// call stops when nothing is due, when `cancellation` is set, when the
    /// namespace's writer session in this runtime is closed or its namespace
    /// deleted, after [`MAX_LOST_COMPACTION_RACES`] abandoned units in a row,
    /// or after it publishes [`MAX_COMPACTION_UNITS_PER_CALL`] units. A close
    /// or a delete stops the loop before its next unit, and `cancellation` also
    /// stops a unit in progress at its next block. The response's outcome is
    /// [`MetadataMaintenanceOutcome::CaughtUp`] only when the call stopped
    /// because no unit was due.
    pub(crate) async fn compact_namespace(
        &self,
        namespace_id: &NamespaceId,
        mut observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        let closed = self.publisher.session_compaction_cancellation(namespace_id);
        let stopped = || {
            cancellation.is_cancelled()
                || closed
                    .as_ref()
                    .is_some_and(MetadataCompactionCancellation::is_cancelled)
        };
        let mut lost_races = 0;
        let mut published_units = 0;
        let mut skipped = Vec::new();
        let outcome = loop {
            if lost_races >= MAX_LOST_COMPACTION_RACES
                || published_units >= MAX_COMPACTION_UNITS_PER_CALL
                || stopped()
            {
                break MetadataMaintenanceOutcome::Due;
            }
            let compaction = self
                .compact_once(namespace_id, observed.take(), cancellation)
                .await?;
            match compaction {
                MetadataCompactionOutcome::Published {
                    skipped: unit_skipped,
                    ..
                } => {
                    for reason in unit_skipped {
                        if !skipped.contains(&reason) {
                            skipped.push(reason);
                        }
                    }
                    published_units += 1;
                    lost_races = 0;
                }
                MetadataCompactionOutcome::Abandoned => lost_races += 1,
                MetadataCompactionOutcome::NotNeeded if cancellation.is_cancelled() => {
                    break MetadataMaintenanceOutcome::Due
                }
                MetadataCompactionOutcome::NotNeeded => break MetadataMaintenanceOutcome::CaughtUp,
                MetadataCompactionOutcome::Cancelled => break MetadataMaintenanceOutcome::Due,
            }
        };
        Ok(MetadataMaintenanceResponse {
            namespace_id: namespace_id.clone(),
            outcome,
            compaction_units: loonfs_types::u64_from_usize(published_units),
            skipped,
        })
    }

    /// Returns the manifest that a unit plans from, or how the unit ends
    /// before it runs.
    ///
    /// The basis is `observed` when it's given. Otherwise it's this runtime's
    /// writer position, and without a position, the current manifest. The
    /// caller has the namespace's compaction lock, but `observed` and the
    /// position may have been read before it: another unit may have
    /// published since, in this runtime or in another process. A unit that
    /// planned from the older manifest would compact runs that another
    /// compaction already replaced, and end as abandoned after the whole
    /// rewrite. So before a due unit plans from either, one HEAD checks the
    /// basis's manifest for a successor, and a read of the manifest boundary
    /// follows it. The unit plans from the newest manifest when the HEAD
    /// finds a successor or the basis is at or below the boundary. For a
    /// position, the check is a strong read's
    /// (see [`RuntimeCore::checked_writer_position`]), which moves the
    /// position onto the manifests after it that have its writer epoch, or
    /// drops the position.
    async fn planning_basis(
        &self,
        namespace_id: &NamespaceId,
        observed: Option<Held<LoadedManifest>>,
    ) -> Result<ControlFlow<MetadataCompactionOutcome, Held<LoadedManifest>>> {
        let namespace = self.core.namespace_store(namespace_id);
        let (basis, from_position) = match observed {
            Some(basis) => (basis, false),
            None => match self.load_position_basis(namespace_id).await? {
                Some(basis) => (basis, true),
                None => {
                    let current = namespace.load_live_basis(self.core.observation()).await?;
                    return Ok(self.screen(namespace_id, current));
                }
            },
        };
        let basis = match self.screen(namespace_id, basis) {
            ControlFlow::Continue(basis) => basis,
            screened => return Ok(screened),
        };
        let basis = if from_position {
            self.core
                .checked_writer_position(namespace_id, None)
                .await?;
            match self.load_position_basis(namespace_id).await? {
                Some(basis) => basis,
                None => namespace.load_live_basis(self.core.observation()).await?,
            }
        } else {
            namespace
                .refresh_live(basis, self.core.observation())
                .await?
        };
        Ok(self.screen(namespace_id, basis))
    }

    /// Returns `basis` when a unit is due on it, and `NotNeeded` otherwise.
    fn screen(
        &self,
        namespace_id: &NamespaceId,
        basis: Held<LoadedManifest>,
    ) -> ControlFlow<MetadataCompactionOutcome, Held<LoadedManifest>> {
        if !self
            .engine(namespace_id)
            .metadata_compaction_due(basis.manifest())
        {
            return ControlFlow::Break(MetadataCompactionOutcome::NotNeeded);
        }
        ControlFlow::Continue(basis)
    }

    /// Runs one compaction unit under the namespace's compaction lock.
    ///
    /// The unit waits for the lock before it plans, so two units of the
    /// namespace in this runtime never plan from the same manifest, and no
    /// commit or fold waits for a unit. Under the lock,
    /// [`Self::planning_basis`] returns the manifest that the unit plans
    /// from, starting with `observed`, a manifest loaded after anything this
    /// call published. The unit waits for a compaction permit from the
    /// runtime's execution budget, and `cancellation` ends either wait or
    /// stops the compaction at its next block.
    pub(crate) async fn compact_once(
        &self,
        namespace_id: &NamespaceId,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionOutcome::Cancelled);
        }
        let turns = self.publisher.namespace_turns(namespace_id);
        let _turn = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            turn = turns.compaction.lock().instrument(phase_span!(
                self.core,
                Phase::WaitForCompactionLock,
                namespace_id
            )) => turn,
        };
        let basis = match self.planning_basis(namespace_id, observed).await? {
            ControlFlow::Continue(basis) => basis,
            ControlFlow::Break(outcome) => return Ok(outcome),
        };
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            permit = self.core.compaction_permit().instrument(phase_span!(
                self.core,
                Phase::WaitForCompactionPermit,
                namespace_id
            )) => permit,
        };
        let started_at_ms = self.core.timer().monotonic_now_ms();
        let outcome = self
            .engine(namespace_id)
            .compact_metadata_unit(basis, cancellation)
            .await
            .map_err(Error::from);
        let elapsed_ms = self
            .core
            .timer()
            .monotonic_now_ms()
            .saturating_sub(started_at_ms);
        self.core
            .instruments()
            .compactions
            .finished(&outcome, elapsed_ms);
        if let Ok(loonfs_core::CompactionUnitOutcome::Published { manifest, .. }) = &outcome {
            self.publisher
                .invalidate_namespace(
                    namespace_id,
                    NamespaceChange::Published(Box::new(manifest.clone())),
                )
                .await;
        }
        outcome.map(compaction_outcome)
    }
}
