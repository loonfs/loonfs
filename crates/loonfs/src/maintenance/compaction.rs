//! The compaction loop that [`Maintenance`] and each writer session run over
//! a namespace: compaction units under the namespace's compaction lock.

use super::{Maintenance, MetadataCompactionOutcome};
use crate::publisher::NamespaceChange;
use crate::{Error, Result};
use crate::{
    MetadataMaintenanceOutcome, MetadataMaintenanceResponse, NamespaceId, SharedObjectStore,
};
use loonfs_core::control::PublicationBasis;
use loonfs_core::{CoreError, MetadataCompactionCancellation};
use std::ops::ControlFlow;

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
            manifest_no: manifest.manifest.manifest_no,
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
    /// A mutating engine that commits as this value's writer ID.
    pub(super) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = self.core.writer_engine(&self.writer_id, namespace_id);
        #[cfg(test)]
        let engine = match self.segment_row_budget {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    /// Loads the current manifest of a namespace that isn't deleted, without
    /// the WAL tail above it, as a basis that a compaction unit can plan and
    /// publish from.
    pub(super) async fn load_live_basis(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<PublicationBasis> {
        let checked = self.core.observation();
        let manifest = self
            .core
            .namespace_store(namespace_id)
            .load_current_manifest()
            .await?;
        if manifest.payload().status.is_deleted() {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            }
            .into());
        }
        Ok(PublicationBasis { manifest, checked })
    }

    /// Returns the manifest of this runtime's writer position as a basis,
    /// loaded through the segment cache, or `None` when the runtime has no
    /// position for the namespace or the position's last check is at the
    /// revalidation bound.
    pub(super) async fn load_position_basis(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<PublicationBasis>> {
        let Some(position) = self.publisher.current_position(namespace_id) else {
            return Ok(None);
        };
        let segment_cache = self.core.metadata_segment_cache();
        Ok(
            loonfs_core::control::load_position_basis(self.core.store(), &segment_cache, &position)
                .await?,
        )
    }

    /// Runs size-tiered compaction units on `namespace_id` while a unit is
    /// due, and reports how many units the call published.
    ///
    /// This is the namespace's one compaction loop, which a writer session
    /// runs after its folds and a host's metadata calls run after their
    /// folds. Each unit compacts the whole size-tiered window that the
    /// namespace's run policy selects. The first unit plans from `observed`, the current manifest
    /// that the caller loaded from the store, when it's given. The call stops when nothing is
    /// due, when `cancellation` is set, when the namespace's writer session in this
    /// runtime is closed or its namespace deleted, after
    /// [`MAX_LOST_COMPACTION_RACES`] abandoned units in a row, or after it
    /// publishes [`MAX_COMPACTION_UNITS_PER_CALL`] units. A close or a delete
    /// stops the loop before its next unit, and `cancellation` also stops a
    /// unit in progress at its next block. The response's outcome is
    /// [`MetadataMaintenanceOutcome::CaughtUp`] only when the call stopped
    /// because no unit was due.
    pub(crate) async fn compact_namespace(
        &self,
        namespace_id: &NamespaceId,
        mut observed: Option<PublicationBasis>,
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
            let won = match compaction {
                MetadataCompactionOutcome::Published {
                    skipped: unit_skipped,
                    ..
                } => {
                    for reason in unit_skipped {
                        if !skipped.contains(&reason) {
                            skipped.push(reason);
                        }
                    }
                    true
                }
                MetadataCompactionOutcome::Abandoned => false,
                MetadataCompactionOutcome::NotNeeded if cancellation.is_cancelled() => {
                    break MetadataMaintenanceOutcome::Due
                }
                MetadataCompactionOutcome::NotNeeded => break MetadataMaintenanceOutcome::CaughtUp,
                MetadataCompactionOutcome::Cancelled => break MetadataMaintenanceOutcome::Due,
            };
            if won {
                published_units += 1;
                lost_races = 0;
            } else {
                lost_races += 1;
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
    /// writer position while the position's last check is within the
    /// revalidation bound, and without such a position, the current manifest.
    /// The caller has the namespace's compaction lock, but `observed` and the
    /// position may have been read before it: another unit may have published
    /// since, in this runtime or in another process. So before a due unit
    /// plans from either, one HEAD checks the basis's manifest for a
    /// successor, and the unit plans from the newer manifest when one exists.
    /// For a position, the check is a strong read's (see
    /// [`RuntimeCore::checked_writer_position`]), which moves the position
    /// onto the manifests after it that have its writer epoch, or drops the
    /// position.
    async fn planning_basis(
        &self,
        namespace_id: &NamespaceId,
        observed: Option<PublicationBasis>,
    ) -> Result<ControlFlow<MetadataCompactionOutcome, PublicationBasis>> {
        let (basis, from_position) = match observed {
            Some(basis) => (basis, false),
            None => match self.load_position_basis(namespace_id).await? {
                Some(basis) => (basis, true),
                None => {
                    let current = self.load_live_basis(namespace_id).await?;
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
                None => self.load_live_basis(namespace_id).await?,
            }
        } else if self
            .core
            .namespace_store(namespace_id)
            .manifest_has_successor(basis.manifest.manifest().manifest_no)
            .await
            .map_err(CoreError::from)?
        {
            self.load_live_basis(namespace_id).await?
        } else {
            return Ok(ControlFlow::Continue(basis));
        };
        Ok(self.screen(namespace_id, basis))
    }

    /// Returns `basis` when a unit is due on it, and otherwise that the unit
    /// isn't needed.
    fn screen(
        &self,
        namespace_id: &NamespaceId,
        basis: PublicationBasis,
    ) -> ControlFlow<MetadataCompactionOutcome, PublicationBasis> {
        if !self
            .engine(namespace_id)
            .metadata_compaction_due(&basis.manifest)
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
        observed: Option<PublicationBasis>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionOutcome::Cancelled);
        }
        let turns = self.publisher.namespace_turns(namespace_id);
        let _turn = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            turn = turns.compaction.lock() => turn,
        };
        let basis = match self.planning_basis(namespace_id, observed).await? {
            ControlFlow::Continue(basis) => basis,
            ControlFlow::Break(outcome) => return Ok(outcome),
        };
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            permit = self.core.compaction_permit() => permit,
        };
        let started_at_ms = self.core.timer().monotonic_now_ms();
        let outcome = self
            .engine(namespace_id)
            .compact_metadata_unit(Some(basis), cancellation)
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
                .invalidate_namespace(namespace_id, NamespaceChange::Published(manifest.clone()))
                .await;
        }
        outcome.map(compaction_outcome)
    }
}
