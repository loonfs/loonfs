//! The compaction loop of a writer session: compaction units under the
//! namespace's compaction lock, planned from the session's position.

use super::registry::{NamespaceChange, NamespaceSession};
use super::NamespacePublisher;
use crate::maintenance::MetadataCompactionOutcome;
use crate::trace::phase_span;
use crate::{Error, Result};
use crate::{MetadataMaintenanceOutcome, MetadataMaintenanceResponse, SharedObjectStore};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::WriterPosition;
use loonfs_core::{MetadataCompactionCancellation, Phase};
use std::ops::ControlFlow;
use tracing::Instrument;

/// Abandoned compaction units in a row after which
/// [`NamespaceSession::compact_namespace`] stops, so a loop whose units
/// keep finding their inputs replaced, or keep losing every publication
/// attempt, can't spin.
const MAX_LOST_COMPACTION_RACES: usize = 3;

/// Compaction units that one call of [`NamespaceSession::compact_namespace`]
/// publishes before it returns, so a namespace with a backlog cannot keep
/// its caller waiting for long.
pub(crate) const MAX_COMPACTION_UNITS_PER_CALL: usize = 16;

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

impl NamespaceSession {
    /// Runs this session's compaction units while a unit is due, as
    /// [`NamespacePublisher::compact_namespace`] describes.
    pub(crate) async fn compact_namespace(
        &self,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        self.publisher
            .compact_namespace(observed, cancellation)
            .await
    }

    /// Runs one compaction unit of this session, as
    /// [`NamespacePublisher::compact_once`] describes.
    pub(crate) async fn compact_once(
        &self,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        self.publisher.compact_once(observed, cancellation).await
    }
}

impl NamespacePublisher {
    /// Returns a mutating engine that commits as this session's writer ID,
    /// with no subject, because maintenance acts in the service context
    /// whatever subject the runtime has.
    fn compaction_engine(&self) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = loonfs_core::NamespaceWriterEngine::writer(
            self.context
                .runtime_core
                .namespace_store(&self.namespace_id),
            self.context.writer_id.clone(),
        );
        #[cfg(test)]
        let engine = match self
            .shared
            .upgrade()
            .and_then(|shared| shared.segment_row_budget())
        {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    /// Runs size-tiered compaction units on this session's namespace while a
    /// unit is due, and reports how many units the call published.
    ///
    /// This is the namespace's one compaction loop, which the session runs
    /// after its folds and a host's metadata calls run after their folds.
    /// Each unit compacts the whole size-tiered window that the namespace's
    /// run policy selects. The first unit checks whether it's due on
    /// `observed`, the current manifest that the caller loaded from the
    /// store, when it's given. The call stops when nothing is due, when
    /// `cancellation` is set, when the session is closed or its namespace
    /// deleted, after [`MAX_LOST_COMPACTION_RACES`] abandoned units in a row,
    /// or after it publishes [`MAX_COMPACTION_UNITS_PER_CALL`] units. A close
    /// or a delete stops the loop before its next unit, and `cancellation`
    /// also stops a unit in progress at its next block. The response's
    /// outcome is [`MetadataMaintenanceOutcome::CaughtUp`] only when the call
    /// stopped because no unit was due.
    pub(super) async fn compact_namespace(
        &self,
        mut observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        let stopped = || cancellation.is_cancelled() || self.compaction_cancellation.is_cancelled();
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
            let compaction = self.compact_once(observed.take(), cancellation).await?;
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
            namespace_id: self.namespace_id.clone(),
            outcome,
            compaction_units: loonfs_types::u64_from_usize(published_units),
            skipped,
        })
    }

    /// Returns the position that a unit plans from, or how the unit ends
    /// before it runs.
    ///
    /// The unit is due when the run policy selects a unit on `observed`
    /// when it's given, and otherwise on the manifest of the session's
    /// position, or on the current manifest when the session has no
    /// position. Checking that reads the store only without a position, and
    /// claims nothing, so a namespace with nothing due is left alone. A due
    /// unit plans from the session's position. The caller has the
    /// namespace's compaction lock, but the position may have been read
    /// before it: another unit may have published since, in this runtime or
    /// in another process. A unit that planned from the older manifest would
    /// compact runs that another compaction already replaced, and end as
    /// abandoned after the whole rewrite. So before a due unit plans from a
    /// position that it already had, one HEAD checks the position's manifest
    /// for a successor, and a read of the manifest boundary follows it, as a
    /// strong read's check does (see
    /// [`RuntimeCore::checked_writer_position`](crate::runtime_core::RuntimeCore::checked_writer_position)),
    /// which moves the position onto the manifests after it that have its
    /// writer epoch, or drops it. A session without a position then loads
    /// one, which claims the writer epoch first when the session has none
    /// (see [`Self::writer_position`]).
    async fn planning_position(
        &self,
        observed: Option<Held<LoadedManifest>>,
    ) -> Result<ControlFlow<MetadataCompactionOutcome, WriterPosition>> {
        let core = &self.context.runtime_core;
        let namespace = core.namespace_store(&self.namespace_id);
        let position = self.current_position();
        let screened = match (observed, &position) {
            (Some(observed), _) => observed,
            (None, Some(position)) => match namespace.load_position_basis(position).await? {
                Some(basis) => basis,
                None => {
                    core.forget_namespace_view(&self.namespace_id).await;
                    namespace.load_live_basis(core.observation()).await?
                }
            },
            (None, None) => namespace.load_live_basis(core.observation()).await?,
        };
        if !self
            .compaction_engine()
            .metadata_compaction_due(screened.manifest())
        {
            return Ok(ControlFlow::Break(MetadataCompactionOutcome::NotNeeded));
        }
        if position.is_some() {
            core.checked_writer_position(&self.namespace_id, None)
                .await?;
        }
        Ok(ControlFlow::Continue(self.writer_position().await?))
    }

    /// Runs one compaction unit under the namespace's compaction lock.
    ///
    /// The unit waits for the lock before it plans, so two units of the
    /// namespace in this runtime never plan from the same manifest, and no
    /// commit or fold waits for a unit. Under the lock,
    /// [`Self::planning_position`] returns the position that the unit plans
    /// from, after a check of `observed`, a manifest loaded after anything
    /// this call published. The unit waits for a compaction permit from the
    /// runtime's execution budget, and `cancellation` ends either wait or
    /// stops the compaction at its next block.
    pub(super) async fn compact_once(
        &self,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionOutcome::Cancelled);
        }
        let core = &self.context.runtime_core;
        let namespace_id = &self.namespace_id;
        let _turn = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            turn = self.turns.compaction.lock().instrument(phase_span!(
                core,
                Phase::WaitForCompactionLock,
                namespace_id
            )) => turn,
        };
        let position = match self.planning_position(observed).await? {
            ControlFlow::Continue(position) => position,
            ControlFlow::Break(outcome) => return Ok(outcome),
        };
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            permit = core.compaction_permit().instrument(phase_span!(
                core,
                Phase::WaitForCompactionPermit,
                namespace_id
            )) => permit,
        };
        let started_at_ms = core.timer().monotonic_now_ms();
        let outcome = self
            .compaction_engine()
            .compact_metadata_unit_from(&position, cancellation)
            .await
            .map_err(Error::from);
        let elapsed_ms = core
            .timer()
            .monotonic_now_ms()
            .saturating_sub(started_at_ms);
        core.instruments()
            .compactions
            .finished(&outcome, elapsed_ms);
        let registry = self.registry()?;
        let outcome = registry
            .finish_namespace_mutation(namespace_id, outcome, |outcome| match outcome {
                loonfs_core::CompactionUnitOutcome::Published { manifest, .. } => {
                    NamespaceChange::Published(Box::new(manifest.clone()))
                }
                _ => NamespaceChange::Unchanged,
            })
            .await;
        outcome.map(compaction_outcome)
    }
}
