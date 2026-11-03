//! The compaction loop that [`Maintenance`](crate::Maintenance) runs, and
//! that each writer session runs over its own namespace: metadata steps, WAL
//! folds, compaction units, and the runtime's compactor epochs.

use super::{
    MetadataCompactionOutcome, MetadataCompactionReport, MetadataPassReport, WalFoldStepOutcome,
};
use crate::publisher::{FoldTrigger, NamespaceChange, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::{self, operation_span, phase_span};
use crate::{Error, Result};
use crate::{
    ErrorCode, FoldWalOutcome, FoldWalResponse, MaintainMetadataOptions,
    MetadataMaintenanceOutcome, MetadataMaintenanceResponse, NamespaceId, SharedObjectStore,
    WriterState,
};
use loonfs_core::control::NamespaceStorageDiagnostics;
use loonfs_core::control::{NamespaceReadAnchor, PublicationBasis};
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_core::{CoreError, MetadataCompactionCancellation, Phase};
use loonfs_types::{CompactorEpoch, WriterId};
use std::ops::ControlFlow;
use std::sync::Arc;
use tracing::Instrument;

/// Abandoned compaction units in a row after which
/// [`Compactor::metadata_while_due`] stops, so a loop whose units
/// keep finding their inputs replaced, or keep losing every publication
/// attempt, can't spin.
const MAX_LOST_COMPACTION_RACES: u32 = 3;

/// Compaction units that one call of [`Compactor::metadata_while_due`]
/// publishes before it returns,
/// so a namespace with a backlog cannot keep its caller waiting for long.
pub(super) const MAX_COMPACTION_UNITS_PER_CALL: u32 = 16;

/// Whether metadata maintenance folds the WAL tail before it compacts.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TailFold {
    /// Folds the tail when the runtime's fold trigger finds it due, with
    /// `max_wal_tail_objects` as the WAL object count.
    WhenDue { max_wal_tail_objects: u64 },
    /// Leaves the tail to the writer session's fold task and the host's idle
    /// fold. The session's position stores the tail, so a step reads only the
    /// current manifest.
    Never,
    /// Leaves the tail to the caller, which folded it with
    /// [`Compactor::fold_when_due`] before the call. A step reads only the
    /// current manifest, and reports metadata caught up only when
    /// `tail_caught_up` says that the caller's fold left the tail caught up.
    /// A writer session without a position compacts this way, because it has
    /// no position to read the tail from.
    Folded { tail_caught_up: bool },
}

impl TailFold {
    pub(super) fn from_options(options: &MaintainMetadataOptions) -> Self {
        Self::WhenDue {
            max_wal_tail_objects: options.max_wal_tail_objects.get(),
        }
    }
}

/// The manifest that a compaction unit plans from, and where the caller read
/// it.
pub(crate) enum PlanningBasis {
    /// The current manifest, loaded from the store after anything the caller
    /// published.
    Loaded(PublicationBasis),
    /// The manifest of this runtime's writer position, which doesn't show a
    /// publication by another process, such as that process's compactor
    /// claim.
    Position(PublicationBasis),
}

impl PlanningBasis {
    fn basis(&self) -> &PublicationBasis {
        match self {
            Self::Loaded(basis) | Self::Position(basis) => basis,
        }
    }

    fn into_basis(self) -> PublicationBasis {
        match self {
            Self::Loaded(basis) | Self::Position(basis) => basis,
        }
    }
}

/// What one metadata step did, and whether it left a WAL fold due.
pub(crate) struct MetadataStepReport {
    pub(crate) response: MetadataPassReport,
    pub(crate) wal_fold: MetadataMaintenanceOutcome,
}

/// Converts what one compaction unit did into its wire form.
fn compaction_outcome(outcome: loonfs_core::CompactionUnitOutcome) -> MetadataCompactionOutcome {
    match outcome {
        loonfs_core::CompactionUnitOutcome::NotNeeded => MetadataCompactionOutcome::NotNeeded,
        loonfs_core::CompactionUnitOutcome::TooLarge { .. } => {
            MetadataCompactionOutcome::MetadataCompactionRequired
        }
        loonfs_core::CompactionUnitOutcome::Published {
            manifest,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            ..
        } => MetadataCompactionOutcome::Published {
            manifest_no: manifest.manifest.manifest_no,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments: loonfs_types::u64_from_usize(output_segments),
        },
        loonfs_core::CompactionUnitOutcome::Cancelled => MetadataCompactionOutcome::Cancelled,
        loonfs_core::CompactionUnitOutcome::Abandoned => MetadataCompactionOutcome::Abandoned,
        loonfs_core::CompactionUnitOutcome::Fenced => MetadataCompactionOutcome::Fenced,
    }
}

/// The compaction loop of one writable runtime, acting as one writer ID.
///
/// It shares the runtime's compactor epochs, so a session's compaction and
/// the runtime's [`Maintenance`](crate::Maintenance) values never fence each
/// other.
#[derive(Clone)]
pub(crate) struct Compactor {
    pub(crate) core: RuntimeCore,
    pub(crate) publisher: PublisherRegistry,
    pub(crate) writer: Arc<WriterState>,
    pub(crate) writer_id: WriterId,
    /// A narrowed row budget for the tests that need a family group whose
    /// window is too large for a `metadata` pass. See
    /// [`Maintenance::starve_compaction_row_budget`](crate::Maintenance::starve_compaction_row_budget).
    #[cfg(test)]
    pub(super) compaction_row_budget: Option<std::num::NonZeroUsize>,
    /// A narrowed per-segment row budget for the tests that need many
    /// compacted segments. See
    /// [`Maintenance::narrow_segment_row_budget`](crate::Maintenance::narrow_segment_row_budget).
    #[cfg(test)]
    pub(super) segment_row_budget: Option<std::num::NonZeroUsize>,
}

impl Compactor {
    /// Returns the compaction loop of the runtime that `core`, `publisher`,
    /// and `writer` belong to, acting as `writer_id`.
    pub(crate) fn new(
        core: RuntimeCore,
        publisher: PublisherRegistry,
        writer: Arc<WriterState>,
        writer_id: WriterId,
    ) -> Self {
        Self {
            core,
            publisher,
            writer,
            writer_id,
            #[cfg(test)]
            compaction_row_budget: None,
            #[cfg(test)]
            segment_row_budget: None,
        }
    }

    /// A mutating engine that commits as this loop's writer ID.
    pub(super) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = self.core.writer_engine(&self.writer_id, namespace_id);
        #[cfg(test)]
        let engine = match self.compaction_row_budget {
            Some(rows) => engine.narrow_compaction_input_rows(rows),
            None => engine,
        };
        #[cfg(test)]
        let engine = match self.segment_row_budget {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    async fn load_live_anchor(&self, namespace_id: &NamespaceId) -> Result<NamespaceReadAnchor> {
        let observed = self.core.observation();
        Ok(self
            .core
            .namespace_store(namespace_id)
            .load_live_read_anchor(observed)
            .await?)
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

    /// Returns the manifest of `position` as a basis, loaded through the
    /// segment cache, or `None` when `position` is `None` or the position's
    /// last check is at the revalidation bound.
    async fn load_position_basis(
        &self,
        position: Option<&loonfs_core::publish::WriterPosition>,
    ) -> Result<Option<PublicationBasis>> {
        let Some(position) = position else {
            return Ok(None);
        };
        let segment_cache = self.core.metadata_segment_cache();
        Ok(
            loonfs_core::control::load_position_basis(self.core.store(), &segment_cache, position)
                .await?,
        )
    }

    /// Runs one metadata pass, or returns `None` when `cancellation` stops
    /// the pass before its compaction unit starts.
    ///
    /// Cancellation drops the manifest or anchor load and the fold at once,
    /// and the compaction unit stops at its next block.
    pub(crate) async fn metadata_step(
        &self,
        namespace_id: &NamespaceId,
        tail_fold: TailFold,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<Option<MetadataStepReport>> {
        let (observed, wal_tail_objects, wal_fold, wal_caught_up) = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(None),
            folded = self.fold_if_due(namespace_id, tail_fold) => folded?,
        };
        let compaction = self
            .compact_once(
                namespace_id,
                compaction_policy,
                loonfs_core::CompactionAdmission::WithinRowBudget,
                observed,
                cancellation,
            )
            .await?;
        let response = MetadataPassReport {
            namespace_id: namespace_id.clone(),
            wal_fold,
            compaction,
        };
        tracing::debug!(
            wal_tail_objects_before = wal_tail_objects,
            wal_fold = ?response.wal_fold,
            compaction = ?response.compaction,
            "metadata maintenance pass concluded"
        );
        Ok(Some(MetadataStepReport {
            response,
            wal_fold: if wal_caught_up {
                MetadataMaintenanceOutcome::CaughtUp
            } else {
                MetadataMaintenanceOutcome::Due
            },
        }))
    }

    /// Reads the state that a pass plans from, and folds the WAL tail when a
    /// fold is due.
    ///
    /// Returns the manifest for the pass's compaction unit to plan from, the
    /// unfolded WAL objects that the pass found, what the fold did, and
    /// whether the WAL tail leaves metadata caught up.
    ///
    /// A fold attempt can move the manifest, so after a fold attempt this
    /// returns `None` for the manifest, and the compaction unit finds the
    /// manifest itself. A tail that waits for its idle fold age leaves
    /// metadata due. A session's own compaction (`TailFold::Never`) doesn't
    /// fold, so it takes the unfolded WAL objects from the session's position
    /// (`None` when the runtime has no position for the namespace). It takes
    /// the manifest from the position too while the position's last check is
    /// within the revalidation bound, and otherwise it loads only the current
    /// manifest, without the WAL tail. A tail that the position shows
    /// unfolded leaves metadata due for the session's fold task or the host's
    /// idle fold, unless it's under the fold trigger and the runtime doesn't
    /// fold idle tails. `TailFold::Folded` reads the same state, and takes
    /// whether the tail leaves metadata caught up from the caller.
    async fn fold_if_due(
        &self,
        namespace_id: &NamespaceId,
        tail_fold: TailFold,
    ) -> Result<(Option<PlanningBasis>, Option<u64>, WalFoldStepOutcome, bool)> {
        let TailFold::WhenDue {
            max_wal_tail_objects,
        } = tail_fold
        else {
            // The position comes first. A fold publishes its manifest before
            // it moves the position, so the manifest loaded after the position
            // is the position's manifest or a newer one, and a tail that the
            // position shows folded is folded into that manifest.
            let position = self.publisher.wal_fold_input(namespace_id).await;
            let manifest = match self.load_position_basis(position.as_ref()).await? {
                Some(basis) => PlanningBasis::Position(basis),
                None => PlanningBasis::Loaded(self.load_live_basis(namespace_id).await?),
            };
            let manifest_no = manifest.basis().manifest.manifest().manifest_no;
            let behind = position
                .as_ref()
                .map(|position| position.read_basis.basis.manifest_no())
                .filter(|position_manifest_no| manifest_no < *position_manifest_no);
            // The publish order rules this out, so a manifest behind the
            // position is a bug, and metadata stays due instead of the step
            // stopping the maintenance loop.
            if let Some(position_manifest_no) = behind {
                tracing::warn!(
                    namespace_id = namespace_id.as_str(),
                    %manifest_no,
                    %position_manifest_no,
                    "the current manifest is older than the writer position's manifest"
                );
            }
            // In a runtime that doesn't fold idle tails, no fold runs for a
            // tail under the fold trigger until the next commit, so that tail
            // leaves metadata caught up, as `TailFold::WhenDue` reports it.
            let trigger = FoldTrigger::new(self.core.config());
            let tail_waits = position.as_ref().map(|position| {
                let objects = position.wal_tail_objects();
                objects > 0
                    && (trigger.folds_idle_tails()
                        || trigger.fold_is_due(
                            FOLD_AT_WAL_OBJECTS.get(),
                            objects,
                            position.wal_tail_inline_bytes(),
                        ))
            });
            let tail_caught_up = match tail_fold {
                TailFold::Folded { tail_caught_up } => tail_caught_up,
                TailFold::WhenDue { .. } | TailFold::Never => tail_waits == Some(false),
            };
            let objects = position.map(|position| position.wal_tail_objects());
            return Ok((
                Some(manifest),
                objects,
                WalFoldStepOutcome::NotNeeded,
                tail_caught_up && behind.is_none(),
            ));
        };
        let anchor = self.load_live_anchor(namespace_id).await?;
        let status = NamespaceStorageDiagnostics::from(&anchor);
        let trigger = FoldTrigger::new(self.core.config());
        let idle_fold_due_at_ms = status
            .wal_tail_newest_commit_at_ms
            .and_then(|committed_at_ms| trigger.idle_fold_due_at_ms(committed_at_ms));
        let inline_bytes = if status.wal_tail_objects > 0 {
            self.publisher
                .wal_tail_inline_bytes(namespace_id)
                .await
                .unwrap_or(0)
        } else {
            0
        };
        let fold = trigger.fold_is_due(max_wal_tail_objects, status.wal_tail_objects, inline_bytes)
            || idle_fold_due_at_ms.is_some_and(|due_at_ms| self.core.now_ms() >= due_at_ms);
        if !fold {
            return Ok((
                Some(PlanningBasis::Loaded(anchor.publication_basis())),
                Some(status.wal_tail_objects),
                WalFoldStepOutcome::NotNeeded,
                idle_fold_due_at_ms.is_none(),
            ));
        }
        let wal_fold = match self.run_wal_fold(namespace_id).await {
            Ok(folded) => match folded.outcome {
                FoldWalOutcome::Published => WalFoldStepOutcome::Folded {
                    manifest_head_seq: folded.manifest_head_seq,
                },
                FoldWalOutcome::AlreadyCurrent => WalFoldStepOutcome::AlreadyPublished {
                    attempted_seq: folded.target_head_seq,
                    current_manifest_no: folded.manifest_no,
                },
            },
            // The head seq of the anchor the pass chose is reported when
            // concurrent updates prevent every fold attempt from publishing.
            Err(Error::Core(error)) if error.code() == ErrorCode::StaleHead => {
                WalFoldStepOutcome::RetriesExhausted {
                    observed_head_seq: anchor.read_state.seq,
                }
            }
            Err(error) => return Err(error),
        };
        let folded = matches!(wal_fold, WalFoldStepOutcome::Folded { .. });
        Ok((None, Some(status.wal_tail_objects), wal_fold, folded))
    }

    /// Folds the WAL tail when a fold is due, as a metadata step with
    /// `TailFold::WhenDue` does, and returns whether the tail that the call
    /// leaves keeps metadata caught up.
    ///
    /// A fold is due when the tail reaches `max_wal_tail_objects` WAL objects
    /// or the runtime's inline fold size, or when it reaches the idle fold
    /// age.
    ///
    /// The decision reads the tail from the store, so it suits a writer
    /// session that has no position, which then compacts with
    /// `TailFold::Folded`.
    pub(crate) async fn fold_when_due(
        &self,
        namespace_id: &NamespaceId,
        max_wal_tail_objects: u64,
    ) -> Result<bool> {
        let tail_fold = TailFold::WhenDue {
            max_wal_tail_objects,
        };
        let (_, _, _, caught_up) = self.fold_if_due(namespace_id, tail_fold).await?;
        Ok(caught_up)
    }

    /// Runs metadata steps while compaction is due, and `tail_fold` determines
    /// whether each step may fold the tail first.
    pub(crate) async fn metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MetadataCompactionCancellation,
        tail_fold: TailFold,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<MetadataMaintenanceResponse> {
        trace::in_operation(
            operation_span!(self.core, "maintain_metadata_while_due", namespace_id),
            async {
                let mut lost_races = 0;
                let mut published_units = 0;
                let outcome = 'maintenance: loop {
                    if lost_races >= MAX_LOST_COMPACTION_RACES
                        || published_units >= MAX_COMPACTION_UNITS_PER_CALL
                        || cancellation.is_cancelled()
                    {
                        break MetadataMaintenanceOutcome::Due;
                    }
                    let Some(step) = self
                        .metadata_step(namespace_id, tail_fold, compaction_policy, cancellation)
                        .await?
                    else {
                        break 'maintenance MetadataMaintenanceOutcome::Due;
                    };
                    let won = match step.response.compaction {
                        MetadataCompactionOutcome::Published { .. } => true,
                        MetadataCompactionOutcome::Abandoned => false,
                        MetadataCompactionOutcome::MetadataCompactionRequired => {
                            match self
                                .run_compaction_unit(namespace_id, cancellation)
                                .await?
                                .compaction
                            {
                                MetadataCompactionOutcome::Published { .. } => true,
                                MetadataCompactionOutcome::Abandoned => false,
                                MetadataCompactionOutcome::NotNeeded
                                | MetadataCompactionOutcome::MetadataCompactionRequired
                                | MetadataCompactionOutcome::Cancelled
                                | MetadataCompactionOutcome::Fenced => {
                                    break 'maintenance MetadataMaintenanceOutcome::Due
                                }
                            }
                        }
                        MetadataCompactionOutcome::NotNeeded if cancellation.is_cancelled() => {
                            break 'maintenance MetadataMaintenanceOutcome::Due
                        }
                        MetadataCompactionOutcome::NotNeeded => break 'maintenance step.wal_fold,
                        MetadataCompactionOutcome::Cancelled
                        | MetadataCompactionOutcome::Fenced => {
                            break 'maintenance MetadataMaintenanceOutcome::Due
                        }
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
                    compaction_units: u64::from(published_units),
                })
            },
        )
        .await
    }

    pub(crate) async fn compactor_epoch(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CompactorEpoch> {
        // Keep the claim lock locked across publication so concurrent groups share one epoch.
        let mut epochs = self.writer.compactor_epochs.lock().await;
        if let Some(epoch) = epochs.get(namespace_id) {
            return Ok(*epoch);
        }
        let claimed = self
            .engine(namespace_id)
            .claim_compactor()
            .await
            .map_err(Error::from)?;
        if let Some(published) = claimed.published {
            self.publisher
                .record_manifest_published(namespace_id, published)
                .await;
        }
        epochs.insert(namespace_id.clone(), claimed.response);
        Ok(claimed.response)
    }

    /// Returns the manifest that a unit plans from (`None` when the unit
    /// loads the current manifest itself), or how the unit ends before it
    /// runs.
    ///
    /// The basis is `observed` when it's given. Otherwise the basis is this
    /// runtime's writer position while the position's last check is within
    /// the revalidation bound, and without such a position, it's the current
    /// manifest when this runtime has no claim to plan under. Before a due
    /// unit plans from a position, the call sends one HEAD to check whether
    /// the position's manifest has a successor, and when it does, the unit
    /// plans from the current manifest instead.
    async fn planning_basis(
        &self,
        namespace_id: &NamespaceId,
        claimed: Option<CompactorEpoch>,
        observed: Option<PlanningBasis>,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<ControlFlow<MetadataCompactionOutcome, Option<PublicationBasis>>> {
        let observed = match observed {
            Some(basis) => Some(basis),
            None => {
                let position = self.publisher.read_position_fold_input(namespace_id);
                self.load_position_basis(position.as_ref())
                    .await?
                    .map(PlanningBasis::Position)
            }
        };
        let basis = match observed {
            Some(basis) => Some(basis),
            // With a claim, the call skips the check that a unit is due,
            // because the unit loads the manifest itself, and checking here
            // first would load the manifest twice.
            None if claimed.is_some() => None,
            None => Some(PlanningBasis::Loaded(
                self.load_live_basis(namespace_id).await?,
            )),
        };
        let screened = self
            .screen(
                namespace_id,
                claimed,
                basis.as_ref().map(PlanningBasis::basis),
                compaction_policy,
            )
            .await;
        if let Some(outcome) = screened {
            return Ok(ControlFlow::Break(outcome));
        }
        // A position doesn't show another process's publications, such as a
        // newer compactor claim. A unit planned from it would read its inputs
        // and write its outputs before its manifest put found the claim, so
        // the unit plans from the current manifest when the position's
        // manifest has a successor.
        let basis = match basis {
            Some(PlanningBasis::Position(basis))
                if self
                    .core
                    .namespace_store(namespace_id)
                    .manifest_has_successor(basis.manifest.manifest().manifest_no)
                    .await
                    .map_err(CoreError::from)? =>
            {
                let current = self.load_live_basis(namespace_id).await?;
                let screened = self
                    .screen(namespace_id, claimed, Some(&current), compaction_policy)
                    .await;
                if let Some(outcome) = screened {
                    return Ok(ControlFlow::Break(outcome));
                }
                Some(current)
            }
            basis => basis.map(PlanningBasis::into_basis),
        };
        Ok(ControlFlow::Continue(basis))
    }

    /// Returns how a unit that plans from `basis` ends before it runs, or
    /// `None` when the unit runs.
    ///
    /// The unit is fenced when `basis` has a compactor epoch other than
    /// `claimed` (this runtime's claim), and the call then forgets that
    /// claim. The unit isn't needed when no unit is due on `basis`. Without
    /// a `basis`, the unit runs.
    async fn screen(
        &self,
        namespace_id: &NamespaceId,
        claimed: Option<CompactorEpoch>,
        basis: Option<&PublicationBasis>,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Option<MetadataCompactionOutcome> {
        let basis = basis?;
        if let Some(epoch) = claimed.filter(|epoch| *epoch != basis.manifest.compactor_epoch()) {
            self.forget_fenced_compactor_epoch(namespace_id, epoch)
                .await;
            return Some(MetadataCompactionOutcome::Fenced);
        }
        (!loonfs_core::metadata_compaction_due(&basis.manifest, compaction_policy))
            .then_some(MetadataCompactionOutcome::NotNeeded)
    }

    async fn forget_fenced_compactor_epoch(
        &self,
        namespace_id: &NamespaceId,
        fenced_epoch: CompactorEpoch,
    ) {
        let mut epochs = self.writer.compactor_epochs.lock().await;
        // An older attempt can finish after another request has claimed again.
        if epochs.get(namespace_id) == Some(&fenced_epoch) {
            epochs.remove(namespace_id);
        }
    }

    /// Runs one compaction unit under this runtime's compactor epoch.
    ///
    /// `observed`, when given, was read after anything this call published,
    /// and [`Self::planning_basis`] returns the manifest that the unit plans
    /// from, starting with `observed`. The unit waits for a compaction permit
    /// from the runtime's execution budget, and `cancellation` ends that wait
    /// or stops the compaction at its next block.
    pub(crate) async fn compact_once(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        admission: loonfs_core::CompactionAdmission,
        observed: Option<PlanningBasis>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        if cancellation.is_cancelled() {
            return Ok(MetadataCompactionOutcome::Cancelled);
        }
        let claimed = self
            .writer
            .compactor_epochs
            .lock()
            .await
            .get(namespace_id)
            .copied();
        let basis = match self
            .planning_basis(namespace_id, claimed, observed, compaction_policy)
            .await?
        {
            ControlFlow::Continue(basis) => basis,
            ControlFlow::Break(outcome) => return Ok(outcome),
        };
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            permit = self.core.compaction_permit() => permit,
        };
        let compactor_epoch = self.compactor_epoch(namespace_id).await?;
        // A claim that this call made, or that another call made while this
        // call waited for its permit, is newer than the manifest the call
        // planned from. The writer position moves onto the claim, so the unit
        // plans from the position, or loads the current manifest itself.
        let in_epoch =
            |basis: &PublicationBasis| basis.manifest.compactor_epoch() == compactor_epoch;
        let basis = match basis.filter(in_epoch) {
            Some(basis) => Some(basis),
            None => {
                let position = self.publisher.read_position_fold_input(namespace_id);
                self.load_position_basis(position.as_ref())
                    .await?
                    .filter(in_epoch)
            }
        };
        let started_at_ms = self.core.timer().monotonic_now_ms();
        let outcome = self
            .engine(namespace_id)
            .compact_metadata_unit(
                basis,
                compaction_policy,
                admission,
                compactor_epoch,
                cancellation,
            )
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
        match &outcome {
            Ok(loonfs_core::CompactionUnitOutcome::Published { manifest, .. }) => {
                self.publisher
                    .invalidate_namespace(
                        namespace_id,
                        NamespaceChange::Published(manifest.clone()),
                    )
                    .await;
            }
            Ok(loonfs_core::CompactionUnitOutcome::Fenced) => {
                self.forget_fenced_compactor_epoch(namespace_id, compactor_epoch)
                    .await;
            }
            Ok(_) | Err(_) => {}
        }
        outcome.map(compaction_outcome)
    }

    /// Runs one compaction unit of any size for
    /// [`Maintenance::compact_metadata`](crate::Maintenance::compact_metadata)
    /// and [`Self::metadata_while_due`].
    pub(super) async fn run_compaction_unit(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<MetadataCompactionReport> {
        let compaction = self
            .compact_once(
                namespace_id,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                None,
                cancellation,
            )
            .await?;
        Ok(MetadataCompactionReport {
            namespace_id: namespace_id.clone(),
            compaction,
        })
    }

    /// Folds the WAL tail for metadata maintenance and
    /// [`Maintenance::fold_wal`](crate::Maintenance::fold_wal).
    ///
    /// In a runtime with the namespace's writer session, the session folds
    /// the tail after any fold that it's already running, from its position
    /// (see [`PublisherRegistry::fold_wal_through_session`]), so the fold
    /// reads no WAL objects and publishes on the position's manifest. A
    /// position whose last check is at the revalidation bound is discarded,
    /// and the fold loads the tail from the store. A namespace without a
    /// session, or whose session is fenced, is folded from the store.
    pub(super) async fn run_wal_fold(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        if let Some(folded) = self.publisher.fold_wal_through_session(namespace_id).await {
            return folded;
        }
        async {
            let _permit = self.core.fold_permit().await;
            let engine = self
                .engine(namespace_id)
                .with_writer_position(self.publisher.read_position_fold_input(namespace_id));
            let result = engine.fold_wal().await.map_err(Error::from);
            self.publisher
                .finish_publication(namespace_id, result)
                .await
                .inspect_err(|error| tracing::debug!(%error))
        }
        .instrument(phase_span!(self.core, Phase::WalFold, namespace_id))
        .await
    }
}
