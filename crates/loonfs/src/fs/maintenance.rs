//! [`Maintenance`]'s explicit maintenance: steps, GC, the checkpoint
//! inventory, WAL folds, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through the runtime's public snapshot
//! calls, and its hosts drive it.

use crate::publisher::NamespaceChange;
use crate::trace::phase_span;
use crate::Maintenance;
use crate::NamespaceDiagnostics;
use crate::{
    AdvanceRetentionFloorOptions, AdvanceRetentionResponse, Checkpoint, ErrorCode, FoldWalOutcome,
    FoldWalResponse, ListCheckpointsResponse, MaintainMetadataOptions, MaintenanceCancellation,
    MetadataCompactionOutcome, MetadataCompactionResponse, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, MetadataPassReport, NamespaceId, PinId,
    SetRetentionWindowResponse, SharedObjectStore, WalFoldStepOutcome,
};
use crate::{Error, Result};
use loonfs_core::cache::NamespaceStorageDiagnostics;
use loonfs_core::control::{CurrentManifest, NamespaceReadAnchor};
use loonfs_core::CheckpointPageCursor;
use loonfs_core::CoreError;
use loonfs_types::CompactorEpoch;
use loonfs_types::PageRequest;
use std::num::NonZeroU64;
use std::sync::Arc;
use tracing::Instrument;

mod floor_acknowledgment;
#[cfg(test)]
mod tests;

pub use floor_acknowledgment::{
    AcknowledgeRetentionFloorOutcome, AcknowledgeRetentionFloorResponse, RetentionFloors,
};

/// Abandoned compaction units in a row after which
/// [`Maintenance::maintain_metadata_while_due`] stops, so a loop whose units
/// keep finding their inputs replaced, or keep losing every publication
/// attempt, can't spin.
const MAX_LOST_COMPACTION_RACES: u32 = 3;

/// Compaction units that one call of
/// [`Maintenance::maintain_metadata_while_due`] publishes before it returns,
/// so a namespace with a backlog cannot hold its caller for long.
const MAX_COMPACTION_UNITS_PER_CALL: u32 = 16;

/// Whether metadata maintenance folds the WAL tail before it compacts.
#[derive(Debug, Clone, Copy)]
enum TailFold {
    /// Folds the tail when the runtime's fold trigger finds it due, with
    /// `max_wal_tail_objects` as the WAL object count.
    WhenDue { max_wal_tail_objects: u64 },
    /// Leaves the tail to the writer session's fold task and the host's idle
    /// fold. The session's position stores the tail, so a step reads only the
    /// current manifest.
    Never,
}

impl TailFold {
    fn from_options(options: &MaintainMetadataOptions) -> Self {
        Self::WhenDue {
            max_wal_tail_objects: options.max_wal_tail_objects.get(),
        }
    }
}

/// What one metadata step did, and whether it left a WAL fold due.
struct MetadataStepReport {
    response: MetadataPassReport,
    wal_fold: MetadataMaintenanceOutcome,
}

/// A pager over existing checkpoints.
pub type CheckpointsPager = loonfs_types::Pager<ListCheckpointsResponse, Error>;

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
            output_segments: u64::try_from(output_segments).unwrap_or(u64::MAX),
        },
        loonfs_core::CompactionUnitOutcome::Cancelled => MetadataCompactionOutcome::Cancelled,
        loonfs_core::CompactionUnitOutcome::Abandoned => MetadataCompactionOutcome::Abandoned,
        loonfs_core::CompactionUnitOutcome::Fenced => MetadataCompactionOutcome::Fenced,
    }
}

impl Maintenance {
    /// A mutating engine under this handle's actor identity.
    fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = self.core.writer_engine(&self.actor, namespace_id);
        #[cfg(test)]
        let engine = match self.compaction_row_budget {
            Some(rows) => engine.with_metadata_run_policy(loonfs_core::MetadataRunPolicy {
                max_decoded_input_rows_per_step: rows,
                ..self.core.inner.config.metadata_run_policy
            }),
            None => engine,
        };
        #[cfg(test)]
        let engine = match self.segment_row_budget {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    /// Returns namespace state and storage details used by maintenance.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.diagnostics",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "diagnostics",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn diagnostics(&self, namespace_id: &NamespaceId) -> Result<NamespaceDiagnostics> {
        let _permit = self.core.inner.config.execution_budget.read_permit().await;
        self.core.record_trace_context(&tracing::Span::current());
        let diagnostics = loonfs_core::cache::load_namespace_diagnostics(
            self.core.store(),
            namespace_id,
            Arc::clone(&self.core.inner.timer),
        )
        .await?;
        let live_snapshots = self.count_live_snapshots(namespace_id).await?;
        Ok(Self::namespace_diagnostics(diagnostics, live_snapshots))
    }

    async fn count_live_snapshots(&self, namespace_id: &NamespaceId) -> Result<u64> {
        let now_ms = self.core.now_ms();
        let page_limit = loonfs_types::PaginationPolicy::default().max_limit();
        let mut cursor = None;
        let mut live_snapshots = 0_u64;
        loop {
            let page = self
                .engine(namespace_id)
                .list_snapshots_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::new(page_limit),
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            let live = page
                .items
                .iter()
                .filter(|snapshot| snapshot.is_live(now_ms))
                .count();
            live_snapshots = live_snapshots.saturating_add(live as u64);
            let Some(next_cursor) = page.next_cursor else {
                return Ok(live_snapshots);
            };
            cursor = Some(next_cursor);
        }
    }

    fn namespace_diagnostics(
        diagnostics: NamespaceStorageDiagnostics,
        live_snapshots: u64,
    ) -> NamespaceDiagnostics {
        NamespaceDiagnostics {
            namespace_id: diagnostics.namespace_id,
            created_at_ms: diagnostics.created_at_ms,
            created_by: diagnostics.created_by,
            fork_basis: diagnostics.fork_basis,
            head_seq: diagnostics.head_seq,
            retention_floor_seq: diagnostics.retention_floor_seq,
            acknowledged_floor_seq: diagnostics.acknowledged_floor_seq,
            current_manifest_no: Some(diagnostics.current_manifest_no),
            wal_tail_objects: diagnostics.wal_tail_objects,
            live_snapshots,
        }
    }

    async fn load_live_anchor(&self, namespace_id: &NamespaceId) -> Result<NamespaceReadAnchor> {
        let observed = loonfs_core::time::Observation::now(Arc::clone(&self.core.inner.timer));
        Ok(
            loonfs_core::control::load_live_read_anchor(self.core.store(), namespace_id, observed)
                .await?,
        )
    }

    /// Loads the current manifest of a namespace that isn't deleted, without
    /// the WAL tail above it.
    async fn load_live_manifest(&self, namespace_id: &NamespaceId) -> Result<CurrentManifest> {
        let manifest =
            loonfs_core::control::load_namespace_current_manifest(self.core.store(), namespace_id)
                .await
                .map_err(CoreError::from)?
                .state;
        if manifest.envelope.payload().status.is_deleted() {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            }
            .into());
        }
        Ok(manifest)
    }

    /// Runs [`Self::maintain_metadata_with_options`] with the default
    /// thresholds.
    pub async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataPassReport> {
        self.maintain_metadata_with_options(namespace_id, &MaintainMetadataOptions::default())
            .await
    }

    /// Folds the WAL tail at `max_wal_tail_objects` WAL objects, at the
    /// runtime's inline fold size when this runtime's writer session knows
    /// the tail's inline bytes, or once the tail's newest commit reaches the
    /// runtime's idle fold age on this handle's wall clock (see
    /// [`LoonFsBuilder::idle_fold_after_ms`](crate::LoonFsBuilder::idle_fold_after_ms)).
    /// Then runs one compaction unit whose window lists at most the run
    /// policy's `max_decoded_input_rows_per_step` rows, and reports a larger
    /// window as [`MetadataCompactionOutcome::MetadataCompactionRequired`]. A
    /// fold waits for a fold permit from the runtime's execution budget, and a
    /// unit that finds work waits for a compaction permit from it.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintain_metadata",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintain_metadata",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn maintain_metadata_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataPassReport> {
        self.core.record_trace_context(&tracing::Span::current());
        let step = self
            .metadata_step(
                namespace_id,
                TailFold::from_options(options),
                options.compaction_policy,
                &MaintenanceCancellation::new(),
            )
            .await?;
        Ok(step
            .expect("a pass without a cancellation runs to its end")
            .response)
    }

    /// Runs one metadata pass, or returns `None` when `cancellation` stops
    /// the pass before its compaction unit starts.
    ///
    /// Cancellation drops the manifest or anchor load and the fold at once,
    /// and the compaction unit stops at its next block.
    async fn metadata_step(
        &self,
        namespace_id: &NamespaceId,
        tail_fold: TailFold,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        cancellation: &MaintenanceCancellation,
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
                observed.as_ref(),
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
    /// fold is due. Returns the current manifest for the pass's compaction
    /// unit to plan from, the unfolded WAL objects that the pass found, what
    /// the fold did, and whether the WAL tail leaves metadata caught up.
    ///
    /// A fold attempt can move the manifest, so after a fold attempt this
    /// returns `None` for the manifest, and the compaction unit loads the
    /// current manifest itself. A tail that waits for its idle fold age leaves
    /// metadata due. A session's own compaction (`TailFold::Never`) doesn't
    /// fold, so it takes the unfolded WAL objects from the session's position
    /// (`None` when the runtime has no position for the namespace), and then
    /// reads only the current manifest. A tail that the position shows
    /// unfolded leaves metadata due for the session's fold task or the host's
    /// idle fold.
    async fn fold_if_due(
        &self,
        namespace_id: &NamespaceId,
        tail_fold: TailFold,
    ) -> Result<(
        Option<CurrentManifest>,
        Option<u64>,
        WalFoldStepOutcome,
        bool,
    )> {
        let TailFold::WhenDue {
            max_wal_tail_objects,
        } = tail_fold
        else {
            // The position comes first. A fold publishes its manifest before
            // it moves the position, so the manifest loaded after the position
            // is the position's manifest or a newer one, and a tail that the
            // position shows folded is folded into that manifest.
            let position = self.publisher.wal_fold_input(namespace_id).await;
            let manifest = self.load_live_manifest(namespace_id).await?;
            let manifest_no = manifest.manifest().manifest_no;
            let behind = position
                .as_ref()
                .map(|position| position.basis.manifest_no())
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
            let objects = position.map(|position| position.wal_tail_objects);
            return Ok((
                Some(manifest),
                objects,
                WalFoldStepOutcome::NotNeeded,
                objects == Some(0) && behind.is_none(),
            ));
        };
        let anchor = self.load_live_anchor(namespace_id).await?;
        let status = NamespaceStorageDiagnostics::from(&anchor);
        let trigger = self.writer.fold_trigger;
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
                Some(anchor.current_manifest().clone()),
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
            // The head seq of the anchor the pass decided on is reported when
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

    /// Runs [`Self::maintain_metadata_while_due_with_options`] with the
    /// default thresholds.
    pub async fn maintain_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        self.maintain_metadata_while_due_with_options(
            namespace_id,
            cancellation,
            &MaintainMetadataOptions::default(),
        )
        .await
    }

    /// Repeats [`Self::maintain_metadata_with_options`] while it finds
    /// compaction due. When a pass reports a window too large for it, runs
    /// [`Self::compact_metadata`].
    ///
    /// Stops when nothing is due, when another process holds the compactor
    /// epoch, when `cancellation` is set, after three abandoned units in a
    /// row, or after it publishes 16 compaction units. Work still due after
    /// the 16th unit waits for the next call. A unit that loses its manifest
    /// put to another publication tries again over the manifest that won, so
    /// only an abandoned unit counts as a lost race. Setting `cancellation`
    /// ends a wait for a permit and drops a fold in progress at once, and a
    /// running compaction unit stops at its next block. A dropped call
    /// leaves what a crash would: unreferenced segments, or a manifest the
    /// runtime learns of the way it learns of another process's. Fails with
    /// the first error. Keeps no state between calls.
    ///
    /// The response reports the compaction units the call published, and an
    /// outcome. The outcome is [`MetadataMaintenanceOutcome::CaughtUp`] when
    /// nothing is due now and nothing can become due without a new commit
    /// under `options`, based on the state this call read. It is
    /// [`MetadataMaintenanceOutcome::Due`] at a work limit, after lost races,
    /// on fencing or cancellation, when a compaction unit does not finish,
    /// or while a tail waits for the idle fold age.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        // Boxing erases the loop's nested step futures. Without it, a host
        // that awaits this call inside its own scheduling futures exceeds
        // rustc's type-recursion depth.
        Box::pin(self.metadata_while_due(
            namespace_id,
            cancellation,
            TailFold::from_options(options),
            options.compaction_policy,
        ))
        .await
    }

    /// Repeats metadata steps without a fold while compaction is due, for a
    /// writer session whose fold task folds its own tail.
    ///
    /// Each step reads the current manifest and no WAL objects, because the
    /// session's position stores the tail. Stops and returns like
    /// [`Self::maintain_metadata_while_due_with_options`], except that a tail
    /// that the position shows unfolded leaves metadata due.
    pub(crate) async fn compact_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        self.metadata_while_due(
            namespace_id,
            cancellation,
            TailFold::Never,
            MaintainMetadataOptions::default().compaction_policy,
        )
        .await
    }

    /// Runs metadata steps while compaction is due, and `tail_fold` decides
    /// whether each step may fold the tail first.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintain_metadata_while_due",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintain_metadata_while_due",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
        tail_fold: TailFold,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<MetadataMaintenanceResponse> {
        self.core.record_trace_context(&tracing::Span::current());
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
                        | MetadataCompactionOutcome::Fenced
                        | MetadataCompactionOutcome::Unknown => {
                            break 'maintenance MetadataMaintenanceOutcome::Due
                        }
                    }
                }
                MetadataCompactionOutcome::NotNeeded if cancellation.is_cancelled() => {
                    break 'maintenance MetadataMaintenanceOutcome::Due
                }
                MetadataCompactionOutcome::NotNeeded => break 'maintenance step.wal_fold,
                MetadataCompactionOutcome::Cancelled
                | MetadataCompactionOutcome::Fenced
                | MetadataCompactionOutcome::Unknown => {
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
    }

    async fn compactor_epoch(&self, namespace_id: &NamespaceId) -> Result<CompactorEpoch> {
        // Hold the claim lock across publication so concurrent groups share one epoch.
        let mut epochs = self.writer.compactor_epochs.lock().await;
        if let Some(epoch) = epochs.get(namespace_id) {
            return Ok(*epoch);
        }
        let claimed = self
            .engine(namespace_id)
            .claim_compactor()
            .await
            .map_err(Error::Core)?;
        if let Some(published) = claimed.published {
            self.publisher
                .record_manifest_published(namespace_id, published)
                .await;
        }
        epochs.insert(namespace_id.clone(), claimed.response);
        Ok(claimed.response)
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
    /// `observed`, when given, was loaded after anything this call published.
    /// The unit waits for a compaction permit from the runtime's execution
    /// budget, and `cancellation` ends that wait or stops the merge at its
    /// next block.
    async fn compact_once(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        admission: loonfs_core::CompactionAdmission,
        observed: Option<&CurrentManifest>,
        cancellation: &MaintenanceCancellation,
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
        if let Some(epoch) = claimed {
            if observed.is_some_and(|manifest| epoch != manifest.compactor_epoch()) {
                self.forget_fenced_compactor_epoch(namespace_id, epoch)
                    .await;
                return Ok(MetadataCompactionOutcome::Fenced);
            }
        }
        let due = |manifest: &CurrentManifest| {
            loonfs_core::cache::metadata_compaction_due(manifest, compaction_policy)
        };
        let unit_due = match observed {
            Some(manifest) => due(manifest),
            // A claimed unit reads less than loading the manifest to check
            // first.
            None => claimed.is_some() || due(&self.load_live_manifest(namespace_id).await?),
        };
        if !unit_due {
            return Ok(MetadataCompactionOutcome::NotNeeded);
        }
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(MetadataCompactionOutcome::Cancelled),
            permit = self.writer.execution_budget.compaction_permit() => permit,
        };
        let compactor_epoch = self.compactor_epoch(namespace_id).await?;
        let started_at_ms = self.core.inner.timer.monotonic_now_ms();
        let outcome = self
            .engine(namespace_id)
            .compact_metadata_unit(
                compaction_policy,
                admission,
                compactor_epoch,
                cancellation.metadata_compaction(),
            )
            .await
            .map_err(Error::Core);
        let elapsed_ms = self
            .core
            .inner
            .timer
            .monotonic_now_ms()
            .saturating_sub(started_at_ms);
        self.core
            .instruments()
            .compaction_finished(&outcome, elapsed_ms);
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

    /// Runs one metadata compaction unit in the caller's task, whatever the
    /// size of its window.
    ///
    /// Use this when [`MetadataCompactionOutcome::MetadataCompactionRequired`]
    /// is reported, and repeat it while it publishes to compact every
    /// eligible group. The unit waits for a compaction permit from the
    /// runtime's execution budget.
    ///
    /// Cancelling `cancellation` ends the wait for a permit and reports the
    /// unit cancelled. A running unit stops at its next block.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.compact_metadata",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "compact_metadata",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn compact_metadata(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionResponse> {
        self.run_compaction_unit(namespace_id, cancellation).await
    }

    /// Runs one compaction unit of any size for [`Self::compact_metadata`]
    /// and [`Self::maintain_metadata_while_due_with_options`].
    async fn run_compaction_unit(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let compaction = self
            .compact_once(
                namespace_id,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                None,
                cancellation,
            )
            .await?;
        Ok(MetadataCompactionResponse {
            namespace_id: namespace_id.clone(),
            compaction,
        })
    }

    /// Runs [`Self::gc_with_options`] with the default grace window.
    pub async fn gc(&self, namespace_id: &NamespaceId) -> Result<crate::GcResponse> {
        self.gc_with_options(namespace_id, &crate::GcOptions::default())
            .await
    }

    /// Runs one complete garbage-collection pass for one namespace.
    ///
    /// Every call rebuilds the current live roots and keeps no cursor. A pass
    /// runs only when asked. A deleted namespace is collected, which is how
    /// its objects are reclaimed; a namespace that does not exist returns
    /// `namespace_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.gc",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "gc",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn gc_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &crate::GcOptions,
    ) -> Result<crate::GcResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let segment_cache = self.core.metadata_segment_cache();
        let report = loonfs_core::gc_namespace(
            self.core.store(),
            Some(segment_cache.as_ref()),
            Some(&self.core.inner.completed_upload_sessions),
            namespace_id,
            options,
            &self.core.mutation_context(&self.actor),
            Arc::clone(&self.core.inner.timer),
        )
        .await
        .map_err(Error::Core)?;
        // A caller may drop the response, so its counts survive only when
        // this shared pass records them here.
        self.core.instruments().gc_pass(&report);
        // A pass publishes no manifest, so the head didn't move. The
        // publisher keeps its position, and the cached head stays confirmed.
        // The collection grace covers the revalidation bound, so a head
        // inside its bound never lists a segment that a pass deleted
        // (format.md sections 11.2 and 11.4).
        self.publisher
            .invalidate_namespace(namespace_id, NamespaceChange::Unchanged)
            .await;
        Ok(report)
    }

    /// Lists every pin as a checkpoint, in ascending id order, including
    /// fork pins and expired snapshots that garbage collection has not yet
    /// deleted. This is the inventory of what keeps manifests from
    /// collection. The cursor resumes a live listing.
    pub fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager {
        let maintenance = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_types::Pager::new(move |request| {
            let maintenance = maintenance.clone();
            let namespace_id = namespace_id.clone();
            async move {
                maintenance
                    .checkpoints_page(&namespace_id, super::core::decode_page_request(request)?)
                    .await
            }
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.list_checkpoints",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "list_checkpoints",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListCheckpointsResponse> {
        let _permit = self.core.inner.config.execution_budget.read_permit().await;
        self.core.record_trace_context(&tracing::Span::current());
        let page = self
            .engine(namespace_id)
            .list_checkpoints_page(request)
            .await
            .map_err(Error::from)?;
        Ok(ListCheckpointsResponse {
            namespace_id: namespace_id.clone(),
            checkpoints: page.items,
            next_cursor: super::core::encode_next_cursor(page.next_cursor.as_ref())?,
        })
    }

    /// Returns one existing pin of any owner, as the checkpoint listing
    /// returns it, including an expired snapshot that garbage collection has
    /// not yet deleted. A missing id returns `snapshot_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.get_checkpoint",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_checkpoint",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn get_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Result<Checkpoint> {
        let _permit = self.core.inner.config.execution_budget.read_permit().await;
        self.core.record_trace_context(&tracing::Span::current());
        self.engine(namespace_id)
            .get_checkpoint(checkpoint_id)
            .await
            .map_err(Error::from)
    }

    /// Folds the visible WAL tail into a new manifest.
    ///
    /// A namespace with no unfolded tail reports
    /// [`FoldWalOutcome::AlreadyCurrent`] and publishes nothing. The fold
    /// waits for a fold permit from the runtime's execution budget. This runs
    /// no compaction; [`Self::maintain_metadata`] folds and then runs one
    /// compaction unit.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.fold_wal",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "fold_wal",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn fold_wal(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.run_wal_fold(namespace_id).await
    }

    /// Runs [`Self::advance_retention_floor_with_options`] toward the folded
    /// manifest head.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        self.advance_retention_floor_with_options(
            namespace_id,
            &AdvanceRetentionFloorOptions::default(),
        )
        .await
    }

    /// Advances the namespace retention floor toward `options.target` when a
    /// verified checkpoint makes it safe.
    ///
    /// Whatever the target, the floor doesn't pass a commit dated less than
    /// [`GC_DEFAULT_GRACE_WINDOW_MS`](crate::GC_DEFAULT_GRACE_WINDOW_MS)
    /// before the runtime's wall clock (format spec, "Advancing the retention
    /// floor"), and the response includes the floor that the advance reached.
    /// Advancing the floor abandons the replay history below it, and a later
    /// base compaction keeps, for each file, every revision above the floor
    /// and the newest revision at or below it. The runtime doesn't schedule
    /// an advance. The reference server makes one on each full maintenance
    /// pass, toward [`RetentionTarget::Window`](crate::RetentionTarget::Window)
    /// with its `retention_window_ms` as the default window, so a namespace
    /// with neither its own window nor that setting keeps its whole history.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.advance_retention_floor",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "advance_retention_floor",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn advance_retention_floor_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<AdvanceRetentionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .advance_retention_floor(options.target)
            .await
            .map_err(Error::from);
        self.publisher
            .finish_namespace_mutation(namespace_id, result, NamespaceChange::of_report)
            .await
            .map(|report| report.response)
    }

    /// Sets the namespace's retention window.
    ///
    /// `None` clears the window, so a host's scheduled advance uses the
    /// host's own setting again.
    ///
    /// The call publishes a manifest that changes nothing else, and if the
    /// namespace already has the window, it publishes nothing. See
    /// [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.set_retention_window",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "set_retention_window",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn set_retention_window(
        &self,
        namespace_id: &NamespaceId,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<SetRetentionWindowResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .set_retention_window(retention_window_ms)
            .await
            .map_err(Error::from);
        self.publisher
            .finish_namespace_mutation(namespace_id, result, NamespaceChange::of_report)
            .await
            .map(|report| report.response)
    }

    /// Shared implementation for metadata maintenance and [`Self::fold_wal`].
    async fn run_wal_fold(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        async {
            let _permit = self.writer.execution_budget.fold_permit().await;
            let result = self
                .engine(namespace_id)
                .fold_wal()
                .await
                .map_err(Error::from);
            self.publisher
                .finish_namespace_mutation(namespace_id, result, NamespaceChange::of_report)
                .await
                .map(|report| report.response)
                .inspect_err(|error| tracing::debug!(%error))
        }
        .instrument(phase_span!(self.core, "wal_fold", namespace_id))
        .await
    }
}

impl crate::Namespace<crate::Writable> {
    /// Grants `admin` on the root row to `principal_id`, keeping every other
    /// root grant, through a commit no subject check applies to.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.recover_administrator",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "recover_administrator",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn recover_administrator(
        &self,
        principal_id: &loonfs_types::PrincipalId,
        actor: &loonfs_types::ActorId,
    ) -> Result<loonfs_types::RecoverAdministratorResponse> {
        use loonfs_types::api::v0::FilesystemChange;
        use loonfs_types::{AccessGrants, AccessRight, AccessRights};

        self.core.record_trace_context(&tracing::Span::current());
        let (engine, context) = self
            .core
            .pinned_read(&self.namespace_id, crate::cache::HeadConfirmation::View)
            .await?;
        let root = engine.root_access(&context).await?;
        let grants = root.access.grants;
        let mut entries: std::collections::BTreeMap<_, _> = grants
            .iter()
            .map(|(principal, rights)| (principal.clone(), rights))
            .collect();
        let rights = grants
            .get(principal_id)
            .union(AccessRights::from_iter([AccessRight::Admin]));
        entries.insert(principal_id.clone(), rights);
        let grants = AccessGrants::new(entries).map_err(|error| {
            Error::Core(crate::CoreError::InvalidCommitField {
                field: "grants",
                message: error.to_string(),
                precondition_index: None,
            })
        })?;
        let request = crate::publish::CommitIntent::single(
            loonfs_types::CommitId::generate(),
            actor.clone(),
            Some("administrator recovery".to_owned()),
            crate::publish::FilesystemOperation::UpdateAccess {
                path: loonfs_types::AbsolutePath::root(),
                boundary: root.access.boundary,
                grants,
                expected_inode_id: Some(loonfs_types::ROOT_INODE_ID),
                expected_access_revision_no: Some(root.access_revision_no),
            },
        );
        let commit = self
            .commit_candidate_inner(crate::publish::CommitCandidate::maintenance(request))
            .await?;
        let access_revision_no = commit
            .events
            .iter()
            .find_map(|event| match event {
                FilesystemChange::AccessChanged {
                    access_revision_no, ..
                } => Some(*access_revision_no),
                _ => None,
            })
            .expect("administrator recovery should publish an access change");
        Ok(loonfs_types::RecoverAdministratorResponse {
            namespace_id: commit.namespace_id,
            commit_id: commit.commit_id,
            committed_seq: commit.committed_seq,
            access_revision_no,
        })
    }
}
