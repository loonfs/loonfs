//! [`Maintenance`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! folds, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through this handle's public
//! checkpoint calls, and its hosts drive it.

use crate::trace::phase_span;
use crate::Maintenance;
use crate::NamespaceDiagnostics;
use crate::{
    AdvanceRetentionResponse, Checkpoint, CompactionStepOutcome, CreateCheckpointOptions,
    DeleteCheckpointResponse, ErrorCode, FoldWalOutcome, FoldWalResponse, ListCheckpointsResponse,
    MaintenanceCancellation, MetadataCompactionOutcome, MetadataCompactionResponse,
    MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId, PinId, SharedObjectStore,
    SnapshotSummary, WalFoldStepOutcome,
};
use crate::{Error, Result};
use loonfs_core::cache::NamespaceStorageDiagnostics;
use loonfs_core::control::NamespaceReadAnchor;
use loonfs_core::CheckpointPageCursor;
use loonfs_types::CompactorEpoch;
use loonfs_types::PageRequest;
use tokio::time::Instant;
use tracing::Instrument;

#[cfg(test)]
mod tests;

/// Lost races in a row after which
/// [`Maintenance::maintain_metadata_while_due`] stops, so a loop that keeps
/// losing to folds or another compactor cannot spin.
const MAX_LOST_COMPACTION_RACES: u32 = 3;

/// Compaction units, bounded or streaming, that one call of
/// [`Maintenance::maintain_metadata_while_due`] publishes before it returns,
/// so a namespace with a backlog cannot hold its caller for long.
const MAX_COMPACTION_UNITS_PER_CALL: u32 = 16;

/// What one compaction unit left for its caller.
enum CompactionStep {
    Fenced,
    /// The unit is finished, and this is what it did.
    Concluded(CompactionStepOutcome),
    /// A family group has outgrown a bounded step. The caller reports that a
    /// streaming compaction is required, or runs it.
    CompactionPlanned(loonfs_core::MetadataCompactionSpec),
}

/// A pager over existing checkpoints.
pub type CheckpointsPager = loonfs_types::Pager<ListCheckpointsResponse, Error>;

fn metadata_compaction_response(
    namespace_id: &NamespaceId,
    outcome: loonfs_core::MetadataCompactionJobOutcome,
) -> MetadataCompactionResponse {
    let compaction = match outcome {
        loonfs_core::MetadataCompactionJobOutcome::Published {
            manifest_no,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
        } => MetadataCompactionOutcome::Published {
            manifest_no,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments: u64::try_from(output_segments).unwrap_or(u64::MAX),
        },
        loonfs_core::MetadataCompactionJobOutcome::Cancelled => {
            MetadataCompactionOutcome::Cancelled
        }
        loonfs_core::MetadataCompactionJobOutcome::Abandoned => {
            MetadataCompactionOutcome::Abandoned
        }
        loonfs_core::MetadataCompactionJobOutcome::Fenced => MetadataCompactionOutcome::Fenced,
    };
    MetadataCompactionResponse {
        namespace_id: namespace_id.clone(),
        compaction,
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
            Some(rows) => engine.starve_compaction_row_budget(rows),
            None => engine,
        };
        #[cfg(test)]
        let engine = match self.segment_row_budget {
            Some(rows) => engine.narrow_segment_row_budget(rows),
            None => engine,
        };
        engine
    }

    pub(crate) fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.core.invalidate_namespace_read_cache(namespace_id);
    }

    fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if crate::fs::should_invalidate_after_result(&result) {
            self.invalidate_namespace(namespace_id);
        }
        result
    }

    /// Returns namespace state and storage details used by maintenance.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.diagnostics",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.diagnostics",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn diagnostics(&self, namespace_id: &NamespaceId) -> Result<NamespaceDiagnostics> {
        self.core.record_trace_context(&tracing::Span::current());
        let diagnostics =
            loonfs_core::cache::load_namespace_diagnostics(self.core.store(), namespace_id).await?;
        let (live_checkpoints, live_snapshots) = self.count_live_checkpoints(namespace_id).await?;
        Ok(Self::namespace_diagnostics(
            diagnostics,
            live_checkpoints,
            live_snapshots,
        ))
    }

    async fn count_live_checkpoints(&self, namespace_id: &NamespaceId) -> Result<(u64, u64)> {
        let now_ms = self.core.now_ms()?;
        let page_limit = loonfs_types::PaginationPolicy::default().max_limit();
        let mut cursor = None;
        let mut live_checkpoints = 0_u64;
        let mut live_snapshots = 0_u64;
        loop {
            let page = self
                .engine(namespace_id)
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::new(page_limit),
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            for checkpoint in page.items {
                if let loonfs_types::CheckpointOwnerSummary::User { .. } = checkpoint.owner {
                    live_checkpoints = live_checkpoints.saturating_add(1);
                } else if SnapshotSummary::from_checkpoint(checkpoint)
                    .is_some_and(|snapshot| snapshot.is_live(now_ms))
                {
                    live_snapshots = live_snapshots.saturating_add(1);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok((live_checkpoints, live_snapshots));
            };
            cursor = Some(next_cursor);
        }
    }

    fn namespace_diagnostics(
        diagnostics: NamespaceStorageDiagnostics,
        live_checkpoints: u64,
        live_snapshots: u64,
    ) -> NamespaceDiagnostics {
        NamespaceDiagnostics {
            namespace_id: diagnostics.namespace_id,
            created_at_ms: diagnostics.created_at_ms,
            created_by: diagnostics.created_by,
            fork_basis: diagnostics.fork_basis,
            head_seq: diagnostics.head_seq,
            retention_floor_seq: diagnostics.retention_floor_seq,
            current_manifest_no: Some(diagnostics.current_manifest_no),
            wal_tail_objects: diagnostics.wal_tail_objects,
            live_snapshots,
            live_checkpoints,
        }
    }

    async fn load_live_anchor(&self, namespace_id: &NamespaceId) -> Result<NamespaceReadAnchor> {
        Ok(loonfs_core::control::load_live_read_anchor(self.core.store(), namespace_id).await?)
    }

    /// Runs [`Self::maintain_metadata_with_options`] with the default
    /// thresholds.
    pub async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataMaintenanceResponse> {
        self.maintain_metadata_with_options(namespace_id, &MetadataMaintenanceOptions::default())
            .await
    }

    /// Folds the WAL tail at the WAL object threshold, at the inline byte
    /// threshold when the writer knows the count, or once the tail's newest
    /// commit is `idle_fold_after_ms` old on this handle's wall clock. Then
    /// runs one bounded compaction step. A fold waits for a fold permit from
    /// the runtime's execution budget, and a step that finds work waits for
    /// a compaction permit from it.
    pub async fn maintain_metadata_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &MetadataMaintenanceOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        self.metadata_step(namespace_id, options)
            .await
            .map(|(response, _)| response)
    }

    async fn metadata_step(
        &self,
        namespace_id: &NamespaceId,
        options: &MetadataMaintenanceOptions,
    ) -> Result<(MetadataMaintenanceResponse, bool)> {
        let anchor = self.load_live_anchor(namespace_id).await?;
        let status = NamespaceStorageDiagnostics::from(&anchor);
        let inline_bytes = if status.wal_tail_objects > 0 {
            self.publisher
                .wal_tail_inline_bytes(namespace_id)
                .await
                .unwrap_or(0)
        } else {
            0
        };
        let now_ms = self.core.now_ms()?;
        let fold = options.fold_is_due(status.wal_tail_objects, inline_bytes)
            || options.idle_fold_is_due(status.wal_tail_newest_commit_at_ms, now_ms);
        let response = self
            .fold_then_compact(namespace_id, fold, &anchor, options.compaction_policy)
            .await?;
        tracing::debug!(
            wal_tail_objects_before = status.wal_tail_objects,
            wal_fold = ?response.wal_fold,
            compaction = ?response.compaction,
            "metadata maintenance pass concluded"
        );
        let wal_caught_up = match response.wal_fold {
            WalFoldStepOutcome::Folded { .. } => true,
            WalFoldStepOutcome::NotNeeded => {
                options.idle_fold_after_ms == 0 || status.wal_tail_newest_commit_at_ms.is_none()
            }
            WalFoldStepOutcome::AlreadyPublished { .. }
            | WalFoldStepOutcome::RetriesExhausted { .. } => false,
        };
        Ok((response, wal_caught_up))
    }

    /// Runs [`Self::maintain_metadata_while_due_with_options`] with the
    /// default thresholds.
    pub async fn maintain_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<bool> {
        self.maintain_metadata_while_due_with_options(
            namespace_id,
            cancellation,
            &MetadataMaintenanceOptions::default(),
        )
        .await
    }

    /// Repeats [`Self::maintain_metadata_with_options`] while it finds
    /// compaction due. When a step reports that a streaming compaction is
    /// required, runs [`Self::compact_metadata_with_cancellation`].
    ///
    /// Stops when nothing is due, when another process holds the compactor
    /// epoch, when `cancellation` is set, after three lost races in a row, or
    /// after it publishes 16 compaction units, bounded or streaming. Work
    /// still due after the 16th unit waits for the next call. Setting
    /// `cancellation` ends a wait for a permit and drops a bounded call in
    /// progress at once; a streaming compaction stops at its next block. A
    /// dropped call leaves what a crash would: unreferenced segments, or a
    /// manifest the runtime learns of the way it learns of another
    /// process's. Fails with the first error. Keeps no state between calls.
    ///
    /// Returns `true` when nothing is due now and nothing can become due
    /// without a new commit under `options`, based on the state this call
    /// read. Returns `false` at a work limit, after lost races, on fencing or cancellation,
    /// when streaming compaction does not finish, or while a tail waits for
    /// the idle fold age.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.maintain_metadata_while_due",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.maintain_metadata_while_due",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
        options: &MetadataMaintenanceOptions,
    ) -> Result<bool> {
        self.core.record_trace_context(&tracing::Span::current());
        let mut lost_races = 0;
        let mut published_units = 0;
        while lost_races < MAX_LOST_COMPACTION_RACES
            && published_units < MAX_COMPACTION_UNITS_PER_CALL
            && !cancellation.is_cancelled()
        {
            let (step, wal_caught_up) = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(false),
                step = self.metadata_step(namespace_id, options) => step?,
            };
            let won = match step.compaction {
                CompactionStepOutcome::UnitPublished {} => true,
                CompactionStepOutcome::ManifestAdvanced {} => false,
                CompactionStepOutcome::MetadataCompactionRequired {} => {
                    match self
                        .compact_metadata_with_cancellation(namespace_id, cancellation)
                        .await?
                        .compaction
                    {
                        MetadataCompactionOutcome::Published { .. }
                        | MetadataCompactionOutcome::BoundedMergePublished => true,
                        MetadataCompactionOutcome::Abandoned => false,
                        MetadataCompactionOutcome::NotNeeded
                        | MetadataCompactionOutcome::Cancelled
                        | MetadataCompactionOutcome::Fenced => return Ok(false),
                    }
                }
                CompactionStepOutcome::NotNeeded {} => {
                    return Ok(wal_caught_up && !cancellation.is_cancelled())
                }
                CompactionStepOutcome::Fenced {} => return Ok(false),
            };
            if won {
                published_units += 1;
                lost_races = 0;
            } else {
                lost_races += 1;
            }
        }
        Ok(false)
    }

    /// Optionally folds the WAL tail, then runs one compaction step.
    /// `observed` is the anchor the caller decided on. Its head seq is
    /// reported when concurrent updates prevent every fold attempt from
    /// publishing.
    async fn fold_then_compact(
        &self,
        namespace_id: &NamespaceId,
        fold: bool,
        observed: &NamespaceReadAnchor,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<MetadataMaintenanceResponse> {
        let wal_fold = if fold {
            match self.run_wal_fold(namespace_id).await {
                Ok(folded) => match folded.outcome {
                    FoldWalOutcome::Published => WalFoldStepOutcome::Folded {
                        manifest_head_seq: folded.manifest_head_seq,
                    },
                    // In both cases, this fold did not publish a manifest.
                    FoldWalOutcome::AlreadyCurrent | FoldWalOutcome::ManifestAdvanced => {
                        WalFoldStepOutcome::AlreadyPublished {
                            attempted_seq: folded.target_head_seq,
                            current_manifest_no: folded.manifest_no,
                        }
                    }
                },
                Err(Error::Core(error)) if error.code() == ErrorCode::StaleHead => {
                    WalFoldStepOutcome::RetriesExhausted {
                        observed_head_seq: observed.read_state.seq,
                    }
                }
                Err(error) => return Err(error),
            }
        } else {
            WalFoldStepOutcome::NotNeeded
        };
        // A fold attempt can move the manifest, so the step observes it again.
        let observed = (!fold).then_some(observed);
        let compaction = self
            .run_compaction_step(namespace_id, compaction_policy, observed)
            .await?;
        Ok(MetadataMaintenanceResponse {
            namespace_id: namespace_id.clone(),
            wal_fold,
            compaction,
        })
    }

    /// Runs one bounded compaction step for one metadata family.
    ///
    /// A family group whose oldest run no longer fits one unit reports that
    /// the metadata compaction job is required.
    async fn run_compaction_step(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        observed: Option<&NamespaceReadAnchor>,
    ) -> Result<CompactionStepOutcome> {
        Ok(
            match self
                .compact_once(namespace_id, compaction_policy, observed)
                .await?
            {
                CompactionStep::Concluded(outcome) => outcome,
                CompactionStep::Fenced => CompactionStepOutcome::Fenced {},
                CompactionStep::CompactionPlanned(_) => {
                    CompactionStepOutcome::MetadataCompactionRequired {}
                }
            },
        )
    }

    async fn compactor_epoch(&self, namespace_id: &NamespaceId) -> Result<CompactorEpoch> {
        // Hold the claim lock across publication so concurrent groups share one epoch.
        let mut epochs = self.writer.compactor_epochs.lock().await;
        if let Some(epoch) = epochs.get(namespace_id) {
            return Ok(*epoch);
        }
        let epoch = self
            .engine(namespace_id)
            .claim_compactor()
            .await
            .map_err(Error::Core)?;
        epochs.insert(namespace_id.clone(), epoch);
        Ok(epoch)
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

    /// `observed`, when given, was loaded after anything this call published.
    async fn compact_once(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
        observed: Option<&NamespaceReadAnchor>,
    ) -> Result<CompactionStep> {
        let claimed = self
            .writer
            .compactor_epochs
            .lock()
            .await
            .get(namespace_id)
            .copied();
        let due = |anchor: &NamespaceReadAnchor| {
            loonfs_core::cache::metadata_compaction_due(anchor, compaction_policy)
        };
        let step_due = match observed {
            // A claim the manifest no longer carries is a fence the step reports.
            Some(anchor) => {
                claimed.is_some_and(|epoch| epoch != anchor.compactor_epoch()) || due(anchor)
            }
            // A claimed step reads less than loading an anchor to check first.
            None => claimed.is_some() || due(&self.load_live_anchor(namespace_id).await?),
        };
        if !step_due {
            return Ok(CompactionStep::Concluded(
                CompactionStepOutcome::NotNeeded {},
            ));
        }
        let _permit = self.writer.execution_budget.compaction_permit().await;
        let compactor_epoch = self.compactor_epoch(namespace_id).await?;
        let outcome = self
            .engine(namespace_id)
            .metadata_compaction_step(compaction_policy, compactor_epoch)
            .await
            .map_err(Error::Core)?;
        Ok(CompactionStep::Concluded(match outcome {
            loonfs_core::CompactionStepOutcome::NotNeeded { .. } => {
                CompactionStepOutcome::NotNeeded {}
            }
            loonfs_core::CompactionStepOutcome::UnitPublished {
                group,
                merged_delta_rows,
                input_runs,
                decoded_input_rows,
                decoded_input_bytes,
                bottom_anchored_merge_blocked,
                ..
            } => {
                self.invalidate_namespace(namespace_id);
                tracing::info!(
                    families = ?group.families(),
                    merged_delta_rows,
                    input_runs,
                    decoded_input_rows,
                    decoded_input_bytes,
                    bottom_anchored_merge_blocked,
                    "metadata compaction unit published"
                );
                CompactionStepOutcome::UnitPublished {}
            }
            loonfs_core::CompactionStepOutcome::CompactionPlanned { spec, .. } => {
                return Ok(CompactionStep::CompactionPlanned(spec))
            }
            loonfs_core::CompactionStepOutcome::Fenced => {
                self.forget_fenced_compactor_epoch(namespace_id, compactor_epoch)
                    .await;
                return Ok(CompactionStep::Fenced);
            }
            loonfs_core::CompactionStepOutcome::Superseded => {
                tracing::info!(
                    "current manifest changed before compaction published; a later step retries"
                );
                CompactionStepOutcome::ManifestAdvanced {}
            }
        }))
    }

    /// Runs one metadata compaction unit in the caller's task.
    ///
    /// The unit is a bounded merge when the selected window fits one step,
    /// and otherwise one streaming compaction of a family group. Use this
    /// when [`CompactionStepOutcome::MetadataCompactionRequired`] is reported, and
    /// repeat it while it publishes to compact every eligible group. Each
    /// merge waits for a compaction permit from the runtime's execution
    /// budget, the step that plans the unit first and then the streaming
    /// compaction.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.compact_metadata",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.compact_metadata",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn compact_metadata(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataCompactionResponse> {
        self.compact_metadata_with_cancellation(namespace_id, &MaintenanceCancellation::new())
            .await
    }

    /// `compact_metadata` with caller-owned cancellation. Cancelling drops the
    /// step that plans the unit, or ends the streaming compaction's wait for
    /// a permit, and reports the unit cancelled. A running streaming
    /// compaction stops at its next block.
    pub async fn compact_metadata_with_cancellation(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let cancelled = || {
            Ok(MetadataCompactionResponse {
                namespace_id: namespace_id.clone(),
                compaction: MetadataCompactionOutcome::Cancelled,
            })
        };
        let planned = tokio::select! {
            biased;
            () = cancellation.cancelled() => return cancelled(),
            planned = self.compact_once(
                namespace_id,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                None,
            ) => planned?,
        };
        let spec = match planned {
            CompactionStep::Fenced => {
                return Ok(MetadataCompactionResponse {
                    namespace_id: namespace_id.clone(),
                    compaction: MetadataCompactionOutcome::Fenced,
                });
            }
            CompactionStep::CompactionPlanned(spec) => spec,
            CompactionStep::Concluded(CompactionStepOutcome::UnitPublished {}) => {
                return Ok(MetadataCompactionResponse {
                    namespace_id: namespace_id.clone(),
                    compaction: MetadataCompactionOutcome::BoundedMergePublished,
                })
            }
            CompactionStep::Concluded(_) => {
                return Ok(MetadataCompactionResponse {
                    namespace_id: namespace_id.clone(),
                    compaction: MetadataCompactionOutcome::NotNeeded,
                })
            }
        };
        let _permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return cancelled(),
            permit = self.writer.execution_budget.compaction_permit() => permit,
        };
        let outcome = self
            .run_streaming_compaction(namespace_id, &spec, cancellation.metadata_compaction())
            .await;
        outcome.map(|outcome| metadata_compaction_response(namespace_id, outcome))
    }

    /// Runs a planned compaction under this runtime's remembered epoch.
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to record compaction duration.
    pub(crate) async fn run_streaming_compaction(
        &self,
        namespace_id: &NamespaceId,
        spec: &loonfs_core::MetadataCompactionSpec,
        cancellation: &loonfs_core::MetadataCompactionCancellation,
    ) -> Result<loonfs_core::MetadataCompactionJobOutcome> {
        let compactor_epoch = self.compactor_epoch(namespace_id).await?;
        let started = Instant::now();
        let outcome = self
            .engine(namespace_id)
            .run_metadata_compaction(spec, compactor_epoch, cancellation)
            .await
            .map_err(Error::Core);
        if matches!(
            outcome,
            Ok(loonfs_core::MetadataCompactionJobOutcome::Fenced)
        ) {
            self.forget_fenced_compactor_epoch(namespace_id, compactor_epoch)
                .await;
        }
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.core
            .instruments()
            .compaction_finished(&outcome, elapsed_ms);
        match &outcome {
            Ok(loonfs_core::MetadataCompactionJobOutcome::Published { .. }) => {
                // Compaction changed the manifest, so cached views are stale.
                self.invalidate_namespace(namespace_id);
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(
                namespace_id = %namespace_id,
                families = ?spec.families(),
                error = %error.public_message(),
                "streaming metadata compaction failed; a later step plans it again"
            ),
        }
        outcome
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
        name = "loonfs.maintenance.gc",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.gc",
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
        let report = loonfs_core::gc_namespace(
            self.core.store(),
            namespace_id,
            options,
            &self.core.mutation_context(&self.actor)?,
        )
        .await
        .map_err(Error::Core)?;
        // A caller may drop the response, so its counts survive only when
        // this shared pass records them here.
        self.core.instruments().gc_pass(&report);
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.invalidate_namespace(namespace_id);
        Ok(report)
    }

    /// Creates a new user checkpoint for the current namespace head that
    /// lasts until it is deleted.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
    ) -> Result<Checkpoint> {
        self.create_checkpoint_with_options(namespace_id, name, &CreateCheckpointOptions::default())
            .await
    }

    /// Creates a new user checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest for retention and provenance. Every call
    /// creates its own pin under a fresh id; the name is a label, not a key.
    /// When WAL objects follow the current manifest, they are first folded
    /// into a new manifest; this is not a request to compact metadata. The
    /// call waits for a fold permit from the runtime's execution budget, even
    /// when the tail turns out to be folded already. The pin lasts until it
    /// is deleted, either explicitly or by garbage collection after its
    /// expiry plus grace
    /// ([format section 8](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#8-pins)).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.create_checkpoint",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.create_checkpoint",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_checkpoint_with_options(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        options: &CreateCheckpointOptions,
    ) -> Result<Checkpoint> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = {
            let _permit = self.writer.execution_budget.fold_permit().await;
            self.engine(namespace_id)
                .create_checkpoint(name.to_owned(), options.ttl_ms)
                .await
        }
        .map_err(Error::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Lists existing checkpoints in ascending id order, including expired
    /// checkpoints that garbage collection has not yet deleted. The cursor
    /// resumes a live listing and does not create a snapshot.
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
        name = "loonfs.maintenance.list_checkpoints",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.list_checkpoints",
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

    /// Deletes a user-owned pin. A missing id returns `checkpoint_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.delete_checkpoint",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.delete_checkpoint",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Result<DeleteCheckpointResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .delete_checkpoint(checkpoint_id)
            .await
            .map_err(Error::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Folds the visible WAL tail into a new manifest.
    ///
    /// A namespace with no unfolded tail reports
    /// [`FoldWalOutcome::AlreadyCurrent`] and publishes nothing. The fold
    /// waits for a fold permit from the runtime's execution budget. This runs
    /// no compaction; [`Self::maintain_metadata`] folds and then runs one
    /// compaction step.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.fold_wal",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.fold_wal",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn fold_wal(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.run_wal_fold(namespace_id).await
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    ///
    /// Advancing the floor abandons the replay history below it. Nothing
    /// schedules it, so an unattended deployment keeps its whole history.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.advance_retention_floor",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.advance_retention_floor",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.load_live_anchor(namespace_id).await?;
        let result = self
            .engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(Error::from);
        self.finish_namespace_mutation(namespace_id, result)
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
            if result.is_ok() {
                self.publisher.record_fold_outcome(namespace_id).await;
            }
            self.finish_namespace_mutation(namespace_id, result)
                .inspect_err(|error| tracing::debug!(%error))
        }
        .instrument(phase_span!(self.core, "wal_fold", namespace_id))
        .await
    }
}

impl crate::Namespace<crate::Writable> {
    /// Grants `admin` on the root row to `principal_id`, keeping every other
    /// root grant, through a commit no subject check applies to.
    pub async fn recover_administrator(
        &self,
        principal_id: &loonfs_types::PrincipalId,
        actor: &loonfs_types::ActorId,
    ) -> Result<loonfs_types::RecoverAdministratorResponse> {
        use loonfs_types::api::v0::FilesystemChange;
        use loonfs_types::{AccessGrants, AccessRight, AccessRights};

        let (engine, context) = self.core.pinned_metadata_read(&self.namespace_id).await?;
        let (boundary, grants, current) = engine.root_access(&context).await?;
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
        let request = crate::publish::CommitRequest::single(
            loonfs_types::CommitId::generate(),
            actor.clone(),
            Some("administrator recovery".to_owned()),
            crate::publish::FilesystemOperation::UpdateAccess {
                path: loonfs_types::AbsolutePath::root(),
                boundary,
                grants,
                expected_inode_id: Some(loonfs_types::ROOT_INODE_ID),
                expected_access_revision_no: Some(current),
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
            .ok_or_else(|| {
                Error::Core(crate::CoreError::Internal(
                    "administrator recovery published no access change".to_owned(),
                ))
            })?;
        Ok(loonfs_types::RecoverAdministratorResponse {
            namespace_id: commit.namespace_id,
            commit_id: commit.commit_id,
            committed_seq: commit.committed_seq,
            access_revision_no,
        })
    }
}
