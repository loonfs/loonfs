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
    MaintenanceCancellation, MaintenanceProbe, MetadataCompactionOutcome,
    MetadataCompactionResponse, MetadataMaintenanceOptions, MetadataMaintenanceResponse,
    NamespaceId, PinId, SharedObjectStore, SnapshotSummary, WalFoldStepOutcome,
};
use crate::{ChangeSeq, Error, Result};
use loonfs_api::CompactorEpoch;
use loonfs_api::PageRequest;
use loonfs_core::cache::NamespaceStorageDiagnostics;
use loonfs_core::CheckpointPageCursor;
use tokio::time::Instant;
use tracing::Instrument;

#[cfg(test)]
mod tests;

/// What one compaction unit left for its caller.
enum CompactionStep {
    Fenced,
    /// The unit is finished, and this is what it did.
    Concluded(CompactionStepOutcome),
    /// A family group has outgrown a bounded step. The caller starts the job
    /// as background work, or runs it in its own task.
    CompactionPlanned(loonfs_core::MetadataCompactionSpec),
}

/// A pager over existing checkpoints.
pub type CheckpointsPager = loonfs_api::Pager<ListCheckpointsResponse, Error>;

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
        name = "loonfs.get_namespace_diagnostics",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "get_namespace_diagnostics",
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
        let page_limit = loonfs_api::PaginationPolicy::default().max_limit();
        let mut cursor = None;
        let mut live_checkpoints = 0_u64;
        let mut live_snapshots = 0_u64;
        loop {
            let page = self
                .engine(namespace_id)
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_api::EffectiveLimit::new(page_limit),
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            for checkpoint in page.items {
                if let loonfs_api::CheckpointOwnerSummary::User { .. } = checkpoint.owner {
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

    async fn load_maintenance_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStorageDiagnostics> {
        Ok(loonfs_core::cache::load_namespace_diagnostics(self.core.store(), namespace_id).await?)
    }

    /// Folds the WAL tail at the WAL object threshold, at the inline byte
    /// threshold when the writer knows the count, or once the tail's newest
    /// commit is `idle_fold_after_ms` old on this handle's wall clock. Then
    /// runs one bounded compaction step.
    pub async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        self.maintain_metadata_step(namespace_id, options)
            .await
            .map(|(response, _)| response)
    }

    /// Does what [`Self::maintain_metadata`] does, and also returns when a
    /// tail this step did not fold goes idle on this handle's wall clock.
    /// That time is after the step's clock reading and at most
    /// `idle_fold_after_ms` past it.
    pub(crate) async fn maintain_metadata_step(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
    ) -> Result<(MetadataMaintenanceResponse, Option<u64>)> {
        let status = self.load_maintenance_status(namespace_id).await?;
        let inline_bytes = if status.wal_tail_objects > 0 {
            self.publisher
                .wal_tail_inline_bytes(namespace_id)
                .await
                .unwrap_or(0)
        } else {
            0
        };
        let now_ms = self.core.now_ms()?;
        let idle_fold_due_in_ms =
            options.idle_fold_due_in_ms(status.wal_tail_newest_commit_at_ms, now_ms);
        let fold = options.fold_is_due(status.wal_tail_objects, inline_bytes)
            || idle_fold_due_in_ms == Some(0);
        let idle_fold_at_ms = idle_fold_due_in_ms
            .filter(|_| !fold)
            .map(|due_in_ms| now_ms.saturating_add(due_in_ms));
        let response = self
            .fold_then_compact(
                namespace_id,
                fold,
                status.head_seq,
                options.compaction_policy,
            )
            .await?;
        tracing::debug!(
            wal_tail_objects_before = status.wal_tail_objects,
            wal_fold = ?response.wal_fold,
            compaction = ?response.compaction,
            "metadata maintenance pass concluded"
        );
        Ok((response, idle_fold_at_ms))
    }

    /// Checks the WAL object threshold, the age of the tail's newest commit,
    /// and manifest descriptors without replaying the tail.
    ///
    /// The age is measured on this handle's wall clock. Inline byte thresholds
    /// use publication hints instead. Active leases may prevent an eligible
    /// merge from running until their expiry.
    pub async fn probe_metadata(
        &self,
        namespace_id: &NamespaceId,
        options: &MetadataMaintenanceOptions,
    ) -> Result<MaintenanceProbe> {
        let now_ms = self.core.now_ms()?;
        let cache = self.core.metadata_segment_cache();
        loonfs_core::cache::metadata_maintenance_due(
            self.core.store(),
            Some(cache.as_ref()),
            namespace_id,
            |wal_tail_objects, wal_tail_newest_commit_at_ms| {
                wal_tail_objects >= options.max_wal_tail_objects.get()
                    || options.idle_fold_is_due(wal_tail_newest_commit_at_ms, now_ms)
            },
            options.compaction_policy,
        )
        .await
        .map(|due| {
            if due {
                MaintenanceProbe::Due
            } else {
                MaintenanceProbe::Idle
            }
        })
        .map_err(Error::Core)
    }

    /// Optionally folds the WAL tail, then runs one compaction step.
    /// `observed_head_seq` is reported when concurrent updates prevent every
    /// fold attempt from publishing.
    async fn fold_then_compact(
        &self,
        namespace_id: &NamespaceId,
        fold: bool,
        observed_head_seq: ChangeSeq,
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
                    WalFoldStepOutcome::RetriesExhausted { observed_head_seq }
                }
                Err(error) => return Err(error),
            }
        } else {
            WalFoldStepOutcome::NotNeeded
        };
        let compaction = self
            .run_compaction_step(namespace_id, compaction_policy)
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
    ) -> Result<CompactionStepOutcome> {
        Ok(
            match self.compact_once(namespace_id, compaction_policy).await? {
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
        let mut epochs = self.compactor_epochs.lock().await;
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
        let mut epochs = self.compactor_epochs.lock().await;
        // An older attempt can finish after another request has claimed again.
        if epochs.get(namespace_id) == Some(&fenced_epoch) {
            epochs.remove(namespace_id);
        }
    }

    async fn compact_once(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<CompactionStep> {
        let claimed = self
            .compactor_epochs
            .lock()
            .await
            .contains_key(namespace_id);
        if !claimed
            && !loonfs_core::cache::metadata_maintenance_due(
                self.core.store(),
                Some(self.core.metadata_segment_cache().as_ref()),
                namespace_id,
                |_, _| false,
                compaction_policy,
            )
            .await
            .map_err(Error::Core)?
        {
            return Ok(CompactionStep::Concluded(
                CompactionStepOutcome::NotNeeded {},
            ));
        }
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
    /// repeat it while it publishes to compact every eligible group.
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

    /// `compact_metadata` with caller-owned cancellation.
    pub async fn compact_metadata_with_cancellation(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let spec = match self
            .compact_once(
                namespace_id,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
            )
            .await?
        {
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

    /// Runs one complete garbage-collection pass for one namespace.
    ///
    /// Every call rebuilds the current live roots and keeps no cursor. A pass
    /// runs only when asked here or by a writer's collection job, which
    /// schedules one for each upload deadline it created. A deleted namespace
    /// is collected, which is how its objects are reclaimed; a namespace that
    /// does not exist returns `namespace_not_found`.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.gc_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.gc_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn gc(
        &self,
        namespace_id: &NamespaceId,
        options: &crate::GcOptions,
    ) -> Result<crate::GcResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        // Core collection reports an empty pass for a namespace that does not exist.
        loonfs_core::control::load_namespace_read_state(self.core.store(), namespace_id)
            .await
            .map_err(crate::CoreError::ControlObjectLoad)?;
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

    /// Creates a new user checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest for retention and provenance. Every call
    /// creates its own pin under a fresh id; the name is a label, not a key.
    /// When WAL objects follow the current manifest, they are first folded
    /// into a new manifest; this is not a request to compact metadata. The pin
    /// lasts until it is deleted, either explicitly or by garbage collection
    /// after its expiry plus grace
    /// ([format section 8](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#8-pins)).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.checkpoint_create",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.checkpoint_create",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        options: CreateCheckpointOptions,
    ) -> Result<Checkpoint> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .engine(namespace_id)
            .create_checkpoint(options.name, options.ttl_ms)
            .await
            .map_err(Error::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Creates a checkpoint pager beginning at `request.cursor`.
    pub fn list_checkpoints_pager(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> CheckpointsPager {
        let cursor = request.cursor.as_ref().map(|cursor| {
            loonfs_api::encode_cursor(cursor).expect("typed checkpoint cursor should encode")
        });
        let limit = request.limit;
        let maintenance = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let maintenance = maintenance.clone();
            let namespace_id = namespace_id.clone();
            async move {
                let cursor = cursor
                    .as_deref()
                    .map(loonfs_api::decode_cursor)
                    .transpose()
                    .map_err(|error| crate::CoreError::InvalidCursor(error.to_string()))?;
                maintenance
                    .list_checkpoints_page(&namespace_id, PageRequest { limit, cursor })
                    .await
            }
        })
    }

    /// Lists one page of existing checkpoints in ascending id order, including
    /// expired checkpoints that garbage collection has not yet deleted. The
    /// cursor resumes a live listing and does not create a snapshot.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.list_checkpoints",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.list_checkpoints",
            method = "list_checkpoints_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListCheckpointsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let (mut response, next_cursor) = self
            .list_checkpoints_page_typed(namespace_id, request)
            .await?;
        response.next_cursor = super::core::encode_next_cursor(next_cursor.as_ref())?;
        Ok(response)
    }

    async fn list_checkpoints_page_typed(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<(ListCheckpointsResponse, Option<CheckpointPageCursor>)> {
        let page = self
            .engine(namespace_id)
            .list_checkpoints_page(request)
            .await
            .map_err(Error::from)?;
        let next_cursor = page.next_cursor;
        Ok((
            ListCheckpointsResponse {
                namespace_id: namespace_id.clone(),
                checkpoints: page.items,
                next_cursor: None,
            },
            next_cursor,
        ))
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
    /// [`FoldWalOutcome::AlreadyCurrent`] and publishes nothing. This runs no
    /// compaction; [`Self::maintain_metadata`] folds and then runs one
    /// compaction step.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.wal_fold",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.wal_fold",
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
        self.load_maintenance_status(namespace_id).await?;
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
        principal_id: &loonfs_api::PrincipalId,
        actor_id: loonfs_api::ActorId,
    ) -> Result<loonfs_api::RecoverAdministratorResponse> {
        use loonfs_api::v0::FilesystemChange;
        use loonfs_api::{AccessGrants, AccessRight, AccessRights};

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
            loonfs_api::CommitId::generate(),
            actor_id,
            Some("administrator recovery".to_owned()),
            crate::publish::FilesystemOperation::UpdateAccess {
                path: loonfs_api::AbsolutePath::root(),
                boundary,
                grants,
                expected_inode_id: Some(loonfs_api::ROOT_INODE_ID),
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
        Ok(loonfs_api::RecoverAdministratorResponse {
            namespace_id: commit.namespace_id,
            commit_id: commit.commit_id,
            committed_seq: commit.committed_seq,
            access_revision_no,
        })
    }
}
