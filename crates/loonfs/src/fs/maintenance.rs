//! [`FsMaintenance`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! flushes, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through this handle's public
//! checkpoint calls, and its hosts drive it.

use crate::trace::phase_span;
use crate::FsMaintenance;
use crate::NamespaceDiagnostics;
use crate::{
    AdvanceRetentionResponse, Checkpoint, CheckpointId, CreateCheckpointOptions, ErrorCode,
    FlushWalOutcome, FlushWalResponse, ListCheckpointsResponse, MaintenanceCancellation,
    MaintenanceProbe, MetadataCompactionOutcome, MetadataCompactionResponse,
    MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RunMaintenanceRequest,
    RunMaintenanceResponse, SharedObjectStore, WalFlushStepOutcome,
};
use crate::{ChangeSeq, Result, RuntimeError};
use loonfs_api::PageRequest;
use loonfs_core::cache::{load_namespace_flush_basis, NamespaceStorageDiagnostics};
use loonfs_core::CheckpointPageCursor;
use tokio::time::Instant;
use tracing::Instrument;

#[cfg(test)]
mod tests;

/// What one reorganization unit left for its caller.
enum ReorganizationStep {
    /// The unit is finished, and this is what it did.
    Concluded(ReorganizeStepOutcome),
    /// A family group has outgrown a bounded step. The caller starts the job
    /// as background work, or runs it in its own task.
    CompactionPlanned(loonfs_core::MetadataCompactionSpec),
}

/// A pager over active checkpoints.
pub type CheckpointsPager = loonfs_api::Pager<ListCheckpointsResponse, RuntimeError>;

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
        loonfs_core::MetadataCompactionJobOutcome::Superseded => {
            MetadataCompactionOutcome::Superseded
        }
    };
    MetadataCompactionResponse {
        namespace_id: namespace_id.clone(),
        compaction,
    }
}

impl FsMaintenance {
    /// A mutating engine under this handle's actor identity.
    fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceWriterEngine<SharedObjectStore> {
        let engine = self.core.writer_engine(&self.actor, namespace_id);
        #[cfg(test)]
        let engine = match self.reorganization_row_budget {
            Some(rows) => engine.starve_reorganization_row_budget(rows),
            None => engine,
        };
        engine
    }

    /// Drops this runtime's read caches; publisher projections reload because
    /// their keys include the head etag and basis identity.
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
    pub async fn get_namespace_diagnostics(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceDiagnostics> {
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
        let now_ms = self.actor.mutation_context()?.now_ms;
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
                .map_err(RuntimeError::from)?;
            for checkpoint in page.items {
                match checkpoint.owner {
                    loonfs_api::CheckpointOwnerSummary::User { .. } => {
                        live_checkpoints = live_checkpoints.saturating_add(1);
                    }
                    loonfs_api::CheckpointOwnerSummary::Snapshot { .. }
                        if checkpoint
                            .expires_at_ms
                            .is_some_and(|expiry| expiry > now_ms) =>
                    {
                        live_snapshots = live_snapshots.saturating_add(1);
                    }
                    loonfs_api::CheckpointOwnerSummary::Fork { .. }
                    | loonfs_api::CheckpointOwnerSummary::Snapshot { .. } => {}
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
            head_seq: diagnostics.head_seq,
            retention_floor_seq: diagnostics.retention_floor_seq,
            current_manifest_no: diagnostics.current_manifest_no,
            wal_tail_segments: diagnostics.wal_tail_segments,
            live_snapshots,
            live_checkpoints,
        }
    }

    async fn load_maintenance_status(
        &self,
        namespace_id: &NamespaceId,
        collects_only: bool,
    ) -> Result<NamespaceDiagnostics> {
        let diagnostics =
            match loonfs_core::cache::load_namespace_diagnostics(self.core.store(), namespace_id)
                .await
            {
                Ok(diagnostics) => diagnostics,
                Err(error) if error.code() == ErrorCode::NamespaceDeleted && collects_only => {
                    // These control objects survive deleted-namespace reclamation.
                    loonfs_core::cache::load_deleted_namespace_diagnostics(
                        self.core.store(),
                        namespace_id,
                    )
                    .await?
                }
                Err(error) => return Err(error.into()),
            };
        Ok(Self::namespace_diagnostics(diagnostics, 0, 0))
    }

    /// Runs one maintenance job for one namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.run",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.run",
            namespace_id = %namespace_id,
            kind = tracing::field::Empty,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: RunMaintenanceRequest,
    ) -> Result<RunMaintenanceResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let kind = match &request {
            RunMaintenanceRequest::Metadata(_) => "metadata",
            RunMaintenanceRequest::MetadataCompaction(_) => "metadata_compaction",
            RunMaintenanceRequest::Gc(_) => "gc",
            RunMaintenanceRequest::Retention(_) => "retention",
        };
        span.record("kind", kind);
        match request {
            RunMaintenanceRequest::Metadata(request) => {
                let options = MetadataMaintenanceOptions::from_request(request)?;
                self.maintain_metadata(namespace_id, options)
                    .await
                    .map(RunMaintenanceResponse::Metadata)
            }
            RunMaintenanceRequest::MetadataCompaction(_) => self
                .compact_metadata(namespace_id)
                .await
                .map(RunMaintenanceResponse::MetadataCompaction),
            RunMaintenanceRequest::Gc(request) => {
                self.load_maintenance_status(namespace_id, true).await?;
                let mut config = crate::options::gc_config_from_request(request);
                config
                    .max_objects
                    .get_or_insert(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
                self.gc_namespace(namespace_id, &config)
                    .await
                    .map(RunMaintenanceResponse::Gc)
            }
            RunMaintenanceRequest::Retention(_) => {
                self.load_maintenance_status(namespace_id, false).await?;
                self.run_retention(namespace_id)
                    .await
                    .map(RunMaintenanceResponse::Retention)
            }
        }
    }

    /// Flushes the WAL tail once it reaches `options.max_wal_tail_segments`, then runs
    /// one bounded reorganization step.
    pub async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        let status = self.load_maintenance_status(namespace_id, false).await?;
        let flush = options.flush_is_due(status.wal_tail_segments);
        let response = self
            .flush_then_reorganize(
                namespace_id,
                flush,
                status.head_seq,
                options.compaction_policy,
            )
            .await?;
        tracing::debug!(
            wal_tail_segments_before = status.wal_tail_segments,
            wal_flush = ?response.wal_flush,
            reorganize = ?response.reorganize,
            "metadata maintenance pass concluded"
        );
        Ok(response)
    }

    /// Whether the WAL tail or metadata manifest has bounded work waiting.
    pub async fn metadata_probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        let threshold = MetadataMaintenanceOptions::default()
            .max_wal_tail_segments
            .get();
        let cache = self.core.metadata_segment_cache();
        loonfs_core::cache::metadata_maintenance_due(
            self.core.store(),
            Some(cache.as_ref()),
            namespace_id,
            threshold,
        )
        .await
        .map(|due| {
            if due {
                MaintenanceProbe::Due
            } else {
                MaintenanceProbe::Idle
            }
        })
        .map_err(RuntimeError::Core)
    }

    /// Optionally flushes the WAL tail, then runs one reorganization step.
    /// `observed_head_seq` is reported when concurrent updates prevent every
    /// flush attempt from publishing.
    async fn flush_then_reorganize(
        &self,
        namespace_id: &NamespaceId,
        flush: bool,
        observed_head_seq: ChangeSeq,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<MetadataMaintenanceResponse> {
        let wal_flush = if flush {
            match self.run_wal_flush(namespace_id).await {
                Ok(flush) => match flush.outcome {
                    FlushWalOutcome::Published => WalFlushStepOutcome::Flushed {
                        manifest_head_seq: flush.manifest_head_seq,
                    },
                    // In both cases, this flush did not update the root.
                    FlushWalOutcome::AlreadyCurrent | FlushWalOutcome::RootAdvanced => {
                        WalFlushStepOutcome::AlreadyPublished {
                            attempted_seq: flush.target_head_seq,
                            current_manifest_no: flush.manifest_no,
                        }
                    }
                },
                Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
                    WalFlushStepOutcome::RetriesExhausted { observed_head_seq }
                }
                Err(error) => return Err(error),
            }
        } else {
            WalFlushStepOutcome::NotNeeded
        };
        let reorganize = self
            .run_reorganization(namespace_id, compaction_policy)
            .await?;
        Ok(MetadataMaintenanceResponse {
            namespace_id: namespace_id.clone(),
            wal_flush,
            reorganize,
        })
    }

    /// Runs one bounded reorganization step for one metadata family.
    ///
    /// A family group whose oldest run no longer fits one unit reports that
    /// the metadata compaction job is required.
    async fn run_reorganization(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<ReorganizeStepOutcome> {
        Ok(
            match self
                .reorganize_once(namespace_id, compaction_policy)
                .await?
            {
                ReorganizationStep::Concluded(outcome) => outcome,
                ReorganizationStep::CompactionPlanned(_) => {
                    ReorganizeStepOutcome::CompactionRequired
                }
            },
        )
    }

    /// The one reorganization unit both paths run, and what it left for its
    /// caller.
    ///
    /// A bounded step reports a planned job; [`Self::compact_metadata`] runs it.
    ///
    /// Explicit compaction bypasses automatic size thresholds.
    async fn reorganize_once(
        &self,
        namespace_id: &NamespaceId,
        compaction_policy: loonfs_core::MetadataCompactionPolicy,
    ) -> Result<ReorganizationStep> {
        let report = self
            .engine(namespace_id)
            .reorganize_metadata(compaction_policy)
            .await
            .map_err(RuntimeError::Core)?;
        Ok(ReorganizationStep::Concluded(match report.outcome {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {
                ReorganizeStepOutcome::NotNeeded
            }
            loonfs_core::MetadataReorganizeOutcome::UnitPublished {
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
                    "metadata reorganization unit published"
                );
                ReorganizeStepOutcome::UnitPublished
            }
            loonfs_core::MetadataReorganizeOutcome::CompactionPlanned { spec, .. } => {
                return Ok(ReorganizationStep::CompactionPlanned(spec))
            }
            loonfs_core::MetadataReorganizeOutcome::Superseded => {
                tracing::info!(
                    "metadata root changed before reorganization published; a later step retries"
                );
                ReorganizeStepOutcome::RootAdvanced
            }
        }))
    }

    /// Runs one streaming metadata compaction in the caller's task.
    ///
    /// Use this when [`ReorganizeStepOutcome::CompactionRequired`] is reported.
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
        self.compact_metadata_with(namespace_id, &MaintenanceCancellation::new())
            .await
    }

    /// `compact_metadata` with caller-owned cancellation.
    pub async fn compact_metadata_with(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let spec = match self
            .reorganize_once(
                namespace_id,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
            )
            .await?
        {
            ReorganizationStep::CompactionPlanned(spec) => spec,
            ReorganizationStep::Concluded(ReorganizeStepOutcome::UnitPublished) => {
                return Ok(MetadataCompactionResponse {
                    namespace_id: namespace_id.clone(),
                    compaction: MetadataCompactionOutcome::BoundedMergePublished,
                })
            }
            ReorganizationStep::Concluded(_) => {
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

    /// Runs one streaming compaction to its end and says what that end was.
    ///
    /// Both paths reach this: the background task the maintenance runner
    /// spawns, which awaits nothing and reads the ending from the log, and
    /// [`Self::compact_metadata`], which hands the ending to its caller.
    /// Every ending short of a publication leaves the manifest where it was
    /// and the segments the job wrote unreferenced, so there is nothing to
    /// undo here — the caller gives its claim back and a later step plans the
    /// group again.
    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to record compaction duration.
    pub(crate) async fn run_streaming_compaction(
        &self,
        namespace_id: &NamespaceId,
        spec: &loonfs_core::MetadataCompactionSpec,
        cancellation: &loonfs_core::MetadataCompactionCancellation,
    ) -> Result<loonfs_core::MetadataCompactionJobOutcome> {
        let started = Instant::now();
        let outcome = self
            .engine(namespace_id)
            .run_metadata_compaction(spec, cancellation)
            .await
            .map_err(RuntimeError::Core);
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.core
            .instruments()
            .compaction_finished(&outcome, elapsed_ms);
        match &outcome {
            Ok(loonfs_core::MetadataCompactionJobOutcome::Published {
                manifest_no,
                rows_read,
                rows_written,
                output_segments,
                ..
            }) => {
                // Compaction changed the manifest, so cached views are stale.
                self.invalidate_namespace(namespace_id);
                tracing::info!(
                    namespace_id = %namespace_id,
                    families = ?spec.families(),
                    rows_read,
                    rows_written,
                    output_segments,
                    manifest_no = manifest_no.0,
                    "streaming metadata compaction rebuilt a family group"
                );
            }
            Ok(outcome) => tracing::info!(
                namespace_id = %namespace_id,
                families = ?spec.families(),
                outcome = ?outcome,
                "streaming metadata compaction ended without publishing; a later step plans it \
                 again"
            ),
            Err(error) => tracing::warn!(
                namespace_id = %namespace_id,
                families = ?spec.families(),
                error = %error.public_message(),
                "streaming metadata compaction failed; a later step plans it again"
            ),
        }
        outcome
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Bounded calls return an enumeration cursor; every resume rebuilds the
    /// current live roots. A pass runs only when asked here or by a writer's
    /// collection job, which schedules one for each upload deadline it created.
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
    pub async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &crate::GcConfig,
    ) -> Result<crate::GcResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let report = loonfs_core::gc_namespace(
            self.core.store(),
            namespace_id,
            config,
            &self.actor.mutation_context()?,
        )
        .await
        .map_err(RuntimeError::Core)?;
        // A caller may drop the response, so its counts survive only when
        // this shared pass records them here.
        self.core.instruments().gc_pass(&report);
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.invalidate_namespace(namespace_id);
        Ok(report)
    }

    /// Creates a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance.
    /// Every call is its own pin under its own id, held until released — the
    /// name is a label, not a key. If the current head has no manifest yet,
    /// one is published first for the current durable namespace state; this
    /// is not a request to compact metadata.
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
            .map_err(RuntimeError::from);
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

    /// Lists one page of active checkpoints in ascending id order. The cursor
    /// resumes a live listing and does not create a snapshot.
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
            .map_err(RuntimeError::from)?;
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

    /// Releases a user-owned checkpoint by id. Idempotent.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.release_checkpoint",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.release_checkpoint",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .release_checkpoint(checkpoint_id)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Flushes any visible WAL tail, then runs one reorganization step.
    ///
    /// This is equivalent to a metadata maintenance pass with a one-segment
    /// flush threshold. It reports both the flush and reorganization outcomes.
    /// An empty WAL tail reports [`WalFlushStepOutcome::NotNeeded`].
    ///
    /// This checks only whether a WAL tail exists. It does not require the
    /// head to contain enough hints to count every segment.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.wal_flush",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.wal_flush",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn flush_wal(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataMaintenanceResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let basis = load_namespace_flush_basis(self.core.store(), namespace_id).await?;
        self.flush_then_reorganize(
            namespace_id,
            basis.has_unflushed_wal_tail,
            basis.head_seq,
            loonfs_core::MetadataCompactionPolicy::CompactImmediately,
        )
        .await
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
        self.load_maintenance_status(namespace_id, false).await?;
        self.run_retention(namespace_id).await
    }

    /// Shared implementation for metadata maintenance and [`Self::flush_wal`].
    async fn run_wal_flush(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        async {
            let result = self
                .engine(namespace_id)
                .flush_wal()
                .await
                .map_err(RuntimeError::from);
            self.finish_namespace_mutation(namespace_id, result)
                .inspect_err(|error| tracing::debug!(%error))
        }
        .instrument(phase_span!(self.core, "wal_flush", namespace_id))
        .await
    }

    /// Shared implementation for retention maintenance operations.
    async fn run_retention(&self, namespace_id: &NamespaceId) -> Result<AdvanceRetentionResponse> {
        let result = self
            .engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }
}
