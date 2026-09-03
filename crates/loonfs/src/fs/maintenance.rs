//! [`FsAdmin`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! flushes, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through this handle's public
//! checkpoint calls, and its hosts drive it.

use crate::maintenance_runner::CompactionStart;
use crate::trace::phase_span;
use crate::FsAdmin;
use crate::NamespaceDiagnostics;
use crate::{
    AdvanceRetentionResponse, Checkpoint, CheckpointId, CreateCheckpointOptions,
    CreateSnapshotOptions, ErrorCode, FlushWalOutcome, FlushWalResponse, ListCheckpointsResponse,
    ListSnapshotsResponse, MaintenanceRunRequest, MaintenanceRunResponse,
    MetadataCompactionOutcome, MetadataCompactionResponse, MetadataMaintenanceOptions,
    MetadataMaintenanceResponse, NamespaceId, ReleaseCheckpointResponse, ReleaseSnapshotResponse,
    ReorganizeStepOutcome, SharedObjectStore, SnapshotSummary, WalFlushStepOutcome,
};
use crate::{ChangeSeq, Result, RuntimeError};
use loonfs_api::PageRequest;
use loonfs_core::cache::{load_namespace_flush_basis, NamespaceStorageDiagnostics};
use loonfs_core::CheckpointPageCursor;
use std::num::NonZeroU32;
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
    outcome: loonfs_core::MetadataCompactionJobOutcome,
) -> MetadataCompactionResponse {
    let outcome = match outcome {
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
    MetadataCompactionResponse { outcome }
}

impl FsAdmin {
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

    /// Drops everything this runtime caches for a namespace: the read
    /// caches, and — when this handle runs over a writer's runtime — the
    /// rebuildable half of that namespace's publisher state.
    pub(crate) fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.core.invalidate_namespace_read_cache(namespace_id);
        if let Some(writer) = &self.writer {
            writer.publisher.invalidate_projection(namespace_id);
        }
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
        request: MaintenanceRunRequest,
    ) -> Result<MaintenanceRunResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let kind = match &request {
            MaintenanceRunRequest::Metadata(_) => "metadata",
            MaintenanceRunRequest::MetadataCompaction(_) => "metadata_compaction",
            MaintenanceRunRequest::Gc(_) => "gc",
            MaintenanceRunRequest::Retention(_) => "retention",
        };
        span.record("kind", kind);
        match request {
            MaintenanceRunRequest::Metadata(request) => {
                let options = MetadataMaintenanceOptions::from_request(request)?;
                self.maintain_metadata(namespace_id, options)
                    .await
                    .map(MaintenanceRunResponse::Metadata)
            }
            MaintenanceRunRequest::MetadataCompaction(_) => self
                .compact_metadata(namespace_id)
                .await
                .map(MaintenanceRunResponse::MetadataCompaction),
            MaintenanceRunRequest::Gc(request) => {
                self.load_maintenance_status(namespace_id, true).await?;
                let mut config = crate::options::gc_config_from_request(request);
                config
                    .max_objects
                    .get_or_insert(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
                self.gc_namespace(namespace_id, &config)
                    .await
                    .map(MaintenanceRunResponse::Gc)
            }
            MaintenanceRunRequest::Retention(_) => {
                self.load_maintenance_status(namespace_id, false).await?;
                self.run_retention(namespace_id)
                    .await
                    .map(MaintenanceRunResponse::Retention)
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
            .flush_then_reorganize(namespace_id, flush, status.head_seq)
            .await?;
        tracing::debug!(
            wal_tail_segments_before = status.wal_tail_segments,
            wal_flush = ?response.wal_flush,
            reorganize = ?response.reorganize,
            "metadata maintenance step concluded"
        );
        Ok(response)
    }

    /// Optionally flushes the WAL tail, then runs one reorganization step.
    /// `observed_head_seq` is reported when concurrent updates prevent every
    /// flush attempt from publishing.
    async fn flush_then_reorganize(
        &self,
        namespace_id: &NamespaceId,
        flush: bool,
        observed_head_seq: ChangeSeq,
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
        let reorganize = self.run_reorganization(namespace_id).await?;
        Ok(MetadataMaintenanceResponse {
            wal_flush,
            reorganize,
        })
    }

    /// Runs one bounded reorganization step for one metadata family.
    /// Writer-scheduled maintenance can call this again while work remains.
    ///
    /// A family group whose oldest run no longer fits one unit is rebuilt by
    /// a streaming compaction instead, which this step starts as background
    /// work and does not wait for. While that job runs its group is left
    /// alone and the step reorganizes the other groups; the plan it carries is what
    /// tells the planner which group that is.
    async fn run_reorganization(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ReorganizeStepOutcome> {
        // Writers with background maintenance spread a rebuild across delta
        // merges. Manual-only handles report that compaction is required
        // instead of starting work they cannot finish in the background.
        let frozen_base = match &self.writer {
            Some(writer) => writer.compactions.amortization(namespace_id),
            None => loonfs_core::FrozenBasePolicy::CompactImmediately,
        };
        Ok(
            match self.reorganize_once(namespace_id, frozen_base).await? {
                ReorganizationStep::Concluded(outcome) => outcome,
                ReorganizationStep::CompactionPlanned(spec) => {
                    self.start_streaming_compaction(namespace_id, spec)
                }
            },
        )
    }

    /// The one reorganization unit both paths run, and what it left for its
    /// caller.
    ///
    /// A step starts the planned job as background work; an explicit
    /// [`Self::compact_metadata`] runs it here. Everything before that point
    /// is the same call, so the bookkeeping a published unit leaves behind —
    /// the cache invalidation, the published-merge count — happens once,
    /// whichever path asked.
    ///
    /// `frozen_base` is what the caller wants done about a group whose base
    /// no bounded window can reach. It is the one thing the two paths
    /// genuinely disagree about, so it is a parameter rather than something
    /// read back out of this handle.
    async fn reorganize_once(
        &self,
        namespace_id: &NamespaceId,
        frozen_base: loonfs_core::FrozenBasePolicy,
    ) -> Result<ReorganizationStep> {
        // A handle with no runner has no jobs, which reads to the planner
        // exactly as a namespace nothing is running for. It could not start
        // one either way.
        let active = self
            .writer
            .as_ref()
            .and_then(|writer| writer.compactions.active_spec(namespace_id));
        let report = self
            .engine(namespace_id)
            .reorganize_metadata(loonfs_core::MetadataCompactionView {
                active: active.as_ref(),
                frozen_base,
            })
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
                // Track delta-only merges across steps so the runner knows
                // when to schedule a full compaction.
                if let Some(writer) = &self.writer {
                    writer.compactions.record_merge(
                        namespace_id,
                        group,
                        bottom_anchored_merge_blocked,
                    );
                }
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

    /// Starts the job a step planned, unless something already owns the
    /// namespace's one slot or this handle schedules no background work.
    fn start_streaming_compaction(
        &self,
        namespace_id: &NamespaceId,
        spec: loonfs_core::MetadataCompactionSpec,
    ) -> ReorganizeStepOutcome {
        let families = spec.families();
        let input_runs = spec.input_runs();
        let input_rows = spec.input_rows();
        let started = match &self.writer {
            Some(writer) => writer.compactions.start(self, namespace_id, spec),
            None => CompactionStart::NoRunner,
        };
        match started {
            CompactionStart::Started => {
                tracing::info!(
                    families = ?families,
                    input_runs,
                    input_rows,
                    "a family group outgrew one reorganization step; a streaming metadata \
                     compaction is rebuilding it"
                );
                ReorganizeStepOutcome::CompactionStarted
            }
            CompactionStart::Queued => {
                tracing::info!(
                    families = ?families,
                    input_runs,
                    input_rows,
                    "a family group outgrew one reorganization step; its streaming metadata \
                     compaction is waiting for a process compaction permit"
                );
                ReorganizeStepOutcome::CompactionAtCapacity
            }
            CompactionStart::AlreadyRunning => {
                tracing::info!(
                    families = ?families,
                    "a streaming metadata compaction is already running for this namespace; this \
                     group waits for it to finish"
                );
                ReorganizeStepOutcome::CompactionRunning
            }
            CompactionStart::NoRunner => {
                tracing::warn!(
                    families = ?families,
                    "a family group needs a streaming metadata compaction, and this handle \
                     schedules no background work; run `FsAdmin::compact_metadata` to rebuild it"
                );
                ReorganizeStepOutcome::CompactionRequired
            }
        }
    }

    /// Runs one streaming metadata compaction in the caller's task.
    ///
    /// Use this when automatic maintenance is disabled or
    /// [`ReorganizeStepOutcome::CompactionRequired`] is reported. Dropping the
    /// returned future cancels the compaction. Handles connected to a writer
    /// share that writer's compaction limits and shutdown signal; standalone
    /// handles rely on the caller to limit concurrency.
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
        self.core.record_trace_context(&tracing::Span::current());
        let spec = match self
            .reorganize_once(
                namespace_id,
                loonfs_core::FrozenBasePolicy::CompactImmediately,
            )
            .await?
        {
            ReorganizationStep::CompactionPlanned(spec) => spec,
            ReorganizationStep::Concluded(ReorganizeStepOutcome::UnitPublished) => {
                return Ok(MetadataCompactionResponse {
                    outcome: MetadataCompactionOutcome::BoundedMergePublished,
                })
            }
            ReorganizationStep::Concluded(_) => {
                return Ok(MetadataCompactionResponse {
                    outcome: MetadataCompactionOutcome::NotNeeded,
                })
            }
        };
        let Some(writer) = &self.writer else {
            // Standalone handles have no shared concurrency limit.
            let cancellation = loonfs_core::MetadataCompactionCancellation::default();
            let outcome = self
                .run_streaming_compaction(namespace_id, &spec, &cancellation)
                .await;
            return outcome.map(metadata_compaction_response);
        };
        let Some(mut claim) = writer.compactions.claim(namespace_id, &spec) else {
            return Ok(MetadataCompactionResponse {
                outcome: MetadataCompactionOutcome::AlreadyRunning,
            });
        };
        if !claim.admitted().await {
            let outcome = loonfs_core::MetadataCompactionJobOutcome::Cancelled;
            self.core.instruments().compaction_not_admitted();
            return Ok(metadata_compaction_response(outcome));
        }
        let outcome = self
            .run_streaming_compaction(namespace_id, &spec, claim.cancellation())
            .await;
        claim.finished(matches!(
            outcome,
            Ok(loonfs_core::MetadataCompactionJobOutcome::Published { .. })
        ));
        outcome.map(metadata_compaction_response)
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
                // A full compaction includes the base run, so reset the count
                // of delta-only merges.
                if let Some(writer) = &self.writer {
                    writer
                        .compactions
                        .clear_published_delta_merges(namespace_id, spec.group());
                }
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

    /// Creates a snapshot of the current namespace state.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.snapshot_create",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.snapshot_create",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        options: CreateSnapshotOptions,
    ) -> Result<Checkpoint> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .engine(namespace_id)
            .create_snapshot(options.name, options.expires_at_ms)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Creates a snapshot only when the namespace has quota for it.
    ///
    /// A caller that exceeds the quota releases its tentative snapshot before
    /// returning the quota error.
    pub async fn create_snapshot_with_quota(
        &self,
        namespace_id: &NamespaceId,
        options: CreateSnapshotOptions,
        now_ms: u64,
        max_live: usize,
    ) -> Result<Checkpoint> {
        let checkpoint = self.create_snapshot(namespace_id, options).await?;
        if let Err(error) = self
            .ensure_live_snapshot_limit(namespace_id, now_ms, max_live, 0)
            .await
        {
            self.release_snapshot(namespace_id, &checkpoint.checkpoint_id)
                .await?;
            return Err(error);
        }
        Ok(checkpoint)
    }

    async fn ensure_live_snapshot_limit(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
        max_live: usize,
        additional_live: usize,
    ) -> Result<()> {
        let page_limit = loonfs_api::PaginationPolicy::default().max_limit();
        let mut cursor = None;
        let mut live_with_additional = additional_live;
        let quota_error = || {
            RuntimeError::Core(loonfs_core::Error::SnapshotQuotaExceeded {
                namespace_id: namespace_id.clone(),
                max_live,
            })
        };
        if live_with_additional > max_live {
            return Err(quota_error());
        }
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
                if SnapshotSummary::from_checkpoint(checkpoint)
                    .is_some_and(|snapshot| snapshot.expires_at_ms > now_ms)
                {
                    live_with_additional = live_with_additional.saturating_add(1);
                    if live_with_additional > max_live {
                        return Err(quota_error());
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(());
            };
            cursor = Some(next_cursor);
        }
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
        let admin = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let admin = admin.clone();
            let namespace_id = namespace_id.clone();
            async move {
                let cursor = cursor
                    .as_deref()
                    .map(loonfs_api::decode_cursor)
                    .transpose()
                    .map_err(|error| crate::CoreError::InvalidCursor(error.to_string()))?;
                admin
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

    /// Lists one page of live snapshots.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.list_snapshots",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.list_snapshots",
            method = "list_snapshots_page",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListSnapshotsResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let now_ms = self.actor.mutation_context()?.now_ms;
        let requested = request.limit.as_usize();
        let mut cursor = request.cursor;
        let mut snapshots = Vec::with_capacity(requested);
        loop {
            let remaining = requested - snapshots.len();
            let limit = NonZeroU32::new(u32::try_from(remaining).map_err(|error| {
                RuntimeError::Core(loonfs_core::Error::Internal(format!(
                    "snapshot page limit does not fit u32: {error}"
                )))
            })?)
            .expect("a snapshot page with room remaining has a nonzero limit");
            let page = self
                .engine(namespace_id)
                .list_checkpoints_page(PageRequest {
                    limit: loonfs_api::EffectiveLimit::new(limit),
                    cursor,
                })
                .await
                .map_err(RuntimeError::from)?;
            snapshots.extend(page.items.into_iter().filter_map(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint)
                    .filter(|snapshot| snapshot.expires_at_ms > now_ms)
            }));
            match page.next_cursor {
                Some(next_cursor) if snapshots.len() < requested => cursor = Some(next_cursor),
                next_cursor => {
                    return Ok(ListSnapshotsResponse {
                        namespace_id: namespace_id.clone(),
                        snapshots,
                        next_cursor: super::core::encode_next_cursor(next_cursor.as_ref())?,
                    })
                }
            }
        }
    }

    /// Extends a live snapshot, capped from its durable creation time.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.snapshot_extend",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.snapshot_extend",
            namespace_id = %namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        requested_expires_at_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<SnapshotSummary> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .extend_snapshot(snapshot_id, requested_expires_at_ms, max_lifetime_ms)
            .await
            .map_err(RuntimeError::from)
            .and_then(|checkpoint| {
                SnapshotSummary::from_checkpoint(checkpoint).ok_or_else(|| {
                    RuntimeError::Core(loonfs_core::Error::Internal(
                        "snapshot extension returned a non-snapshot checkpoint".to_owned(),
                    ))
                })
            });
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Releases a snapshot. Repeated releases succeed.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.snapshot_release",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.snapshot_release",
            namespace_id = %namespace_id,
            snapshot_id = %snapshot_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn release_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<ReleaseSnapshotResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .release_snapshot(snapshot_id)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
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
    /// This is equivalent to a metadata maintenance step with a one-segment
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
        self.flush_then_reorganize(namespace_id, basis.has_unflushed_wal_tail, basis.head_seq)
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
