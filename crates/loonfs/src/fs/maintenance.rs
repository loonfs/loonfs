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
    MaintenancePlan, MaintenanceStepResponse, MetadataMaintenanceOptions,
    MetadataMaintenanceResponse, NamespaceId, ReleaseCheckpointResponse, ReorganizeStepOutcome,
    SharedObjectStore, WalFlushStepOutcome,
};
use crate::{ChangeSeq, Result, RuntimeError};
use loonfs_api::PageRequest;
use loonfs_core::cache::load_namespace_flush_basis;
use loonfs_core::CheckpointPageCursor;
use tokio::time::Instant;
use tracing::Instrument;

#[cfg(test)]
mod tests;

/// What one explicit [`FsAdmin::compact_metadata`] call did.
///
/// The job's own endings are [`loonfs_core::MetadataCompactionJobOutcome`],
/// unchanged from what background work reports for the same job. The three
/// variants beside it are the ways a call runs no job at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompactionOutcome {
    /// No family group has outgrown a bounded reorganization step and nothing
    /// was published: either the namespace needed no upkeep at all, or the
    /// bounded unit the planner chose lost the root race and a later call
    /// takes it again.
    NoWork,
    /// The planner chose a bounded merge instead, and this call published it,
    /// exactly as an ordinary maintenance step would have. No family group
    /// has outgrown a step, so no job ran.
    BoundedMergePublished,
    /// A job already holds this namespace's compaction slot. One runs at a
    /// time per namespace, so this call ran none.
    AlreadyRunning,
    /// A job ran here, and this is how it ended.
    Ran(loonfs_core::MetadataCompactionJobOutcome),
}

/// What one reorganization unit left for its caller.
enum ReorganizationStep {
    /// The unit is finished, and this is what it did.
    Concluded(ReorganizeStepOutcome),
    /// A family group has outgrown a bounded step. The caller starts the job
    /// as background work, or runs it in its own task.
    CompactionPlanned(loonfs_core::MetadataCompactionSpec),
}

/// Fetches active-checkpoint pages as needed.
#[must_use]
pub struct CheckpointsPager {
    admin: FsAdmin,
    namespace_id: NamespaceId,
    request: PageRequest<CheckpointPageCursor>,
    pending: Option<ListCheckpointsResponse>,
    exhausted: bool,
}

impl CheckpointsPager {
    /// Returns the next checkpoint page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListCheckpointsResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .admin
            .list_checkpoints_page_typed(&self.namespace_id, self.request.clone())
            .await;
        Some(page.and_then(|(mut page, next_cursor)| {
            page.next_cursor = super::core::encode_next_cursor(next_cursor.as_ref())?;
            self.exhausted = next_cursor.is_none();
            self.request.cursor = next_cursor;
            Ok(page)
        }))
    }

    /// Returns at most `max_items` checkpoints.
    ///
    /// Unused checkpoints from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<Checkpoint>> {
        let mut checkpoints = Vec::new();
        while checkpoints.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - checkpoints.len()).min(page.checkpoints.len());
            if take < page.checkpoints.len() {
                let remaining = page.checkpoints.split_off(take);
                checkpoints.extend(page.checkpoints);
                page.checkpoints = remaining;
                self.pending = Some(page);
                break;
            }
            checkpoints.extend(page.checkpoints);
        }
        Ok(checkpoints)
    }
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
        if let Some(publisher) = &self.publisher {
            publisher.invalidate_projection(namespace_id);
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
        Ok(loonfs_core::cache::load_namespace_diagnostics(self.core.store(), namespace_id).await?)
    }

    /// Runs one bounded maintenance step against a namespace.
    ///
    /// The step runs the actions present in `plan`, in this order: metadata
    /// maintenance, retention advancement, then garbage collection. A plan
    /// with no actions is rejected.
    ///
    /// Each selected action has its own report. Concurrent publication is
    /// reported as an outcome rather than an error.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.step",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.step",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        mut plan: MaintenancePlan,
    ) -> Result<MaintenanceStepResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        if plan.is_empty() {
            return Err(RuntimeError::Config(
                "a maintenance step must select at least one action".to_owned(),
            ));
        }
        if let Some(gc) = &mut plan.gc {
            // Maintenance steps always bound GC work. Direct `gc_namespace`
            // calls remain unbounded unless the caller sets a limit.
            gc.max_objects
                .get_or_insert(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
        }
        let collects_only = plan.gc.is_some() && plan.metadata.is_none() && !plan.advance_retention;
        let status_before = match self.get_namespace_diagnostics(namespace_id).await {
            Ok(status) => status,
            // A tombstoned namespace keeps reclaimable derived state — WAL
            // segments, metadata segments, manifests, checkpoint records — until GC
            // ages it out, and a collection-only step is the reclamation
            // path. It proceeds against the tombstone; the summary comes
            // from the two control objects that outlive reclamation,
            // because the manifest and chain a live summary consults may
            // already be reaped. Any other plan still refuses: a tombstone
            // has nothing to flush, reorganize, or retain.
            Err(RuntimeError::Core(error))
                if error.code() == ErrorCode::NamespaceDeleted && collects_only =>
            {
                loonfs_core::cache::load_deleted_namespace_diagnostics(
                    self.core.store(),
                    namespace_id,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let metadata_maintenance = if let Some(options) = plan.metadata {
            Some(
                self.run_metadata(namespace_id, options, &status_before)
                    .await?,
            )
        } else {
            None
        };
        let retention = if plan.advance_retention {
            Some(self.run_retention(namespace_id).await?)
        } else {
            None
        };
        let gc = if let Some(config) = &plan.gc {
            Some(self.gc_namespace(namespace_id, config).await?)
        } else {
            None
        };

        Ok(MaintenanceStepResponse {
            namespace_id: namespace_id.clone(),
            status_before,
            metadata_maintenance,
            retention,
            gc,
        })
    }

    /// Flushes the visible WAL tail after it reaches the threshold, then runs
    /// one bounded reorganization step.
    async fn run_metadata(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
        status_before: &NamespaceDiagnostics,
    ) -> Result<MetadataMaintenanceResponse> {
        let flush = options.flush_is_due(status_before.wal_tail_segments);
        self.flush_then_reorganize(namespace_id, flush, status_before.head_seq)
            .await
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
        let frozen_base = match &self.compactions {
            Some(compactions) => compactions.amortization(namespace_id),
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
            .compactions
            .as_ref()
            .and_then(|compactions| compactions.active_spec(namespace_id));
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
                if let Some(compactions) = &self.compactions {
                    compactions.record_merge(namespace_id, group, bottom_anchored_merge_blocked);
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
        let started = match &self.compactions {
            Some(compactions) => compactions.start(self, namespace_id, spec),
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
    ) -> Result<MetadataCompactionOutcome> {
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
                return Ok(MetadataCompactionOutcome::BoundedMergePublished)
            }
            ReorganizationStep::Concluded(_) => return Ok(MetadataCompactionOutcome::NoWork),
        };
        let Some(compactions) = &self.compactions else {
            // Standalone handles have no shared concurrency limit.
            let cancellation = loonfs_core::MetadataCompactionCancellation::default();
            let outcome = self
                .run_streaming_compaction(namespace_id, &spec, &cancellation)
                .await;
            return Ok(MetadataCompactionOutcome::Ran(outcome?));
        };
        let Some(mut claim) = compactions.claim(namespace_id, &spec) else {
            return Ok(MetadataCompactionOutcome::AlreadyRunning);
        };
        if !claim.admitted().await {
            let outcome = loonfs_core::MetadataCompactionJobOutcome::Cancelled;
            self.core.instruments().compaction_not_admitted();
            return Ok(MetadataCompactionOutcome::Ran(outcome));
        }
        let outcome = self
            .run_streaming_compaction(namespace_id, &spec, claim.cancellation())
            .await;
        claim.finished(matches!(
            outcome,
            Ok(loonfs_core::MetadataCompactionJobOutcome::Published { .. })
        ));
        Ok(MetadataCompactionOutcome::Ran(outcome?))
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
                if let Some(compactions) = &self.compactions {
                    compactions.clear_published_delta_merges(namespace_id, spec.group());
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
    /// current live roots. A step sweeps only when asked — here, or through
    /// [`MaintenancePlan::gc`] — and the one thing that asks on its own is a
    /// writer's collection job, which schedules a pass for each upload
    /// deadline that writer created.
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

    /// Creates a checkpoint pager beginning at `request.cursor`.
    pub fn list_checkpoints_pager(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> CheckpointsPager {
        CheckpointsPager {
            admin: self.clone(),
            namespace_id: namespace_id.clone(),
            request,
            pending: None,
            exhausted: false,
        }
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
    /// One name over the one step path: exactly [`Self::run_maintenance`]
    /// with a retention-only plan. It keeps a name of its own because of what
    /// it costs — advancing the floor abandons the replay history below it,
    /// which is a decision rather than upkeep. Nothing schedules it: no
    /// maintenance job exists for retention, so an unattended deployment keeps
    /// its whole history until a call arrives here.
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
        let step = self
            .run_maintenance(
                namespace_id,
                MaintenancePlan {
                    advance_retention: true,
                    ..MaintenancePlan::default()
                },
            )
            .await?;
        Ok(step
            .retention
            .expect("a plan selecting a retention advance reports it"))
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

    /// The one implementation both the step and
    /// [`Self::advance_retention_floor`] reach. Reached only through a plan
    /// that names it: nothing surrenders replay history without being asked.
    async fn run_retention(&self, namespace_id: &NamespaceId) -> Result<AdvanceRetentionResponse> {
        let result = self
            .engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }
}
