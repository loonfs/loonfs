//! [`FsAdmin`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! flushes, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through this handle's public
//! checkpoint calls, and its hosts drive it.

use crate::maintenance_runner::CompactionStart;
use crate::FsAdmin;
use crate::NamespaceStatusResponse;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CoreError, CreateCheckpointOptions,
    CreateCheckpointResponse, ErrorCode, FlushWalOutcome, FlushWalResponse,
    ListCheckpointsResponse, MaintenancePlan, MaintenanceStepResponse, MetadataMaintenanceOptions,
    MetadataMaintenanceResponse, NamespaceId, ReleaseCheckpointResponse, ReorganizeStepOutcome,
    SharedObjectStore, WalFlushStepOutcome,
};
use crate::{ChangeSeq, Result, RuntimeError};
use loonfs_api::{encode_cursor, PageRequest};
use loonfs_core::cache::{load_namespace_fold_basis, load_namespace_head_summary};
use loonfs_core::CheckpointPageCursor;
use tokio::time::Instant;

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

    /// Summarizes a namespace's current head: manifest, latest checkpoint,
    /// WAL tail, and retention floor.
    pub async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        Ok(load_namespace_head_summary(self.core.store(), namespace_id).await?)
    }

    /// Runs one bounded maintenance step against a namespace.
    ///
    /// Selection is presence: the step runs exactly the actions `plan`
    /// carries, in the order metadata upkeep, retention advance, garbage
    /// collection. Nothing surrenders replay history or sweeps objects
    /// unless the plan named it, and a plan naming nothing is rejected
    /// rather than answered with an empty report.
    ///
    /// Every action reports separately, and an absent report means the plan
    /// did not select that action. Losing the head race or being superseded
    /// by another publisher is an outcome, not an error.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.step",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.step",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn maintenance_step_namespace(
        &self,
        namespace_id: &NamespaceId,
        plan: MaintenancePlan,
    ) -> Result<MaintenanceStepResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        if plan.is_empty() {
            return Err(RuntimeError::Config(
                "a maintenance step must select at least one action".to_owned(),
            ));
        }
        let mut plan = plan;
        if let Some(gc) = &mut plan.gc {
            // Every pass a step runs is bounded, however the plan reached
            // here; only a direct `gc_namespace` call sweeps unbounded.
            gc.max_objects
                .get_or_insert(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
        }
        let collects_only = plan.gc.is_some() && plan.metadata.is_none() && !plan.advance_retention;
        let status_before = match self.namespace_status(namespace_id).await {
            Ok(status) => status,
            // A tombstoned namespace keeps reclaimable derived state — WAL
            // segments, tables, manifests, checkpoint records — until GC
            // ages it out, and a collection-only step is the reclamation
            // path. It proceeds against the tombstone; the summary comes
            // from the two control objects that outlive reclamation,
            // because the manifest and chain a live summary consults may
            // already be reaped. Any other plan still refuses: a tombstone
            // has nothing to flush, reorganize, or retain.
            Err(RuntimeError::Core(error))
                if error.code() == ErrorCode::NamespaceDeleted && collects_only =>
            {
                loonfs_core::cache::load_deleted_namespace_head_summary(
                    self.core.store(),
                    namespace_id,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let metadata = if let Some(options) = plan.metadata {
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
            metadata,
            retention,
            gc,
        })
    }

    /// The one metadata-upkeep implementation: fold the visible WAL tail
    /// once it has reached the threshold, then merge one reorganization
    /// unit. The two travel together because the fold is what creates the
    /// delta runs the merge consumes.
    async fn run_metadata(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
        status_before: &NamespaceStatusResponse,
    ) -> Result<MetadataMaintenanceResponse> {
        let fold = status_before.wal_tail_segments >= options.max_wal_tail_segments.get();
        self.fold_then_reorganize(namespace_id, fold, status_before.head_seq)
            .await
    }

    /// Folds the tail when asked, then merges one reorganization unit,
    /// reporting both. `observed_head_seq` is what the caller saw before the
    /// fold, and is only reported when another publisher wins the head race.
    async fn fold_then_reorganize(
        &self,
        namespace_id: &NamespaceId,
        fold: bool,
        observed_head_seq: ChangeSeq,
    ) -> Result<MetadataMaintenanceResponse> {
        let wal_flush = if fold {
            match self.run_wal_flush(namespace_id).await {
                Ok(flush) => match flush.outcome {
                    FlushWalOutcome::Published => WalFlushStepOutcome::Flushed {
                        manifest_head_seq: flush.manifest_head_seq,
                    },
                    FlushWalOutcome::AlreadyCurrent | FlushWalOutcome::Superseded => {
                        WalFlushStepOutcome::Superseded {
                            attempted_seq: flush.target_head_seq,
                            current_manifest_id: flush.manifest_id,
                        }
                    }
                },
                Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
                    WalFlushStepOutcome::RaceLost { observed_head_seq }
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

    /// One bounded reorganization unit per step: folds one family group of
    /// L0 delta rows into the base when enough L0 runs have piled up (see
    /// `loonfs-core`'s `reorganize_metadata`). Explicit steps stay bounded at
    /// one unit per call; the returned outcome lets writer-scheduled
    /// background work keep folding until nothing is left.
    ///
    /// A family group whose oldest run no longer fits one unit is rebuilt by
    /// a streaming compaction instead, which this step starts as background
    /// work and does not wait for. While that job runs its group is left
    /// alone and the step folds the other groups; the plan it carries is what
    /// tells the planner which group that is.
    async fn run_reorganization(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ReorganizeStepOutcome> {
        // A writer's own upkeep amortizes the rebuild across delta merges,
        // because it is the thing that will eventually run the job and
        // rebuilding a whole group for every eight delta runs would reread
        // megabytes to fold in a sliver. A handle with no background work
        // behind it has nowhere to run a job at all, so it says the namespace
        // needs one as soon as the group's base freezes rather than
        // publishing delta merges that never reach the rebuild.
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
                folded_l0_rows,
                input_runs,
                decoded_input_rows,
                decoded_input_bytes,
                bottom_anchored_merge_blocked,
                ..
            } => {
                self.invalidate_namespace(namespace_id);
                // A merge that had to start above the group's base leaves the
                // base frozen and the group's retention stopped. One step
                // cannot see how long that has been true, so the count that
                // decides when to stop merging deltas and start the job lives
                // here.
                if let Some(background) = &self.compactions {
                    background.record_merge(namespace_id, group, bottom_anchored_merge_blocked);
                }
                tracing::info!(
                    families = ?group.families(),
                    folded_l0_rows,
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
                tracing::info!("metadata reorganization unit superseded; will retry");
                ReorganizeStepOutcome::Superseded
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

    /// Runs one planned streaming metadata compaction to its end, in the
    /// caller's own task.
    ///
    /// This is the whole of metadata maintenance for a deployment that
    /// schedules none: bounded steps run through
    /// [`Self::maintenance_step_namespace`], and the one piece of upkeep that
    /// does not fit a step runs here. It plans exactly as a step does, so a
    /// step reporting [`ReorganizeStepOutcome::CompactionRequired`] is what
    /// says a call here has work to do.
    ///
    /// The job is the same one background work runs: the same executor, the
    /// same finalizer, and no durable record that it is running. Dropping the
    /// returned future stops it, at the cost of the work it had done. A
    /// handle attached to a writer's background work shares that writer's
    /// namespace slots and compaction permits, so this waits its turn beside
    /// the writer's own jobs and is cancelled by that writer's shutdown. A
    /// standalone handle has neither, and the caller's own call rate is the
    /// only limit on how many of these run at once.
    ///
    /// Planning here does not amortize. A caller who asks for this asked for
    /// the long rebuild, so a family group whose base no bounded window can
    /// reach is compacted on this call rather than after two more delta
    /// merges have published over it — which, under sustained writes, is a
    /// wait with no end.
    pub async fn compact_metadata(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataCompactionOutcome> {
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
            // No runner behind this handle, so there is no slot to claim and
            // no permit to wait for.
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
    // Compaction duration enters monotonic time at this metrics boundary, so
    // its value cannot affect the manifest the deterministic job publishes.
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
                manifest_id,
                rows_read,
                rows_written,
                output_segments,
                ..
            }) => {
                // The manifest moved, so every cached view of this namespace
                // is one manifest behind.
                self.invalidate_namespace(namespace_id);
                // The group's base is no longer frozen, so the delta merges it
                // published over that base are spent. It starts counting again
                // from nothing.
                if let Some(background) = &self.compactions {
                    background.clear_published_delta_merges(namespace_id, spec.group());
                }
                tracing::info!(
                    namespace_id = %namespace_id,
                    families = ?spec.families(),
                    rows_read,
                    rows_written,
                    output_segments,
                    manifest_id = manifest_id.0,
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
                error = %error,
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
    pub async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &crate::GcConfig,
    ) -> Result<crate::GcResponse> {
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
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        options: CreateCheckpointOptions,
    ) -> Result<CreateCheckpointResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .engine(namespace_id)
            .create_checkpoint(options.name, options.ttl_ms)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Lists every active checkpoint by following bounded pages.
    pub async fn list_checkpoints_all(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ListCheckpointsResponse> {
        let limit = super::core::default_page_limit();
        let (mut response, mut next_cursor) = self
            .list_checkpoints_page_typed(
                namespace_id,
                PageRequest {
                    limit,
                    cursor: None,
                },
            )
            .await?;
        while let Some(cursor) = next_cursor {
            let (page, page_next_cursor) = self
                .list_checkpoints_page_typed(
                    namespace_id,
                    PageRequest {
                        limit,
                        cursor: Some(cursor),
                    },
                )
                .await?;
            response.checkpoints.extend(page.checkpoints);
            next_cursor = page_next_cursor;
        }
        Ok(response)
    }

    /// Lists one page of active checkpoints in ascending id order. The cursor
    /// resumes a live listing and does not create a snapshot.
    pub async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListCheckpointsResponse> {
        let (mut response, next_cursor) = self
            .list_checkpoints_page_typed(namespace_id, request)
            .await?;
        response.next_cursor = next_cursor
            .as_ref()
            .map(encode_cursor)
            .transpose()
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
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
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let result = self
            .engine(namespace_id)
            .release_checkpoint(checkpoint_id)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Folds any visible WAL tail, then merges one reorganization unit.
    ///
    /// The same upkeep [`Self::maintenance_step_namespace`] runs for a
    /// metadata plan, at a threshold of one segment. The reorganization unit
    /// rides along and is reported beside the fold — upkeep is one action,
    /// and folding a tail is what creates the delta runs a merge consumes. A
    /// namespace with an empty tail has nothing to fold and reports
    /// [`WalFlushStepOutcome::NotNeeded`]: this is the flush an operator
    /// asks for, not a way to publish a manifest for a namespace that has
    /// never been written to.
    ///
    /// It asks whether there is a tail rather than how long one is, so it
    /// does not read the namespace status. That matters for the one
    /// namespace shape status refuses to answer for — a head that does not
    /// describe its own WAL tail — because folding the tail is the repair.
    pub async fn flush_wal(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataMaintenanceResponse> {
        let basis = load_namespace_fold_basis(self.core.store(), namespace_id).await?;
        self.fold_then_reorganize(namespace_id, basis.has_unflushed_wal_tail, basis.head_seq)
            .await
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    ///
    /// One name over the one step path: exactly
    /// [`Self::maintenance_step_namespace`] with a retention-only plan. It
    /// keeps a name of its own because of what it costs — advancing the
    /// floor abandons the replay history below it, which is a decision
    /// rather than upkeep. Nothing schedules it: no maintenance job exists
    /// for retention, so an unattended deployment keeps its whole history
    /// until a call arrives here.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let step = self
            .maintenance_step_namespace(
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

    /// The one implementation both the step and [`Self::flush_wal`] reach:
    /// fold the tail, advance the root, invalidate what the fold
    /// invalidated.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.maintenance.wal_flush",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "maintenance.wal_flush",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    async fn run_wal_flush(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .engine(namespace_id)
            .flush_wal()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
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
