//! [`FsAdmin`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! flushes, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through this handle's public
//! checkpoint calls, and its hosts drive it.

use crate::FsAdmin;
use crate::NamespaceStatusResponse;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CreateCheckpointOptions, CreateCheckpointResponse,
    ErrorCode, FlushWalOutcome, FlushWalResponse, ListCheckpointsResponse, MaintenancePlan,
    MaintenanceStepResponse, MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, SharedObjectStore, WalFlushStepOutcome,
};
use crate::{ChangeSeq, Result, RuntimeError};
use loonfs_core::cache::{load_namespace_fold_basis, load_namespace_head_summary};

impl FsAdmin {
    /// A mutating engine under this handle's actor identity.
    fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_core::NamespaceEngine<SharedObjectStore> {
        self.core.writer_engine(&self.actor, namespace_id)
    }

    /// Drops everything this runtime caches for a namespace: the read
    /// caches, and — when this handle runs over a writer's runtime — the
    /// rebuildable half of that namespace's publisher state.
    fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
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
        level = "info",
        name = "loonfs.maintenance.step",
        err,
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
        let reorganize = match self.run_reorganization(namespace_id).await? {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {
                ReorganizeStepOutcome::NotNeeded
            }
            loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. }
            // A partial fold's steps each publish a manifest and each move
            // the group along, so they read as published units at the
            // coarseness this outcome carries. What they did in detail is in
            // the log line `run_reorganization` writes.
            | loonfs_core::MetadataReorganizeOutcome::PartialFoldAdvanced { .. }
            | loonfs_core::MetadataReorganizeOutcome::PartialFoldCompleted { .. } => {
                ReorganizeStepOutcome::UnitPublished
            }
            loonfs_core::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                ReorganizeStepOutcome::BudgetExhausted
            }
            loonfs_core::MetadataReorganizeOutcome::Superseded => ReorganizeStepOutcome::Superseded,
        };
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
    /// A family group whose oldest run no longer fits one step folds a slice
    /// at a time instead, over as many steps as that takes. Those steps
    /// publish a manifest each and report their progress here.
    async fn run_reorganization(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_core::MetadataReorganizeOutcome> {
        let report = self
            .engine(namespace_id)
            .reorganize_metadata()
            .await
            .map_err(RuntimeError::Core)?;
        match &report.outcome {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {}
            loonfs_core::MetadataReorganizeOutcome::UnitPublished {
                families,
                folded_l0_rows,
                input_runs,
                decoded_input_rows,
                decoded_input_bytes,
                ..
            } => {
                self.invalidate_namespace(namespace_id);
                tracing::info!(
                    families = ?families,
                    folded_l0_rows,
                    input_runs,
                    decoded_input_rows,
                    decoded_input_bytes,
                    "metadata reorganization unit published"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::PartialFoldAdvanced {
                families,
                partitions,
                decoded_input_rows,
                decoded_input_bytes,
                output_rows,
                cursor,
                drops,
                ..
            } => {
                self.invalidate_namespace(namespace_id);
                tracing::info!(
                    families = ?families,
                    partitions,
                    decoded_input_rows,
                    decoded_input_bytes,
                    output_rows,
                    cursor = cursor.as_str(),
                    drops = ?drops,
                    "metadata partial fold advanced"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::PartialFoldCompleted {
                families,
                output_segments,
                output_rows,
                ..
            } => {
                self.invalidate_namespace(namespace_id);
                tracing::info!(
                    families = ?families,
                    output_segments,
                    output_rows,
                    "metadata partial fold completed"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::BudgetExhausted { families, .. } => {
                tracing::warn!(
                    families = ?families,
                    "metadata reorganization input does not fit the per-step budget"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::Superseded => {
                tracing::info!("metadata reorganization unit superseded; will retry");
            }
        }
        Ok(report.outcome)
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
        level = "info",
        name = "loonfs.maintenance.checkpoint_create",
        err,
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

    /// Lists every active checkpoint record on a namespace, oldest first.
    ///
    /// This is how a pin is found again when the creation response is gone:
    /// every call to [`Self::create_checkpoint`] mints its own record under
    /// its own id, so a label identifies nothing, and a record nobody can
    /// name is a garbage-collection root nobody can release. A record whose
    /// expiry has passed but which no collection pass has released yet is
    /// still active and still listed, with its expiry in the answer.
    ///
    /// A read: it releases nothing and reaps nothing.
    pub async fn list_checkpoints(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ListCheckpointsResponse> {
        self.engine(namespace_id)
            .list_checkpoints()
            .await
            .map_err(RuntimeError::from)
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
        level = "info",
        name = "loonfs.maintenance.wal_flush",
        err,
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
