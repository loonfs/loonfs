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
    ErrorCode, FlushWalOutcome, FlushWalResponse, ListCheckpointsResponse, MaintenanceStepKind,
    MaintenanceStepOptions, MaintenanceStepResponse, NamespaceId, ReleaseCheckpointResponse,
    ReorganizeStepOutcome, SharedObjectStore, WalFlushStepOutcome,
};
use crate::{Result, RuntimeError};
use loonfs_core::cache::load_namespace_head_summary;

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
        let summary = load_namespace_head_summary(self.core.store(), namespace_id).await?;
        Ok(NamespaceStatusResponse {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            current_manifest_id: summary.current_manifest_id,
            wal_tail_segments: summary.wal_tail_segments,
            retention_floor_seq: summary.retention_floor_seq,
        })
    }

    /// Runs one bounded maintenance step against a namespace.
    ///
    /// A full step does four things in order: flush the visible WAL tail
    /// once it reaches `options.max_wal_tail_segments`, merge one bounded
    /// metadata reorganization unit, and — each strictly opt-in — advance
    /// the retention floor (`options.retention`) and run one bounded
    /// garbage-collection pass (`options.gc`). Nothing surrenders replay
    /// history or sweeps objects unless the caller asked for it.
    /// `options.only` restricts the step to a single sub-step.
    ///
    /// Every part reports separately. Losing the head race or being
    /// superseded by another publisher is an outcome, not an error.
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
        options: MaintenanceStepOptions,
    ) -> Result<MaintenanceStepResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let mut options = options;
        if let Some(gc) = &mut options.gc {
            if gc.max_objects.is_none() {
                gc.max_objects = Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
            }
        }
        if options.max_wal_tail_segments == 0 {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        }
        // A threshold above the write-rejection cap would make the step
        // answer `not needed` while every publish is being rejected — the
        // one tool that relieves backpressure refusing to act.
        let reject_writes_at_segments =
            loonfs_core::publish::WalTailPolicy::DEFAULT.reject_writes_at_segments;
        if options.max_wal_tail_segments > reject_writes_at_segments {
            return Err(RuntimeError::Config(format!(
                "max_wal_tail_segments may not exceed the write-rejection threshold \
                 ({reject_writes_at_segments})"
            )));
        }

        let runs = |kind: MaintenanceStepKind| options.only.is_none_or(|only| only == kind);
        let status_before = match self.namespace_status(namespace_id).await {
            Ok(status) => status,
            // A tombstoned namespace keeps reclaimable derived state — WAL
            // segments, tables, manifests, checkpoint records — until GC
            // ages it out, and a GC-only step is the reclamation path. It
            // proceeds against the tombstone; the summary comes from the
            // two control objects that outlive reclamation, because the
            // manifest and chain a live summary consults may already be
            // reaped. Full steps still refuse: a tombstone has nothing to
            // flush or reorganize.
            Err(RuntimeError::Core(error))
                if error.code() == ErrorCode::NamespaceDeleted
                    && options.only == Some(MaintenanceStepKind::Gc) =>
            {
                let summary = loonfs_core::cache::load_deleted_namespace_head_summary(
                    self.core.store(),
                    namespace_id,
                )
                .await?;
                NamespaceStatusResponse {
                    namespace_id: summary.namespace_id,
                    head_seq: summary.head_seq,
                    current_manifest_id: summary.current_manifest_id,
                    wal_tail_segments: summary.wal_tail_segments,
                    retention_floor_seq: summary.retention_floor_seq,
                }
            }
            Err(error) => return Err(error),
        };
        let observed_head_seq = status_before.head_seq;

        let wal_flush = if runs(MaintenanceStepKind::WalFlush)
            && status_before.wal_tail_segments >= options.max_wal_tail_segments
        {
            match self.run_step_wal_flush(namespace_id).await {
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

        let reorganize = if runs(MaintenanceStepKind::Reorganize) {
            match self.run_step_reorganization(namespace_id).await? {
                loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {
                    ReorganizeStepOutcome::NotNeeded
                }
                loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. } => {
                    ReorganizeStepOutcome::UnitPublished
                }
                loonfs_core::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                    ReorganizeStepOutcome::BudgetExhausted
                }
                loonfs_core::MetadataReorganizeOutcome::Superseded => {
                    ReorganizeStepOutcome::Superseded
                }
            }
        } else {
            ReorganizeStepOutcome::NotNeeded
        };

        // Retention advance is idempotent: with nothing new to cover it
        // reports the floor already in place. It never runs implicitly —
        // an unattended deployment retains its full replay history until an
        // operator opts in here or restricts a step to this sub-step.
        let retention_runs = match options.only {
            Some(only) => only == MaintenanceStepKind::Retention,
            None => options.retention,
        };
        let retention_floor_seq = if retention_runs {
            self.run_step_retention(namespace_id)
                .await?
                .retention_floor_seq
        } else {
            status_before.retention_floor_seq
        };

        let gc = if runs(MaintenanceStepKind::Gc) {
            self.run_step_gc(namespace_id, options.gc.as_ref()).await?
        } else {
            None
        };

        Ok(MaintenanceStepResponse {
            namespace_id: namespace_id.clone(),
            status_before,
            wal_flush,
            reorganize,
            retention_floor_seq,
            gc,
        })
    }

    /// One bounded reorganization unit per step: folds one family group of
    /// L0 delta rows into the base when enough L0 runs have piled up (see
    /// `loonfs-core`'s `reorganize_metadata`). Explicit steps stay bounded at
    /// one unit per call; the returned outcome lets writer-scheduled
    /// background work keep folding until nothing is left.
    async fn run_step_reorganization(
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

    async fn run_step_gc(
        &self,
        namespace_id: &NamespaceId,
        config: Option<&crate::GcConfig>,
    ) -> Result<Option<crate::GcResponse>> {
        let Some(config) = config else {
            return Ok(None);
        };
        Ok(Some(self.gc_namespace(namespace_id, config).await?))
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Bounded calls return an enumeration cursor; every resume rebuilds the
    /// current live roots. A step sweeps only when asked — here, or through
    /// [`MaintenanceStepOptions::gc`] — and the one thing that asks on its
    /// own is a writer's collection job, which schedules a pass for each
    /// upload deadline that writer created.
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

    /// Flushes the visible WAL tail into metadata tables and advances the
    /// metadata root, creating no checkpoint record.
    ///
    /// One name over the one step path: exactly
    /// [`Self::maintenance_step_namespace`] restricted to
    /// [`MaintenanceStepKind::WalFlush`], with the threshold at a single
    /// segment so any visible tail is folded. A namespace with an empty
    /// tail has nothing to fold and reports
    /// [`WalFlushStepOutcome::NotNeeded`]: this is the flush an operator
    /// asks for, not a way to publish a manifest for a namespace that has
    /// never been written to.
    pub async fn flush_wal(&self, namespace_id: &NamespaceId) -> Result<WalFlushStepOutcome> {
        let step = self
            .maintenance_step_namespace(
                namespace_id,
                MaintenanceStepOptions {
                    max_wal_tail_segments: 1,
                    only: Some(MaintenanceStepKind::WalFlush),
                    ..MaintenanceStepOptions::default()
                },
            )
            .await?;
        Ok(step.wal_flush)
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    ///
    /// One name over the one step path: exactly
    /// [`Self::maintenance_step_namespace`] restricted to
    /// [`MaintenanceStepKind::Retention`]. It keeps a name of its own
    /// because of what it costs — advancing the floor abandons the replay
    /// history below it, which is a decision rather than upkeep. Nothing
    /// schedules it: no maintenance job exists for retention, so an
    /// unattended deployment keeps its whole history until a call arrives
    /// here.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let step = self
            .maintenance_step_namespace(
                namespace_id,
                MaintenanceStepOptions {
                    only: Some(MaintenanceStepKind::Retention),
                    ..MaintenanceStepOptions::default()
                },
            )
            .await?;
        Ok(AdvanceRetentionResponse {
            namespace_id: step.namespace_id,
            retention_floor_seq: step.retention_floor_seq,
        })
    }

    /// The one implementation both the full step and [`Self::flush_wal`]
    /// reach: fold the tail, advance the root, invalidate what the fold
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
    async fn run_step_wal_flush(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .engine(namespace_id)
            .flush_wal()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// The one implementation both the full step and
    /// [`Self::advance_retention_floor`] reach. Reached only through
    /// [`MaintenanceStepKind::Retention`] or `options.retention`: nothing
    /// surrenders replay history without being asked.
    async fn run_step_retention(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = self
            .engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }
}
