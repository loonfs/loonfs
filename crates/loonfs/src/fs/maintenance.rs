//! [`FsAdmin`]'s explicit maintenance: steps, GC, checkpoints, WAL
//! flushes, and retention. Grep building and collection live in
//! `loonfs-grep`.

use crate::FsAdmin;
use crate::NamespaceStatusResponse;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CreateCheckpointOptions, CreateCheckpointResponse,
    ErrorCode, FlushWalOutcome, FlushWalResponse, MaintenanceStepKind, MaintenanceStepOptions,
    MaintenanceStepResponse, NamespaceId, ReleaseCheckpointResponse, ReorganizeStepOutcome,
    WalFlushStepOutcome,
};
use crate::{Result, RuntimeError};
use loonfs_api::v0::{DisableGrepIndexResponse, EnableGrepIndexResponse};
use loonfs_core::cache::load_namespace_head_summary;

impl FsAdmin {
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
        name = "loonfs.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
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
        let status_before = self.namespace_status(namespace_id).await?;
        let observed_head_seq = status_before.head_seq;

        let wal_flush = if runs(MaintenanceStepKind::WalFlush)
            && status_before.wal_tail_segments >= options.max_wal_tail_segments
        {
            match self.flush_wal(namespace_id).await {
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
            self.advance_retention_floor(namespace_id)
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
            .core
            .namespace_engine(namespace_id)
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
                self.core.invalidate_namespace_cache(namespace_id);
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

    /// Folds reorganization units until the trigger reports nothing left to
    /// do. Only writer-scheduled background steps drain like this — an
    /// explicit maintenance step keeps its one-unit-per-call cost bound —
    /// so fold debt created by a burst of writes is settled by the burst's
    /// own background step instead of waiting for future threshold
    /// crossings that may never come.
    pub(crate) async fn drain_reorganization_backlog(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<()> {
        // Four family groups exist today; the bound is a livelock guard,
        // not a scheduling policy.
        const MAX_UNITS_PER_DRAIN: usize = 16;
        for _ in 0..MAX_UNITS_PER_DRAIN {
            match self.run_step_reorganization(namespace_id).await? {
                loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. } => {}
                loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. }
                | loonfs_core::MetadataReorganizeOutcome::BudgetExhausted { .. }
                | loonfs_core::MetadataReorganizeOutcome::Superseded => break,
            }
        }
        Ok(())
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

    /// Enables the independent grep root and starts checkpointed backfill.
    pub async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse> {
        let outcome = self
            .grep_worker()
            .enable(namespace_id)
            .await
            .map_err(RuntimeError::Grep)?;
        match outcome {
            loonfs_grep::GrepEnableOutcome::Enabled { target_seq } => Ok(EnableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                built_through_seq: target_seq,
                already_enabled: false,
            }),
            loonfs_grep::GrepEnableOutcome::AlreadyEnabled { built_through_seq } => {
                Ok(EnableGrepIndexResponse {
                    namespace_id: namespace_id.clone(),
                    built_through_seq,
                    already_enabled: true,
                })
            }
            loonfs_grep::GrepEnableOutcome::Superseded => Err(RuntimeError::Grep(
                loonfs_grep::GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                },
            )),
        }
    }

    /// Disables the independent grep root; its segments become grep-GC
    /// candidates.
    pub async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse> {
        let outcome = self
            .grep_worker()
            .disable(namespace_id)
            .await
            .map_err(RuntimeError::Grep)?;
        match outcome {
            loonfs_grep::GrepDisableOutcome::Disabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: true,
            }),
            loonfs_grep::GrepDisableOutcome::NotEnabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: false,
            }),
            loonfs_grep::GrepDisableOutcome::Superseded => Err(RuntimeError::Grep(
                loonfs_grep::GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                },
            )),
        }
    }

    fn grep_worker(&self) -> loonfs_grep::GrepWorker<crate::SharedObjectStore> {
        loonfs_grep::GrepWorker::new(
            self.core.inner.store.clone(),
            self.core.inner.config.writer_id.clone(),
            self.core.inner.writer_session_id.clone(),
            self.core.inner.config.writer_version.clone(),
        )
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Bounded calls return an enumeration cursor; every resume rebuilds the
    /// current live roots. Never runs implicitly: callers opt in here or
    /// through [`MaintenanceStepOptions::gc`].
    pub async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &crate::GcConfig,
    ) -> Result<crate::GcResponse> {
        let report = loonfs_core::gc_namespace(
            self.core.store(),
            namespace_id,
            config,
            &self.core.mutation_context()?,
        )
        .await
        .map_err(RuntimeError::Core)?;
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.core.invalidate_namespace_read_cache(namespace_id);
        Ok(report)
    }

    /// Explicitly completes or reaps one incomplete namespace installation.
    pub async fn repair_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<crate::RepairNamespaceResponse> {
        let response = loonfs_core::repair_namespace(
            self.core.store(),
            namespace_id,
            &self.core.mutation_context()?,
        )
        .await
        .map_err(RuntimeError::Core)?;
        self.core.invalidate_namespace_read_cache(namespace_id);
        Ok(response)
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance. If
    /// the current head has no manifest yet, one is published first for the
    /// current durable namespace state; this is not a request to compact
    /// metadata.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
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
            .core
            .namespace_engine(namespace_id)
            .create_checkpoint(options.name, options.ttl_ms)
            .await
            .map_err(RuntimeError::from);
        self.core.finish_namespace_mutation(namespace_id, result)
    }

    /// Releases a user-owned checkpoint by id. Idempotent.
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let result = self
            .core
            .namespace_engine(namespace_id)
            .release_checkpoint(checkpoint_id)
            .await
            .map_err(RuntimeError::from);
        self.core.finish_namespace_mutation(namespace_id, result)
    }

    /// Flushes the WAL tail and advances the metadata root, creating no
    /// checkpoint record.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn flush_wal(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        let result = self
            .core
            .namespace_engine(namespace_id)
            .flush_wal()
            .await
            .map_err(RuntimeError::from);
        self.core.finish_namespace_mutation(namespace_id, result)
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = self
            .core
            .namespace_engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.core.finish_namespace_mutation(namespace_id, result)
    }
}
