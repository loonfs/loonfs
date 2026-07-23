//! Explicit core maintenance: ticks, GC, checkpoints, WAL flushes, and
//! retention. Grep building and collection live in `loonfs-grep`.

use super::core::FsCore;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CreateCheckpointOptions, CreateCheckpointResponse,
    ErrorCode, FlushWalOutcome, FlushWalResponse, MaintenanceTickOptions, MaintenanceTickOutcome,
    MaintenanceTickResult, NamespaceId, ReleaseCheckpointResponse,
};
use crate::{Result, RuntimeError};
use loonfs_api::v0::{DisableGramsIndexResponse, EnableGramsIndexResponse};

impl FsCore {
    /// Runs one bounded maintenance step against a namespace.
    ///
    /// Publishes a checkpoint once the visible WAL tail reaches
    /// `options.max_wal_tail_segments`. Losing the head race or being
    /// superseded by another checkpoint is reported as an outcome, not an
    /// error.
    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub(crate) async fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        if options.max_wal_tail_segments == 0 {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        }
        // A threshold above the write-rejection cap would make the tick
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

        let status_before = self.namespace_status(namespace_id).await?;
        let observed_head_seq = status_before.head_seq;
        if status_before.wal_tail_segments < options.max_wal_tail_segments {
            self.run_tick_reorganization(namespace_id).await?;
            let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
            return Ok(MaintenanceTickResult {
                namespace_id: namespace_id.clone(),
                status_before,
                outcome: MaintenanceTickOutcome::NotNeeded,
                gc,
            });
        }

        let flush = match self.flush_wal(namespace_id).await {
            Ok(flush) => flush,
            Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
                self.run_tick_reorganization(namespace_id).await?;
                let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
                return Ok(MaintenanceTickResult {
                    namespace_id: namespace_id.clone(),
                    status_before,
                    outcome: MaintenanceTickOutcome::WalFlushRaceLost { observed_head_seq },
                    gc,
                });
            }
            Err(error) => return Err(error),
        };
        self.run_tick_reorganization(namespace_id).await?;

        let outcome = match flush.outcome {
            FlushWalOutcome::Published => MaintenanceTickOutcome::WalFlushed {
                manifest_head_seq: flush.manifest_head_seq,
            },
            FlushWalOutcome::AlreadyCurrent | FlushWalOutcome::Superseded => {
                MaintenanceTickOutcome::WalFlushSuperseded {
                    attempted_seq: flush.target_head_seq,
                    current_manifest_id: flush.manifest_id,
                }
            }
        };

        let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
        Ok(MaintenanceTickResult {
            namespace_id: namespace_id.clone(),
            status_before,
            outcome,
            gc,
        })
    }

    /// One bounded reorganization unit per tick: folds one family group of
    /// L0 delta rows into the base when enough L0 runs have piled up (see
    /// `loonfs-core`'s `reorganize_metadata`). Explicit ticks stay bounded at
    /// one unit per call; the returned outcome lets writer-scheduled
    /// background work keep folding until nothing is left.
    async fn run_tick_reorganization(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_core::MetadataReorganizeOutcome> {
        let report = self
            .namespace_engine(namespace_id)
            .reorganize_metadata()
            .await
            .map_err(RuntimeError::Core)?;
        match &report.outcome {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {}
            loonfs_core::MetadataReorganizeOutcome::UnitPublished {
                families,
                folded_l0_rows,
                ..
            } => {
                self.invalidate_namespace_cache(namespace_id);
                tracing::info!(
                    families = ?families,
                    folded_l0_rows,
                    "metadata reorganization unit published"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::Superseded => {
                tracing::info!("metadata reorganization unit superseded; will retry");
            }
        }
        Ok(report.outcome)
    }

    /// Folds reorganization units until the trigger reports nothing left to
    /// do. Only writer-scheduled background ticks drain like this — an
    /// explicit maintenance tick keeps its one-unit-per-call cost bound —
    /// so fold debt created by a burst of writes is settled by the burst's
    /// own background tick instead of waiting for future threshold
    /// crossings that may never come.
    pub(super) async fn drain_reorganization_backlog(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<()> {
        // Four family groups exist today; the bound is a livelock guard,
        // not a scheduling policy.
        const MAX_UNITS_PER_DRAIN: usize = 16;
        for _ in 0..MAX_UNITS_PER_DRAIN {
            match self.run_tick_reorganization(namespace_id).await? {
                loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. } => {}
                loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. }
                | loonfs_core::MetadataReorganizeOutcome::Superseded => break,
            }
        }
        Ok(())
    }

    async fn run_tick_gc(
        &self,
        namespace_id: &NamespaceId,
        config: Option<&crate::GcConfig>,
    ) -> Result<Option<crate::GcReport>> {
        let Some(config) = config else {
            return Ok(None);
        };
        Ok(Some(self.gc_namespace(namespace_id, config).await?))
    }

    /// Enables the independent grep root and starts checkpointed backfill.
    pub(crate) async fn enable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGramsIndexResponse> {
        let outcome = self
            .grep_worker()
            .enable(namespace_id)
            .await
            .map_err(RuntimeError::Grep)?;
        match outcome {
            loonfs_grep::GrepEnableOutcome::Enabled { target_seq } => {
                Ok(EnableGramsIndexResponse {
                    namespace_id: namespace_id.clone(),
                    built_through_seq: target_seq,
                    already_enabled: false,
                })
            }
            loonfs_grep::GrepEnableOutcome::AlreadyEnabled { built_through_seq } => {
                Ok(EnableGramsIndexResponse {
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
    pub(crate) async fn disable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGramsIndexResponse> {
        let outcome = self
            .grep_worker()
            .disable(namespace_id)
            .await
            .map_err(RuntimeError::Grep)?;
        match outcome {
            loonfs_grep::GrepDisableOutcome::Disabled => Ok(DisableGramsIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: true,
            }),
            loonfs_grep::GrepDisableOutcome::NotEnabled => Ok(DisableGramsIndexResponse {
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
            self.inner.store.clone(),
            self.inner.config.writer_id.clone(),
            self.inner.writer_session_id.clone(),
            self.inner.config.writer_version.clone(),
        )
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Never runs implicitly: callers opt in here or through
    /// [`MaintenanceTickOptions::gc`].
    pub(crate) async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &crate::GcConfig,
    ) -> Result<crate::GcReport> {
        let report = loonfs_core::gc_namespace(
            self.store(),
            namespace_id,
            config,
            &self.mutation_context()?,
        )
        .await
        .map_err(RuntimeError::Core)?;
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.invalidate_namespace_read_cache(namespace_id);
        Ok(report)
    }

    /// Explicitly completes or reaps one incomplete namespace installation.
    pub(crate) async fn repair_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<crate::RepairNamespaceResponse> {
        let response =
            loonfs_core::repair_namespace(self.store(), namespace_id, &self.mutation_context()?)
                .await
                .map_err(RuntimeError::Core)?;
        self.invalidate_namespace_read_cache(namespace_id);
        Ok(response)
    }

    /// Waits until every scheduled background maintenance tick has finished.
    ///
    /// Call this to quiesce before shutdown, or in tests that assert on
    /// post-maintenance state. Panicked ticks surface as a runtime-task
    /// error.
    pub(crate) async fn wait_for_background_maintenance(&self) -> Result<()> {
        self.inner.background.drain().await
    }

    /// Rejects any further background maintenance scheduling.
    pub(crate) fn shut_down_background(&self) {
        self.inner.background.shut_down();
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance. If
    /// the current head has no manifest yet, one is published first for the
    /// current durable namespace state; this is not a request to compact
    /// metadata.
    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        options: CreateCheckpointOptions,
    ) -> Result<CreateCheckpointResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let result = self
            .namespace_engine(namespace_id)
            .create_checkpoint(options.name, options.ttl_ms)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Releases a user-owned checkpoint by id. Idempotent.
    pub(crate) async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let result = self
            .namespace_engine(namespace_id)
            .release_checkpoint(checkpoint_id)
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Flushes the WAL tail and advances the metadata root, creating no
    /// checkpoint record.
    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub(crate) async fn flush_wal(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let result = self
            .namespace_engine(namespace_id)
            .flush_wal()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    pub(crate) async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = self
            .namespace_engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }
}
