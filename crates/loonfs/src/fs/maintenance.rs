//! Explicit maintenance: ticks, index builds, GC, checkpoints, WAL
//! flushes, and retention.

use super::core::FsCore;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CoreError, CreateCheckpointOptions,
    CreateCheckpointResponse, ErrorCode, FlushWalOutcome, FlushWalResponse, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, NamespaceId, ReleaseCheckpointResponse,
};
use crate::{Result, RuntimeError};
use loonfs_api::{DisableGramsIndexResponse, EnableGramsIndexResponse};

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

        let status_before = self.namespace_status(namespace_id).await?;
        let observed_head_seq = status_before.head_seq;
        if status_before.wal_tail_segments < options.max_wal_tail_segments {
            self.run_tick_reorganization(namespace_id).await?;
            self.run_tick_grams_index(namespace_id).await?;
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
                self.run_tick_grams_index(namespace_id).await?;
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
        self.run_tick_grams_index(namespace_id).await?;

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

    /// Publishes the `index.grams` feature entry, scheduling gram index
    /// backfill; maintenance ticks build it from then on.
    pub(crate) async fn enable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGramsIndexResponse> {
        let outcome = self
            .namespace_engine(namespace_id)
            .enable_grams_index()
            .await
            .map_err(RuntimeError::Core)?;
        self.record_grams_hint(namespace_id, true);
        self.invalidate_namespace_cache(namespace_id);
        match outcome {
            loonfs_core::GramIndexEnableOutcome::Enabled { built_through_seq } => {
                Ok(EnableGramsIndexResponse {
                    namespace_id: namespace_id.clone(),
                    built_through_seq,
                    already_enabled: false,
                })
            }
            loonfs_core::GramIndexEnableOutcome::AlreadyEnabled { built_through_seq } => {
                Ok(EnableGramsIndexResponse {
                    namespace_id: namespace_id.clone(),
                    built_through_seq,
                    already_enabled: true,
                })
            }
            loonfs_core::GramIndexEnableOutcome::Superseded => {
                Err(RuntimeError::Core(CoreError::CheckpointUnavailable(
                    "enabling the gram index lost a manifest publication race; retry".to_owned(),
                )))
            }
        }
    }

    /// Removes the `index.grams` feature entry and its segment references;
    /// the segments become garbage-collection candidates.
    pub(crate) async fn disable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGramsIndexResponse> {
        let outcome = self
            .namespace_engine(namespace_id)
            .disable_grams_index()
            .await
            .map_err(RuntimeError::Core)?;
        self.record_grams_hint(namespace_id, false);
        self.invalidate_namespace_cache(namespace_id);
        match outcome {
            loonfs_core::GramIndexDisableOutcome::Disabled => Ok(DisableGramsIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: true,
            }),
            loonfs_core::GramIndexDisableOutcome::NotEnabled => Ok(DisableGramsIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: false,
            }),
            loonfs_core::GramIndexDisableOutcome::Superseded => {
                Err(RuntimeError::Core(CoreError::CheckpointUnavailable(
                    "disabling the gram index lost a manifest publication race; retry".to_owned(),
                )))
            }
        }
    }

    fn record_grams_hint(&self, namespace_id: &NamespaceId, enabled: bool) {
        self.inner
            .grams_enabled_hints
            .lock()
            .expect("grams hint lock poisoned")
            .insert(namespace_id.clone(), enabled);
    }

    pub(super) fn grams_hint(&self, namespace_id: &NamespaceId) -> Option<bool> {
        self.inner
            .grams_enabled_hints
            .lock()
            .expect("grams hint lock poisoned")
            .get(namespace_id)
            .copied()
    }

    /// One bounded gram index build step, then one bounded fold step,
    /// under the configured [`crate::GramIndexBuildPolicy`]. A namespace
    /// without the feature entry reports and costs nothing.
    async fn run_tick_grams_index(&self, namespace_id: &NamespaceId) -> Result<bool> {
        self.run_tick_grams_index_with_policy(namespace_id, self.inner.config.gram_index_build)
            .await
    }

    /// [`Self::run_tick_grams_index`] with explicit budgets. Production
    /// paths run the handle's configured policy; tests inject small
    /// budgets to shape states — like a multi-step fold — that neither
    /// the defaults nor any sane configuration produce at test scale.
    async fn run_tick_grams_index_with_policy(
        &self,
        namespace_id: &NamespaceId,
        policy: loonfs_core::GramIndexBuildPolicy,
    ) -> Result<bool> {
        let engine = self.namespace_engine(namespace_id);
        // The same shared decoded-block cache reads use: index segments
        // are immutable and keyed by payload checksum, so blocks queries
        // already decoded serve the maintenance merge from memory.
        let table_cache = Some(self.inner.metadata_table_cache.as_ref());
        let build = engine
            .build_grams_index_step(policy, table_cache)
            .await
            .map_err(RuntimeError::Core)?;
        let mut published = false;
        match &build.outcome {
            loonfs_core::GramIndexBuildOutcome::Published {
                built_through_seq,
                indexed_revisions,
                materialized,
                ..
            } => {
                published = true;
                self.invalidate_namespace_cache(namespace_id);
                tracing::info!(
                    built_through_seq = built_through_seq.0,
                    indexed_revisions,
                    materialized,
                    "gram index build step published"
                );
            }
            loonfs_core::GramIndexBuildOutcome::UnsupportedFeatureVersion { found } => {
                tracing::warn!(
                    found,
                    "gram index feature version is not supported; skipping"
                );
            }
            loonfs_core::GramIndexBuildOutcome::Superseded => {
                published = true;
                tracing::info!("gram index build step superseded; will retry");
            }
            loonfs_core::GramIndexBuildOutcome::NotEnabled
            | loonfs_core::GramIndexBuildOutcome::UpToDate { .. } => {}
        }
        // `UnsupportedFeatureVersion` also records false: this writer cannot
        // advance that index, so scheduling it a tick per publish is waste.
        self.record_grams_hint(
            namespace_id,
            !matches!(
                build.outcome,
                loonfs_core::GramIndexBuildOutcome::NotEnabled
                    | loonfs_core::GramIndexBuildOutcome::UnsupportedFeatureVersion { .. }
            ),
        );
        let fold = engine
            .fold_grams_index_step(policy, table_cache)
            .await
            .map_err(RuntimeError::Core)?;
        if let loonfs_core::GramIndexFoldOutcome::StepPublished {
            merged_rows,
            segments_written,
            completed,
        } = &fold.outcome
        {
            // Any published fold step is continuing work, completed or
            // not: an unfinished fold keeps a cursor to step, and a
            // completing one may have just made the next tier eligible —
            // a delta fold's final step can create the threshold-th mid
            // run. The next drain iteration discovers either, and
            // `NotNeeded` is what ends the drain, exactly as metadata
            // reorganization drains after every published unit.
            published = true;
            self.invalidate_namespace_cache(namespace_id);
            tracing::info!(
                merged_rows,
                segments_written,
                completed,
                "gram index fold step published"
            );
        }
        Ok(published)
    }

    /// Builds gram index steps until the watermark reaches the head,
    /// each step under the configured [`crate::GramIndexBuildPolicy`].
    /// Only writer-scheduled background ticks drain like this, mirroring
    /// [`Self::drain_reorganization_backlog`].
    pub(super) async fn drain_grams_index_backlog(&self, namespace_id: &NamespaceId) -> Result<()> {
        self.drain_grams_index_backlog_with_policy(namespace_id, self.inner.config.gram_index_build)
            .await
    }

    /// [`Self::drain_grams_index_backlog`] with explicit budgets, for the
    /// same reason as [`Self::run_tick_grams_index_with_policy`].
    pub(super) async fn drain_grams_index_backlog_with_policy(
        &self,
        namespace_id: &NamespaceId,
        policy: loonfs_core::GramIndexBuildPolicy,
    ) -> Result<()> {
        const MAX_STEPS_PER_DRAIN: usize = 16;
        for _ in 0..MAX_STEPS_PER_DRAIN {
            if !self
                .run_tick_grams_index_with_policy(namespace_id, policy)
                .await?
            {
                break;
            }
        }
        Ok(())
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
