//! The shared runtime core behind the purpose-specific handles: caches,
//! writer session identity, and the operation surface, each method a thin,
//! cache-aware delegation to `loonfs-core`.

use crate::background::BackgroundWork;
use crate::cache::{RuntimeCacheStatsInner, RuntimeControlCache};
use crate::config::{validate_config, FsConfig};
use crate::content_tokens::ContentAdmission;
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent};
use crate::publisher::PublisherRegistry;
use crate::time::current_time_ms;
use crate::writer_session::WriterSessionRegistry;
use crate::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry,
    BeginDirectPutUploadTargetResponse, BeginUploadRequest, BeginUploadResponse, ChangeSeq,
    ChangesResponse, CheckpointId, CommitId, CommitOp, CommitPrecondition, CommitRequest,
    CommitResponse, CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions,
    CoreError, CreateCheckpointOptions, CreateCheckpointResponse, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions,
    ErrorCode, FlushWalOutcome, FlushWalResponse, InodeId, ListChangesOptions,
    ListFileRevisionsResponse, ListPathEntriesResponse, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, MoveOptions, NamespaceId,
    NamespaceStatusResponse, NamespaceSummary, ObjectStore, PutFileOptions,
    ReleaseCheckpointResponse, RestoreRevisionOptions, RevisionNo, RuntimeCacheStats,
    UndeleteOptions, UploadContentResponse,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    encode_directory_cursor, encode_file_revisions_cursor, generated_id, AbsolutePath,
    CapabilityDocument, DirectoryPageCursor, DisableGramsIndexResponse, EffectiveLimit,
    EnableGramsIndexResponse, FileRevision, FileRevisionsPageCursor, GrepRequest, GrepResponse,
    Page, PageRequest, PaginationPolicy, UploadId, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_GC_MIN_GRACE_WINDOW_MS, LIMIT_QUERY_GREP_DEFAULT,
    LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0, PROTOCOL_VERSION,
};
use loonfs_core::cache::{
    load_namespace_head_summary, MetadataTableCache, WalTailProjectionCache,
    WalTailProjectionCacheConfig,
};
use loonfs_core::{MutationContext, NamespaceEngine};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Shared runtime core. [`FsWriter`](crate::FsWriter),
/// [`FsReader`](crate::FsReader), and [`FsAdmin`](crate::FsAdmin) each wrap
/// one; handles derived from the same core share its caches and store.
///
/// `FsCore` is cheap to clone.
#[derive(Clone)]
pub(crate) struct FsCore {
    pub(crate) inner: Arc<FsInner>,
}

pub(crate) struct FsInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: FsConfig,
    pub(crate) writer_session_id: String,
    pub(crate) control_cache: Mutex<RuntimeControlCache>,
    pub(crate) metadata_table_cache: Arc<MetadataTableCache>,
    pub(crate) wal_tail_projection_cache: Arc<WalTailProjectionCache>,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
    pub(crate) background: BackgroundWork,
    /// The core's publication service: every mutation — direct handle
    /// calls and server submissions alike — publishes through it. It holds
    /// this core weakly, so the ownership does not cycle.
    pub(crate) publisher: PublisherRegistry,
    /// Session-owned writer state (acquired epochs, fencing), deliberately
    /// outside every rebuildable cache; see [`crate::writer_session`].
    pub(crate) writer_sessions: WriterSessionRegistry,
    /// Per-namespace gram-index enablement, learned in-process: `None`
    /// until any tick, enable, or disable observes the namespace. Publishes
    /// consult it so index catch-up is scheduled by index lag, not only by
    /// the WAL-segment threshold — one publish can add more tail files
    /// than a grep will scan without ever crossing that threshold.
    pub(crate) grams_enabled_hints: Mutex<BTreeMap<NamespaceId, bool>>,
}

/// Lock accessors for the runtime caches.
///
/// Poisoning is propagated as a panic: a poisoned cache means another thread
/// panicked mid-update, and serving from it could violate the consistency the
/// caches promise.
impl FsInner {
    pub(crate) fn control_cache(&self) -> MutexGuard<'_, RuntimeControlCache> {
        self.control_cache
            .lock()
            .expect("control cache lock poisoned")
    }
}

impl FsCore {
    /// Opens a runtime core. `shared_metadata_table_cache` substitutes an
    /// existing decoded-block cache for a freshly sized one, so handles
    /// with distinct actor identities can still share warmed blocks —
    /// sound across cores because entries are keyed by immutable
    /// identities (payload checksums and manifest object keys); the
    /// sharing caller owns the sizing decision, and
    /// `config.runtime_cache.metadata_table_cache` goes unused.
    pub(crate) fn open_with_background(
        store: SharedObjectStore,
        config: FsConfig,
        background: BackgroundWork,
        shared_metadata_table_cache: Option<Arc<MetadataTableCache>>,
    ) -> Result<Self> {
        validate_config(&config)?;
        // Store the policy normalized so the config always reads as the
        // budgets maintenance will actually run (the core normalizes again
        // at each step; this keeps the two views identical).
        let config = FsConfig {
            gram_index_build: config.gram_index_build.normalized(),
            ..config
        };
        let metadata_table_cache = shared_metadata_table_cache.unwrap_or_else(|| {
            Arc::new(MetadataTableCache::new(
                config.runtime_cache.metadata_table_cache.clone(),
            ))
        });
        let wal_tail_projection_cache =
            Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
                max_entries: config.runtime_cache.max_cached_namespaces,
                max_rows: config.runtime_cache.max_cached_wal_tail_projection_rows,
                max_decoded_bytes: config
                    .runtime_cache
                    .max_cached_wal_tail_projection_decoded_bytes,
            }));
        let min_publish_interval = std::time::Duration::from_millis(config.min_publish_interval_ms);
        let trace_mode = config.trace_mode.as_str();
        let trace_store_kind = config.trace_store_kind.as_str();
        // Cyclic by design: the core owns its publication service, and the
        // service reaches back into the core through a weak reference.
        Ok(Self {
            inner: Arc::new_cyclic(|weak| FsInner {
                store,
                config,
                writer_session_id: generated_id("wrs"),
                control_cache: Mutex::new(RuntimeControlCache::default()),
                metadata_table_cache,
                wal_tail_projection_cache,
                cache_stats: RuntimeCacheStatsInner::default(),
                background,
                publisher: PublisherRegistry::from_core(
                    weak.clone(),
                    min_publish_interval,
                    trace_mode,
                    trace_store_kind,
                ),
                writer_sessions: WriterSessionRegistry::default(),
                grams_enabled_hints: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// The core's publication service; see [`crate::publisher`].
    pub(crate) fn publisher(&self) -> &PublisherRegistry {
        &self.inner.publisher
    }

    /// This runtime's shared decoded-block cache handle, for builders
    /// that open another core sharing it.
    pub(crate) fn metadata_table_cache(&self) -> Arc<MetadataTableCache> {
        Arc::clone(&self.inner.metadata_table_cache)
    }

    fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
    }

    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(namespace_id)
            .bootstrap_namespace(loonfs_core::BootstrapOptions {
                allow_existing: options.allow_existing,
            })
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source` into `target` at the source's current head.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub(crate) async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(source)
            .fork_namespace(target)
            .await
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(source);
        }
        if result.is_ok() {
            self.invalidate_namespace_cache(target);
        }
        result
    }

    /// Deletes a namespace: a fenced, terminal head transition (format
    /// spec, "Tombstones and deletion"). Commits acknowledged before the
    /// swap stay committed; reads, writes, forks, and re-creation of the id
    /// fail with `namespace_deleted` afterward. Deletion does not reclaim
    /// storage; reclamation is future maintenance work.
    ///
    /// Sequenced as a barrier through the publication service: mutations
    /// admitted before the delete publish first, and mutations admitted
    /// after it fail once it succeeds.
    pub(crate) async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.inner
            .publisher
            .submit_delete(namespace_id.clone(), options)
            .await
            .map_err(RuntimeError::Core)
    }

    /// The delete itself, run by the publication service once the barrier
    /// admits it. Only the service calls this; everything else must go
    /// through [`Self::delete_namespace`] so the barrier holds.
    pub(crate) async fn delete_namespace_unqueued(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        // Serialize with any engine holder so the delete takes its turn
        // behind an in-flight publication for the namespace.
        let engine = self.commit_engine(namespace_id);
        let result = {
            let _engine = engine.lock().await;
            self.namespace_engine(namespace_id)
                .delete_namespace(options)
                .await
                .map_err(RuntimeError::from)
        };
        if result.is_ok() {
            // Only a namespace that is actually gone releases its session
            // state: a failed delete (a fenced deleter, say) must not erase
            // the fencing record and hand the session a fresh epoch.
            self.inner.writer_sessions.remove(namespace_id);
        }
        self.invalidate_namespace_cache_for_delete(namespace_id);
        result
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    ///
    /// The embedded runtime implements every v0 plane, so the answer is a
    /// constant. Callers should still gate on the document rather than on
    /// the backend kind, so the same logic works against remote deployments
    /// that implement less.
    pub(crate) fn capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![
                PROFILE_CORE_V0.to_owned(),
                PROFILE_ADMIN_V0.to_owned(),
                PROFILE_QUERY_V0.to_owned(),
            ],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                (FEATURE_UPLOADS_DIRECT_PUT.to_owned(), false),
                (FEATURE_QUERY_GREP.to_owned(), true),
            ]),
            limits: {
                let mut limits = PaginationPolicy::default().capability_limits();
                limits.insert(
                    LIMIT_QUERY_GREP_DEFAULT.to_owned(),
                    loonfs_core::grep_limits::DEFAULT_GREP_PAGE_LIMIT as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_MAX.to_owned(),
                    loonfs_core::grep_limits::MAX_GREP_PAGE_LIMIT as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES.to_owned(),
                    loonfs_core::grep_limits::MAX_GREP_SCAN_FILES as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES.to_owned(),
                    loonfs_core::grep_limits::MAX_GREP_TAIL_FILES as u64,
                );
                limits.insert(
                    LIMIT_GC_MIN_GRACE_WINDOW_MS.to_owned(),
                    loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
                );
                limits
            },
        }
    }

    /// Summarizes a namespace's current head: manifest, latest checkpoint,
    /// WAL tail, and retention floor.
    pub(crate) async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        let summary = load_namespace_head_summary(self.store(), namespace_id).await?;
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
    async fn drain_reorganization_backlog(&self, namespace_id: &NamespaceId) -> Result<()> {
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

    fn grams_hint(&self, namespace_id: &NamespaceId) -> Option<bool> {
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
    async fn drain_grams_index_backlog(&self, namespace_id: &NamespaceId) -> Result<()> {
        self.drain_grams_index_backlog_with_policy(namespace_id, self.inner.config.gram_index_build)
            .await
    }

    /// [`Self::drain_grams_index_backlog`] with explicit budgets, for the
    /// same reason as [`Self::run_tick_grams_index_with_policy`].
    async fn drain_grams_index_backlog_with_policy(
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
        let report =
            loonfs_core::gc_namespace(self.store(), namespace_id, config, &self.mutation_context())
                .await
                .map_err(RuntimeError::Core)?;
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.invalidate_namespace_read_cache(namespace_id);
        Ok(report)
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    #[tracing::instrument(
        level = "info",
        name = "loon.stat",
        err,
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub(crate) async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let entry = engine
            .resolve_path_with_runtime_context(absolute_path, &read_context)
            .await?;
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::MaterializedTables.as_str(),
        );
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(entry)
    }

    /// Lists the children of a directory path.
    ///
    /// Entries-only convenience over [`Self::list_path_entries`].
    pub(crate) async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        Ok(self
            .list_path_entries(namespace_id, absolute_path)
            .await?
            .entries)
    }

    /// Lists a directory together with the head the listing was read from.
    ///
    /// The envelope and every entry come from one consistent head, so an
    /// empty directory still reports which state answered the question. Entries
    /// are returned in canonical name-key order, matching paged listings.
    pub(crate) async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListPathEntriesResponse> {
        let limit = default_page_limit();
        let mut cursor = None;
        let mut entries = Vec::new();
        let mut envelope = None;
        loop {
            let (page, next_cursor) = self
                .list_path_entries_page_typed(
                    namespace_id,
                    absolute_path,
                    PageRequest { limit, cursor },
                )
                .await?;
            let envelope_ref = envelope.get_or_insert_with(|| ListPathEntriesResponse {
                namespace_id: page.namespace_id.clone(),
                absolute_path: page.absolute_path.clone(),
                head_seq: page.head_seq,
                entries: Vec::new(),
                next_cursor: None,
            });
            entries.extend(page.entries);
            cursor = next_cursor;
            if cursor.is_none() {
                envelope_ref.entries = entries;
                return Ok(envelope.expect("first page initializes response envelope"));
            }
        }
    }

    /// Lists one page of a directory together with the head the page was read from.
    pub(crate) async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<ListPathEntriesResponse> {
        let (mut response, next_cursor) = self
            .list_path_entries_page_typed(namespace_id, absolute_path, request)
            .await?;
        response.next_cursor = next_cursor
            .as_ref()
            .map(encode_directory_cursor)
            .transpose()
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
        Ok(response)
    }

    async fn list_path_entries_page_typed(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<(ListPathEntriesResponse, Option<DirectoryPageCursor>)> {
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let request_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
        let page = engine
            .list_path_page_with_runtime_context(listed_path.as_str(), request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        let head_seq = page
            .items
            .first()
            .map(|entry| entry.head_seq)
            .or(request_head_seq)
            .unwrap_or(read_context.head.seq);
        let next_cursor = page.next_cursor;
        let response = ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            absolute_path: listed_path.as_str().to_owned(),
            head_seq,
            entries: page.items,
            next_cursor: None,
        };
        Ok((response, next_cursor))
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub(crate) async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_with_runtime_context(
                absolute_path,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Content search over the namespace's gram index.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        // The manifest features map gates this read, and the metadata root
        // can move without the head moving (index enablement, folds), so a
        // head-revalidated anchor is not enough: confirm the root still
        // names the pinned manifest and reload the anchor when it moved.
        let root =
            loonfs_core::control::load_namespace_metadata_root_control(self.store(), namespace_id)
                .await
                .map_err(|error| {
                    RuntimeError::Core(CoreError::MetadataProjection(
                        loonfs_core::MetadataProjectionLoadError::LoadHead(error),
                    ))
                })?;
        let (engine, read_context) =
            if root.state.manifest_object_id != read_context.manifest_object_id {
                self.invalidate_namespace_read_cache(namespace_id);
                self.pinned_read(namespace_id).await?
            } else {
                (engine, read_context)
            };
        let response = engine
            .grep_with_runtime_context(request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(response)
    }

    /// Lists one page of a file path's revision history.
    pub(crate) async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let fallback_inode_id = request.cursor.as_ref().map(|cursor| cursor.inode_id);
        let page = engine
            .list_file_revisions_page_with_runtime_context(absolute_path, request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            fallback_inode_id,
        )?)
    }

    /// Lists one page of a file inode's revision history.
    pub(crate) async fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let page = engine
            .list_file_revisions_for_inode_page_with_runtime_context(
                inode_id,
                request,
                &read_context,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            Some(inode_id),
        )?)
    }

    /// Reads the content of one historical file revision by path.
    pub(crate) async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_with_runtime_context(
                absolute_path,
                revision_no,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Reads the content of one historical file revision by inode id.
    pub(crate) async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_for_inode_with_runtime_context(
                inode_id,
                revision_no,
                &read_context,
                self.inner.config.max_read_content_bytes,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first; metadata referencing them is
    /// published only afterward. `options.behavior` selects create-only or
    /// replace semantics.
    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub(crate) async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        // Parse the mutation path before staging content so an invalid or
        // root path cannot orphan an already-written content blob; the
        // intent carries the parsed path from here on.
        let absolute_path = loonfs_core::path::parse_mutation_path(absolute_path)?;
        // The bytes become durable content before the publish is submitted,
        // so the publication queue never waits on a content upload and an
        // upload failure surfaces here with nothing published. A publish
        // that fails afterward leaves only a GC-covered orphan behind an
        // unmoved head.
        let cached_content_store_id = self
            .load_namespace_catalog_cached(namespace_id)
            .await?
            .map(|catalog| catalog.content_store_id().clone());
        let stored = match cached_content_store_id {
            Some(content_store_id) => {
                loonfs_core::content::store_bytes_as_content_with_store_id(
                    &self.inner.store,
                    content_store_id,
                    bytes,
                )
                .await?
            }
            None => {
                loonfs_core::content::store_bytes_as_content(&self.inner.store, namespace_id, bytes)
                    .await?
            }
        };
        // The admission tells batch validation not to re-probe for the blob
        // whose acked write this handle just observed.
        let content_admission = ContentAdmission::for_durable_content_write(
            namespace_id.clone(),
            stored.content_ref.clone(),
            current_time_ms(),
        );
        self.publish_candidate(
            namespace_id,
            NamespaceMutationCandidate::PathWithContentAdmission {
                intent: PathMutationIntent::PutFile {
                    commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                    absolute_path,
                    content_ref: stored.content_ref,
                    behavior: options.behavior,
                },
                admissions: vec![content_admission],
            },
        )
        .await
    }

    /// Publishes a file revision that points at an already-durable content
    /// ref.
    ///
    /// Use this when content was staged separately, for example through the
    /// upload protocol.
    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub(crate) async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::PutFile {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                content_ref,
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Creates a directory at an absolute path.
    pub(crate) async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CreateDir {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                parents: options.parents,
            },
        )
        .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is background garbage collection.
    pub(crate) async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::DeletePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
            },
        )
        .await
    }

    /// Moves a path within the same namespace.
    pub(crate) async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::MovePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    pub(crate) async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CopyFilePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub(crate) async fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::RestoreRevision {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                source_revision_no,
            },
        )
        .await
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` and binds it at `absolute_path`. The inode id is the one
    /// the delete reported (also visible in the change feed).
    pub(crate) async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: &str,
        options: UndeleteOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::Undelete {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                inode_id,
                deleted_at_seq,
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
            },
        )
        .await
    }

    /// Restores a prior revision of an inode, guarded by a base-revision
    /// precondition.
    ///
    /// The commit appends a new current revision from `source_revision_no`
    /// and fails if the inode's current revision is no longer
    /// `base_revision_no`.
    pub(crate) async fn restore_file_revision_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        let commit_id = options.commit_id.unwrap_or_else(CommitId::generate);
        let request = CommitRequest {
            commit_id,
            preconditions: vec![CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no: base_revision_no,
            }],
            ops: vec![CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            }],
            message: None,
        };
        self.commit_operations(namespace_id, request).await
    }

    /// Starts a durable upload session for a namespace.
    pub(crate) async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_upload(request)
            .await?)
    }

    /// Starts a direct_put upload session and returns the internal target for server-side signing.
    pub(crate) async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_direct_put_upload_target(content_ref)
            .await?)
    }

    /// Uploads whole-file content into an upload session.
    pub(crate) async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .upload_content(upload_id, bytes)
            .await?)
    }

    /// Completes an upload session when the expected content ref matches.
    pub(crate) async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .complete_upload(upload_id, request)
            .await?)
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface for clients that need their own commit
    /// ids, preconditions, and operation lists.
    pub(crate) async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_candidate(namespace_id, NamespaceMutationCandidate::Commit(request))
            .await
    }

    /// Submits explicit semantic commit requests, returning one result per
    /// request in order. Requests admitted together usually publish
    /// together, batched by the publication service.
    pub(crate) async fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.publish_through_publisher(
            namespace_id,
            requests
                .into_iter()
                .map(NamespaceMutationCandidate::Commit)
                .collect(),
        )
        .await
    }

    async fn publish_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
    ) -> Result<CommitResponse> {
        self.publish_candidate(namespace_id, NamespaceMutationCandidate::Path(intent))
            .await
    }

    async fn publish_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: NamespaceMutationCandidate,
    ) -> Result<CommitResponse> {
        self.publish_through_publisher(namespace_id, vec![candidate])
            .await
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(RuntimeError::Core(CoreError::Internal(
                    "empty publication batch".to_owned(),
                )))
            })
    }

    /// Publishes direct submissions through the core's publication service
    /// (see [`crate::publisher`]): batching is adaptive, every submitter
    /// receives its own durable result, and admitted work is owned by the
    /// service's tasks — a cancelled caller abandons only its result
    /// delivery, never the publication. Candidates are admitted in order,
    /// so one call's requests usually publish as one batch.
    async fn publish_through_publisher(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let submissions = candidates.into_iter().map(|candidate| {
            self.inner
                .publisher
                .submit_candidate(namespace_id.clone(), candidate)
        });
        futures::future::join_all(submissions)
            .await
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect()
    }

    /// Publishes already-classified namespace mutation candidates as one
    /// batch: one WAL segment, one head compare-and-swap.
    ///
    /// This is the engine-level publish the publication service's tasks
    /// drive; results match candidates in order. Everything else submits
    /// through [`Self::publish_through_publisher`].
    pub(crate) async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        let store = self.store();
        if self.commit_engine_cache_enabled() {
            // Warm the immutable catalog through the control cache so a
            // recreated engine starts seeded; a load failure here surfaces
            // as the publish view's own, properly shaped error instead.
            self.load_namespace_catalog_cached(namespace_id).await.ok();
            let engine = self.commit_engine(namespace_id);
            let mut publish = {
                let context = self.mutation_context();
                let cache_config = &self.inner.config.runtime_cache;
                // Boxing erases the engine's deeply nested publish future;
                // without it, callers awaiting a put or commit (CLI, server,
                // embedding crates) exceed rustc's type-recursion depth.
                let tail_options = loonfs_core::publish::PublishTailOptions {
                    max_tail_rows: cache_config.max_cached_wal_tail_projection_rows,
                    max_tail_decoded_bytes: cache_config
                        .max_cached_wal_tail_projection_decoded_bytes,
                };
                Box::pin(async {
                    let mut engine = engine.lock().await;
                    engine
                        .publish_batch_with_tail_options(
                            &store,
                            candidates,
                            &context,
                            &tail_options,
                        )
                        .await
                })
                .await
            };
            {
                let _span = tracing::info_span!(
                    "publisher.batch_update_cache",
                    phase = "batch_update_cache",
                    mode = self.inner.config.trace_mode.as_str(),
                    store_kind = self.inner.config.trace_store_kind.as_str(),
                    batch_size
                )
                .entered();
                match publish.resulting_read_state.take() {
                    // A landed publish hands the caches exactly the state a
                    // rebuild would recompute; use it instead of dropping.
                    Some(state) => self.seed_namespace_read_cache(namespace_id, state),
                    None => {
                        let runtime_results = publish
                            .results
                            .iter()
                            .map(|result| result.clone().map_err(RuntimeError::Core))
                            .collect::<Vec<_>>();
                        self.invalidate_namespace_cache_after_batch(namespace_id, &runtime_results);
                    }
                }
            }
            let wal_tail_segments = publish.wal_tail_segments;
            let results = publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
            self.maybe_auto_tick_after_publish(namespace_id, wal_tail_segments);
            return results;
        }

        // Cache-disabled diagnostic mode: a throwaway engine per publish,
        // but the session's epoch and fencing still come from the registry —
        // no cache configuration disables session state.
        let mut engine = loonfs_core::publish::NamespaceCommitEngine::new(namespace_id.clone())
            .writer_session(self.inner.writer_sessions.state(namespace_id));
        let context = self.mutation_context();
        // Boxed for the same type-recursion reason as the cached-engine path.
        let results: Vec<_> = Box::pin(engine.publish_batch(&store, candidates, &context))
            .await
            .results
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect();
        {
            let _span = tracing::info_span!(
                "publisher.batch_update_cache",
                phase = "batch_update_cache",
                mode = self.inner.config.trace_mode.as_str(),
                store_kind = self.inner.config.trace_store_kind.as_str(),
                batch_size
            )
            .entered();
            self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        }
        results
    }

    /// Schedules a maintenance tick after a publish that observed the WAL
    /// tail at or past the checkpoint threshold. Ticks are spawned on the
    /// handle's owning runtime — never on a hidden LoonFS runtime — so no
    /// writer (and no server batch pipeline) waits behind a checkpoint or
    /// base rebuild. The per-namespace singleflight claim dedupes concurrent
    /// publishers and is released on every outcome, including tick panics
    /// and dropped tasks. The cache-disabled diagnostic mode never reaches
    /// this, as it tracks no tail projection to observe.
    fn maybe_auto_tick_after_publish(&self, namespace_id: &NamespaceId, wal_tail_segments: u64) {
        let options = MaintenanceTickOptions::default();
        let run_full_tick = wal_tail_segments >= options.max_wal_tail_segments;
        // Below the WAL threshold, index catch-up alone is still scheduled
        // when the gram index is (or may be) enabled: index lag is measured
        // in files, not segments, and one publish can put more files in the
        // tail than a grep will scan. `None` means this process has not
        // observed the namespace yet; the drain it schedules learns the
        // answer, so the discovery cost is one cheap step per namespace per
        // process. Flush cadence is unchanged — only the full tick moves
        // WAL segments into tables.
        let index_may_lag = self.grams_hint(namespace_id) != Some(false);
        if !run_full_tick && !index_may_lag {
            return;
        }
        if !self.inner.background.try_claim(namespace_id, run_full_tick) {
            return;
        }
        let mut claim = BackgroundTickClaim {
            fs: self.clone(),
            namespace_id: namespace_id.clone(),
            releases_on_drop: true,
        };
        self.inner.background.spawn(async move {
            let mut run_full_tick = run_full_tick;
            loop {
                if let Err(error) = claim
                    .fs
                    .run_auto_maintenance(&claim.namespace_id, options, run_full_tick)
                    .await
                {
                    tracing::info!(
                        phase = "auto_maintenance_tick",
                        result = "error",
                        error = %error,
                        "post-publish maintenance tick failed"
                    );
                }
                if !claim.finish_tick() {
                    break;
                }
                run_full_tick = true;
            }
        });
    }

    async fn run_auto_maintenance(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
        run_full_tick: bool,
    ) -> Result<()> {
        if run_full_tick {
            self.maintenance_tick_namespace(namespace_id, options)
                .await?;
            self.drain_reorganization_backlog(namespace_id).await?;
        }
        self.drain_grams_index_backlog(namespace_id).await
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

    /// Snapshots the runtime cache counters.
    pub(crate) fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner.cache_stats.snapshot(
            self.inner.metadata_table_cache.stats(),
            self.inner.wal_tail_projection_cache.stats(),
        )
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub(crate) async fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> Result<ChangesResponse> {
        let limit = match options.limit {
            Some(limit) => limit,
            None => PaginationPolicy::default()
                .resolve_limit(None)
                .map_err(|error| RuntimeError::Config(error.to_string()))?,
        };
        Ok(self
            .namespace_engine(namespace_id)
            .list_changes_after(after_seq, limit)
            .await?)
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

    pub(crate) fn store(&self) -> &dyn ObjectStore {
        self.inner.store.as_ref()
    }

    pub(crate) fn namespace_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceEngine<SharedObjectStore> {
        self.namespace_engine_with_store(namespace_id, self.inner.store.clone())
    }

    pub(crate) fn namespace_engine_with_store<S: ObjectStore>(
        &self,
        namespace_id: &NamespaceId,
        store: S,
    ) -> NamespaceEngine<S> {
        NamespaceEngine::builder(store)
            .namespace_id(namespace_id.clone())
            .writer_id(self.inner.config.writer_id.clone())
            .writer_session_id(self.inner.writer_session_id.clone())
            .writer_version(self.inner.config.writer_version.clone())
            .build()
            .expect("validated runtime config should build namespace engine")
    }

    pub(crate) fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_session_id: self.inner.writer_session_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms(),
        }
    }
}

/// Holds a background singleflight claim across a spawned tick. Dropping —
/// on completion, panic, or a task discarded with its runtime — releases the
/// namespace for the next scheduling decision.
struct BackgroundTickClaim {
    fs: FsCore,
    namespace_id: NamespaceId,
    releases_on_drop: bool,
}

impl BackgroundTickClaim {
    fn finish_tick(&mut self) -> bool {
        // Disarm before releasing the slot so a new owner cannot claim it
        // and then be accidentally released by this guard's later drop.
        self.releases_on_drop = false;
        let run_deferred_full_tick = self.fs.inner.background.finish_tick(&self.namespace_id);
        self.releases_on_drop = run_deferred_full_tick;
        run_deferred_full_tick
    }
}

impl Drop for BackgroundTickClaim {
    fn drop(&mut self) {
        if self.releases_on_drop {
            self.fs.inner.background.release(&self.namespace_id);
        }
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
        _ => false,
    }
}

fn default_page_limit() -> EffectiveLimit {
    PaginationPolicy::default()
        .resolve_limit(None)
        .expect("default pagination policy must resolve its default limit")
}

fn file_revisions_page_response(
    namespace_id: NamespaceId,
    head_seq: ChangeSeq,
    page: Page<FileRevision, FileRevisionsPageCursor>,
    fallback_inode_id: Option<InodeId>,
) -> std::result::Result<ListFileRevisionsResponse, CoreError> {
    let inode_id = page
        .items
        .first()
        .map(|revision| revision.inode_id)
        .or_else(|| page.next_cursor.as_ref().map(|cursor| cursor.inode_id))
        .or(fallback_inode_id)
        .ok_or_else(|| {
            CoreError::InvalidCursor("empty revision page lacks inode identity".into())
        })?;
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(encode_file_revisions_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}

#[cfg(test)]
// Like the crate's integration tests, assertions use panic in impossible
// match arms for precise failure diagnostics.
#[allow(clippy::panic)]
mod tests {
    use crate::{
        ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, DisplayName, InodeId,
        NameKey, NamePolicy, RevisionNo,
    };

    /// Runs build steps until the index reports up to date, so each call
    /// after one write lands that write's revisions as one delta run.
    async fn drain_grams_builds(engine: &loonfs_core::NamespaceEngine<crate::SharedObjectStore>) {
        for _ in 0..8 {
            let report = engine
                .build_grams_index_step(loonfs_core::GramIndexBuildPolicy::default(), None)
                .await
                .expect("build step");
            match report.outcome {
                loonfs_core::GramIndexBuildOutcome::Published { .. } => {}
                loonfs_core::GramIndexBuildOutcome::UpToDate { .. } => return,
                other => panic!("unexpected build outcome: {other:?}"),
            }
        }
        panic!("the build backlog must drain");
    }

    async fn drive_grams_fold_to_completion(
        engine: &loonfs_core::NamespaceEngine<crate::SharedObjectStore>,
        policy: loonfs_core::GramIndexBuildPolicy,
    ) {
        for _ in 0..64 {
            let report = engine
                .fold_grams_index_step(policy, None)
                .await
                .expect("fold step");
            match report.outcome {
                loonfs_core::GramIndexFoldOutcome::StepPublished { completed, .. } => {
                    if completed {
                        return;
                    }
                }
                other => panic!("expected a published fold step, got {other:?}"),
            }
        }
        panic!("the fold walk must terminate");
    }

    async fn grams_segment_levels(
        store: &crate::SharedObjectStore,
        namespace_id: &crate::NamespaceId,
    ) -> Vec<u32> {
        let root =
            loonfs_core::control::load_namespace_metadata_root_control(&**store, namespace_id)
                .await
                .expect("metadata root");
        let manifest_key = loonfs_objectstore::keys::metadata_manifest_object(
            namespace_id.as_str(),
            &root.state.manifest_object_id,
        );
        let manifest_bytes = store
            .get(&manifest_key, None)
            .await
            .expect("read namespace manifest")
            .expect("namespace manifest exists");
        let manifest = loonfs_api::wire::manifest::decode_namespace_manifest_json(&manifest_bytes)
            .expect("decode manifest");
        manifest
            .payload
            .index_files
            .iter()
            .filter(|descriptor| descriptor.family == "grams")
            .map(|descriptor| descriptor.level)
            .collect()
    }

    /// Review regression: a completed delta fold used to report no
    /// continuing work, so a drain stopped at the tier transition and the
    /// base fold its completion had just made eligible waited for the next
    /// external trigger. The drain must keep going after every published
    /// fold step until the trigger reports `NotNeeded`.
    ///
    /// The scenario needs a fold whose completing step lands in a drain
    /// iteration with no build publish, which the default budgets only
    /// produce past ~131k index rows; the injected policy shapes the same
    /// state at test scale, driving the exact drain the background path
    /// runs.
    #[tokio::test]
    async fn grams_drain_continues_across_a_fold_tier_transition() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store: crate::SharedObjectStore = std::sync::Arc::new(
            loonfs_objectstore::local_fs_store::LocalFsStore::new(temp_dir.path())
                .expect("local store"),
        );
        let namespace_id = crate::NamespaceId::parse("grams-drain-tiers").expect("namespace id");
        let writer = crate::FsWriter::builder_with_store(store.clone())
            .writer_id("grams-drain-writer")
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, crate::CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/alpha.txt",
                b"alpha shared\n",
                crate::PutFileOptions::default(),
            )
            .await
            .expect("write alpha");
        writer
            .core()
            .enable_grams_index(&namespace_id)
            .await
            .expect("enable");

        // One existing mid run, so the in-flight delta fold's completion
        // is exactly what crosses the two-run mid threshold.
        let engine = writer.core().namespace_engine(&namespace_id);
        drain_grams_builds(&engine).await;
        drive_grams_fold_to_completion(
            &engine,
            loonfs_core::GramIndexBuildPolicy {
                max_l0_runs: 1,
                ..loonfs_core::GramIndexBuildPolicy::default()
            },
        )
        .await;

        // Two fresh delta runs, then a delta fold parked mid-walk by a
        // one-row step budget — and no build backlog left, so no drain
        // iteration after this publishes a build unit.
        for path in ["/bravo.txt", "/charlie.txt"] {
            writer
                .put_file_bytes(
                    &namespace_id,
                    path,
                    format!("{path} shared\n").as_bytes(),
                    crate::PutFileOptions::default(),
                )
                .await
                .expect("write file");
            drain_grams_builds(&engine).await;
        }
        let parked = engine
            .fold_grams_index_step(
                loonfs_core::GramIndexBuildPolicy {
                    max_l0_runs: 2,
                    max_fold_rows_per_step: 1,
                    ..loonfs_core::GramIndexBuildPolicy::default()
                },
                None,
            )
            .await
            .expect("first fold step");
        assert!(
            matches!(
                parked.outcome,
                loonfs_core::GramIndexFoldOutcome::StepPublished {
                    completed: false,
                    ..
                }
            ),
            "a one-row budget must park the delta fold mid-walk, got {:?}",
            parked.outcome
        );
        let levels = grams_segment_levels(&store, &namespace_id).await;
        assert!(
            !levels.contains(&2),
            "no base may exist before the drain, got levels {levels:?}"
        );

        // One drain must finish the parked delta fold, discover that its
        // completion tipped the mid threshold, and run the base fold —
        // with no further write or explicit tick.
        writer
            .core()
            .drain_grams_index_backlog_with_policy(
                &namespace_id,
                loonfs_core::GramIndexBuildPolicy {
                    max_l0_runs: 2,
                    max_mid_runs: 2,
                    ..loonfs_core::GramIndexBuildPolicy::default()
                },
            )
            .await
            .expect("drain");
        let levels = grams_segment_levels(&store, &namespace_id).await;
        assert!(
            !levels.is_empty() && levels.iter().all(|level| *level == 2),
            "the drain must cross the tier transition into the base fold, \
             got levels {levels:?}"
        );

        writer.shutdown_background().await.expect("writer shutdown");
    }

    #[test]
    fn explicit_commit_facade_exports_constructor_types() {
        let display_name = DisplayName::parse("Report.txt").expect("valid display name");
        let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
        let precondition = CommitPrecondition::BindingIs {
            parent_inode_id: InodeId(1),
            name_key,
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(3),
            bind_delta_index: 4,
        };

        let request = CommitRequest {
            commit_id: CommitId::generate(),
            preconditions: vec![precondition],
            ops: vec![
                CommitOp::RestoreRevision {
                    inode_id: InodeId(2),
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                },
                CommitOp::Rename {
                    inode_id: InodeId(2),
                    new_parent_inode_id: InodeId(1),
                    new_display_name: "report.txt".to_owned(),
                },
            ],
            message: None,
        };

        assert_eq!(request.preconditions.len(), 1);
        assert_eq!(request.ops.len(), 2);
    }
}
