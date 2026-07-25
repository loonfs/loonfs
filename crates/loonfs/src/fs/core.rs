//! [`FsCore`]: the shared runtime state behind the handles, its
//! constructor, and the background-step bookkeeping.

use crate::background::BackgroundWork;
use crate::cache::{RuntimeCacheStatsInner, RuntimeControlCache};
use crate::config::{validate_config, FsConfig};
use crate::publisher::{PublishObserver, PublisherRegistry};
use crate::time::current_time_ms;
use crate::writer_session::WriterSessionRegistry;
use crate::{
    ChangeSeq, CoreError, ErrorCode, InodeId, ListFileRevisionsResponse, NamespaceId, ObjectStore,
    RuntimeCacheStats,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    encode_cursor, generated_id, CapabilityDocument, EffectiveLimit, FileRevision,
    FileRevisionsPageCursor, Page, PaginationPolicy, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_GC_MIN_GRACE_WINDOW_MS,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0,
    PROTOCOL_VERSION,
};
use loonfs_core::cache::{
    MetadataTableCache, WalTailProjectionCache, WalTailProjectionCacheConfig,
};
use loonfs_core::{MutationContext, NamespaceEngine};
use loonfs_grep::GrepService;
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
    pub(crate) grep_service: GrepService,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
    pub(crate) background: BackgroundWork,
    /// Optional synchronous notification after a mutation batch durably
    /// advances a namespace. Callers promise that it does not block.
    pub(crate) publish_observer: Option<PublishObserver>,
    /// The core's publication service: every mutation — direct handle
    /// calls and server submissions alike — publishes through it. It holds
    /// this core weakly, so the ownership does not cycle.
    pub(crate) publisher: PublisherRegistry,
    /// Session-owned writer state (acquired epochs, fencing), deliberately
    /// outside every rebuildable cache; see [`crate::writer_session`].
    pub(crate) writer_sessions: WriterSessionRegistry,
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
        publish_observer: Option<PublishObserver>,
    ) -> Result<Self> {
        validate_config(&config)?;
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
                grep_service: GrepService::new(),
                cache_stats: RuntimeCacheStatsInner::default(),
                background,
                publish_observer,
                publisher: PublisherRegistry::from_core(
                    weak.clone(),
                    min_publish_interval,
                    trace_mode,
                    trace_store_kind,
                ),
                writer_sessions: WriterSessionRegistry::default(),
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

    pub(super) fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
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
                    loonfs_grep::DEFAULT_GREP_PAGE_LIMIT as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_MAX.to_owned(),
                    loonfs_grep::MAX_GREP_PAGE_LIMIT as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES.to_owned(),
                    loonfs_grep::MAX_GREP_SCAN_FILES as u64,
                );
                limits.insert(
                    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES.to_owned(),
                    loonfs_grep::MAX_GREP_TAIL_FILES as u64,
                );
                limits.insert(
                    LIMIT_GC_MIN_GRACE_WINDOW_MS.to_owned(),
                    loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
                );
                limits.insert(
                    LIMIT_COMMIT_MAX_OPERATIONS.to_owned(),
                    loonfs_core::limits::MAX_COMMIT_OPERATIONS as u64,
                );
                limits
            },
        }
    }

    /// Snapshots the runtime cache counters.
    pub(crate) fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner.cache_stats.snapshot(
            self.inner.metadata_table_cache.stats(),
            self.inner.wal_tail_projection_cache.stats(),
        )
    }

    /// Waits until every scheduled background maintenance step has finished.
    ///
    /// Call this to quiesce before shutdown, or in tests that assert on
    /// post-maintenance state. Panicked steps surface as a runtime-task
    /// error.
    pub(crate) async fn wait_for_background_maintenance(&self) -> Result<()> {
        self.inner.background.drain().await
    }

    /// Rejects any further background maintenance scheduling.
    pub(crate) fn shut_down_background(&self) {
        self.inner.background.shut_down();
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

    pub(crate) fn mutation_context(&self) -> Result<MutationContext> {
        Ok(MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_session_id: self.inner.writer_session_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms()?,
        })
    }
}

/// Holds a background singleflight claim across a spawned step. Dropping —
/// on completion, panic, or a task discarded with its runtime — releases the
/// namespace for the next scheduling decision.
pub(super) struct BackgroundStepClaim {
    pub(super) fs: FsCore,
    pub(super) namespace_id: NamespaceId,
}

impl Drop for BackgroundStepClaim {
    fn drop(&mut self) {
        self.fs.inner.background.release(&self.namespace_id);
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
        _ => false,
    }
}

pub(super) fn default_page_limit() -> EffectiveLimit {
    PaginationPolicy::default()
        .resolve_limit(None)
        .expect("default pagination policy must resolve its default limit")
}

pub(super) fn file_revisions_page_response(
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
        .map(encode_cursor)
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
