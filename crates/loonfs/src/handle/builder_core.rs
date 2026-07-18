//! Shared plumbing for the three handle builders: where the store comes
//! from, the common builder state, and the runtime the handles bind to.

use crate::background::BackgroundWork;
use crate::config::FsConfig;
use crate::fs::FsCore;
use crate::{
    GramIndexBuildPolicy, ObjectStoreMetricsRecorder, Result, RuntimeCacheConfig, RuntimeError,
    SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_core::cache::MetadataTableCache;
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use std::sync::Arc;

/// Where a handle's object-store client comes from.
pub(super) enum StoreSource {
    /// Built from configuration inside the handle's runtime ownership domain.
    Config(StoreConfig),
    /// Supplied by the caller, who owns the sharing decision.
    Shared(SharedObjectStore),
}

/// Builder state every handle shares: the store source, cache sizing, and
/// trace/metrics wiring.
pub(super) struct HandleBuilderCore {
    pub(super) source: StoreSource,
    pub(super) min_publish_interval_ms: u64,
    pub(super) max_read_content_bytes: Option<u64>,
    pub(super) runtime_cache: RuntimeCacheConfig,
    pub(super) gram_index_build: GramIndexBuildPolicy,
    /// An existing decoded-block cache to share instead of sizing a fresh
    /// one from `runtime_cache`; see [`FsCore::open_with_background`].
    pub(super) shared_metadata_table_cache: Option<Arc<MetadataTableCache>>,
    pub(super) trace_mode: TraceMode,
    pub(super) trace_store_kind: Option<TraceStoreKind>,
    pub(super) metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
}

impl HandleBuilderCore {
    pub(super) fn from_config(store_config: StoreConfig) -> Self {
        Self::new(StoreSource::Config(store_config))
    }

    pub(super) fn from_store(store: SharedObjectStore) -> Self {
        Self::new(StoreSource::Shared(store))
    }

    pub(super) fn new(source: StoreSource) -> Self {
        Self {
            source,
            min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            max_read_content_bytes: None,
            runtime_cache: RuntimeCacheConfig::default(),
            gram_index_build: GramIndexBuildPolicy::default(),
            shared_metadata_table_cache: None,
            trace_mode: TraceMode::Embedded,
            trace_store_kind: None,
            metrics_recorder: None,
        }
    }

    pub(super) fn open(
        self,
        writer_id: String,
        writer_version: String,
        background: BackgroundWork,
    ) -> Result<FsCore> {
        let (store, derived_kind) = match self.source {
            StoreSource::Config(config) => {
                let kind = TraceStoreKind::from(config.kind());
                let store = config.configured_object_store().map_err(|error| {
                    RuntimeError::Config(format!("invalid store config: {error}"))
                })?;
                (Arc::new(store) as SharedObjectStore, kind)
            }
            StoreSource::Shared(store) => (store, TraceStoreKind::Unknown),
        };
        let trace_store_kind = self.trace_store_kind.unwrap_or(derived_kind);
        let store = match self.metrics_recorder {
            Some(recorder) => Arc::new(
                InstrumentedObjectStore::new(store, recorder).store_kind(trace_store_kind.as_str()),
            ) as SharedObjectStore,
            None => store,
        };
        FsCore::open_with_background(
            store,
            FsConfig {
                writer_id,
                writer_version,
                min_publish_interval_ms: self.min_publish_interval_ms,
                max_read_content_bytes: self.max_read_content_bytes,
                runtime_cache: self.runtime_cache,
                gram_index_build: self.gram_index_build,
                trace_mode: self.trace_mode,
                trace_store_kind,
            },
            background,
            self.shared_metadata_table_cache,
        )
    }
}

/// Resolves the runtime that will own a handle's background tasks.
///
/// Handles are opened inside the runtime that owns them, so the current
/// runtime is the owner. Building outside a runtime is a configuration
/// error, not a panic.
pub(super) fn owning_runtime() -> Result<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        RuntimeError::Config(
            "handle builders must run inside the Tokio runtime that will own the handle".to_owned(),
        )
    })
}
