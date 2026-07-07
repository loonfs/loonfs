//! Purpose-specific runtime handles.
//!
//! One handle per job, each opened asynchronously inside the Tokio runtime
//! that will drive it:
//!
//! - [`FsWriter`] mutates namespaces and optionally schedules non-destructive
//!   maintenance after writes, controlled by
//!   [`FsBackgroundWork`](crate::FsBackgroundWork).
//! - [`FsReader`] serves latest-view reads. It owns no writer session and
//!   starts no maintenance.
//! - [`FsAdmin`] runs explicit maintenance: status, checkpoints, retention
//!   advancement, and garbage collection, always as one-shot calls in the
//!   caller's task.
//!
//! Builders prefer [`StoreConfig`](crate::StoreConfig) so the object-store
//! client is constructed inside the handle's runtime ownership domain. The
//! `builder_with_store` escape hatches are for callers who know the store is
//! safe in that domain — do not use them to share one provider client across
//! unrelated runtimes; open another handle from config instead.

mod admin;
mod reader;
mod writer;

pub use admin::{FsAdmin, FsAdminBuilder};
pub use reader::{FsReader, FsReaderBuilder};
pub use writer::{FsWriter, FsWriterBuilder};

use crate::background::BackgroundWork;
use crate::config::FsConfig;
use crate::fs::FsCore;
use crate::{
    ObjectStoreMetricsRecorder, Result, RuntimeCacheConfig, RuntimeError, SharedObjectStore,
    StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use std::sync::Arc;

/// Where a handle's object-store client comes from.
enum StoreSource {
    /// Built from configuration inside the handle's runtime ownership domain.
    Config(StoreConfig),
    /// Supplied by the caller, who owns the sharing decision.
    Shared(SharedObjectStore),
}

/// Builder state every handle shares: the store source, cache sizing, and
/// trace/metrics wiring.
struct HandleBuilderCore {
    source: StoreSource,
    runtime_cache: RuntimeCacheConfig,
    trace_mode: TraceMode,
    trace_store_kind: Option<TraceStoreKind>,
    metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
}

impl HandleBuilderCore {
    fn from_config(store_config: StoreConfig) -> Self {
        Self::new(StoreSource::Config(store_config))
    }

    fn from_store(store: SharedObjectStore) -> Self {
        Self::new(StoreSource::Shared(store))
    }

    fn new(source: StoreSource) -> Self {
        Self {
            source,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Embedded,
            trace_store_kind: None,
            metrics_recorder: None,
        }
    }

    fn open(
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
                runtime_cache: self.runtime_cache,
                trace_mode: self.trace_mode,
                trace_store_kind,
            },
            background,
        )
    }
}

/// Resolves the runtime that will own a handle's background tasks.
///
/// Handles are opened inside the runtime that owns them, so the current
/// runtime is the owner. Building outside a runtime is a configuration
/// error, not a panic.
fn owning_runtime() -> Result<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        RuntimeError::Config(
            "handle builders must run inside the Tokio runtime that will own the handle".to_owned(),
        )
    })
}
