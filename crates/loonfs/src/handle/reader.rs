//! The read-only runtime handle.

use super::HandleBuilderCore;
use crate::background::BackgroundWork;
use crate::config::default_writer_version;
use crate::fs::FsCore;
use crate::metrics::ObjectStoreMetricsRecorder;
use crate::{
    CapabilityDocument, Result, RuntimeCacheConfig, RuntimeCacheStats, SharedObjectStore,
    StoreConfig, TraceMode, TraceStoreKind,
};
use std::sync::Arc;

/// Read-only handle for latest namespace views.
///
/// `FsReader` serves stat, list, read, revision, and change-feed queries. It
/// owns no writer session, publishes nothing, and never schedules
/// maintenance, so read-only workers cannot accidentally participate in
/// writer scheduling. Reads revalidate cached control state against durable
/// state, so a standalone reader stays consistent without any writer
/// coordination.
///
/// The handle is runtime-bound: open it with `build().await` inside the
/// Tokio runtime that will drive its reads. `FsReader` is cheap to clone.
#[derive(Clone)]
pub struct FsReader {
    pub(crate) core: FsCore,
}

impl FsReader {
    /// Starts a reader builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsReaderBuilder {
        FsReaderBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a reader builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsReaderBuilder {
        FsReaderBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Wraps a shared runtime core; used by [`FsWriter::reader`](crate::FsWriter::reader).
    pub(super) fn from_core(core: FsCore) -> Self {
        Self { core }
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn capabilities(&self) -> CapabilityDocument {
        self.core.capabilities()
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    // Read operations live in `fs/reads.rs`.

    /// Shuts down handle-owned background work. Readers own none, so this
    /// settles immediately; it exists so every handle shares one shutdown
    /// shape.
    pub async fn shutdown_background(&self) -> Result<()> {
        Ok(())
    }
}

/// Builder for [`FsReader`].
pub struct FsReaderBuilder {
    core: HandleBuilderCore,
}

impl FsReaderBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self { core }
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Caps the file content size the buffered read APIs will materialize
    /// for one call, checked against resolved metadata before any content
    /// fetch; over-limit reads fail with `content_too_large`. Unset by
    /// default: embedded callers read files of any size.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = Some(max_read_content_bytes);
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.core.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    ///
    /// Config-built stores derive this automatically; setting it overrides
    /// the derived label.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.core.trace_store_kind = Some(trace_store_kind);
        self
    }

    /// Installs object-store metrics collection for this handle.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the reader inside the Tokio runtime that will drive its reads.
    pub async fn build(self) -> Result<FsReader> {
        // Readers never mutate, so the engine identity below is inert; it
        // exists because shared internals require a non-empty actor.
        let background = BackgroundWork::inert();
        Ok(FsReader {
            core: self.core.open(
                "loonfs-reader".to_owned(),
                default_writer_version(),
                background,
            )?,
        })
    }
}
