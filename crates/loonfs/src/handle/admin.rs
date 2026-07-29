//! The administrative and maintenance runtime handle.

use super::HandleBuilderCore;
use crate::config::default_writer_version;
use crate::fs::{ReadCore, WriterIdentity};
use crate::metrics::ObjectStoreMetricsRecorder;
use crate::publisher::PublisherRegistry;
use crate::{
    Result, RuntimeCacheConfig, RuntimeCacheStats, RuntimeError, SharedObjectStore, StoreConfig,
    TraceMode, TraceStoreKind,
};
use std::sync::Arc;

/// Administrative and maintenance handle.
///
/// `FsAdmin` owns the explicit maintenance surface: namespace status and
/// inspection, checkpoint creation, retention advancement, garbage
/// collection, and one-shot maintenance steps. Every call runs in the
/// caller's async task — the admin handle starts no workers of its own.
///
/// Admin operations that mutate durable control state carry the builder's
/// `actor_id` for tracing, reports, and auditability.
#[derive(Clone)]
pub struct FsAdmin {
    pub(crate) core: ReadCore,
    pub(crate) actor: WriterIdentity,
    /// `invalidate_engine` matters only where a publisher exists for the
    /// namespace, so a standalone admin holds `None` — its
    /// engines-to-invalidate do not exist — and an admin built over a
    /// writer's runtime for background maintenance holds that writer's
    /// registry.
    pub(crate) publisher: Option<PublisherRegistry>,
}

impl FsAdmin {
    /// Starts an admin builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsAdminBuilder {
        FsAdminBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts an admin builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsAdminBuilder {
        FsAdminBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Builds an admin over a writer's own runtime: its read core, its
    /// identity, and its publication service. Writer-scheduled background
    /// maintenance uses this so it runs the same operations an operator
    /// runs — and so its invalidations reach the writer's caches and
    /// publisher engines rather than a private copy of them.
    pub(crate) fn from_writer_parts(
        core: ReadCore,
        actor: WriterIdentity,
        publisher: PublisherRegistry,
    ) -> Self {
        Self {
            core,
            actor,
            publisher: Some(publisher),
        }
    }

    /// Snapshots the runtime cache counters, so maintenance work driven
    /// through this handle is observable alongside writer and reader work.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    // Maintenance operations live in `fs/maintenance.rs`.
}

/// Builder for [`FsAdmin`].
pub struct FsAdminBuilder {
    core: HandleBuilderCore,
    actor_id: Option<String>,
    actor_version: String,
}

impl FsAdminBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            actor_id: None,
            actor_version: default_writer_version(),
        }
    }

    /// Sets the actor id recorded by admin operations that mutate durable
    /// control state. Required.
    pub fn actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    /// Sets the actor version recorded in mutation context.
    pub fn actor_version(mut self, actor_version: impl Into<String>) -> Self {
        self.actor_version = actor_version.into();
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Shares `writer`'s decoded-block cache with this handle instead of
    /// opening a separate one, so explicit maintenance reuses blocks that
    /// writer-side reads already decoded — and warms them back. Sound
    /// because entries are keyed by immutable identities; only for a
    /// writer in the same runtime ownership domain. The cache keeps the
    /// writer's byte budget: [`Self::runtime_cache`] still sizes this
    /// handle's other caches, but its decoded-block budget goes unused.
    pub fn shared_metadata_table_cache(mut self, writer: &super::FsWriter) -> Self {
        self.core.shared_metadata_table_cache = Some(writer.metadata_table_cache());
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

    /// Opens the admin handle inside the Tokio runtime that will drive its
    /// one-shot maintenance calls.
    pub async fn build(self) -> Result<FsAdmin> {
        let actor_id = self
            .actor_id
            .ok_or_else(|| RuntimeError::Config("actor_id is required".to_owned()))?;
        let actor = WriterIdentity::new(actor_id, self.actor_version)?;
        Ok(FsAdmin {
            core: self.core.open_read_core()?,
            actor,
            publisher: None,
        })
    }
}
