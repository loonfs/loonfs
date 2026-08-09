//! The administrative and maintenance runtime handle.

use super::HandleBuilderCore;
use crate::fs::{ReadCore, WriterIdentity};
use crate::maintenance_runner::BackgroundCompactions;
use crate::metrics::{MetricsRecorder, ObjectStoreMetricsRecorder};
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
    /// A tail projection to invalidate exists only where a publisher does,
    /// so a standalone admin holds `None`, and an admin built over a
    /// writer's runtime for background maintenance holds that writer's
    /// registry.
    pub(crate) publisher: Option<PublisherRegistry>,
    /// The streaming metadata compactions a writer's maintenance runner is
    /// running, when this handle has a writer behind it.
    ///
    /// A maintenance step consults it twice: to leave alone the group a job
    /// is rebuilding, and to start a job when it plans one. A handle with no
    /// writer behind it holds `None`, schedules no background work of any
    /// kind, and reports the plan instead of starting it.
    pub(crate) compactions: Option<BackgroundCompactions>,
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
        compactions: BackgroundCompactions,
    ) -> Self {
        Self {
            core,
            actor,
            publisher: Some(publisher),
            compactions: Some(compactions),
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
    compactions: Option<BackgroundCompactions>,
}

impl FsAdminBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            actor_id: None,
            compactions: None,
        }
    }

    /// Sets the actor id recorded by admin operations that mutate durable
    /// control state. Required.
    pub fn actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
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

    /// Lets this handle's maintenance steps start background work on
    /// `writer`'s maintenance runner instead of declining it.
    ///
    /// One kind of upkeep does not fit in a step: a family group that has
    /// outgrown one step's budgets is rebuilt by a streaming compaction that
    /// runs for as long as it takes and publishes once at the end. Without
    /// this, an explicit step reports that the group needs one and starts
    /// nothing, because a handle that owns no background work has nowhere to
    /// run it. With it, an operator's step starts the same job the writer's
    /// own steps start, the two share the one-job-per-namespace rule, and
    /// `writer`'s shutdown cancels and drains it.
    ///
    /// Only for a writer in the same runtime ownership domain, like
    /// [`Self::shared_metadata_table_cache`].
    pub fn background_maintenance(mut self, writer: &super::FsWriter) -> Self {
        self.compactions = Some(writer.background_compactions());
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

    /// Installs raw object-store sample collection for this handle.
    ///
    /// Combines with [`Self::metrics_recorder`]: one wrapper feeds both.
    pub fn object_store_metrics_recorder(
        mut self,
        recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    ) -> Self {
        self.core.object_store_metrics_recorder = Some(recorder);
        self
    }

    /// Installs the metrics recorder this handle reports its instruments to
    /// (see [`crate::metrics`]). An admin registers the object-store and
    /// collection instruments; the explicit maintenance it drives reports
    /// through the writer that scheduled it.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the admin handle inside the Tokio runtime that will drive its
    /// one-shot maintenance calls.
    pub async fn build(self) -> Result<FsAdmin> {
        let actor_id = self
            .actor_id
            .ok_or_else(|| RuntimeError::Config("actor_id is required".to_owned()))?;
        let actor = WriterIdentity::new(actor_id)?;
        Ok(FsAdmin {
            core: self.core.open_read_core()?,
            actor,
            publisher: None,
            compactions: self.compactions,
        })
    }
}
