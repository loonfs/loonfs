//! The administrative and maintenance runtime handle.

use super::HandleBuilderCore;
use crate::background::{BackgroundWork, FsBackgroundWork};
use crate::config::default_writer_version;
use crate::fs::Fs;
use crate::{
    AdvanceRetentionResponse, CreateCheckpointResponse, GcConfig, GcReport, MaintenanceTickOptions,
    MaintenanceTickResult, NamespaceId, NamespaceStatusResponse, ObjectStoreMetricsRecorder,
    Result, RuntimeCacheConfig, RuntimeError, SharedObjectStore, StoreConfig, TraceMode,
    TraceStoreKind,
};
use std::sync::Arc;

/// Administrative and maintenance handle.
///
/// `FsAdmin` owns the explicit maintenance surface: namespace status and
/// inspection, checkpoint creation, retention advancement, garbage
/// collection, and one-shot maintenance ticks. Every call runs in the
/// caller's async task — the admin handle starts no workers of its own.
///
/// Admin operations that mutate durable control state carry the builder's
/// `actor_id` for tracing, reports, and auditability.
#[derive(Clone)]
pub struct FsAdmin {
    core: Fs,
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

    /// Summarizes a namespace's current head: manifest, latest checkpoint,
    /// WAL tail, and retention floor.
    pub async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        self.core.namespace_status(namespace_id).await
    }

    /// Runs one bounded maintenance step against a namespace.
    ///
    /// Publishes a checkpoint once the visible WAL tail reaches
    /// `options.max_wal_tail_segments`. Losing the head race or being
    /// superseded by another checkpoint is reported as an outcome, not an
    /// error.
    pub async fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        self.core
            .maintenance_tick_namespace(namespace_id, options)
            .await
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance. If
    /// the current head has no manifest yet, one is published first for the
    /// current durable namespace state; this is not a request to compact
    /// metadata.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        self.core.create_checkpoint(namespace_id).await
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        self.core.advance_retention_floor(namespace_id).await
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Never runs implicitly: callers opt in here or through
    /// [`MaintenanceTickOptions::gc`].
    pub async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &GcConfig,
    ) -> Result<GcReport> {
        self.core.gc_namespace(namespace_id, config).await
    }

    /// Closes the admin handle. Admin calls are one-shot in the caller's
    /// task, so this settles immediately; it exists so every handle shares
    /// one shutdown shape.
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
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
        let background = BackgroundWork::new(FsBackgroundWork::ManualOnly, None);
        Ok(FsAdmin {
            core: self.core.open(actor_id, self.actor_version, background)?,
        })
    }
}
