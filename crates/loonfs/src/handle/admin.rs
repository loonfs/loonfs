//! The administrative and maintenance runtime handle.

use super::HandleBuilderCore;
use crate::background::BackgroundWork;
use crate::config::default_writer_version;
use crate::fs::FsCore;
use crate::metrics::ObjectStoreMetricsRecorder;
use crate::{
    AdvanceRetentionResponse, CheckpointId, CreateCheckpointOptions, CreateCheckpointResponse,
    DisableGramsIndexResponse, EnableGramsIndexResponse, FlushWalResponse, GcConfig, GcReport,
    MaintenanceTickOptions, MaintenanceTickResult, NamespaceId, NamespaceStatusResponse,
    ReleaseCheckpointResponse, RepairNamespaceResponse, Result, RuntimeCacheConfig,
    RuntimeCacheStats, RuntimeError, SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use std::sync::Arc;

/// Administrative and maintenance handle.
///
/// `FsAdmin` owns the explicit maintenance surface: namespace status and
/// inspection, checkpoint creation, retention advancement, garbage
/// collection, incomplete-installation repair, and one-shot maintenance
/// ticks. Every call runs in the caller's async task — the admin handle starts
/// no workers of its own.
///
/// Admin operations that mutate durable control state carry the builder's
/// `actor_id` for tracing, reports, and auditability.
#[derive(Clone)]
pub struct FsAdmin {
    core: FsCore,
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

    /// Snapshots the runtime cache counters, so maintenance work driven
    /// through this handle is observable alongside writer and reader work.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
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

    /// Enables the independent grep root for a namespace. Backfill and
    /// incremental upkeep are driven by `loonfs-grep`'s explicit worker.
    /// Idempotent.
    pub async fn enable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGramsIndexResponse> {
        self.core.enable_grams_index(namespace_id).await
    }

    /// Disables the independent grep root. Grep-owned garbage collection
    /// later reclaims its unreferenced segments. Idempotent.
    pub async fn disable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGramsIndexResponse> {
        self.core.disable_grams_index(namespace_id).await
    }

    /// Creates or reuses a named checkpoint pinning the current namespace
    /// head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance,
    /// and is a garbage-collection root until released or expired. If the
    /// current head has no manifest yet, one is published first for the
    /// current durable namespace state; this is not a request to compact
    /// metadata.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        options: CreateCheckpointOptions,
    ) -> Result<CreateCheckpointResponse> {
        self.core.create_checkpoint(namespace_id, options).await
    }

    /// Releases a user-owned checkpoint by id.
    ///
    /// Idempotent: releasing an already-released or reaped record succeeds.
    /// The record is reaped by a later garbage-collection pass; its basis
    /// becomes collectable only on the pass after that.
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        self.core
            .release_checkpoint(namespace_id, checkpoint_id)
            .await
    }

    /// Flushes the WAL tail and advances the metadata root to a manifest
    /// covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the
    /// visible WAL tail into a published manifest and advances
    /// `metadata/root.json`, creating no checkpoint record. Superseded
    /// manifests become garbage-collection candidates once nothing pins
    /// them.
    pub async fn flush_wal(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse> {
        self.core.flush_wal(namespace_id).await
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

    /// Repairs one incomplete namespace installation explicitly.
    ///
    /// Completable post-head installs receive their missing completion
    /// descriptor. Non-completable debris is reaped only after the fixed
    /// repair safety window; younger debris is reported as in flight.
    pub async fn repair_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<RepairNamespaceResponse> {
        self.core.repair_namespace(namespace_id).await
    }

    /// Shuts down handle-owned background work. Admin calls are one-shot in
    /// the caller's task, so this settles immediately; it exists so every
    /// handle shares one shutdown shape.
    pub async fn shutdown_background(&self) -> Result<()> {
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

    /// Shares `writer`'s decoded-block cache with this handle instead of
    /// opening a separate one, so explicit maintenance reuses blocks that
    /// writer-side reads already decoded — and warms them back. Sound
    /// because entries are keyed by immutable identities; only for a
    /// writer in the same runtime ownership domain. The cache keeps the
    /// writer's byte budget: [`Self::runtime_cache`] still sizes this
    /// handle's other caches, but its decoded-block budget goes unused.
    pub fn shared_metadata_table_cache(mut self, writer: &super::FsWriter) -> Self {
        self.core.shared_metadata_table_cache = Some(writer.core().metadata_table_cache());
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
        let background = BackgroundWork::inert();
        Ok(FsAdmin {
            core: self.core.open(actor_id, self.actor_version, background)?,
        })
    }
}
