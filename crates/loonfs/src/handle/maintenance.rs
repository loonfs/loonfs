//! The maintenance runtime handle.

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

/// Maintenance handle: namespace diagnostics, operator checkpoints, WAL flush,
/// metadata reorganization and compaction, garbage collection, and retention.
/// Each call runs in the caller's task; the handle starts no background work.
/// Operations that mutate durable control state record the builder's `actor_id`.
#[derive(Clone)]
pub struct FsMaintenance {
    pub(crate) core: ReadCore,
    pub(crate) actor: WriterIdentity,
    pub(crate) writer: Option<WriterParts>,
    /// A narrowed per-step row budget for the tests that need a family group
    /// whose base run no bounded step can fold. See
    /// [`Self::starve_reorganization_row_budget`].
    #[cfg(test)]
    pub(crate) reorganization_row_budget: Option<std::num::NonZeroUsize>,
}

#[derive(Clone)]
pub(crate) struct WriterParts {
    pub(crate) publisher: PublisherRegistry,
    pub(crate) compactions: BackgroundCompactions,
}

impl FsMaintenance {
    /// Starts a maintenance builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsMaintenanceBuilder {
        FsMaintenanceBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a maintenance builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsMaintenanceBuilder {
        FsMaintenanceBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Builds a maintenance handle over a writer's own runtime: its read core, its
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
            writer: Some(WriterParts {
                publisher,
                compactions,
            }),
            #[cfg(test)]
            reorganization_row_budget: None,
        }
    }

    /// Narrows the rows one reorganization step this handle drives may
    /// decode, so a namespace a test can build in seconds ends up with a base
    /// run no bounded step can fold.
    ///
    /// Test-only, and the one shipped number that has to move to reach that
    /// state: planning, running, and publishing the job are the shipped path
    /// either way.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn starve_reorganization_row_budget(
        mut self,
        max_decoded_input_rows_per_step: std::num::NonZeroUsize,
    ) -> Self {
        self.reorganization_row_budget = Some(max_decoded_input_rows_per_step);
        self
    }

    /// Snapshots the runtime cache counters, so maintenance work driven
    /// through this handle is observable alongside writer and reader work.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    // Maintenance operations live in `fs/maintenance.rs`.
}

/// Builder for [`FsMaintenance`].
#[must_use]
pub struct FsMaintenanceBuilder {
    core: HandleBuilderCore,
    actor_id: Option<String>,
    writer: Option<WriterParts>,
}

impl FsMaintenanceBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            actor_id: None,
            writer: None,
        }
    }

    /// Sets the actor id recorded by maintenance operations that mutate durable
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

    /// Connects this maintenance handle to a writer from the same runtime.
    ///
    /// The maintenance handle shares the writer's decoded-block cache, publisher, and
    /// background compactions. The shared cache keeps the writer's byte
    /// budget; [`Self::runtime_cache`] still configures this handle's other
    /// caches.
    pub fn over_writer(mut self, writer: &super::FsWriter) -> Self {
        self.core.shared_metadata_segment_cache = Some(writer.metadata_segment_cache());
        self.writer = Some(WriterParts {
            publisher: writer.publisher(),
            compactions: writer.background_compactions(),
        });
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
    /// (see [`crate::metrics`]). A maintenance handle registers the object-store and
    /// collection instruments; the explicit maintenance it drives reports
    /// through the writer that scheduled it.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the maintenance handle inside the Tokio runtime that will drive its
    /// one-shot maintenance calls.
    pub async fn build(self) -> Result<FsMaintenance> {
        let actor_id = self
            .actor_id
            .ok_or_else(|| RuntimeError::Config("actor_id is required".to_owned()))?;
        let actor = WriterIdentity::new(actor_id)?;
        Ok(FsMaintenance {
            core: self.core.open_read_core()?,
            actor,
            writer: self.writer,
            #[cfg(test)]
            reorganization_row_budget: None,
        })
    }
}
