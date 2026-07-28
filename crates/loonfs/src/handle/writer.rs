//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::background::{BackgroundWork, FsBackgroundWork};
use crate::config::default_writer_version;
use crate::fs::{ReadCore, WriterBits, WriterIdentity};
use crate::metrics::ObjectStoreMetricsRecorder;
use crate::publisher::{PublishObserver, PublisherRegistry};
use crate::{
    CapabilityDocument, ChangeSeq, NamespaceId, Result, RuntimeCacheConfig, RuntimeCacheStats,
    RuntimeError, SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_core::cache::MetadataTableCache;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Write-capable handle for normal application and server use.
///
/// `FsWriter` owns a writer session identity and the full mutation surface:
/// file and directory mutations, commit publication, uploads, and namespace
/// lifecycle. With [`FsBackgroundWork::Enabled`] it may also schedule
/// non-destructive maintenance after writes, spawned on its owning runtime.
/// Retention advancement and garbage collection stay explicit
/// [`FsAdmin`](crate::FsAdmin) work.
///
/// The handle is runtime-bound: open it with `build().await` inside the
/// long-lived Tokio runtime that will drive it, and do not share one writer
/// across unrelated runtimes — open another from [`StoreConfig`] instead.
/// `FsWriter` is cheap to clone; clones share the session, caches, and
/// background-work state.
#[derive(Clone)]
pub struct FsWriter {
    pub(crate) core: ReadCore,
    /// The writer half of the runtime: the session identity, the
    /// background-work policy, and the publish observer. Publisher workers
    /// hold this weakly, so dropping every clone of this handle stops new
    /// publication work.
    pub(crate) bits: Arc<WriterBits>,
    pub(crate) publisher: PublisherRegistry,
}

impl FsWriter {
    /// Starts a writer builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a writer builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Derives a read-only handle over this writer's store and caches.
    ///
    /// The reader lives in the writer's runtime ownership domain and sees
    /// its cache updates immediately; use it for read paths in the same
    /// process, such as a server's read endpoints. For a reader driven by a
    /// different runtime, open one with [`FsReader::builder`].
    pub fn reader(&self) -> FsReader {
        FsReader::from_read_core(self.core.clone())
    }

    /// This writer's shared decoded-block cache handle, for builders that
    /// open another core sharing it.
    pub(crate) fn metadata_table_cache(&self) -> Arc<MetadataTableCache> {
        self.core.metadata_table_cache()
    }

    /// This writer's object-store client, instrumented exactly as the
    /// handle's own traffic is.
    ///
    /// Server integrations that read LoonFS-owned objects outside the handle
    /// surface — the grep root and the grep worker's keyspace — use this so
    /// their requests are measured like every other request instead of
    /// escaping instrumentation on a second, raw client.
    pub fn object_store(&self) -> SharedObjectStore {
        self.core.shared_store()
    }

    /// This writer's publication service (see [`crate::publisher`]).
    ///
    /// The direct mutation methods on this handle submit through the same
    /// service; hosts that classify their own mutation candidates — the
    /// reference server, for example — submit here directly. Clones share
    /// the writer's per-namespace publishers.
    pub fn publisher(&self) -> PublisherRegistry {
        self.publisher.clone()
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

    // Namespace lifecycle lives in `fs/namespaces.rs`; mutation, commit,
    // and upload operations in `fs/writes.rs` and `fs/uploads.rs`.

    /// Waits until every writer-scheduled maintenance task has finished,
    /// without closing the handle. Panicked tasks surface as a runtime-task
    /// error.
    pub async fn wait_for_background_work(&self) -> Result<()> {
        self.bits.background.drain().await
    }

    /// Shuts down writer-scheduled background work: settles admitted
    /// publications whose callers are gone, then rejects new maintenance
    /// scheduling and waits for in-flight maintenance tasks to settle,
    /// surfacing panics. Work that claimed its maintenance slot but has not
    /// started when the shutdown lands is refused, never left running
    /// unobserved.
    ///
    /// Foreground calls remain usable afterward; this settles only
    /// handle-owned background work, and with
    /// [`FsBackgroundWork::ManualOnly`] it is nearly trivial. For a
    /// terminal shutdown that also refuses later submissions with
    /// `shutting_down`, call
    /// [`PublisherRegistry::close_admission`](crate::publisher::PublisherRegistry::close_admission)
    /// via [`Self::publisher`] first, as the reference server does.
    /// Dropping the handle without calling this is best-effort cleanup,
    /// not the documented graceful shutdown path.
    pub async fn shutdown_background(&self) -> Result<()> {
        // Publications first — they can schedule the maintenance the second
        // step settles. Draining without closing admission keeps the
        // handle usable; a caller that keeps submitting concurrently just
        // keeps the drain waiting.
        self.publisher.drain().await?;
        self.bits.background.shut_down();
        self.bits.background.drain().await
    }
}

/// Builder for [`FsWriter`].
pub struct FsWriterBuilder {
    core: HandleBuilderCore,
    writer_id: Option<String>,
    writer_version: String,
    background_work: FsBackgroundWork,
    max_concurrent_maintenance: usize,
    min_publish_interval_ms: u64,
    publish_observer: Option<PublishObserver>,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            writer_version: default_writer_version(),
            background_work: FsBackgroundWork::ManualOnly,
            max_concurrent_maintenance: crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            publish_observer: None,
        }
    }

    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the writer version used in mutation context.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Sets the writer's background maintenance policy.
    ///
    /// Defaults to [`FsBackgroundWork::ManualOnly`]: right for CLIs, tests,
    /// scripts, and short-lived embedders. Long-lived servers should opt
    /// into [`FsBackgroundWork::Enabled`] explicitly.
    pub fn background_work(mut self, background_work: FsBackgroundWork) -> Self {
        self.background_work = background_work;
        self
    }

    /// Caps how many writer-scheduled maintenance steps may run at once
    /// across all namespaces this writer serves. Each namespace already runs
    /// at most one step at a time; this bounds the fan-out when many
    /// namespaces cross their thresholds together. Requests beyond the cap
    /// are coalesced by namespace and run as permits become available. Defaults to
    /// [`crate::DEFAULT_MAX_CONCURRENT_MAINTENANCE`]. The limit must be
    /// greater than zero; [`FsBackgroundWork::ManualOnly`] is the only way
    /// to disable scheduling.
    pub fn max_concurrent_maintenance(mut self, max_concurrent_maintenance: usize) -> Self {
        self.max_concurrent_maintenance = max_concurrent_maintenance;
        self
    }

    /// Caps the file content size the buffered read APIs will materialize
    /// for one call, checked against resolved metadata before any content
    /// fetch; over-limit reads fail with `content_too_large`. Unset by
    /// default: embedded callers read files of any size. Servers set this
    /// so one proxied read cannot buffer arbitrarily large content.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = Some(max_read_content_bytes);
        self
    }

    /// Sets the minimum interval between publication starts per namespace,
    /// in milliseconds (see [`crate::publisher`]).
    ///
    /// A cold namespace publishes immediately; the interval only paces
    /// follow-up batches, so concurrent publishes amortize into fewer,
    /// larger WAL segments — with each caller still awaiting its own
    /// durable, visible result. Defaults to 15 ms; zero keeps only the
    /// batching that in-flight publications force.
    pub fn min_publish_interval_ms(mut self, min_publish_interval_ms: u64) -> Self {
        self.min_publish_interval_ms = min_publish_interval_ms;
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
    ///
    /// The handle wraps its object store before opening; callers do not
    /// need to construct an instrumented store manually.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Registers a synchronous observer called after each successful durable
    /// publication with the namespace and the batch's highest committed
    /// sequence.
    ///
    /// The observer runs on the publication task and must not block. Use a
    /// non-blocking channel send to hand work to another task. Writers that
    /// register no observer retain the default publication behavior.
    pub fn publish_observer(
        mut self,
        observer: impl Fn(&NamespaceId, ChangeSeq) + Send + Sync + 'static,
    ) -> Self {
        self.publish_observer = Some(Arc::new(observer));
        self
    }

    /// Opens the writer inside the Tokio runtime that will own it. Any
    /// background work the writer schedules is spawned on that runtime.
    ///
    /// Construction runs one way only, so nothing here is cyclic: the read
    /// core opens first, the writer's bits are built on top of it, and the
    /// publication service is created last, holding the core strongly and
    /// the bits weakly.
    pub async fn build(self) -> Result<FsWriter> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let identity = WriterIdentity::new(writer_id, self.writer_version)?;
        let max_concurrent_maintenance = NonZeroUsize::new(self.max_concurrent_maintenance)
            .ok_or_else(|| {
                RuntimeError::Config(
                    "`max_concurrent_maintenance` must be greater than zero; \
                     `FsBackgroundWork::ManualOnly` disables scheduling"
                        .to_owned(),
                )
            })?;
        let background = Arc::new(BackgroundWork::new(
            self.background_work,
            Some(owning_runtime()?),
            max_concurrent_maintenance,
        ));
        let core = self.core.open_read_core()?;
        let bits = Arc::new(WriterBits {
            identity,
            background,
            publish_observer: self.publish_observer,
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            std::time::Duration::from_millis(self.min_publish_interval_ms),
            core.trace_mode(),
            core.trace_store_kind(),
        );
        Ok(FsWriter {
            core,
            bits,
            publisher,
        })
    }
}
