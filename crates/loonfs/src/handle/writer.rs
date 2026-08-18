//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::fs::{ReadCore, WriterBits, WriterIdentity};
use crate::maintenance_runner::{register_core_jobs, MaintenanceRunner};
use crate::metrics::{MetricsRecorder, ObjectStoreMetricsRecorder};
use crate::publisher::{PublishObserver, PublisherRegistry};
use crate::{
    CapabilityDocument, ChangeSeq, FsBackgroundWork, MaintenanceHandle, MaintenanceJob,
    MaintenanceJobId, NamespaceId, Result, RuntimeCacheConfig, RuntimeCacheStats, RuntimeError,
    SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_core::cache::{MetadataTableCache, StoredMetadataBlockCache};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Write-capable runtime handle for applications and servers.
///
/// `FsWriter` owns the writer identity, mutations, uploads, namespace
/// lifecycle, and commit publication. With [`FsBackgroundWork::Enabled`], it
/// also schedules metadata maintenance and upload garbage collection.
/// Retention-floor advancement remains an explicit [`FsAdmin`](crate::FsAdmin)
/// operation.
///
/// Build the handle inside the Tokio runtime that will use it. Do not share a
/// provider client across unrelated runtimes; build another handle from
/// [`StoreConfig`]. Clones share identity, caches, background tasks, and
/// shutdown state.
#[derive(Clone)]
pub struct FsWriter {
    pub(crate) core: ReadCore,
    /// The writer half of the runtime: the writer identity, the maintenance
    /// runner, and the publish observer. Publisher workers hold this weakly,
    /// so dropping every clone of this handle stops new publication work.
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

    /// The streaming metadata compactions this writer's runner is running,
    /// for a handle built to share them.
    pub(crate) fn background_compactions(
        &self,
    ) -> crate::maintenance_runner::BackgroundCompactions {
        self.bits.maintenance.compactions()
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

    /// Returns this writer's shared publication service.
    ///
    /// Direct mutation methods and integrations that submit classified
    /// candidates use the same per-namespace publishers. Shutdown closes the
    /// service; callers should not manage its lifecycle separately.
    pub fn publisher(&self) -> PublisherRegistry {
        self.publisher.clone()
    }

    /// Closes maintenance and publication admission before shutdown drains.
    ///
    /// Later mutations fail with `shutting_down`, maintenance nudges are
    /// ignored, and work already admitted keeps running. Calling this more
    /// than once has no additional effect.
    pub fn close_admission_for_shutdown(&self) {
        self.bits.maintenance.close_admission();
        self.publisher.close_admission();
    }

    /// Whether [`Self::shutdown`] has begun on this writer or any clone of
    /// it.
    ///
    /// Mutations submitted from here on fail with `shutting_down`, so a
    /// readiness probe answers "draining" from this and a load balancer can
    /// take the instance out before its in-flight work settles.
    pub fn is_shutting_down(&self) -> bool {
        self.publisher.is_admission_closed()
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

    /// A nudge-only view of this writer's maintenance runner.
    ///
    /// Hosts that compose an extension — a derived index, say — hand this to
    /// whatever observes change, so their triggers reach the same admission
    /// the runtime's own jobs go through. Nudges never block and are ignored
    /// under [`FsBackgroundWork::ManualOnly`] and after shutdown.
    pub fn maintenance(&self) -> MaintenanceHandle {
        self.bits.maintenance.handle()
    }

    /// Registers an extension's maintenance executor alongside the runtime's
    /// own, under the job's own id.
    ///
    /// The registered job shares this writer's permit pool, backoff, and
    /// shutdown; [`Self::maintenance`] is how its work gets admitted.
    /// Registering the same id twice is a configuration error.
    pub fn register_maintenance_job(
        &self,
        job: Arc<dyn MaintenanceJob>,
    ) -> Result<MaintenanceJobId> {
        self.bits.maintenance.register(job)
    }

    /// Returns the maintenance job registered under `job`.
    ///
    /// Hosts may call the job directly to run bounded steps under their own
    /// budget. These are the same idempotent, compare-and-swap-based steps used
    /// by the scheduler, so concurrent or repeated execution may duplicate work
    /// but does not change correctness.
    pub fn maintenance_job(&self, job: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.bits.maintenance.job(job)
    }

    /// Waits for currently scheduled maintenance to finish without closing
    /// admission.
    ///
    /// New work may still be scheduled while this method runs, so it waits for a
    /// quiet point rather than performing terminal shutdown. Task panics are
    /// returned as runtime errors.
    ///
    /// One task this waits for is not a bounded step. A family group that has
    /// outgrown one step is rebuilt by a streaming compaction paced by no
    /// budget, and this waits for that to finish rather than stopping it.
    /// [`Self::shutdown`] cancels it instead, which is what makes a shutdown
    /// short.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.flush_background",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "flush_background",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn flush_background(&self) -> Result<()> {
        let span = tracing::Span::current();
        span.record("mode", self.core.trace_mode());
        span.record("store_kind", self.core.trace_store_kind());
        self.bits.maintenance.drain().await
    }

    /// Gracefully shuts down writer-owned background work.
    ///
    /// Shutdown closes maintenance admission before its first await, then
    /// closes publication admission, drains accepted publications, and waits
    /// for maintenance tasks that were already running. Closing maintenance
    /// first prevents completed publications from scheduling new work during
    /// the drain. Maintenance jobs publish through [`FsAdmin`](crate::FsAdmin),
    /// not the publication service, so the publication queue can only shrink.
    ///
    /// Closing maintenance admission also cancels streaming metadata
    /// compactions. These jobs check for cancellation between block reads and
    /// publish only after the complete merge finishes, so cancellation leaves
    /// the current manifest unchanged. A later maintenance step can plan the
    /// compaction again.
    ///
    /// After shutdown, mutations return `shutting_down`, maintenance nudges
    /// are ignored, and reads continue to work. The method is idempotent and
    /// may be called from any clone because all clones share the same shutdown
    /// state. Task panics are returned as runtime errors.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.shutdown",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "shutdown",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn shutdown(&self) -> Result<()> {
        let span = tracing::Span::current();
        span.record("mode", self.core.trace_mode());
        span.record("store_kind", self.core.trace_store_kind());
        self.close_admission_for_shutdown();
        self.publisher.drain().await?;
        self.bits.maintenance.drain().await
    }
}

/// Builder for [`FsWriter`].
#[must_use]
pub struct FsWriterBuilder {
    core: HandleBuilderCore,
    writer_id: Option<String>,
    background_work: FsBackgroundWork,
    max_concurrent_maintenance: usize,
    min_publish_interval_ms: u64,
    publish_observer: Option<PublishObserver>,
    maintenance_clock: Option<Arc<dyn crate::maintenance_runner::MaintenanceClock>>,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            background_work: FsBackgroundWork::ManualOnly,
            max_concurrent_maintenance: crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            publish_observer: None,
            maintenance_clock: None,
        }
    }

    /// Substitutes the clock the maintenance runner schedules against, so a
    /// test can assert on the times a write path plants without waiting for
    /// them. Scheduling only: no durable stamp comes from here.
    #[cfg(test)]
    pub(crate) fn maintenance_clock(
        mut self,
        clock: Arc<dyn crate::maintenance_runner::MaintenanceClock>,
    ) -> Self {
        self.maintenance_clock = Some(clock);
        self
    }

    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
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
    /// across every job and namespace this writer serves. Each job already
    /// runs at most one step per namespace at a time; this bounds the
    /// fan-out when many namespaces need work together. Requests beyond the
    /// cap coalesce per job and namespace and run as permits free. Defaults to
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

    /// Installs a node-local cache of encoded metadata blocks beneath this
    /// writer's decoded block cache.
    ///
    /// The handle rides on the decoded cache, so every handle built from
    /// this writer's cache reaches the same local cache — the reader this
    /// writer derives, and an admin handle sharing the cache through
    /// [`FsAdminBuilder::shared_metadata_table_cache`]. Nothing else
    /// reaches it: paths that carry no decoded cache carry neither tier.
    ///
    /// Unset by default. The host owns the cache and closes it, and object
    /// storage stays the authority for every read either way.
    ///
    /// [`FsAdminBuilder::shared_metadata_table_cache`]: crate::FsAdminBuilder::shared_metadata_table_cache
    pub fn stored_metadata_block_cache(
        mut self,
        stored_metadata_block_cache: Arc<dyn StoredMetadataBlockCache>,
    ) -> Self {
        self.core.stored_metadata_block_cache = Some(stored_metadata_block_cache);
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
    /// The handle wraps its object store before opening; callers do not
    /// need to construct an instrumented store manually. Combines with
    /// [`Self::metrics_recorder`]: one wrapper feeds both.
    pub fn object_store_metrics_recorder(
        mut self,
        recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    ) -> Self {
        self.core.object_store_metrics_recorder = Some(recorder);
        self
    }

    /// Installs the metrics recorder this handle reports its instruments to
    /// (see [`crate::metrics`]).
    ///
    /// The handle registers its instrument set once, here, and reports into
    /// it from then on: object-store calls, maintenance steps, publications,
    /// and collection passes. A handle built without one registers nothing.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
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
        let identity = WriterIdentity::new(writer_id)?;
        let max_concurrent_maintenance = NonZeroUsize::new(self.max_concurrent_maintenance)
            .ok_or_else(|| {
                RuntimeError::Config(
                    "`max_concurrent_maintenance` must be greater than zero; \
                     `FsBackgroundWork::ManualOnly` disables scheduling"
                        .to_owned(),
                )
            })?;
        let runtime = Some(owning_runtime()?);
        let core = self.core.open_read_core()?;
        let instruments = Arc::clone(core.instruments());
        let maintenance = match self.maintenance_clock {
            Some(clock) => MaintenanceRunner::with_clock(
                self.background_work,
                runtime,
                max_concurrent_maintenance,
                clock,
                instruments,
            ),
            None => MaintenanceRunner::new(
                self.background_work,
                runtime,
                max_concurrent_maintenance,
                instruments,
            ),
        };
        let bits = Arc::new(WriterBits {
            identity,
            maintenance,
            publish_observer: self.publish_observer,
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            std::time::Duration::from_millis(self.min_publish_interval_ms),
            core.trace_mode(),
            core.trace_store_kind(),
        );
        // The runtime's own executors register last: they run as an admin
        // over this writer's parts, so both have to exist first.
        register_core_jobs(&bits.maintenance, &core, &bits, &publisher)?;
        Ok(FsWriter {
            core,
            bits,
            publisher,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateNamespaceOptions, ErrorCode, FsBackgroundWork, FsWriter, MaintenanceJobId,
        NamespaceId, PutFileOptions,
    };
    use loonfs_core::test_support::RecordingStoredMetadataBlockCache;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::namespace_id;
    use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn a_writer_carries_no_stored_block_cache_by_default() {
        let temp_dir = tempdir().expect("tempdir");
        let writer = FsWriter::builder_with_store(Arc::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        ))
        .writer_id("no-stored-block-cache-writer")
        .build()
        .await
        .expect("build writer");

        assert!(writer.metadata_table_cache().stored_block_cache().is_none());
    }

    /// The handle rides on the decoded block cache, which is what puts the
    /// two tiers in the same places: every handle built from this cache
    /// reaches the local cache, and every path that carries no decoded cache
    /// reaches neither tier.
    #[tokio::test]
    async fn the_builder_installs_the_stored_block_cache_on_the_decoded_cache() {
        let temp_dir = tempdir().expect("tempdir");
        let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
        let writer = FsWriter::builder_with_store(Arc::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        ))
        .writer_id("stored-block-cache-writer")
        .stored_metadata_block_cache(stored_blocks.clone())
        .build()
        .await
        .expect("build writer");

        assert!(writer.metadata_table_cache().stored_block_cache().is_some());
        assert!(Arc::ptr_eq(
            &writer.metadata_table_cache(),
            &writer.reader().core.metadata_table_cache()
        ));
    }

    /// A writer whose store parks the first head compare-and-swap, so a
    /// publication can be held open across a shutdown's first poll.
    async fn parked_publication_writer(
        temp_dir: &std::path::Path,
        writer_id: &str,
        namespace_id: &NamespaceId,
    ) -> (FsWriter, Arc<BlockingStore<LocalFsStore>>) {
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir).expect("create local-fs store"),
            KeyPredicate::wal_head(namespace_id),
            OperationClass::CompareAndSwap,
        ));
        let writer = FsWriter::builder_with_store(blocking.clone())
            .writer_id(writer_id)
            .background_work(FsBackgroundWork::Enabled)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        (writer, blocking)
    }

    /// Both admissions must close on the shutdown's first poll, before it
    /// starts draining publications. The drain is a wait, and the runner
    /// stays live across it: a queue still open here lets a nudge landing in
    /// that window be admitted, and lets a finishing step hand its slot on
    /// and spawn work the shutdown already decided to drop. Asserting on the
    /// first poll pins the order on every machine; the integration coverage
    /// in `tests/it/handles.rs` only loses the race on some.
    #[tokio::test]
    async fn shutdown_closes_both_admissions_before_draining_publications() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("parked");
        let (writer, blocking) =
            parked_publication_writer(temp_dir.path(), "shutdown-order-writer", &namespace_id)
                .await;

        // Park a publication so the shutdown's publication drain is still
        // pending when the first poll returns.
        blocking.block_next();
        let put = tokio::spawn({
            let writer = writer.clone();
            let namespace_id = namespace_id.clone();
            async move {
                writer
                    .put_file_bytes(
                        &namespace_id,
                        "/parked.txt",
                        b"body",
                        PutFileOptions::new(loonfs_test_support::test_actor()),
                    )
                    .await
            }
        });
        blocking.wait_until_blocked().await;

        let mut shutdown = Box::pin(writer.shutdown());
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "the parked publication must keep the shutdown pending"
        );
        // Behavioral, not a flag read: a nudge lands after the first poll
        // and must leave nothing admitted behind it.
        writer
            .maintenance()
            .nudge(MaintenanceJobId::METADATA, &namespace_id);
        assert!(
            !writer
                .bits
                .maintenance
                .is_pending(MaintenanceJobId::METADATA, &namespace_id),
            "maintenance admission must already be closed while publications drain"
        );
        // The publication half of the same window: a mutation submitted
        // into the drain would be work the drain then has to wait for.
        let refused = writer
            .put_file_bytes(
                &namespace_id,
                "/late.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("a mutation submitted during the drain must be refused");
        assert_eq!(
            refused.code(),
            ErrorCode::ShuttingDown,
            "a late mutation reports `shutting_down`: {refused:?}"
        );
        assert!(writer.is_shutting_down());

        blocking.release();
        put.await
            .expect("join the parked put")
            .expect("the released put succeeds");
        shutdown.await.expect("shut down the writer");
    }

    /// Shutdown is a state of the shared runtime, not a token one handle
    /// holds: a clone that shuts down is observable from every other clone,
    /// and a second shutdown settles rather than panicking or wedging.
    #[tokio::test]
    async fn a_clone_observes_a_shutdown_and_may_repeat_it() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("clones");
        let (writer, _blocking) =
            parked_publication_writer(temp_dir.path(), "shutdown-clone-writer", &namespace_id)
                .await;
        let clone = writer.clone();
        assert!(!clone.is_shutting_down());

        writer.shutdown().await.expect("shut down the writer");
        assert!(
            clone.is_shutting_down(),
            "a clone sees the runtime it shares shutting down"
        );
        clone
            .shutdown()
            .await
            .expect("a second shutdown settles rather than failing");
        clone
            .flush_background()
            .await
            .expect("a shut runner has nothing left to settle");
    }
}
