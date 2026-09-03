//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::fs::{ReadCore, WriterBits, WriterIdentity};
use crate::metrics::{MetricsRecorder, ObjectStoreMetricsRecorder};
use crate::publisher::{
    CloseNamespaceReport, NamespaceAdvanceHint, NamespaceAdvanceObserver, NamespaceSessionState,
    PublisherRegistry, WriterSessionStats,
};
use crate::{
    CapabilityDocument, FsMaintenance, MaintenanceHint, MaintenanceHintObserver, NamespaceId,
    Result, RuntimeCacheConfig, RuntimeCacheStats, RuntimeError, SharedObjectStore, StoreConfig,
    TraceMode, TraceStoreKind,
};
#[cfg(test)]
use loonfs_core::cache::MetadataSegmentCache;
use loonfs_core::cache::StoredMetadataBlockCache;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// How a writer treats a namespace it has no open session for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSessionPolicy {
    /// The first mutation opens a session. Used by the reference server and CLI.
    OpenOnFirstWrite,
    /// Only [`FsWriter::open_namespace`] opens a session.
    ///
    /// A mutation without an open session fails with `writer_session_closed`.
    ExplicitOpen,
}

/// Write-capable runtime handle for applications and servers.
///
/// `FsWriter` owns the writer identity, mutations, uploads, namespace
/// lifecycle, and commit publication.
///
/// Build the handle inside the Tokio runtime that will use it. Do not share a
/// provider client across unrelated runtimes; build another handle from
/// [`StoreConfig`]. Clones share identity, caches, publication tasks, and
/// shutdown state.
#[derive(Clone)]
pub struct FsWriter {
    pub(crate) core: ReadCore,
    /// The writer half of the runtime: the writer identity and observers.
    /// Publisher workers hold this weakly,
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
    #[cfg(test)]
    pub(crate) fn metadata_segment_cache(&self) -> Arc<MetadataSegmentCache> {
        self.core.metadata_segment_cache()
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

    /// Opens a session for `namespace_id`, or returns if one is open.
    ///
    /// Fails with `writer_capacity_exceeded` at the limit, with
    /// `writer_session_closed` while a close is draining, and with
    /// `shutting_down` after shutdown begins. Opening acquires no writer
    /// epoch; the first publish does.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.open_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "open_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub fn open_namespace(&self, namespace_id: &NamespaceId) -> Result<()> {
        self.core.record_trace_context(&tracing::Span::current());
        self.publisher.open_namespace(namespace_id)?;
        Ok(())
    }

    /// Closes and drains the session for `namespace_id`.
    ///
    /// Admissions stop for every clone of the publisher. Admitted work
    /// finishes, and a later open starts a new session. Calling this for a
    /// closed namespace has no additional effect.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.close_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "close_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn close_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CloseNamespaceReport> {
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self.publisher.close_namespace(namespace_id).await?)
    }

    /// Returns the writer session state for `namespace_id`.
    pub fn namespace_session_state(&self, namespace_id: &NamespaceId) -> NamespaceSessionState {
        self.publisher.namespace_session_state(namespace_id)
    }

    /// Returns this writer's namespace session totals and capacity.
    pub fn writer_session_stats(&self) -> WriterSessionStats {
        self.publisher.writer_session_stats()
    }

    /// Closes publication admission before shutdown drains.
    ///
    /// Later mutations fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub fn close_admission_for_shutdown(&self) {
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
    pub fn get_capabilities(&self) -> CapabilityDocument {
        self.core.get_capabilities()
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    // Namespace lifecycle lives in `fs/namespaces.rs`; mutation, commit,
    // and upload operations in `fs/writes.rs` and `fs/uploads.rs`.

    /// Builds a maintenance handle over this writer's read core and caches.
    /// It shares no publisher or scheduler state.
    pub fn maintenance_handle(&self, actor_id: impl Into<String>) -> Result<FsMaintenance> {
        FsMaintenance::from_read_core(self.core.clone(), actor_id.into())
    }

    /// Stops publication and drains accepted work.
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
        self.core.record_trace_context(&tracing::Span::current());
        self.close_admission_for_shutdown();
        self.publisher.drain().await
    }
}

/// Builder for [`FsWriter`].
#[must_use]
pub struct FsWriterBuilder {
    core: HandleBuilderCore,
    writer_id: Option<String>,
    min_publish_interval_ms: u64,
    namespace_session_policy: NamespaceSessionPolicy,
    max_open_namespaces: NonZeroUsize,
    namespace_advance_observer: Option<NamespaceAdvanceObserver>,
    maintenance_hint_observer: Option<MaintenanceHintObserver>,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            namespace_session_policy: NamespaceSessionPolicy::OpenOnFirstWrite,
            max_open_namespaces: NonZeroUsize::new(crate::config::DEFAULT_MAX_OPEN_NAMESPACES)
                .expect("default maximum open namespaces should be nonzero"),
            namespace_advance_observer: None,
            maintenance_hint_observer: None,
        }
    }

    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
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

    /// Sets how namespaces without an open writer session are handled.
    /// Defaults to [`NamespaceSessionPolicy::OpenOnFirstWrite`].
    pub fn namespace_sessions(mut self, policy: NamespaceSessionPolicy) -> Self {
        self.namespace_session_policy = policy;
        self
    }

    /// Sets the maximum number of writer sessions held at once.
    ///
    /// Opening past the limit fails with `writer_capacity_exceeded`. The
    /// default is [`crate::DEFAULT_MAX_OPEN_NAMESPACES`].
    pub fn max_open_namespaces(mut self, limit: NonZeroUsize) -> Self {
        self.max_open_namespaces = limit;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Installs a node-local encoded-block cache beneath the decoded cache.
    ///
    /// Handles that share this writer's decoded cache also share this local
    /// cache. The host owns and closes it; object storage remains authoritative.
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
    /// it from then on: object-store calls, publications, compactions, and
    /// collection passes. A handle built without one registers nothing.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Registers an observer called with a [`NamespaceAdvanceHint`] after
    /// each publication batch that durably commits at least one mutation.
    ///
    /// The call happens after durable visibility. One batch may cover
    /// several commits, so the hint carries a high-water mark rather than
    /// one commit, and delivery is best-effort. The observer runs
    /// synchronously on the publication task, so it must do nothing but a
    /// non-blocking handoff such as a bounded-channel `try_send`: no
    /// network, filesystem, object-store, lock-contended, or waiting work.
    /// Downstream correctness comes from a durable change-feed cursor,
    /// never from the hints. Writers that register no observer publish
    /// exactly as before.
    pub fn namespace_advance_observer(
        mut self,
        observer: impl Fn(NamespaceAdvanceHint) + Send + Sync + 'static,
    ) -> Self {
        self.namespace_advance_observer = Some(Arc::new(observer));
        self
    }

    /// Registers a non-blocking observer for best-effort maintenance hints.
    /// It runs synchronously on publication and upload tasks and must only
    /// perform a non-blocking handoff.
    pub fn maintenance_hint_observer(
        mut self,
        observer: impl Fn(MaintenanceHint) + Send + Sync + 'static,
    ) -> Self {
        self.maintenance_hint_observer = Some(Arc::new(observer));
        self
    }

    /// Opens the writer inside the Tokio runtime that owns publication tasks.
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
        let runtime = owning_runtime()?;
        let core = self.core.open_read_core()?;
        let bits = Arc::new(WriterBits {
            identity,
            namespace_advance_observer: self.namespace_advance_observer,
            maintenance_hint_observer: self.maintenance_hint_observer,
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            runtime,
            std::time::Duration::from_millis(self.min_publish_interval_ms),
            self.namespace_session_policy,
            self.max_open_namespaces,
        );
        Ok(FsWriter {
            core,
            bits,
            publisher,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{CreateNamespaceOptions, ErrorCode, FsWriter, NamespaceId, PutFileOptions};
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

        assert!(writer
            .metadata_segment_cache()
            .stored_block_cache()
            .is_none());
    }

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

        assert!(writer
            .metadata_segment_cache()
            .stored_block_cache()
            .is_some());
        assert!(Arc::ptr_eq(
            &writer.metadata_segment_cache(),
            &writer.reader().core.metadata_segment_cache()
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
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        (writer, blocking)
    }

    #[tokio::test]
    async fn shutdown_closes_publication_admission_before_draining() {
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
        // A mutation submitted into the drain would be work the drain then
        // has to wait for.
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
    }
}
