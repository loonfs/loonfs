//! The runtime, in a read-only or a writable mode.

use super::{LoonFsBuilder, Maintenance, Namespace};
use crate::fs::{RuntimeCore, WriterBits, WriterIdentity};
use crate::publisher::PublisherRegistry;
use crate::{
    CapabilityDocument, MetadataCache, NamespaceId, Result, SharedObjectStore, StoreConfig,
};
use loonfs_api::{Subject, WriterId};
#[cfg(test)]
use loonfs_core::cache::MetadataSegmentCache;
use std::fmt;
use std::sync::Arc;

/// The LoonFS runtime. Every mode reads, and only [`Writable`] writes.
///
/// Every mode owns the store client and the read budgets, reads through a
/// [`MetadataCache`] it may share with other runtimes, and returns read-only
/// [`Namespace`] handles from [`Self::namespace`]. A
/// writable runtime also owns the writer identity, the publication service,
/// and the admission budgets. It creates and forks namespaces, opens a
/// writable handle on each namespace it writes, runs maintenance through
/// [`Self::maintenance`], and shuts down.
///
/// Build a runtime inside the Tokio runtime that will use it. Do not share a
/// provider client across unrelated runtimes; build another runtime from
/// [`StoreConfig`] instead. Clones are cheap. They share the store client and
/// the runtime's scope in its metadata cache, and, in the writable mode, the
/// publication service and the shutdown state.
#[derive(Clone)]
pub struct LoonFs<M> {
    pub(crate) core: RuntimeCore,
    pub(crate) mode: M,
}

/// The mode of a runtime or a namespace handle that only reads.
#[derive(Debug, Clone, Copy)]
pub struct ReadOnly;

/// The mode of a runtime or a namespace handle that can also write.
#[derive(Clone)]
pub struct Writable {
    /// Publisher workers hold these weakly, so dropping every runtime and
    /// namespace handle that holds them stops new publication work.
    pub(crate) bits: Arc<WriterBits>,
    pub(crate) publisher: PublisherRegistry,
}

impl fmt::Debug for Writable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Writable").finish_non_exhaustive()
    }
}

impl<M> LoonFs<M> {
    /// Returns the subject this runtime acts for, or `None` for an unscoped
    /// service runtime.
    pub fn subject(&self) -> Option<&Subject> {
        self.core.subject.as_ref()
    }

    /// Clones this runtime with the subject used for its reads and, in the
    /// writable mode, its commits and uploads.
    pub fn as_subject(&self, subject: Subject) -> Self
    where
        M: Clone,
    {
        Self {
            core: self.core.as_subject(subject),
            mode: self.mode.clone(),
        }
    }

    /// Returns a runtime over the same store client and caches, for the same
    /// subject, that cannot write.
    ///
    /// It sees this runtime's cache updates at once, so a host can route its
    /// reads through it. For reads driven by a different Tokio runtime, build
    /// a separate runtime with [`LoonFs::reader`].
    pub fn read_only(&self) -> LoonFs<ReadOnly> {
        LoonFs {
            core: self.core.clone(),
            mode: ReadOnly,
        }
    }

    /// Returns a read-only handle on one namespace, for this runtime's
    /// subject.
    ///
    /// Does no IO and cannot fail. A namespace that does not exist fails on
    /// the handle's first read.
    pub fn namespace(&self, namespace_id: &NamespaceId) -> Namespace<ReadOnly> {
        Namespace::new(self.core.clone(), namespace_id.clone())
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn get_capabilities(&self) -> CapabilityDocument {
        self.core.get_capabilities()
    }

    /// Returns the metadata cache this runtime reads through, which
    /// [`Maintenance`] work fills too.
    pub fn metadata_cache(&self) -> &MetadataCache {
        &self.core.inner.metadata_cache
    }

    /// Reads this runtime's wall clock as unix milliseconds.
    pub fn now_ms(&self) -> Result<u64> {
        self.core.now_ms()
    }

    /// Returns this runtime's object-store client, instrumented exactly as
    /// the runtime's own traffic is.
    ///
    /// Server integrations that read LoonFS-owned objects outside the handle
    /// surface, such as the grep manifest and the grep worker's keyspace, use
    /// this so their requests are measured like every other request instead
    /// of escaping instrumentation on a second, raw client.
    pub fn object_store(&self) -> SharedObjectStore {
        self.core.shared_store()
    }

    /// Returns this runtime's view of the decoded segment blocks.
    #[cfg(test)]
    pub(crate) fn metadata_segment_cache(&self) -> Arc<MetadataSegmentCache> {
        self.core.metadata_segment_cache()
    }
}

impl LoonFs<ReadOnly> {
    /// Starts a read-only runtime builder that constructs its object-store
    /// client from configuration inside this runtime's ownership domain.
    pub fn reader(store_config: StoreConfig) -> LoonFsBuilder<ReadOnly> {
        LoonFsBuilder::from_config(store_config)
    }

    /// Starts a read-only runtime builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this runtime's ownership
    /// domain. Do not use it to share one provider client across unrelated
    /// runtimes; build another runtime from [`StoreConfig`] instead.
    pub fn reader_with_store(store: SharedObjectStore) -> LoonFsBuilder<ReadOnly> {
        LoonFsBuilder::from_store(store)
    }
}

impl LoonFs<Writable> {
    /// Starts a writable runtime builder that constructs its object-store
    /// client from configuration inside this runtime's ownership domain.
    pub fn builder(store_config: StoreConfig) -> LoonFsBuilder<Writable> {
        LoonFsBuilder::from_config(store_config)
    }

    /// Starts a writable runtime builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this runtime's ownership
    /// domain. Do not use it to share one provider client across unrelated
    /// runtimes; build another runtime from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> LoonFsBuilder<Writable> {
        LoonFsBuilder::from_store(store)
    }

    /// Opens the writer session for `namespace_id` and returns a handle to it.
    ///
    /// If a handle for this namespace is already open in this runtime, the
    /// new handle shares its session. Opening does no store IO and acquires
    /// no writer epoch; the session's first publish does. The caller owns
    /// the session from here on (see [`Namespace`]). Fails with
    /// `writer_session_closed` while a [`Namespace::close`] of this
    /// namespace's session drains, and with `shutting_down` after shutdown
    /// begins.
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
    pub fn open_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace<Writable>> {
        self.core.record_trace_context(&tracing::Span::current());
        let session = self.mode.publisher.open_session(namespace_id)?;
        Ok(Namespace::open(self, session))
    }

    /// Returns the maintenance capability of this runtime, acting as
    /// `writer_id`.
    ///
    /// Maintenance shares this runtime's store client, caches, and
    /// publication service, so fold decisions see the inline bytes of the
    /// sessions this runtime holds. Operations that mutate durable control
    /// state record `writer_id`. A process that only maintains builds a
    /// writable runtime and never opens a namespace.
    pub fn maintenance(&self, writer_id: WriterId) -> Maintenance {
        Maintenance::new(
            self.core.clone(),
            self.mode.publisher.clone(),
            WriterIdentity { writer_id },
        )
    }

    /// Closes publication admission before shutdown drains.
    ///
    /// Later mutations fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub fn close_admission_for_shutdown(&self) {
        self.mode.publisher.close_admission();
    }

    /// Whether [`Self::shutdown`] has begun on this runtime or any clone of
    /// it.
    ///
    /// Mutations submitted from here on fail with `shutting_down`, so a
    /// readiness probe answers "draining" from this and a load balancer can
    /// take the instance out before its in-flight work settles.
    pub fn is_shutting_down(&self) -> bool {
        self.mode.publisher.is_admission_closed()
    }

    /// Waits for the publication work this runtime has admitted, and the
    /// folds that work started, to finish.
    ///
    /// Admission stays open. Work admitted during the wait may still be
    /// running when this returns. [`Self::shutdown`] closes admission and
    /// then drains. Fails if a publication, deletion, or fold on this runtime
    /// has ever panicked. The runtime contains those panics and keeps
    /// publishing.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.drain",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "drain",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn drain(&self) -> Result<()> {
        self.core.record_trace_context(&tracing::Span::current());
        self.mode.publisher.drain().await
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
        self.mode.publisher.drain().await
    }

    // Namespace create and fork live in `fs/namespaces.rs`.
}

#[cfg(test)]
mod tests {
    use crate::{CreateNamespaceOptions, ErrorCode, LoonFs, NamespaceId, PutFileOptions, Writable};
    use loonfs_core::test_support::RecordingStoredMetadataBlockCache;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::namespace_id;
    use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn a_runtime_carries_no_stored_block_cache_by_default() {
        let temp_dir = tempdir().expect("tempdir");
        let runtime = LoonFs::builder_with_store(Arc::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        ))
        .writer_id("no-stored-block-cache-writer")
        .build()
        .await
        .expect("build runtime");

        assert!(runtime
            .metadata_segment_cache()
            .stored_block_cache()
            .is_none());
    }

    #[tokio::test]
    async fn the_builder_installs_the_stored_block_cache_on_the_decoded_cache() {
        let temp_dir = tempdir().expect("tempdir");
        let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
        let runtime = LoonFs::builder_with_store(Arc::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        ))
        .writer_id("stored-block-cache-writer")
        .stored_metadata_block_cache(stored_blocks.clone())
        .build()
        .await
        .expect("build runtime");

        assert!(runtime
            .metadata_segment_cache()
            .stored_block_cache()
            .is_some());
        assert!(Arc::ptr_eq(
            &runtime.metadata_segment_cache(),
            &runtime.read_only().metadata_segment_cache()
        ));
    }

    /// A runtime whose store parks the first WAL put, so a publication can
    /// be held open across a shutdown's first poll.
    async fn parked_publication_runtime(
        temp_dir: &std::path::Path,
        writer_id: &str,
        namespace_id: &NamespaceId,
    ) -> (LoonFs<Writable>, Arc<BlockingStore<LocalFsStore>>) {
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir).expect("create local-fs store"),
            KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(namespace_id)),
            OperationClass::PutCreateIfAbsent,
        ));
        let runtime = LoonFs::builder_with_store(blocking.clone())
            .writer_id(writer_id)
            .build()
            .await
            .expect("build runtime");
        runtime
            .create_namespace(
                namespace_id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
        (runtime, blocking)
    }

    #[tokio::test]
    async fn shutdown_closes_publication_admission_before_draining() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("parked");
        let (runtime, blocking) =
            parked_publication_runtime(temp_dir.path(), "shutdown-order-writer", &namespace_id)
                .await;
        let namespace = runtime
            .open_namespace(&namespace_id)
            .expect("open namespace");

        // Park a publication so the shutdown's publication drain is still
        // pending when the first poll returns.
        blocking.block_next();
        let put = tokio::spawn({
            let namespace = namespace.clone();
            async move {
                namespace
                    .put_file_bytes(
                        "/parked.txt",
                        b"body",
                        PutFileOptions::new(loonfs_test_support::test_actor()),
                    )
                    .await
            }
        });
        blocking.wait_until_blocked().await;

        let mut shutdown = Box::pin(runtime.shutdown());
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "the parked publication must keep the shutdown pending"
        );
        // A mutation submitted into the drain would be work the drain then
        // has to wait for.
        let refused = namespace
            .put_file_bytes(
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
        assert!(runtime.is_shutting_down());

        blocking.release();
        put.await
            .expect("join the parked put")
            .expect("the released put succeeds");
        shutdown.await.expect("shut down the runtime");
    }

    #[tokio::test]
    async fn a_clone_observes_a_shutdown_and_may_repeat_it() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("clones");
        let (runtime, _blocking) =
            parked_publication_runtime(temp_dir.path(), "shutdown-clone-writer", &namespace_id)
                .await;
        let clone = runtime.clone();
        assert!(!clone.is_shutting_down());

        runtime.shutdown().await.expect("shut down the runtime");
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
