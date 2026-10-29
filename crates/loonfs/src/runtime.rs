//! The runtime, in a read-only or a writable mode.
//!
//! The API has two nouns. A [`LoonFs`] is the runtime. It owns the store
//! client, the caches, and the read budgets. A [`Namespace`] acts on one
//! namespace, and its methods take no namespace id.
//!
//! Both have one of two modes. [`ReadOnly`] reads. [`Writable`] reads and
//! writes. A `LoonFs<ReadOnly>` returns only `Namespace<ReadOnly>` handles,
//! from [`LoonFs::namespace`]; they own no writer session and cost nothing
//! to create. A `LoonFs<Writable>` also owns the writer identity, the
//! publication service, the per-namespace admission limits, and shutdown,
//! and runs its work under an execution budget it may share. It creates and
//! forks namespaces, and [`LoonFs::open_namespace`] returns a
//! `Namespace<Writable>`, which is that namespace's writer session. The host
//! owns each session: it stays open while the host keeps a handle on it.
//!
//! Maintenance is a capability of a writable runtime.
//! [`LoonFs::maintenance`] returns a [`Maintenance`] value that runs folds,
//! compaction, checkpoints, retention, and garbage collection. A process that
//! only maintains builds a writable runtime and never opens a namespace.
//!
//! Build each runtime inside the Tokio runtime where it will be used. Prefer
//! the builder that takes a [`StoreConfig`](crate::StoreConfig); use
//! `builder_with_store` only when the supplied store is safe to use from
//! that Tokio runtime.

use crate::publisher::{NamespaceChange, NamespaceSession, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::{self, operation_span};
use crate::{
    ActorId, CapabilityDocument, CreateNamespaceOptions, Error, ExecutionBudget,
    ForkNamespaceOptions, InlineContentPolicy, Maintenance, MetadataCache, Namespace, NamespaceId,
    Result, SharedObjectStore, StoreConfig,
};
use loonfs_core::NamespaceWriterEngine;
use loonfs_types::{
    u64_from_usize, CompactorEpoch, Subject, WriterId, FEATURE_COMMIT_INLINE_CONTENT,
    LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

mod builder;

pub use builder::LoonFsBuilder;

/// The LoonFS runtime. Every mode reads, and only [`Writable`] writes.
///
/// Every mode owns the store client and the read budgets, reads through a
/// [`MetadataCache`] it may share with other runtimes, and returns read-only
/// [`Namespace`] handles from [`Self::namespace`]. A
/// writable runtime also owns the writer identity, the publication service,
/// and the per-namespace admission limits, and runs its work under an
/// [`ExecutionBudget`] it may share with other runtimes. It creates and forks
/// namespaces, opens a writable handle on each namespace it writes, runs
/// maintenance through [`Self::maintenance`], and shuts down.
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
    /// Publisher workers keep only a weak reference to this state, so
    /// dropping every runtime, namespace handle, and maintenance value that
    /// refers to it stops new publication work.
    pub(crate) writer: Arc<WriterState>,
    pub(crate) publisher: PublisherRegistry,
}

/// The writer state that a writable runtime, its sessions, and every
/// [`Maintenance`] value of the runtime share.
pub(crate) struct WriterState {
    /// The writer ID that the runtime's sessions commit as.
    pub(crate) writer_id: WriterId,
    /// The compactor epoch this runtime has for each namespace. Its
    /// sessions and every `Maintenance` value share it, so they never fence
    /// each other. It's a `tokio` mutex because a first claim keeps it locked
    /// across `claim_compactor().await`, so concurrent groups share one epoch.
    pub(crate) compactor_epochs: tokio::sync::Mutex<BTreeMap<NamespaceId, CompactorEpoch>>,
}

impl fmt::Debug for Writable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Writable").finish_non_exhaustive()
    }
}

/// A handle mode: [`ReadOnly`] or [`Writable`].
///
/// The trait is sealed, so these two are the only modes.
#[allow(
    private_bounds,
    reason = "the crate-private supertrait seals the trait and holds the data each mode adds"
)]
pub trait Mode: sealed::Sealed {}

impl Mode for ReadOnly {}
impl Mode for Writable {}

pub(crate) mod sealed {
    use super::{NamespaceSession, ReadOnly, Writable};
    use std::sync::Arc;

    /// The data each mode adds to a namespace handle.
    pub(crate) trait Sealed {
        /// What a namespace handle keeps for its writer session: nothing in
        /// the read-only mode.
        type Session: Clone;
    }

    impl Sealed for ReadOnly {
        type Session = ();
    }

    impl Sealed for Writable {
        type Session = Arc<NamespaceSession>;
    }
}

impl<M> LoonFs<M> {
    /// Returns the execution budget this runtime's reads, publications, folds, and
    /// merges draw from, which other runtimes may share.
    pub fn execution_budget(&self) -> &ExecutionBudget {
        self.core.execution_budget()
    }

    /// Returns the subject this runtime acts for, or `None` for an unscoped
    /// service runtime.
    pub fn subject(&self) -> Option<&Subject> {
        self.core.subject()
    }

    /// Clones this runtime with the subject used for its reads and, in the
    /// writable mode, its commits and uploads.
    pub fn with_subject(&self, subject: Subject) -> Self
    where
        M: Clone,
    {
        Self {
            core: self.core.with_subject(subject),
            mode: self.mode.clone(),
        }
    }

    /// Returns a runtime over the same store client and caches, for the same
    /// subject, that cannot write.
    ///
    /// It sees this runtime's cache updates at once, so a host can route its
    /// reads through it. For reads driven by a different Tokio runtime, build
    /// a separate runtime with [`LoonFsBuilder::read_only`].
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

    /// Returns the metadata cache this runtime reads through, which
    /// [`Maintenance`] work fills too.
    pub fn metadata_cache(&self) -> &MetadataCache {
        self.core.metadata_cache()
    }

    /// Reads this runtime's wall clock as unix milliseconds.
    pub fn now_ms(&self) -> u64 {
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

    /// Returns the monotonic timer this runtime measures its deadlines and
    /// revalidation checks with, for an extension that runs beside it.
    pub fn monotonic_timer(&self) -> Arc<dyn loonfs_types::MonotonicTimer> {
        Arc::clone(self.core.timer())
    }
}

impl LoonFs<ReadOnly> {
    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn capabilities(&self) -> CapabilityDocument {
        self.core.capabilities()
    }
}

impl LoonFs<Writable> {
    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery"), with inline commit content when this runtime
    /// prepares it.
    pub fn capabilities(&self) -> CapabilityDocument {
        let mut capabilities = self.core.capabilities();
        if let Some(threshold) = self.inline_content_policy().inline_content_threshold_bytes {
            capabilities
                .features
                .insert(FEATURE_COMMIT_INLINE_CONTENT.to_owned(), true);
            capabilities.limits.insert(
                LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION.to_owned(),
                u64_from_usize(threshold),
            );
        }
        capabilities
    }

    /// Returns the inline content policy that this runtime's commits use (see
    /// [`RuntimeConfig::inline_content`](crate::RuntimeConfig::inline_content)).
    pub fn inline_content_policy(&self) -> &InlineContentPolicy {
        &self.core.config().inline_content
    }

    /// Starts a writable runtime builder that constructs its object-store
    /// client from configuration inside this runtime's ownership domain.
    pub fn builder(store_config: StoreConfig) -> LoonFsBuilder<Writable> {
        LoonFsBuilder::from_config(store_config)
    }

    /// Starts a writable runtime builder over a caller-supplied store.
    ///
    /// For callers who are sure the store is safe in this runtime's ownership
    /// domain. Do not use it to share one provider client across unrelated
    /// runtimes; build another runtime from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> LoonFsBuilder<Writable> {
        LoonFsBuilder::from_store(store)
    }

    /// Opens the writer session for `namespace_id` and returns a handle to it.
    ///
    /// If a handle for this namespace is already open in this runtime, the
    /// new handle shares its session unless that session is fenced. Opening
    /// does no store IO and acquires
    /// no writer epoch; the session's first publish does. The caller owns
    /// the session from here on (see [`Namespace`]). Fails with
    /// `writer_session_closed` while a [`Namespace::close`] of this
    /// namespace's session drains or a fenced session finishes its work. Once
    /// that work ends, opening starts a new session even if old clones are
    /// still exist. Fails with `shutting_down` after shutdown begins.
    pub fn open_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace<Writable>> {
        trace::in_operation_sync(
            operation_span!(self.core, "open_namespace", namespace_id),
            || {
                let session = self.mode.publisher.open_session(namespace_id)?;
                Ok(Namespace::open(self, session))
            },
        )
    }

    /// Returns the maintenance capability of this runtime, acting as
    /// `writer_id`.
    ///
    /// Maintenance shares this runtime's store client, caches, and
    /// publication service, so fold decisions see the inline bytes of the
    /// sessions this runtime keeps. It also shares the runtime's compactor
    /// claim, so it never fences this runtime's sessions or another value
    /// from this runtime, and its folds and merges take their permits from
    /// the runtime's [`ExecutionBudget`].
    /// Operations that mutate durable control state record `writer_id`. A
    /// process that only maintains builds a writable runtime and never opens
    /// a namespace.
    pub fn maintenance(&self, writer_id: WriterId) -> Maintenance {
        Maintenance::new(
            self.core.clone(),
            self.mode.publisher.clone(),
            Arc::clone(&self.mode.writer),
            writer_id,
        )
    }

    /// Closes publication admission before shutdown drains, and cancels the
    /// metadata compaction every session is running.
    ///
    /// Later mutations fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub fn close_admission(&self) {
        self.mode.publisher.close_admission();
    }

    /// Whether [`Self::shutdown`] has begun on this runtime or any clone of
    /// it.
    ///
    /// Mutations submitted from here on fail with `shutting_down`, so a
    /// readiness probe reports "draining" from this and a load balancer can
    /// take the instance out before its in-flight work settles.
    pub fn is_shutting_down(&self) -> bool {
        self.mode.publisher.is_admission_closed()
    }

    /// Waits until the publication work this runtime has admitted, the folds
    /// that work started, and the metadata compactions those folds started
    /// have finished.
    ///
    /// Admission stays open. Work admitted during the wait may still be
    /// running when this returns. Fails if a publication, deletion, fold, or
    /// compaction on this runtime has ever panicked. The runtime contains
    /// those panics and keeps publishing.
    pub async fn drain(&self) -> Result<()> {
        trace::in_operation(
            operation_span!(self.core, "drain"),
            self.mode.publisher.drain(),
        )
        .await
    }

    /// Stops publication, cancels session compactions, and drains accepted
    /// work.
    pub async fn shutdown(&self) -> Result<()> {
        trace::in_operation(operation_span!(self.core, "shutdown"), async {
            self.close_admission();
            self.mode.publisher.drain().await
        })
        .await
    }
}

impl LoonFs<Writable> {
    /// A mutating engine that commits as this runtime's writer ID.
    pub(crate) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceWriterEngine<crate::SharedObjectStore> {
        self.core
            .writer_engine(&self.mode.writer.writer_id, namespace_id)
    }

    /// Creates an unrestricted namespace, bootstrapping its durable state.
    ///
    /// An existing namespace is an error.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<crate::NamespaceMetadata> {
        self.create_namespace_with_options(namespace_id, actor, &CreateNamespaceOptions::default())
            .await
    }

    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success. Returns the namespace's post-operation status.
    pub async fn create_namespace_with_options(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &CreateNamespaceOptions,
    ) -> Result<crate::NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "create_namespace", namespace_id),
            async {
                let result = self
                    .engine(namespace_id)
                    .bootstrap_namespace(actor, options)
                    .await
                    .map_err(Error::from);
                self.mode
                    .publisher
                    .finish_namespace_mutation(namespace_id, result, |_| NamespaceChange::Unknown)
                    .await
            },
        )
        .await
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at its current head.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<crate::NamespaceMetadata> {
        self.fork_namespace_with_options(
            source_namespace_id,
            new_namespace_id,
            actor,
            &ForkNamespaceOptions::default(),
        )
        .await
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at the selected current head or unexpired snapshot.
    ///
    /// A fork of the current head folds the source's WAL tail first when it
    /// is not folded, and waits for a fold permit from the runtime's
    /// execution budget either way. A fork of a snapshot never folds and
    /// takes no fold permit.
    pub async fn fork_namespace_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &ForkNamespaceOptions,
    ) -> Result<crate::NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "fork_namespace", source_namespace_id),
            async {
                self.namespace(source_namespace_id)
                    .ensure_administrator()
                    .await?;
                let result = {
                    let _permit = match options.snapshot_id {
                        Some(_) => None,
                        None => Some(self.core.fold_permit().await),
                    };
                    self.engine(source_namespace_id)
                        .fork_namespace(new_namespace_id, actor, options.snapshot_id.as_ref())
                        .await
                }
                .map_err(Error::from);
                if let Some(published) = result
                    .as_ref()
                    .ok()
                    .and_then(|report| report.published.as_ref())
                {
                    self.mode
                        .publisher
                        .raise_hint(source_namespace_id, published.manifest.manifest_no)
                        .await;
                }
                // The runtime drops its view of the source after a fork, as it does
                // after a creation or a deletion (see the read freshness design
                // note), so the source's publisher drops its position instead of
                // moving onto the fork's fold.
                let result = self
                    .mode
                    .publisher
                    .finish_namespace_mutation(source_namespace_id, result, |_| {
                        NamespaceChange::Unknown
                    })
                    .await
                    .map(|report| report.response);
                if result.is_ok() {
                    self.mode
                        .publisher
                        .invalidate_namespace(new_namespace_id, NamespaceChange::Unknown)
                        .await;
                }
                result
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::{ErrorCode, LoonFs, MetadataCache, MetadataCacheStats, NamespaceId, Writable};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::namespace_id;
    use loonfs_test_support::stores::{
        BlockingStore, KeyPredicate, OperationSelector, RecordingStore,
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn converting_a_builder_to_read_only_keeps_the_shared_settings() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("converted");
        let writer = LoonFs::builder_with_store(Arc::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        ))
        .writer_id(loonfs_test_support::ids::writer_id(
            "converted-builder-writer",
        ))
        .build()
        .await
        .expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .put_file("/file.txt", b"body", &loonfs_test_support::test_actor())
            .await
            .expect("put file");

        let cache = MetadataCache::default();
        let recording = Arc::new(RecordingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::any(),
        ));
        let reader = LoonFs::builder_with_store(recording.clone())
            .metadata_cache(cache.clone())
            .config(crate::RuntimeConfig {
                manifest_revalidation_interval_ms: 0,
                max_staleness_ms: 0,
                ..crate::RuntimeConfig::default()
            })
            .read_only()
            .build()
            .await
            .expect("build reader");
        let namespace = reader.namespace(&namespace_id);
        namespace.read_file("/file.txt").await.expect("read");
        recording.reset();
        namespace.read_file("/file.txt").await.expect("reread");

        assert_ne!(
            cache.stats(),
            MetadataCacheStats::default(),
            "the reader reads through the cache given before the conversion"
        );
        assert!(
            recording.count(OperationSelector::Head) > 0,
            "the zero interval and staleness given before the conversion probe on every read"
        );
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
            OperationSelector::PutCreateIfAbsent,
        ));
        let runtime = LoonFs::builder_with_store(blocking.clone())
            .writer_id(loonfs_test_support::ids::writer_id(writer_id))
            .build()
            .await
            .expect("build runtime");
        runtime
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
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
                    .put_file("/parked.txt", b"body", &loonfs_test_support::test_actor())
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
            .put_file("/late.txt", b"body", &loonfs_test_support::test_actor())
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
