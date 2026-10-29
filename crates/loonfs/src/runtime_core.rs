//! The runtime core that read-only and writable runtimes share: the store
//! client, the settings, the execution budget, the clocks, the caches, and
//! the metrics.

use crate::metadata_cache::MetadataCacheBinding;
use crate::metrics::RuntimeInstruments;
use crate::{
    ExecutionBudget, MetadataCache, NamespaceId, ObjectStore, RuntimeConfig, SharedObjectStore,
};
use loonfs_core::cache::{
    CompletedUploadSessionCache, HeadAndWalTailCache, MetadataSegmentCache, ObjectCache,
};
use loonfs_core::time::Observation;
use loonfs_core::{
    BudgetPermit, MetadataRunPolicy, NamespaceReaderEngine, NamespaceStore, NamespaceWriterEngine,
};
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::{
    u64_from_usize, CapabilityDocument, MonotonicTimer, Subject, WriterId, API_GROUP_FILESYSTEM_V0,
    API_GROUP_MAINTENANCE_V0, LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST,
    LIMIT_COMMIT_MAX_CONTENT_TOKENS, LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_COMMIT_MAX_PRECONDITIONS,
    LIMIT_GC_MIN_GRACE_WINDOW_MS, MAX_SUBJECT_PRINCIPALS, PROTOCOL_VERSION,
};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, Weak};

mod read_heads;
mod read_position;
mod read_retry;

pub(crate) use read_heads::HeadConfirmation;
pub(crate) use read_position::{HeadPositions, ReadPositionSlot};
pub(crate) use read_retry::classify_read_result;

/// The object-store client, settings, budget, clocks, caches, and metrics
/// that read-only and writable runtimes share.
#[derive(Clone)]
pub(crate) struct RuntimeCore {
    inner: Arc<RuntimeCoreInner>,
    subject: Option<Subject>,
}

struct RuntimeCoreInner {
    store: SharedObjectStore,
    config: RuntimeConfig,
    execution_budget: ExecutionBudget,
    /// Layout and input limits for metadata compactions, whose merge input
    /// size comes from the execution budget.
    run_policy: MetadataRunPolicy,
    /// The object-store kind label of tracing, or `None` when the caller
    /// didn't identify the backend.
    store_kind: Option<ConfiguredObjectStoreKind>,
    timer: Arc<dyn MonotonicTimer>,
    wall_clock: Arc<dyn crate::WallClock>,
    metadata_cache: MetadataCache,
    /// This core's views of `metadata_cache`. The segment view reads under
    /// the scope of this core's object store, and the head and WAL-tail view
    /// reads under a scope minted for this core.
    metadata_segment_cache: Arc<MetadataSegmentCache>,
    head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
    /// The completed upload sessions that garbage collection keeps between
    /// passes, under the same scope as the head and WAL-tail view.
    completed_upload_sessions: CompletedUploadSessionCache,
    object_cache: Arc<ObjectCache>,
    /// Publication, collection, completed-compaction, and view-read metrics.
    instruments: Arc<RuntimeInstruments>,
    /// The writer sessions whose positions reads start from. A writable
    /// runtime sets it once, after it builds its publisher registry. Weak,
    /// because the registry keeps this core.
    head_positions: OnceLock<Weak<dyn HeadPositions>>,
}

impl RuntimeCore {
    /// Opens a runtime core that reads through `metadata_cache`, the views
    /// that [`MetadataCache::bind`] returned for it.
    #[allow(
        clippy::too_many_arguments,
        reason = "each component is required, and each has its own type"
    )]
    pub(crate) fn open(
        store: SharedObjectStore,
        config: RuntimeConfig,
        execution_budget: ExecutionBudget,
        run_policy: MetadataRunPolicy,
        store_kind: Option<ConfiguredObjectStoreKind>,
        metadata_cache: MetadataCacheBinding,
        instruments: Arc<RuntimeInstruments>,
        timer: Arc<dyn MonotonicTimer>,
        wall_clock: Arc<dyn crate::WallClock>,
    ) -> Self {
        Self {
            subject: None,
            inner: Arc::new(RuntimeCoreInner {
                store,
                config,
                execution_budget,
                run_policy,
                store_kind,
                timer,
                wall_clock,
                metadata_cache: metadata_cache.cache,
                metadata_segment_cache: Arc::new(metadata_cache.segment_blocks),
                head_and_wal_tail_cache: Arc::new(metadata_cache.heads_and_wal_tails),
                completed_upload_sessions: metadata_cache.completed_upload_sessions,
                object_cache: Arc::new(metadata_cache.objects),
                instruments,
                head_positions: OnceLock::new(),
            }),
        }
    }

    pub(crate) fn with_subject(&self, subject: Subject) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            subject: Some(subject),
        }
    }

    /// Returns the subject this core acts for, or `None` for an unscoped
    /// service runtime.
    pub(crate) fn subject(&self) -> Option<&Subject> {
        self.subject.as_ref()
    }

    /// Returns the settings this core was built with.
    pub(crate) fn config(&self) -> &RuntimeConfig {
        &self.inner.config
    }

    /// Returns the execution budget that this core's work draws from.
    pub(crate) fn execution_budget(&self) -> &ExecutionBudget {
        &self.inner.execution_budget
    }

    /// Waits for a read permit from the execution budget. Every public read
    /// takes one.
    pub(crate) async fn read_permit(&self) -> BudgetPermit<'_> {
        self.inner.execution_budget.read_permit().await
    }

    /// Waits for a fold permit from the execution budget.
    pub(crate) async fn fold_permit(&self) -> BudgetPermit<'_> {
        self.inner.execution_budget.fold_permit().await
    }

    /// Waits for a compaction permit from the execution budget.
    pub(crate) async fn compaction_permit(&self) -> BudgetPermit<'_> {
        self.inner.execution_budget.compaction_permit().await
    }

    /// Returns the largest file content that one buffered read returns.
    pub(crate) fn max_read_content_bytes(&self) -> u64 {
        self.inner.config.max_read_content_bytes
    }

    /// Returns the monotonic timer that this core measures its deadlines and
    /// revalidation checks with.
    pub(crate) fn timer(&self) -> &Arc<dyn MonotonicTimer> {
        &self.inner.timer
    }

    /// Reads the monotonic timer now.
    pub(crate) fn observation(&self) -> Observation {
        Observation::now(Arc::clone(&self.inner.timer))
    }

    /// Lets reads start from the positions of `head_positions`'s writer
    /// sessions.
    pub(crate) fn set_head_positions(&self, head_positions: Weak<dyn HeadPositions>) {
        assert!(
            self.inner.head_positions.set(head_positions).is_ok(),
            "a runtime core should get its writer sessions once"
        );
    }

    /// Returns whether reads start from the positions of a writable
    /// runtime's writer sessions.
    #[cfg(test)]
    pub(crate) fn has_head_positions(&self) -> bool {
        self.inner.head_positions.get().is_some()
    }

    /// Returns the position of the namespace's writer session in this
    /// runtime, or `None` when the runtime has no session for the namespace
    /// or the session has no position.
    #[cfg(test)]
    pub(crate) fn writer_position(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<Arc<loonfs_core::publish::WriterPosition>> {
        self.inner
            .head_positions
            .get()?
            .upgrade()?
            .read_position_slot(namespace_id)?
            .current()
    }

    /// Drops the position of the namespace's writer session in this runtime,
    /// if there is one, so reads start from the read cache until the
    /// session's next publish loads the namespace.
    pub(crate) async fn drop_writer_position(&self, namespace_id: &NamespaceId) {
        if let Some(positions) = self.inner.head_positions.get().and_then(Weak::upgrade) {
            positions.drop_position(namespace_id).await;
        }
    }

    /// Reads the handle's wall clock as unix milliseconds.
    pub(crate) fn now_ms(&self) -> u64 {
        self.inner.wall_clock.now_ms()
    }

    /// Returns the metadata cache this core reads through.
    pub(crate) fn metadata_cache(&self) -> &MetadataCache {
        &self.inner.metadata_cache
    }

    /// Returns the completed upload sessions that garbage collection keeps
    /// between passes.
    pub(crate) fn completed_upload_sessions(&self) -> &CompletedUploadSessionCache {
        &self.inner.completed_upload_sessions
    }

    /// This runtime's instrument set, for the publication, maintenance, and
    /// collection paths that report through it.
    pub(crate) fn instruments(&self) -> &Arc<RuntimeInstruments> {
        &self.inner.instruments
    }

    /// This core's view of the decoded segment blocks, for the maintenance
    /// and publication paths that read through it.
    pub(crate) fn metadata_segment_cache(&self) -> Arc<MetadataSegmentCache> {
        Arc::clone(&self.inner.metadata_segment_cache)
    }

    /// This core's view of the head and WAL-tail caches, where each writer
    /// session's position becomes the namespace's cached head, with its WAL
    /// tail, when the position ends.
    pub(crate) fn head_and_wal_tail_cache(&self) -> Arc<HeadAndWalTailCache> {
        Arc::clone(&self.inner.head_and_wal_tail_cache)
    }

    pub(crate) fn trace_mode(&self) -> &'static str {
        self.inner.config.trace_mode.as_str()
    }

    pub(crate) fn trace_store_kind(&self) -> &'static str {
        crate::trace::store_kind_label(self.inner.store_kind)
    }

    /// Returns capabilities implemented by the embedded runtime.
    ///
    /// A host may add extension capabilities before serving this document.
    pub(crate) fn capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            api_groups: vec![
                API_GROUP_FILESYSTEM_V0.to_owned(),
                API_GROUP_MAINTENANCE_V0.to_owned(),
            ],
            features: BTreeMap::new(),
            limits: {
                let mut limits = loonfs_types::pagination_capability_limits();
                limits.insert(
                    LIMIT_GC_MIN_GRACE_WINDOW_MS.to_owned(),
                    loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
                );
                limits.insert(
                    LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST.to_owned(),
                    u64_from_usize(MAX_SUBJECT_PRINCIPALS),
                );
                // The commit ceilings are this crate's, enforced before
                // planning on every transport, so a client can pre-validate a
                // batch instead of discovering the bound on rejection. A host
                // adds its own transport limits on top; these are not its to
                // set.
                for (key, value) in [
                    (
                        LIMIT_COMMIT_MAX_OPERATIONS,
                        loonfs_core::limits::MAX_COMMIT_OPERATIONS,
                    ),
                    (
                        LIMIT_COMMIT_MAX_PRECONDITIONS,
                        loonfs_core::limits::MAX_COMMIT_PRECONDITIONS,
                    ),
                    (
                        LIMIT_COMMIT_MAX_CONTENT_TOKENS,
                        loonfs_core::limits::MAX_COMMIT_CONTENT_TOKENS,
                    ),
                    (
                        LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
                        loonfs_core::limits::MAX_COMMIT_EXTERNAL_CONTENT_REFS,
                    ),
                    (
                        LIMIT_COMMIT_MAX_MESSAGE_BYTES,
                        loonfs_core::limits::MAX_COMMIT_MESSAGE_BYTES,
                    ),
                ] {
                    limits.insert(key.to_owned(), u64_from_usize(value));
                }
                limits
            },
        }
    }

    pub(crate) fn store(&self) -> &dyn ObjectStore {
        self.inner.store.as_ref()
    }

    /// This core's object-store client, for the few in-process consumers
    /// that read LoonFS-owned objects outside the handle surface.
    pub(crate) fn shared_store(&self) -> SharedObjectStore {
        Arc::clone(&self.inner.store)
    }

    /// Returns `namespace_id`'s components: this core's store, caches,
    /// clocks, content write permits, and run policy.
    pub(crate) fn namespace_store(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceStore<SharedObjectStore> {
        NamespaceStore::new(
            Arc::clone(&self.inner.store),
            namespace_id.clone(),
            self.metadata_segment_cache(),
            Arc::clone(&self.inner.head_and_wal_tail_cache),
            Arc::clone(&self.inner.object_cache),
            Arc::clone(&self.inner.wall_clock),
            Arc::clone(&self.inner.timer),
            self.inner.execution_budget.content_writes(),
            self.inner.run_policy,
        )
    }

    /// A read-only engine: no actor identity, so a reader cannot mutate
    /// even by mistake.
    pub(crate) fn reader_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceReaderEngine<SharedObjectStore> {
        let engine = NamespaceReaderEngine::reader(self.namespace_store(namespace_id));
        match &self.subject {
            Some(subject) => engine.with_subject(subject.clone()),
            None => engine,
        }
    }

    /// A mutating engine that commits as `writer_id`, with this core's
    /// subject.
    pub(crate) fn writer_engine(
        &self,
        writer_id: &WriterId,
        namespace_id: &NamespaceId,
    ) -> NamespaceWriterEngine<SharedObjectStore> {
        let engine =
            NamespaceWriterEngine::writer(self.namespace_store(namespace_id), writer_id.clone());
        match &self.subject {
            Some(subject) => engine.with_subject(subject.clone()),
            None => engine,
        }
    }
}
