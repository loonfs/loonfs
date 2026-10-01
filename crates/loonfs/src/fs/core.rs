//! The runtime core that read-only and writable runtimes share, and the extra
//! state a writer holds.

use crate::config::ReadConfig;
use crate::metrics::RuntimeInstruments;
use crate::publisher::{NamespaceAdvanceHint, NamespaceAdvanceObserver};
use crate::{
    ChangeSeq, CoreError, ErrorCode, InodeId, ListFileRevisionsResponse, MaintenanceHint,
    MaintenanceHintObserver, MetadataCache, NamespaceId, NamespacePublication, ObjectStore,
};
use crate::{Error, Result, SharedObjectStore};
use loonfs_api::{
    encode_cursor, CapabilityDocument, FileRevision, FileRevisionsPageCursor, Page, PageCursor,
    PaginationPolicy, Subject, WriterId, API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0,
    FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK,
    FEATURE_SNAPSHOTS, LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST, LIMIT_COMMIT_MAX_CONTENT_TOKENS,
    LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS, LIMIT_COMMIT_MAX_MESSAGE_BYTES,
    LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_COMMIT_MAX_PRECONDITIONS, LIMIT_GC_MIN_GRACE_WINDOW_MS,
    MAX_SUBJECT_PRINCIPALS, PROTOCOL_VERSION,
};
use loonfs_core::cache::{HeadStateCache, MetadataSegmentCache, StoredMetadataBlockCache};
use loonfs_core::{MutationContext, NamespaceReaderEngine, NamespaceWriterEngine};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// The object-store client, configuration, caches, and metrics that
/// read-only and writable runtimes share.
#[derive(Clone)]
pub(crate) struct RuntimeCore {
    pub(crate) inner: Arc<RuntimeCoreInner>,
    pub(crate) subject: Option<Subject>,
}

pub(crate) struct RuntimeCoreInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: ReadConfig,
    pub(crate) timer: Arc<dyn loonfs_api::MonotonicTimer>,
    pub(crate) wall_clock: Arc<dyn crate::WallClock>,
    pub(crate) metadata_cache: MetadataCache,
    /// This core's views of `metadata_cache`, under the scope the cache
    /// minted for this core.
    pub(crate) metadata_segment_cache: Arc<MetadataSegmentCache>,
    pub(crate) head_state: Arc<HeadStateCache>,
    /// Publication, collection, completed-compaction, and view-read metrics.
    pub(crate) instruments: Arc<RuntimeInstruments>,
}

/// Actor identity used by a writer.
#[derive(Clone)]
pub(crate) struct WriterIdentity {
    pub(crate) writer_id: WriterId,
}

/// Writer state shared weakly with the publisher worker.
pub(crate) struct WriterBits {
    pub(crate) inline_content: crate::InlineContentPolicy,
    pub(crate) identity: WriterIdentity,
    pub(crate) wal_fold_permits: Semaphore,
    pub(crate) wal_folds_waiting: AtomicUsize,
    pub(crate) maintenance_hint_observer: Option<MaintenanceHintObserver>,
    /// Optional synchronous notification after a mutation batch durably
    /// advances a namespace. Callers promise that it does not block.
    pub(crate) namespace_advance_observer: Option<NamespaceAdvanceObserver>,
}

impl WriterBits {
    /// Notifies maintenance after every publish attempt and the change observer
    /// after a successful commit.
    pub(crate) fn notify_after_publish(
        &self,
        namespace_id: &NamespaceId,
        publication: &NamespacePublication,
    ) {
        self.send_maintenance_hint(
            namespace_id,
            MaintenanceHint::Published(publication.clone()),
        );

        let Some(through_seq) = publication.committed_through_seq else {
            return;
        };
        let Some(observer) = &self.namespace_advance_observer else {
            return;
        };
        let hint = NamespaceAdvanceHint {
            namespace_id: namespace_id.clone(),
            through_seq,
        };
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(hint)));
        if observed.is_err() {
            tracing::error!(
                namespace_id = %namespace_id,
                through_seq = through_seq.0,
                "namespace advance observer panicked; the commit was already durable"
            );
        }
    }

    pub(crate) fn notify_fold_finished(&self, namespace_id: &NamespaceId) {
        self.send_maintenance_hint(
            namespace_id,
            MaintenanceHint::WalFoldFinished {
                namespace_id: namespace_id.clone(),
            },
        );
    }

    pub(crate) fn send_maintenance_hint(&self, namespace_id: &NamespaceId, hint: MaintenanceHint) {
        let Some(observer) = &self.maintenance_hint_observer else {
            return;
        };
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(hint)));
        if observed.is_err() {
            tracing::error!(
                namespace_id = %namespace_id,
                "maintenance hint observer panicked; the durable state it describes is unaffected"
            );
        }
    }
}

impl WriterIdentity {
    /// Mints an identity, rejecting a blank writer id.
    pub(crate) fn new(writer_id: String) -> Result<Self> {
        let writer_id =
            WriterId::parse(writer_id).map_err(|error| Error::Config(error.to_string()))?;
        Ok(Self { writer_id })
    }
}

impl RuntimeCore {
    pub(crate) fn with_subject(&self, subject: Subject) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            subject: Some(subject),
        }
    }

    /// Opens a runtime core that reads through `metadata_cache` under a
    /// scope of its own, with `stored_metadata_block_cache` as its node-local
    /// encoded tier.
    pub(crate) fn open(
        store: SharedObjectStore,
        config: ReadConfig,
        metadata_cache: MetadataCache,
        stored_metadata_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        instruments: Arc<RuntimeInstruments>,
        timer: Arc<dyn loonfs_api::MonotonicTimer>,
        wall_clock: Arc<dyn crate::WallClock>,
    ) -> Self {
        let (metadata_segment_cache, head_state) = metadata_cache.bind(
            config.metadata_lsm_policy.max_block_memo_bytes,
            stored_metadata_block_cache,
        );
        Self {
            subject: None,
            inner: Arc::new(RuntimeCoreInner {
                store,
                config,
                timer,
                wall_clock,
                metadata_cache,
                metadata_segment_cache: Arc::new(metadata_segment_cache),
                head_state: Arc::new(head_state),
                instruments,
            }),
        }
    }

    /// Reads the handle's wall clock as unix milliseconds.
    pub(crate) fn now_ms(&self) -> Result<u64> {
        Ok(self.inner.wall_clock.now_ms()?)
    }

    /// Stamps a mutation by `actor` with the handle's wall clock.
    pub(crate) fn mutation_context(&self, actor: &WriterIdentity) -> Result<MutationContext> {
        Ok(MutationContext {
            writer_id: actor.writer_id.clone(),
            now_ms: self.now_ms()?,
        })
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

    /// This core's view of the head-state cache, where publishers keep their
    /// WAL tails between publication units.
    pub(crate) fn head_state(&self) -> Arc<HeadStateCache> {
        Arc::clone(&self.inner.head_state)
    }

    pub(crate) fn trace_mode(&self) -> &'static str {
        self.inner.config.trace_mode.as_str()
    }

    pub(crate) fn trace_store_kind(&self) -> &'static str {
        self.inner.config.trace_store_kind.as_str()
    }

    pub(crate) fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.trace_mode());
        span.record("store_kind", self.trace_store_kind());
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
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                (FEATURE_SNAPSHOTS.to_owned(), true),
            ]),
            limits: {
                let mut limits = PaginationPolicy::default().capability_limits();
                limits.insert(
                    LIMIT_GC_MIN_GRACE_WINDOW_MS.to_owned(),
                    loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
                );
                limits.insert(
                    LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST.to_owned(),
                    MAX_SUBJECT_PRINCIPALS as u64,
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
                    limits.insert(key.to_owned(), value as u64);
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

    /// A read-only engine: no actor identity, so a reader cannot mutate
    /// even by mistake.
    pub(crate) fn reader_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceReaderEngine<SharedObjectStore> {
        let engine = NamespaceReaderEngine::reader(self.inner.store.clone(), namespace_id.clone());
        match &self.subject {
            Some(subject) => engine.with_subject(subject.clone()),
            None => engine,
        }
    }

    /// A mutating engine bound to one actor identity.
    pub(crate) fn writer_engine(
        &self,
        actor: &WriterIdentity,
        namespace_id: &NamespaceId,
    ) -> NamespaceWriterEngine<SharedObjectStore> {
        NamespaceWriterEngine::writer(
            self.inner.store.clone(),
            namespace_id.clone(),
            actor.writer_id.clone(),
        )
        .with_wall_clock(self.inner.wall_clock.clone())
        .with_metadata_lsm_policy(self.inner.config.metadata_lsm_policy)
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(Error::Core(error))
            if matches!(error.code(), ErrorCode::StaleHead | ErrorCode::WriterFenced) =>
        {
            true
        }
        _ => false,
    }
}

pub(super) fn encode_next_cursor<C: PageCursor>(
    cursor: Option<&C>,
) -> std::result::Result<Option<String>, CoreError> {
    cursor
        .map(encode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))
}

pub(super) fn file_revisions_page_response(
    namespace_id: NamespaceId,
    head_seq: ChangeSeq,
    page: Page<FileRevision, FileRevisionsPageCursor>,
    inode_id: InodeId,
) -> std::result::Result<ListFileRevisionsResponse, CoreError> {
    let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}
