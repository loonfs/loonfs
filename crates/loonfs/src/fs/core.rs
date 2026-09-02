//! Shared read state and additional state used by writers.

use crate::cache::{RuntimeCacheStatsInner, RuntimeControlCache};
use crate::config::{validate_writer_id, ReadConfig};
use crate::maintenance_runner::{MaintenanceRunner, NamespacePublication};
use crate::metrics::RuntimeInstruments;
use crate::publisher::{NamespaceAdvanceHint, NamespaceAdvanceObserver};
use crate::{
    ChangeSeq, CoreError, ErrorCode, InodeId, ListFileRevisionsResponse, NamespaceId, ObjectStore,
    RuntimeCacheStats,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    encode_cursor, CapabilityDocument, FileRevision, FileRevisionsPageCursor, Page, PageCursor,
    PaginationPolicy, FEATURE_ATTRIBUTES, FEATURE_INODES_LIST_CHILDREN, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_SNAPSHOTS,
    LIMIT_COMMIT_MAX_CONTENT_TOKENS, LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_GC_MIN_GRACE_WINDOW_MS,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
use loonfs_core::cache::{
    MetadataSegmentCache, StoredMetadataBlockCache, WalTailProjectionCache,
    WalTailProjectionCacheConfig,
};
use loonfs_core::time::current_time_ms;
use loonfs_core::{MutationContext, NamespaceReaderEngine, NamespaceWriterEngine};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Shared object-store client, read configuration, caches, and metrics.
#[derive(Clone)]
pub(crate) struct ReadCore {
    pub(crate) inner: Arc<ReadCoreInner>,
}

pub(crate) struct ReadCoreInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: ReadConfig,
    pub(crate) control_cache: Mutex<RuntimeControlCache>,
    pub(crate) metadata_segment_cache: Arc<MetadataSegmentCache>,
    pub(crate) wal_tail_projection_cache: Arc<WalTailProjectionCache>,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
    /// Publication, maintenance, and collection metrics.
    pub(crate) instruments: Arc<RuntimeInstruments>,
}

/// Actor identity used by a writer.
#[derive(Clone)]
pub(crate) struct WriterIdentity {
    pub(crate) writer_id: String,
}

/// Writer state shared weakly with the publisher worker.
pub(crate) struct WriterBits {
    pub(crate) identity: WriterIdentity,
    /// Background maintenance owned by this writer.
    pub(crate) maintenance: MaintenanceRunner,
    /// Optional synchronous notification after a mutation batch durably
    /// advances a namespace. Callers promise that it does not block.
    pub(crate) namespace_advance_observer: Option<NamespaceAdvanceObserver>,
}

impl WriterBits {
    /// Notifies maintenance after every publish attempt and the observer
    /// after a successful commit.
    pub(crate) fn notify_after_publish(
        &self,
        namespace_id: &NamespaceId,
        publication: &NamespacePublication,
    ) {
        // Notify maintenance before running host code.
        self.maintenance
            .nudge_jobs_after_publication(namespace_id, publication);

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
}

impl WriterIdentity {
    /// Mints an identity, rejecting an empty actor id.
    pub(crate) fn new(writer_id: String) -> Result<Self> {
        validate_writer_id(&writer_id)?;
        Ok(Self { writer_id })
    }

    pub(crate) fn mutation_context(&self) -> Result<MutationContext> {
        Ok(MutationContext {
            writer_id: self.writer_id.clone(),
            now_ms: current_time_ms()?,
        })
    }
}

/// Lock accessors for the runtime caches.
///
/// Poisoning is propagated as a panic: a poisoned cache means another thread
/// panicked mid-update, and serving from it could violate the consistency the
/// caches promise.
impl ReadCoreInner {
    pub(crate) fn control_cache(&self) -> MutexGuard<'_, RuntimeControlCache> {
        self.control_cache
            .lock()
            .expect("control cache lock poisoned")
    }
}

impl ReadCore {
    /// Opens a read core. When `shared_metadata_segment_cache` is set, the
    /// core reuses that decoded-block cache instead of creating one from
    /// `config.runtime_cache.metadata_segment_cache`. Sharing is safe because
    /// entries are keyed by immutable payload checksums and manifest keys.
    ///
    /// `stored_metadata_block_cache` provides a node-local encoded-block cache
    /// when this core creates its own decoded cache. A shared decoded cache
    /// already has its encoded cache configured.
    pub(crate) fn open(
        store: SharedObjectStore,
        config: ReadConfig,
        shared_metadata_segment_cache: Option<Arc<MetadataSegmentCache>>,
        stored_metadata_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        instruments: Arc<RuntimeInstruments>,
    ) -> Self {
        let metadata_segment_cache = shared_metadata_segment_cache.unwrap_or_else(|| {
            Arc::new(MetadataSegmentCache::with_stored_block_cache_and_observer(
                config.runtime_cache.metadata_segment_cache.clone(),
                stored_metadata_block_cache,
                instruments.metadata_segment_cache_observer(),
            ))
        });
        let wal_tail_projection_cache = Arc::new(WalTailProjectionCache::new(
            WalTailProjectionCacheConfig {
                max_entries: config.runtime_cache.max_cached_namespaces,
                max_rows: config.runtime_cache.max_cached_wal_tail_projection_rows,
                max_decoded_bytes: config
                    .runtime_cache
                    .max_cached_wal_tail_projection_decoded_bytes,
            },
            instruments.wal_tail_projection_cache_observer(),
        ));
        Self {
            inner: Arc::new(ReadCoreInner {
                store,
                config,
                control_cache: Mutex::new(RuntimeControlCache::default()),
                metadata_segment_cache,
                wal_tail_projection_cache,
                cache_stats: RuntimeCacheStatsInner::new(Arc::clone(&instruments)),
                instruments,
            }),
        }
    }

    /// This runtime's instrument set, for the publication, maintenance, and
    /// collection paths that report through it.
    pub(crate) fn instruments(&self) -> &Arc<RuntimeInstruments> {
        &self.inner.instruments
    }

    /// This runtime's shared decoded-block cache handle, for builders
    /// that open another core sharing it.
    pub(crate) fn metadata_segment_cache(&self) -> Arc<MetadataSegmentCache> {
        Arc::clone(&self.inner.metadata_segment_cache)
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
    pub(crate) fn get_capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![PROFILE_CORE_V0.to_owned(), PROFILE_ADMIN_V0.to_owned()],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                (FEATURE_SNAPSHOTS.to_owned(), true),
                // Attributes are core, implemented by this crate, so the
                // answer does not depend on what a serving host composes.
                (FEATURE_ATTRIBUTES.to_owned(), true),
                (FEATURE_INODES_LIST_CHILDREN.to_owned(), true),
            ]),
            limits: {
                let mut limits = PaginationPolicy::default().capability_limits();
                limits.insert(
                    LIMIT_GC_MIN_GRACE_WINDOW_MS.to_owned(),
                    loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
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

    /// Snapshots the runtime cache counters.
    pub(crate) fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner.cache_stats.snapshot(
            self.inner.metadata_segment_cache.stats(),
            self.inner.wal_tail_projection_cache.stats(),
        )
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
        NamespaceReaderEngine::reader(self.inner.store.clone(), namespace_id.clone())
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
        .expect("a validated actor identity should build a namespace engine")
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
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
    fallback_inode_id: Option<InodeId>,
) -> std::result::Result<ListFileRevisionsResponse, CoreError> {
    let inode_id = page
        .items
        .first()
        .map(|revision| revision.inode_id)
        .or_else(|| page.next_cursor.as_ref().map(|cursor| cursor.inode_id))
        .or(fallback_inode_id)
        .ok_or_else(|| {
            CoreError::InvalidCursor("empty revision page lacks inode identity".into())
        })?;
    let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}
