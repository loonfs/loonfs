//! [`ReadCore`]: the read-side runtime state every handle shares, and the
//! writer-side state — actor identity, background work, publish observer —
//! that only a write-capable handle owns on top of it.

use crate::cache::{RuntimeCacheStatsInner, RuntimeControlCache};
use crate::config::{validate_writer_id, ReadConfig};
use crate::maintenance_runner::MaintenanceRunner;
use crate::metrics::RuntimeInstruments;
use crate::publisher::PublishObserver;
use crate::{
    ChangeSeq, CoreError, ErrorCode, InodeId, ListFileRevisionsResponse, NamespaceId, ObjectStore,
    RuntimeCacheStats,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    encode_cursor, CapabilityDocument, EffectiveLimit, FileRevision, FileRevisionsPageCursor, Page,
    PaginationPolicy, FEATURE_ATTRIBUTES, FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_COMMIT_MAX_CONTENT_TOKENS,
    LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS, LIMIT_COMMIT_MAX_MESSAGE_BYTES,
    LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_GC_MIN_GRACE_WINDOW_MS, PROFILE_ADMIN_V0, PROFILE_CORE_V0,
    PROTOCOL_VERSION,
};
use loonfs_core::cache::{
    MetadataTableCache, StoredMetadataBlockCache, WalTailProjectionCache,
    WalTailProjectionCacheConfig,
};
use loonfs_core::time::current_time_ms;
use loonfs_core::{MutationContext, NamespaceEngine};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Shared read-side runtime state: the object-store client and the read
/// caches. [`FsWriter`](crate::FsWriter), [`FsReader`](crate::FsReader), and
/// [`FsAdmin`](crate::FsAdmin) each hold one; handles built from the same
/// core share its caches and store.
///
/// A read core carries no actor identity, owns no publisher, and starts no
/// background work — it is what a read needs and nothing more. `ReadCore` is
/// cheap to clone.
#[derive(Clone)]
pub(crate) struct ReadCore {
    pub(crate) inner: Arc<ReadCoreInner>,
}

pub(crate) struct ReadCoreInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: ReadConfig,
    pub(crate) control_cache: Mutex<RuntimeControlCache>,
    pub(crate) metadata_table_cache: Arc<MetadataTableCache>,
    pub(crate) wal_tail_projection_cache: Arc<WalTailProjectionCache>,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
    /// Where this runtime's publication, maintenance, and collection
    /// numbers go. Reports nothing unless the handle was built with a
    /// metrics recorder.
    pub(crate) instruments: Arc<RuntimeInstruments>,
}

/// The actor identity a write-capable handle publishes under: immutable
/// plain data, minted once when the handle is built.
#[derive(Clone)]
pub(crate) struct WriterIdentity {
    pub(crate) writer_id: String,
}

/// What a publisher worker needs from the writer that owns it, in one
/// allocation the worker can hold weakly: dropping the writer drops these,
/// and admitted work then settles as `shutting_down` instead of publishing
/// under an identity nobody owns any more.
pub(crate) struct WriterBits {
    pub(crate) identity: WriterIdentity,
    /// Admission for every background maintenance step this writer
    /// schedules. Write paths reach it through a nudge-only handle; only
    /// the writer shuts it down.
    pub(crate) maintenance: MaintenanceRunner,
    /// Optional synchronous notification after a mutation batch durably
    /// advances a namespace. Callers promise that it does not block.
    pub(crate) publish_observer: Option<PublishObserver>,
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
    /// Opens a read core. `shared_metadata_table_cache` substitutes an
    /// existing decoded-block cache for a freshly sized one, so handles
    /// with distinct actor identities can still share warmed blocks —
    /// sound across cores because entries are keyed by immutable
    /// identities (payload checksums and manifest object keys); the
    /// sharing caller owns the sizing decision, and
    /// `config.runtime_cache.metadata_table_cache` goes unused.
    ///
    /// `stored_metadata_block_cache` is the node-local cache of encoded
    /// blocks the fresh decoded cache carries beneath it. It applies only
    /// when this core sizes its own cache: a shared cache already carries
    /// whatever handle it was built with, which is what makes the sharing
    /// caller's sizing decision cover both tiers together.
    pub(crate) fn open(
        store: SharedObjectStore,
        config: ReadConfig,
        shared_metadata_table_cache: Option<Arc<MetadataTableCache>>,
        stored_metadata_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        instruments: Arc<RuntimeInstruments>,
    ) -> Self {
        let metadata_table_cache = shared_metadata_table_cache.unwrap_or_else(|| {
            Arc::new(MetadataTableCache::with_stored_block_cache_and_observer(
                config.runtime_cache.metadata_table_cache.clone(),
                stored_metadata_block_cache,
                instruments.metadata_table_cache_observer(),
            ))
        });
        let wal_tail_projection_cache = Arc::new(WalTailProjectionCache::with_observer(
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
                metadata_table_cache,
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
    pub(crate) fn metadata_table_cache(&self) -> Arc<MetadataTableCache> {
        Arc::clone(&self.inner.metadata_table_cache)
    }

    pub(crate) fn trace_mode(&self) -> &'static str {
        self.inner.config.trace_mode.as_str()
    }

    pub(crate) fn trace_store_kind(&self) -> &'static str {
        self.inner.config.trace_store_kind.as_str()
    }

    pub(super) fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.trace_mode());
        span.record("store_kind", self.trace_store_kind());
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    ///
    /// The runtime implements the core and admin planes, so the answer is a
    /// constant. It describes this crate and nothing else: a host that
    /// composes an extension — the reference server composing `loonfs-grep`,
    /// for one — merges that extension's profile, features, and limits into
    /// this document before serving it. Callers should still gate on the
    /// document rather than on the backend kind, so the same logic works
    /// against remote deployments that implement more or less.
    pub(crate) fn capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![PROFILE_CORE_V0.to_owned(), PROFILE_ADMIN_V0.to_owned()],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                // Attributes are core, implemented by this crate, so the
                // answer does not depend on what a serving host composes.
                (FEATURE_ATTRIBUTES.to_owned(), true),
                // The three transfer keys are the host's to answer, not
                // this runtime's: an embedded engine signs nothing and
                // reads its own bytes. A serving host that can presign
                // replaces all three together.
                (FEATURE_UPLOADS_DIRECT_PUT.to_owned(), false),
                (FEATURE_UPLOADS_DIRECT_MULTIPART.to_owned(), false),
                (FEATURE_DOWNLOADS_DIRECT_GET.to_owned(), false),
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
            self.inner.metadata_table_cache.stats(),
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
    ) -> NamespaceEngine<SharedObjectStore> {
        NamespaceEngine::builder(self.inner.store.clone())
            .namespace_id(namespace_id.clone())
            .build_reader()
            .expect("a namespace id is the reader engine's only requirement")
    }

    /// A mutating engine bound to one actor identity.
    pub(crate) fn writer_engine(
        &self,
        actor: &WriterIdentity,
        namespace_id: &NamespaceId,
    ) -> NamespaceEngine<SharedObjectStore> {
        NamespaceEngine::builder(self.inner.store.clone())
            .namespace_id(namespace_id.clone())
            .writer_id(actor.writer_id.clone())
            .build()
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

pub(super) fn default_page_limit() -> EffectiveLimit {
    PaginationPolicy::default()
        .resolve_limit(None)
        .expect("default pagination policy must resolve its default limit")
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
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(encode_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}
