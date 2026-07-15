//! Runtime caching: control-object reads, WAL-tail projections, and
//! per-namespace commit-engine serialization, plus the cache-management half of [`FsCore`].
//!
//! Every cache revalidates against durable state before its contents are
//! used; nothing here weakens read-after-write consistency.

use crate::fs::{should_invalidate_after_result, FsCore};
use crate::{CommitResponse, CoreError, NamespaceId};
use crate::{Result, RuntimeError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ManifestId, ManifestObjectId};
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheStats, WalTailProjectionCacheStats,
};
use loonfs_core::control::{
    load_namespace_catalog_entry, load_namespace_read_anchor, ControlObjectIdentity,
    ControlObjectLoadError, LoadedHeadControl, LoadedMetadataRootControl,
    VerifiedNamespaceCatalogEntry,
};
use loonfs_core::publish::{NamespaceCommitEngine, SharedWriterSessionState};
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext};
use loonfs_objectstore::keys::{namespace_config, wal_head};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Default)]

pub(crate) struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, NamespaceControlCacheEntry>,
    namespace_order: VecDeque<NamespaceId>,
}

#[derive(Debug, Default)]
struct NamespaceControlCacheEntry {
    head: Option<CachedNamespaceAnchor>,
    /// The namespace's spec-immutable catalog pair (config plus content-store
    /// binding): loaded once, never revalidated.
    catalog: Option<VerifiedNamespaceCatalogEntry>,
    /// This process's commit engine for the namespace: publication
    /// serialization plus rebuildable state (tail projection, catalog).
    /// The session's acquired epoch and fencing live in the writer-session
    /// registry, which this entry's invalidation or eviction never touches.
    engine: Option<Arc<AsyncMutex<NamespaceCommitEngine>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedControl<T> {
    pub(crate) identity: ControlObjectIdentity,
    pub(crate) state: T,
}

/// Head snapshot pinned together with the manifest the metadata root
/// referenced when it was taken. The pair stays consistent even when
/// compaction moves the live root past this head; reads at the pin replay a
/// little more WAL until the cache refreshes.
#[derive(Debug, Clone)]
pub(crate) struct CachedNamespaceAnchor {
    pub(crate) head: CachedControl<HeadState>,
    pub(crate) manifest_id: ManifestId,
    pub(crate) manifest_object_id: ManifestObjectId,
}

/// Snapshot of runtime cache counters.
///
/// These counters are diagnostic. They are useful for tuning cache limits and
/// understanding read/write warmup behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    /// Latest metadata reads served through the metadata-view path.
    pub latest_metadata_view_reads: usize,
    /// WAL-tail projection cache hits.
    pub wal_tail_projection_cache_hits: usize,
    /// WAL-tail projection cache misses.
    pub wal_tail_projection_cache_misses: usize,
    /// WAL-tail projections inserted.
    pub wal_tail_projection_cache_inserts: usize,
    /// WAL-tail projections evicted or invalidated.
    pub wal_tail_projection_cache_evictions: usize,
    /// Metadata rows dropped with evicted WAL-tail projections.
    pub wal_tail_projection_cache_evicted_rows: usize,
    /// Decoded bytes dropped with evicted WAL-tail projections.
    pub wal_tail_projection_cache_evicted_decoded_bytes: usize,
    /// WAL-tail projections too heavy to cache under configured limits.
    pub wal_tail_projection_cache_uncacheable_count: usize,
    /// Metadata rows in WAL-tail projections too heavy to cache.
    pub wal_tail_projection_cache_uncacheable_rows: usize,
    /// Decoded bytes in WAL-tail projections too heavy to cache.
    pub wal_tail_projection_cache_uncacheable_decoded_bytes: usize,
    /// Metadata rows currently retained across cached WAL-tail projections.
    pub wal_tail_projection_cache_cached_rows: usize,
    /// Decoded bytes currently retained across cached WAL-tail projections.
    pub wal_tail_projection_cache_cached_decoded_bytes: usize,
    /// Decoded metadata-table cache hits.
    pub metadata_table_cache_hits: usize,
    /// Decoded metadata-table cache misses.
    pub metadata_table_cache_misses: usize,
    /// Blocks inserted into the decoded metadata-table cache.
    pub metadata_table_cache_inserts: usize,
    /// Blocks evicted from the decoded metadata-table cache.
    pub metadata_table_cache_evictions: usize,
    /// Segments skipped by their bloom filter before any index or data read.
    pub metadata_table_cache_filter_skips: usize,
    /// Segments whose filter admitted a lookup that matched no rows.
    pub metadata_table_cache_filter_false_positives: usize,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeCacheStatsInner {
    latest_metadata_view_reads: AtomicUsize,
}

impl RuntimeCacheStatsInner {
    pub(crate) fn snapshot(
        &self,
        metadata_table_cache: MetadataTableCacheStats,
        wal_tail_projection_cache: WalTailProjectionCacheStats,
    ) -> RuntimeCacheStats {
        RuntimeCacheStats {
            latest_metadata_view_reads: self.latest_metadata_view_reads.load(Ordering::SeqCst),
            wal_tail_projection_cache_hits: wal_tail_projection_cache.hits,
            wal_tail_projection_cache_misses: wal_tail_projection_cache.misses,
            wal_tail_projection_cache_inserts: wal_tail_projection_cache.inserts,
            wal_tail_projection_cache_evictions: wal_tail_projection_cache.evictions,
            wal_tail_projection_cache_evicted_rows: wal_tail_projection_cache.evicted_rows,
            wal_tail_projection_cache_evicted_decoded_bytes: wal_tail_projection_cache
                .evicted_decoded_bytes,
            wal_tail_projection_cache_uncacheable_count: wal_tail_projection_cache
                .uncacheable_count,
            wal_tail_projection_cache_uncacheable_rows: wal_tail_projection_cache.uncacheable_rows,
            wal_tail_projection_cache_uncacheable_decoded_bytes: wal_tail_projection_cache
                .uncacheable_decoded_bytes,
            wal_tail_projection_cache_cached_rows: wal_tail_projection_cache.cached_rows,
            wal_tail_projection_cache_cached_decoded_bytes: wal_tail_projection_cache
                .cached_decoded_bytes,
            metadata_table_cache_hits: metadata_table_cache.hits,
            metadata_table_cache_misses: metadata_table_cache.misses,
            metadata_table_cache_inserts: metadata_table_cache.inserts,
            metadata_table_cache_evictions: metadata_table_cache.evictions,
            metadata_table_cache_filter_skips: metadata_table_cache.filter_skips,
            metadata_table_cache_filter_false_positives: metadata_table_cache
                .filter_false_positives,
        }
    }

    pub(crate) fn record_latest_metadata_view_read(&self) {
        self.latest_metadata_view_reads
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl RuntimeControlCache {
    fn wal_head(&mut self, namespace_id: &NamespaceId) -> Option<CachedNamespaceAnchor> {
        let head = self.namespaces.get(namespace_id)?.head.clone()?;
        self.touch_namespace(namespace_id);
        Some(head)
    }

    fn insert_namespace_head(
        &mut self,
        namespace_id: &NamespaceId,
        head: CachedNamespaceAnchor,
        max_cached_namespaces: usize,
    ) {
        if max_cached_namespaces == 0 {
            return;
        }
        self.namespace_entry(namespace_id, max_cached_namespaces)
            .head = Some(head);
    }

    fn catalog(&mut self, namespace_id: &NamespaceId) -> Option<VerifiedNamespaceCatalogEntry> {
        let catalog = self.namespaces.get(namespace_id)?.catalog.clone()?;
        self.touch_namespace(namespace_id);
        Some(catalog)
    }

    fn insert_catalog(
        &mut self,
        namespace_id: &NamespaceId,
        catalog: VerifiedNamespaceCatalogEntry,
        max_cached_namespaces: usize,
    ) {
        if max_cached_namespaces == 0 {
            return;
        }
        self.namespace_entry(namespace_id, max_cached_namespaces)
            .catalog = Some(catalog);
    }

    fn invalidate_namespace(&mut self, namespace_id: &NamespaceId) {
        // The head anchor goes stale on every mutation; the catalog pair is
        // immutable for the namespace's lifetime (a deleted namespace id
        // never rebinds), and the engine drops only its tail projection —
        // the session's epoch and fencing live in the writer-session
        // registry, which invalidation never touches. A held engine lock
        // means a publish is in flight; that publish revalidates against
        // the live head itself.
        if let Some(entry) = self.namespaces.get_mut(namespace_id) {
            entry.head = None;
            if let Some(engine) = &entry.engine {
                if let Ok(mut engine) = engine.try_lock() {
                    engine.invalidate();
                }
            }
        }
    }

    /// Namespace-terminal removal: the whole entry goes, engine included.
    fn remove_namespace(&mut self, namespace_id: &NamespaceId) {
        if let Some(entry) = self.namespaces.remove(namespace_id) {
            invalidate_dropped_engine(&entry);
        }
        self.namespace_order
            .retain(|candidate| candidate != namespace_id);
    }

    fn engine(
        &mut self,
        namespace_id: &NamespaceId,
        max_cached_namespaces: usize,
        table_cache: Arc<MetadataTableCache>,
        session: SharedWriterSessionState,
    ) -> Arc<AsyncMutex<NamespaceCommitEngine>> {
        if max_cached_namespaces == 0 {
            // Diagnostic mode: nothing is cached, every publish gets a
            // throwaway engine — carrying the session state, which is not
            // a cache and never gets disabled.
            return Arc::new(AsyncMutex::new(
                NamespaceCommitEngine::new(namespace_id.clone())
                    .with_table_cache(table_cache)
                    .with_writer_session(session),
            ));
        }
        let entry = self.namespace_entry(namespace_id, max_cached_namespaces);
        if let Some(engine) = &entry.engine {
            return Arc::clone(engine);
        }
        let mut engine = NamespaceCommitEngine::new(namespace_id.clone())
            .with_table_cache(table_cache)
            .with_writer_session(session);
        if let Some(catalog) = &entry.catalog {
            engine = engine.with_catalog_entry(catalog.clone());
        }
        let engine = Arc::new(AsyncMutex::new(engine));
        entry.engine = Some(Arc::clone(&engine));
        engine
    }

    fn namespace_entry(
        &mut self,
        namespace_id: &NamespaceId,
        max_cached_namespaces: usize,
    ) -> &mut NamespaceControlCacheEntry {
        self.namespaces.entry(namespace_id.clone()).or_default();
        self.touch_namespace(namespace_id);
        while self.namespaces.len() > max_cached_namespaces {
            let Some(evicted) = self.namespace_order.pop_front() else {
                break;
            };
            if let Some(entry) = self.namespaces.remove(&evicted) {
                invalidate_dropped_engine(&entry);
            }
        }
        self.namespaces
            .get_mut(namespace_id)
            .expect("namespace cache entry should exist")
    }

    fn touch_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespace_order
            .retain(|candidate| candidate != namespace_id);
        self.namespace_order.push_back(namespace_id.clone());
    }
}

impl FsCore {
    pub(crate) async fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedNamespaceAnchor, ControlObjectLoadError> {
        let cache_config = &self.inner.config.runtime_cache;
        if !self.control_cache_enabled() {
            return load_namespace_read_anchor(self.store(), namespace_id)
                .await
                .map(cached_anchor);
        }

        let cached = self.inner.control_cache().wal_head(namespace_id);
        if let Some(head) = cached {
            match self
                .cached_control_identity_matches(
                    &wal_head(namespace_id.as_str()),
                    &head.head.identity,
                )
                .await
            {
                Ok(true) => return Ok(head),
                Ok(false) => self
                    .inner
                    .control_cache()
                    .invalidate_namespace(namespace_id),
                Err(error) => {
                    self.inner
                        .control_cache()
                        .invalidate_namespace(namespace_id);
                    return Err(error);
                }
            }
        }

        let loaded = match load_namespace_read_anchor(self.store(), namespace_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.inner
                    .control_cache()
                    .invalidate_namespace(namespace_id);
                return Err(error);
            }
        };
        let head = cached_anchor(loaded);
        self.inner.control_cache().insert_namespace_head(
            namespace_id,
            head.clone(),
            cache_config.max_cached_namespaces,
        );
        Ok(head)
    }

    pub(crate) async fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CachedNamespaceAnchor> {
        match self.load_namespace_head_cached(namespace_id).await {
            Ok(head) => Ok(head),
            Err(ControlObjectLoadError::MissingObject { object_key }) => {
                Err(self.missing_head_read_error(namespace_id, object_key).await)
            }
            Err(error) => Err(RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(error),
            ))),
        }
    }

    async fn missing_head_read_error(
        &self,
        namespace_id: &NamespaceId,
        head_key: String,
    ) -> RuntimeError {
        let descriptor_key = namespace_config(namespace_id.as_str());
        let descriptor_exists = match self.store().head(&descriptor_key).await {
            Ok(value) => value.is_some(),
            Err(error) => {
                return RuntimeError::Core(CoreError::Store {
                    object_key: descriptor_key,
                    message: error.message(),
                })
            }
        };
        if descriptor_exists {
            RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(ControlObjectLoadError::MissingObject {
                    object_key: head_key,
                }),
            ))
        } else {
            RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadNamespaceDescriptor(
                    ControlObjectLoadError::MissingObject {
                        object_key: descriptor_key,
                    },
                ),
            ))
        }
    }

    async fn cached_control_identity_matches(
        &self,
        object_key: &str,
        identity: &ControlObjectIdentity,
    ) -> std::result::Result<bool, ControlObjectLoadError> {
        let metadata = self
            .store()
            .head(object_key)
            .await
            .map_err(|error| ControlObjectLoadError::Store {
                object_key: object_key.to_owned(),
                message: error.message(),
            })?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: object_key.to_owned(),
            })?;
        let Some(etag) = metadata.etag else {
            return Err(ControlObjectLoadError::Store {
                object_key: object_key.to_owned(),
                message: "missing control object etag".to_owned(),
            });
        };
        Ok(etag == identity.etag)
    }

    pub(crate) fn control_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_cached_namespaces > 0
    }

    pub(crate) fn commit_engine_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_cached_namespaces > 0
    }

    pub(crate) fn commit_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> Arc<AsyncMutex<NamespaceCommitEngine>> {
        let cache_config = &self.inner.config.runtime_cache;
        let session = self.inner.writer_sessions.state(namespace_id);
        self.inner.control_cache().engine(
            namespace_id,
            cache_config.max_cached_namespaces,
            Arc::clone(&self.inner.metadata_table_cache),
            session,
        )
    }

    pub(crate) fn runtime_read_context(
        &self,
        anchor: &CachedNamespaceAnchor,
        catalog: Option<VerifiedNamespaceCatalogEntry>,
    ) -> RuntimeReadContext {
        RuntimeReadContext {
            head: anchor.head.state.clone(),
            head_etag: anchor.head.identity.etag.clone(),
            manifest_id: anchor.manifest_id,
            manifest_object_id: anchor.manifest_object_id.clone(),
            table_cache: Arc::clone(&self.inner.metadata_table_cache),
            tail_cache: Arc::clone(&self.inner.wal_tail_projection_cache),
            catalog,
        }
    }

    /// The shared preamble of every pinned read: revalidate or load the head
    /// anchor, pin the read context to it, and hand back the engine. The
    /// context's `head` is the anchor the read is pinned to. The anchor and
    /// the namespace's immutable catalog pair are independent objects, so a
    /// cold read overlaps the two loads instead of paying the catalog's
    /// descriptor chain as extra round-trips; the head's result is inspected
    /// first so error reporting matches the serial order.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let (head, catalog) = futures::join!(
            self.head_for_metadata_read(namespace_id),
            self.load_namespace_catalog_cached(namespace_id)
        );
        let head = head?;
        let read_context = self.runtime_read_context(&head, catalog?);
        Ok((self.namespace_engine(namespace_id), read_context))
    }

    /// Returns the namespace's immutable catalog pair through the control
    /// cache, loading it once per cached namespace. With the control cache
    /// disabled this returns `None` and view loads read it per operation.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<VerifiedNamespaceCatalogEntry>> {
        if !self.control_cache_enabled() {
            return Ok(None);
        }
        if let Some(catalog) = self.inner.control_cache().catalog(namespace_id) {
            return Ok(Some(catalog));
        }
        let loaded = load_namespace_catalog_entry(self.store(), namespace_id)
            .await
            .map_err(|error| {
                RuntimeError::Core(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::from(error),
                ))
            })?;
        self.inner.control_cache().insert_catalog(
            namespace_id,
            loaded.clone(),
            self.inner.config.runtime_cache.max_cached_namespaces,
        );
        Ok(Some(loaded))
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
    pub(crate) fn invalidate_namespace_cache(&self, namespace_id: &NamespaceId) {
        self.invalidate_namespace_read_cache(namespace_id);
    }

    /// Namespace-terminal invalidation: the whole entry is removed, engine
    /// included, because the namespace itself is gone.
    pub(crate) fn invalidate_namespace_cache_for_delete(&self, namespace_id: &NamespaceId) {
        self.inner.control_cache().remove_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
    }

    /// Seeds the read caches with the state one landed publish produced:
    /// the head anchor and the projected WAL tail. Safe by construction —
    /// the anchor is etag-revalidated against the store on every read, so a
    /// wrong seed degrades to today's reload instead of a wrong read.
    pub(crate) fn seed_namespace_read_cache(
        &self,
        namespace_id: &NamespaceId,
        state: loonfs_core::publish::ResultingReadState,
    ) {
        if !self.control_cache_enabled() {
            return;
        }
        let max_cached_namespaces = self.inner.config.runtime_cache.max_cached_namespaces;
        let head_seq = state.head.seq;
        self.inner.control_cache().insert_namespace_head(
            namespace_id,
            CachedNamespaceAnchor {
                head: CachedControl {
                    identity: ControlObjectIdentity {
                        etag: state.head_etag.clone(),
                    },
                    state: state.head,
                },
                manifest_id: state.manifest_id,
                manifest_object_id: state.manifest_object_id,
            },
            max_cached_namespaces,
        );
        self.inner.wal_tail_projection_cache.insert(
            loonfs_core::cache::WalTailProjectionCacheKey {
                namespace_id: namespace_id.clone(),
                manifest_id: state.manifest_id,
                manifest_head_seq: state.manifest_head_seq,
                head_seq,
                head_etag: state.head_etag,
            },
            state.tail_rows,
        );
    }

    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
    }

    pub(crate) fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(namespace_id);
        }
        result
    }

    pub(crate) fn invalidate_namespace_cache_after_batch(
        &self,
        namespace_id: &NamespaceId,
        results: &[Result<CommitResponse>],
    ) {
        if results.iter().any(should_invalidate_after_result) {
            self.invalidate_namespace_read_cache(namespace_id);
        }
    }
}

fn invalidate_dropped_engine(entry: &NamespaceControlCacheEntry) {
    if let Some(engine) = &entry.engine {
        if let Ok(mut engine) = engine.try_lock() {
            engine.invalidate();
        }
    }
}

fn cached_anchor(
    (head, root): (LoadedHeadControl, LoadedMetadataRootControl),
) -> CachedNamespaceAnchor {
    CachedNamespaceAnchor {
        head: CachedControl {
            identity: head.identity,
            state: head.state,
        },
        manifest_id: root.state.manifest_id,
        manifest_object_id: root.state.manifest_object_id,
    }
}
