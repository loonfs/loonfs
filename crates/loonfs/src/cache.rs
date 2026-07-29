//! Runtime caching: control-object reads and WAL-tail projections, plus the
//! cache-management half of [`ReadCore`].
//!
//! Every cache revalidates against durable state before its contents are
//! used; nothing here weakens read-after-write consistency.

use crate::fs::{should_invalidate_after_result, ReadCore};
use crate::{CommitResponse, CoreError, NamespaceId};
use crate::{Result, RuntimeError};
use loonfs_api::wire::control::HeadState;
use loonfs_core::cache::{MetadataTableCacheStats, WalTailProjectionCacheStats};
use loonfs_core::control::{
    load_namespace_read_anchor, ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl,
    MetadataBasis, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext, StoreFailureClass};
use loonfs_objectstore::keys::wal_head;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, NamespaceControlCacheEntry>,
    namespace_order: VecDeque<NamespaceId>,
}

#[derive(Debug, Default)]
struct NamespaceControlCacheEntry {
    head: Option<CachedNamespaceAnchor>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedControl<T> {
    pub(crate) identity: ControlObjectIdentity,
    pub(crate) state: T,
}

/// Head snapshot pinned together with the metadata basis it authorized when
/// the snapshot was taken. The pair stays consistent even when compaction
/// moves the live root past this head; reads at the pin replay a little
/// more WAL until the cache refreshes.
#[derive(Debug, Clone)]
pub(crate) struct CachedNamespaceAnchor {
    pub(crate) head: CachedControl<HeadState>,
    pub(crate) basis: MetadataBasis,
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

    fn invalidate_namespace(&mut self, namespace_id: &NamespaceId) {
        // The head anchor goes stale on every mutation.
        if let Some(entry) = self.namespaces.get_mut(namespace_id) {
            entry.head = None;
        }
    }

    /// Namespace-terminal removal: the whole entry goes.
    fn remove_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespaces.remove(namespace_id);
        self.namespace_order
            .retain(|candidate| candidate != namespace_id);
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
            self.namespaces.remove(&evicted);
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

impl ReadCore {
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

    /// Loads the read anchor, mapping an absent head to the one answer it
    /// can mean: the namespace does not exist.
    pub(crate) async fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CachedNamespaceAnchor> {
        self.load_namespace_head_cached(namespace_id)
            .await
            .map_err(|error| {
                RuntimeError::Core(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::LoadHead(error),
                ))
            })
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
                class: StoreFailureClass::of(&error),
            })?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: object_key.to_owned(),
            })?;
        let Some(etag) = metadata.etag else {
            return Err(ControlObjectLoadError::Store {
                object_key: object_key.to_owned(),
                message: "missing control object etag".to_owned(),
                class: StoreFailureClass::Other,
            });
        };
        Ok(etag == identity.etag)
    }

    pub(crate) fn control_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_cached_namespaces > 0
    }

    pub(crate) fn runtime_read_context(
        &self,
        anchor: &CachedNamespaceAnchor,
    ) -> RuntimeReadContext {
        RuntimeReadContext {
            head: anchor.head.state.clone(),
            head_etag: anchor.head.identity.etag.clone(),
            basis: anchor.basis.clone(),
            table_cache: Arc::clone(&self.inner.metadata_table_cache),
            tail_cache: Arc::clone(&self.inner.wal_tail_projection_cache),
        }
    }

    /// The shared preamble of every pinned read: revalidate or load the head
    /// anchor and pin the read context to it. The context's `head` is the
    /// anchor the read is pinned to, and the namespace's immutable identity
    /// travels inside it, so a read resolves everything it needs from one
    /// object.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        let read_context = self.runtime_read_context(&anchor);
        Ok((self.reader_engine(namespace_id), read_context))
    }

    /// Returns the namespace's immutable identity, read off the cached head
    /// anchor.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&anchor.head.state))
    }

    /// Namespace-terminal invalidation: the whole entry is removed, because
    /// the namespace itself is gone. The publisher that ran the delete is
    /// evicted with its engine and session by the publication service.
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
                basis: state.basis,
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

    /// Drops the namespace's read caches. The publish-side view of the same
    /// state — a namespace publisher's WAL tail projection — is stale for
    /// exactly the same reasons, so a caller that owns a publication service
    /// drops that too; see `FsWriter::invalidate_namespace`.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
    }

    /// Drops the read caches after a publication batch that may have moved
    /// the namespace on. The publisher's own engine needs no invalidation
    /// here: it is the engine that just published, and it revalidates
    /// against the live head on its next unit of work.
    pub(crate) fn invalidate_read_cache_after_batch(
        &self,
        namespace_id: &NamespaceId,
        results: &[Result<CommitResponse>],
    ) {
        if results.iter().any(should_invalidate_after_result) {
            self.invalidate_namespace_read_cache(namespace_id);
        }
    }
}

fn cached_anchor((head, basis): (LoadedHeadControl, MetadataBasis)) -> CachedNamespaceAnchor {
    CachedNamespaceAnchor {
        head: CachedControl {
            identity: head.identity,
            state: head.state,
        },
        basis,
    }
}
