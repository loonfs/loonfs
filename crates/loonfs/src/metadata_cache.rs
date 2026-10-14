//! The decoded metadata cache that one or more runtimes read through.

use crate::metrics::{MetadataCacheInstruments, MetricsRecorder};
use crate::{ObjectStore, SharedObjectStore};
use loonfs_core::cache::{
    CacheScope, HeadStateCache, MetadataSegmentCache, ObjectStoreScope, ReadWorkingMemory,
    SharedHeadState, SharedSegmentBlocks, StoredMetadataBlockCache,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[cfg(test)]
mod tests;

/// Default limit for decoded segment blocks and manifests: 256 MiB.
pub const DEFAULT_MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
/// Default limit for namespace head anchors and WAL-tail projections: 64 MiB.
pub const DEFAULT_MAX_HEAD_STATE_BYTES: usize = 64 * 1024 * 1024;

/// Whether a cache lookup found its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheLookup {
    /// The cache held the entry.
    Hit,
    /// The lookup went to the store.
    Miss,
}

/// Decoded metadata that one or more runtimes read through: segment blocks,
/// manifests, namespace head anchors, and WAL-tail projections.
///
/// Runtimes built with the cache over clones of one [`SharedObjectStore`]
/// share the decoded segment blocks and manifests in the cache. Runtimes over
/// different object stores never read each other's blocks, even when two
/// object stores contain objects under the same keys. Two runtimes built
/// from the same [`StoreConfig`](crate::StoreConfig) each create their own
/// object-store client, so they don't share blocks. Each
/// [`LoonFs`](crate::LoonFs) keeps its own head anchors and WAL-tail
/// projections. The limits bound the whole cache, and the least recently
/// used entry is evicted first, whichever runtime it belongs to. Clones share
/// the cache. A runtime built without one creates a private cache with the
/// default limits.
#[derive(Clone)]
pub struct MetadataCache {
    inner: Arc<MetadataCacheInner>,
}

struct MetadataCacheInner {
    segment_blocks: Arc<SharedSegmentBlocks>,
    head_state: Arc<SharedHeadState>,
    next_runtime_scope: AtomicU64,
    object_store_scopes: Mutex<ObjectStoreScopes>,
    head_anchor_hits: AtomicUsize,
    head_anchor_misses: AtomicUsize,
    instruments: MetadataCacheInstruments,
}

impl fmt::Debug for MetadataCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCache")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl MetadataCache {
    /// Starts a cache builder with the default limits and no metrics
    /// recorder.
    pub fn builder() -> MetadataCacheBuilder {
        MetadataCacheBuilder {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_head_state_bytes: DEFAULT_MAX_HEAD_STATE_BYTES,
            metrics_recorder: None,
        }
    }

    /// Snapshots the cache's counters and occupancy, summed over every
    /// runtime that reads through it.
    pub fn stats(&self) -> MetadataCacheStats {
        let segments = self.inner.segment_blocks.stats();
        let head_state = self.inner.head_state.stats();
        MetadataCacheStats {
            segment_hits: segments.hits,
            segment_misses: segments.misses,
            segment_inserts: segments.inserts,
            segment_evictions: segments.evictions,
            segment_filter_skips: segments.filter_skips,
            segment_filter_false_positives: segments.filter_false_positives,
            segment_bytes: segments.cached_decoded_bytes,
            head_anchor_hits: self.inner.head_anchor_hits.load(Ordering::Relaxed),
            head_anchor_misses: self.inner.head_anchor_misses.load(Ordering::Relaxed),
            wal_tail_hits: head_state.tail_hits,
            wal_tail_misses: head_state.tail_misses,
            wal_tail_inserts: head_state.tail_inserts,
            head_state_evictions: head_state.evictions,
            head_state_rejections: head_state.rejections,
            head_state_bytes: head_state.cached_decoded_bytes,
        }
    }

    /// Returns one runtime's views of the cache, with segment blocks under
    /// the scope of `store` and head state under a new scope for the runtime.
    pub(crate) fn bind(
        &self,
        store: &SharedObjectStore,
        read_working_memory: Arc<ReadWorkingMemory>,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    ) -> MetadataCacheBinding {
        let object_store_scope = self
            .inner
            .object_store_scopes
            .lock()
            .expect("object store scopes lock should not be poisoned")
            .scope_of(store);
        let runtime_scope =
            CacheScope::new(self.inner.next_runtime_scope.fetch_add(1, Ordering::SeqCst));
        MetadataCacheBinding {
            cache: self.clone(),
            segment_blocks: MetadataSegmentCache::new(
                Arc::clone(&self.inner.segment_blocks),
                object_store_scope,
                read_working_memory,
                stored_block_cache,
            ),
            head_state: HeadStateCache::new(Arc::clone(&self.inner.head_state), runtime_scope),
        }
    }

    /// Counts one head anchor lookup that a read used to start from the
    /// cache or that sent it to the store.
    pub(crate) fn record_head_anchor_lookup(&self, lookup: CacheLookup) {
        let inner = &self.inner;
        match lookup {
            CacheLookup::Hit => {
                inner.head_anchor_hits.fetch_add(1, Ordering::Relaxed);
                inner.instruments.namespace_head_cache_hit();
            }
            CacheLookup::Miss => {
                inner.head_anchor_misses.fetch_add(1, Ordering::Relaxed);
                inner.instruments.namespace_head_cache_miss();
            }
        }
    }
}

/// One runtime's views of a [`MetadataCache`].
pub(crate) struct MetadataCacheBinding {
    pub(crate) cache: MetadataCache,
    pub(crate) segment_blocks: MetadataSegmentCache,
    pub(crate) head_state: HeadStateCache,
}

/// The object stores of the runtimes that read through the cache, each with
/// the scope of its segment blocks and manifests.
///
/// An object store is identified by the address of its allocation. Each
/// entry keeps a weak reference to the object store, so the allocation can't
/// be freed and reused by another object store while the entry exists. Each
/// call to `bind` removes the entries of dropped object stores, so the list
/// has one entry for each live object store that a runtime was built over,
/// plus one for each object store dropped since the last call.
#[derive(Default)]
struct ObjectStoreScopes {
    next_scope: u64,
    stores: Vec<(Weak<dyn ObjectStore>, ObjectStoreScope)>,
}

impl ObjectStoreScopes {
    /// Returns the scope of `store`. The first call for `store` mints a new
    /// scope.
    fn scope_of(&mut self, store: &SharedObjectStore) -> ObjectStoreScope {
        self.stores.retain(|(known, _)| known.strong_count() > 0);
        let known_scope = self
            .stores
            .iter()
            .find(|(known, _)| std::ptr::addr_eq(known.as_ptr(), Arc::as_ptr(store)))
            .map(|(_, scope)| *scope);
        known_scope.unwrap_or_else(|| {
            let scope = ObjectStoreScope::new(self.next_scope);
            self.next_scope += 1;
            self.stores.push((Arc::downgrade(store), scope));
            scope
        })
    }
}

/// Builder for a [`MetadataCache`].
#[must_use]
pub struct MetadataCacheBuilder {
    max_segment_bytes: usize,
    max_head_state_bytes: usize,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl MetadataCacheBuilder {
    /// Sets the decoded bytes of segment blocks and manifests the cache
    /// holds. Defaults to [`DEFAULT_MAX_SEGMENT_BYTES`]; zero keeps none.
    pub fn max_segment_bytes(mut self, max_segment_bytes: usize) -> Self {
        self.max_segment_bytes = max_segment_bytes;
        self
    }

    /// Sets the decoded bytes of namespace head anchors and WAL-tail
    /// projections the cache holds. Reads start from this head state, and
    /// writers keep their WAL tails here between publishes. Defaults to
    /// [`DEFAULT_MAX_HEAD_STATE_BYTES`]; zero keeps none, so every publish
    /// reads its tail from the store. An entry heavier than the whole limit
    /// is not kept.
    pub fn max_head_state_bytes(mut self, max_head_state_bytes: usize) -> Self {
        self.max_head_state_bytes = max_head_state_bytes;
        self
    }

    /// Installs the metrics recorder the cache reports its lookups,
    /// evictions, and occupancy to (see [`crate::metrics`]). The cache
    /// registers its instruments once, when it is built. A cache built
    /// without one reports nothing.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Builds the cache.
    pub fn build(self) -> MetadataCache {
        let instruments = MetadataCacheInstruments::new(self.metrics_recorder.as_deref());
        MetadataCache {
            inner: Arc::new(MetadataCacheInner {
                segment_blocks: Arc::new(SharedSegmentBlocks::new(
                    self.max_segment_bytes,
                    instruments.metadata_segment_cache_observer(),
                )),
                head_state: Arc::new(SharedHeadState::new(
                    self.max_head_state_bytes,
                    instruments.head_state_cache_observer(),
                    instruments.wal_tail_projection_cache_observer(),
                )),
                next_runtime_scope: AtomicU64::new(0),
                object_store_scopes: Mutex::default(),
                head_anchor_hits: AtomicUsize::new(0),
                head_anchor_misses: AtomicUsize::new(0),
                instruments,
            }),
        }
    }
}

/// Counters and occupancy of a [`MetadataCache`], summed over every runtime
/// that reads through it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataCacheStats {
    /// Segment block and manifest lookups the cache answered.
    pub segment_hits: usize,
    /// Segment block and manifest lookups the cache could not answer.
    pub segment_misses: usize,
    /// Segment blocks and manifests inserted.
    pub segment_inserts: usize,
    /// Segment blocks and manifests evicted to stay within the segment limit.
    pub segment_evictions: usize,
    /// Segments skipped because their bloom filter ruled out a lookup before
    /// any index or data read.
    pub segment_filter_skips: usize,
    /// Segments whose filter admitted a lookup that matched no rows.
    pub segment_filter_false_positives: usize,
    /// Decoded bytes of segment blocks and manifests held now.
    pub segment_bytes: usize,
    /// Head anchor lookups a read could start from.
    pub head_anchor_hits: usize,
    /// Head anchor lookups that sent a read to the store.
    pub head_anchor_misses: usize,
    /// WAL-tail projection lookups the cache answered.
    pub wal_tail_hits: usize,
    /// WAL-tail projection lookups the cache could not answer.
    pub wal_tail_misses: usize,
    /// WAL-tail projections inserted.
    pub wal_tail_inserts: usize,
    /// Head anchors and WAL-tail projections evicted to stay within the
    /// head-state limit.
    pub head_state_evictions: usize,
    /// Head anchors and WAL-tail projections heavier than the whole
    /// head-state limit, which are never kept.
    pub head_state_rejections: usize,
    /// Decoded bytes of head anchors and WAL-tail projections held now.
    pub head_state_bytes: usize,
}
