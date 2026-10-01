//! The decoded metadata cache that one or more runtimes read through.

use crate::metrics::{MetadataCacheInstruments, MetricsRecorder};
use loonfs_core::cache::{
    CacheScope, HeadStateCache, MetadataSegmentCache, SharedHeadState, SharedSegmentBlocks,
    StoredMetadataBlockCache,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Default limit for decoded segment blocks and manifests: 256 MiB.
pub const DEFAULT_MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
/// Default limit for namespace head anchors and WAL-tail projections: 64 MiB.
pub const DEFAULT_MAX_HEAD_STATE_BYTES: usize = 64 * 1024 * 1024;

/// Decoded metadata that one or more runtimes read through: segment blocks,
/// manifests, namespace head anchors, and WAL-tail projections.
///
/// Each [`LoonFs`](crate::LoonFs) built with the cache reads through it
/// under a scope of its own, so two runtimes never see each other's entries,
/// even over the same store. The limits bound the whole cache, and the least
/// recently used entry is evicted first, whichever runtime it belongs to.
/// Clones share the cache. A runtime built without one creates a private
/// cache with the default limits.
#[derive(Clone)]
pub struct MetadataCache {
    inner: Arc<MetadataCacheInner>,
}

struct MetadataCacheInner {
    segment_blocks: Arc<SharedSegmentBlocks>,
    head_state: Arc<SharedHeadState>,
    max_head_state_bytes: usize,
    next_scope: AtomicU64,
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
            head_anchor_hits: self.inner.head_anchor_hits.load(Ordering::SeqCst),
            head_anchor_misses: self.inner.head_anchor_misses.load(Ordering::SeqCst),
            wal_tail_hits: head_state.tail_hits,
            wal_tail_misses: head_state.tail_misses,
            wal_tail_inserts: head_state.tail_inserts,
            head_state_evictions: head_state.evictions,
            head_state_rejections: head_state.rejections,
            head_state_bytes: head_state.cached_decoded_bytes,
        }
    }

    /// Mints a scope for one runtime and returns that runtime's views of the
    /// two stores.
    pub(crate) fn bind(
        &self,
        max_block_memo_bytes: usize,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    ) -> (MetadataSegmentCache, HeadStateCache) {
        let scope = CacheScope::new(self.inner.next_scope.fetch_add(1, Ordering::SeqCst));
        (
            MetadataSegmentCache::new(
                Arc::clone(&self.inner.segment_blocks),
                scope,
                max_block_memo_bytes,
                stored_block_cache,
            ),
            HeadStateCache::new(Arc::clone(&self.inner.head_state), scope),
        )
    }

    pub(crate) fn max_head_state_bytes(&self) -> usize {
        self.inner.max_head_state_bytes
    }

    /// Counts one head anchor lookup that a read used to start from the
    /// cache or that sent it to the store.
    pub(crate) fn record_head_anchor_lookup(&self, hit: bool) {
        let inner = &self.inner;
        if hit {
            inner.head_anchor_hits.fetch_add(1, Ordering::SeqCst);
            inner.instruments.namespace_head_cache_hit();
        } else {
            inner.head_anchor_misses.fetch_add(1, Ordering::SeqCst);
            inner.instruments.namespace_head_cache_miss();
        }
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
    /// projections the cache holds. Reads start from this head state.
    /// Defaults to [`DEFAULT_MAX_HEAD_STATE_BYTES`]; zero keeps none. An entry
    /// heavier than the whole limit is not kept.
    ///
    /// The publishers of each writable runtime keep their WAL-tail
    /// projections outside the cache, under a separate total of this size
    /// per runtime.
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
                max_head_state_bytes: self.max_head_state_bytes,
                next_scope: AtomicU64::new(0),
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
