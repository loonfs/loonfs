//! Shared caches for decoded manifest state: SST blocks keyed by owner and
//! segment id, validated manifests, and the head state reads start from.
//!
//! The decoded block cache also carries the handle to the optional
//! node-local cache of the same blocks in their encoded form; see
//! [`stored_block_cache`](super::stored_block_cache).

use super::block_load::DEFAULT_BLOCK_MEMO_BYTES;
use super::runs::MetadataRunManifest;
use super::stored_block_cache::StoredMetadataBlockCache;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
    DecodedSegmentBlock, SegmentBlockKind, SegmentCacheKey,
};
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::time::Observation;
use crate::wal::ProjectedWalTail;
use loonfs_api::wire::manifest::MetadataRow;
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Default decoded-byte budget for metadata segment blocks. A value of zero
/// disables the cache.
pub(crate) const DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSegmentCacheConfig {
    pub max_decoded_bytes: usize,
    /// Data-block bytes one read, publication, or fold keeps in its own block
    /// memo, on top of this cache. Zero keeps none.
    pub max_block_memo_bytes: usize,
}

impl Default for MetadataSegmentCacheConfig {
    fn default() -> Self {
        Self {
            max_decoded_bytes: DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES,
            max_block_memo_bytes: DEFAULT_BLOCK_MEMO_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataSegmentCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    /// Segments a scan skipped because their bloom filter ruled the lookup
    /// key out before any index or data fetch.
    pub filter_skips: usize,
    /// Segments whose filter admitted a lookup that then matched no rows.
    /// Approximate: a lookup narrower than the filter key (an exact unbind,
    /// a single revision) can count a true admission here.
    pub filter_false_positives: usize,
}

pub(super) type MetadataSegmentBlockKind = SegmentBlockKind;
pub(super) type MetadataSegmentCacheKey = SegmentCacheKey;
pub(super) type DecodedMetadataSegmentBlock = DecodedSegmentBlock<
    MetadataRow,
    (
        Arc<NamespaceManifestEnvelope>,
        Arc<Vec<MetadataRunManifest>>,
        u64,
    ),
>;

/// The block memo budget of a view or WAL fold that reads through
/// `segment_cache`, or the default without one.
pub(super) fn block_memo_bytes(segment_cache: Option<&MetadataSegmentCache>) -> usize {
    segment_cache.map_or(DEFAULT_BLOCK_MEMO_BYTES, |cache| cache.max_block_memo_bytes)
}

pub struct MetadataSegmentCache {
    blocks: DecodedBlockCache<MetadataSegmentCacheKey, DecodedMetadataSegmentBlock>,
    max_block_memo_bytes: usize,
    stats: MetadataSegmentFilterStatsInner,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    /// Optional node-local cache for encoded blocks. Keeping it with the
    /// decoded cache ensures callers use both cache tiers or neither tier.
    stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
}

impl std::fmt::Debug for MetadataSegmentCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataSegmentCache")
            .field("blocks", &self.blocks)
            .field("max_block_memo_bytes", &self.max_block_memo_bytes)
            .field("stats", &self.stats)
            .field("stored_block_cache", &self.stored_block_cache)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct MetadataSegmentFilterStatsInner {
    filter_skips: AtomicUsize,
    filter_false_positives: AtomicUsize,
}

impl MetadataSegmentCache {
    pub fn new(config: MetadataSegmentCacheConfig) -> Self {
        Self::with_stored_block_cache_and_observer(config, None, None)
    }

    pub fn with_stored_block_cache_and_observer(
        config: MetadataSegmentCacheConfig,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            blocks: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes: config.max_decoded_bytes,
                observer: observer.clone(),
            }),
            max_block_memo_bytes: config.max_block_memo_bytes,
            stats: MetadataSegmentFilterStatsInner::default(),
            observer,
            stored_block_cache,
        }
    }

    /// Returns the node-local encoded-block cache, if one was configured.
    pub fn stored_block_cache(&self) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
        self.stored_block_cache.as_ref()
    }

    /// Resolves one block access through a single-flight cell.
    pub(super) async fn get_or_load<E, F, Fut>(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        fetch: F,
    ) -> Result<DecodedMetadataSegmentBlock, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DecodedMetadataSegmentBlock, E>>,
    {
        self.blocks.get_or_load(cache_key, fetch).await
    }

    pub fn stats(&self) -> MetadataSegmentCacheStats {
        let blocks = self.blocks.stats();
        MetadataSegmentCacheStats {
            hits: blocks.hits,
            misses: blocks.misses,
            inserts: blocks.inserts,
            evictions: blocks.evictions,
            filter_skips: self.stats.filter_skips.load(Ordering::SeqCst),
            filter_false_positives: self.stats.filter_false_positives.load(Ordering::SeqCst),
        }
    }

    pub(super) fn record_filter_skip(&self) {
        self.stats.filter_skips.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.filter_skip();
        }
    }

    pub(super) fn record_filter_false_positive(&self) {
        self.stats
            .filter_false_positives
            .fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.filter_false_positive();
        }
    }

    pub(super) fn get(&self, key: &MetadataSegmentCacheKey) -> Option<DecodedMetadataSegmentBlock> {
        self.blocks.get(key)
    }

    pub(super) fn insert(&self, key: MetadataSegmentCacheKey, block: DecodedMetadataSegmentBlock) {
        self.blocks.insert(key, block);
    }
}

/// Default byte budget for head state, which the publish side also applies
/// to the projections its sessions retain.
pub const DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeadStateCacheStats {
    pub tail_hits: usize,
    pub tail_misses: usize,
    pub tail_inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    /// Entries heavier than the whole budget, which are never kept.
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    pub cached_decoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailProjectionCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub head_seq: ChangeSeq,
}

/// A namespace head and the metadata basis it was read against. Keeping them
/// together ensures reads use a consistent pair. If compaction advances the
/// current manifest, reads may replay additional WAL entries until the anchor
/// refreshes.
#[derive(Debug, Clone)]
pub struct CachedReadAnchor {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    /// The successor check that last confirmed `basis`.
    pub basis_checked: Option<Observation>,
    pub validation: Arc<NamespaceValidation>,
    /// The validation that produced this anchor; a seeded anchor carries zero.
    pub completed_validation_no: u64,
}

/// Lets concurrent reads of one namespace share one revalidation.
#[derive(Debug, Default)]
pub struct NamespaceValidation {
    pub lock: tokio::sync::Mutex<()>,
    pub started: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HeadStateKey {
    Anchor(NamespaceId),
    Tail(WalTailProjectionCacheKey),
}

#[derive(Debug, Clone)]
enum HeadState {
    Anchor(Arc<CachedReadAnchor>),
    Tail(Arc<ProjectedWalTail>),
}

impl HeadState {
    fn into_anchor(self) -> Option<Arc<CachedReadAnchor>> {
        match self {
            Self::Anchor(anchor) => Some(anchor),
            Self::Tail(_) => None,
        }
    }

    fn into_tail(self) -> Option<Arc<ProjectedWalTail>> {
        match self {
            Self::Tail(tail) => Some(tail),
            Self::Anchor(_) => None,
        }
    }
}

impl DecodedBlock for HeadState {
    fn weight(&self) -> usize {
        match self {
            Self::Anchor(anchor) => {
                arc_bytes::<CachedReadAnchor>()
                    + arc_bytes::<NamespaceValidation>()
                    + anchor.head.heap_bytes()
                    + anchor.basis.0.heap_bytes()
            }
            Self::Tail(tail) => tail.decoded_bytes(),
        }
    }
}

/// Namespace head anchors and WAL-tail projections under one byte budget and
/// one recency order. An entry heavier than the whole budget is not kept,
/// because inserting it would evict everything else first.
pub struct HeadStateCache {
    entries: DecodedBlockCache<HeadStateKey, HeadState>,
    max_decoded_bytes: usize,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    tail_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    counters: HeadStateCounters,
}

impl std::fmt::Debug for HeadStateCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HeadStateCache")
            .field("entries", &self.entries)
            .field("max_decoded_bytes", &self.max_decoded_bytes)
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct HeadStateCounters {
    tail_hits: AtomicUsize,
    tail_misses: AtomicUsize,
    tail_inserts: AtomicUsize,
    rejections: AtomicUsize,
    rejected_decoded_bytes: AtomicUsize,
}

impl HeadStateCache {
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self::with_observers(max_decoded_bytes, None, None)
    }

    /// `observer` sees evictions, rejections, and retained bytes across both
    /// kinds of entry; `tail_observer` sees WAL-tail lookups and inserts.
    pub fn with_observers(
        max_decoded_bytes: usize,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
        tail_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            entries: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes,
                observer: observer.clone(),
            }),
            max_decoded_bytes,
            observer,
            tail_observer,
            counters: HeadStateCounters::default(),
        }
    }

    pub fn stats(&self) -> HeadStateCacheStats {
        let entries = self.entries.stats();
        let counters = &self.counters;
        HeadStateCacheStats {
            tail_hits: counters.tail_hits.load(Ordering::SeqCst),
            tail_misses: counters.tail_misses.load(Ordering::SeqCst),
            tail_inserts: counters.tail_inserts.load(Ordering::SeqCst),
            evictions: entries.evictions,
            evicted_decoded_bytes: entries.evicted_decoded_bytes,
            rejections: counters.rejections.load(Ordering::SeqCst),
            rejected_decoded_bytes: counters.rejected_decoded_bytes.load(Ordering::SeqCst),
            cached_decoded_bytes: entries.cached_decoded_bytes,
        }
    }

    /// Returns the namespace's anchor and marks it recently used.
    pub fn get_anchor(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        self.entries
            .get(&HeadStateKey::Anchor(namespace_id.clone()))
            .and_then(HeadState::into_anchor)
    }

    /// Returns the namespace's anchor without marking it used.
    pub fn peek_anchor(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        self.entries
            .peek(&HeadStateKey::Anchor(namespace_id.clone()))
            .and_then(HeadState::into_anchor)
    }

    pub fn insert_anchor(&self, anchor: Arc<CachedReadAnchor>) {
        self.insert(
            HeadStateKey::Anchor(anchor.head.namespace_id.clone()),
            HeadState::Anchor(anchor),
        );
    }

    pub fn invalidate_anchor(&self, namespace_id: &NamespaceId) {
        self.entries
            .remove(&HeadStateKey::Anchor(namespace_id.clone()));
    }

    pub fn get_tail(&self, key: &WalTailProjectionCacheKey) -> Option<Arc<ProjectedWalTail>> {
        if self.max_decoded_bytes == 0 {
            return None;
        }
        let tail = self
            .entries
            .get(&HeadStateKey::Tail(key.clone()))
            .and_then(HeadState::into_tail);
        let counter = match &tail {
            Some(_) => &self.counters.tail_hits,
            None => &self.counters.tail_misses,
        };
        counter.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.tail_observer {
            match &tail {
                Some(_) => observer.hit(),
                None => observer.miss(),
            }
        }
        tail
    }

    pub fn insert_tail(&self, key: WalTailProjectionCacheKey, tail: Arc<ProjectedWalTail>) {
        if self.insert(HeadStateKey::Tail(key), HeadState::Tail(tail)) {
            self.counters.tail_inserts.fetch_add(1, Ordering::SeqCst);
            if let Some(observer) = &self.tail_observer {
                observer.insert();
            }
        }
    }

    fn insert(&self, key: HeadStateKey, entry: HeadState) -> bool {
        if self.max_decoded_bytes == 0 {
            return false;
        }
        let decoded_bytes = entry.weight();
        if decoded_bytes > self.max_decoded_bytes {
            self.counters.rejections.fetch_add(1, Ordering::SeqCst);
            self.counters
                .rejected_decoded_bytes
                .fetch_add(decoded_bytes, Ordering::SeqCst);
            if let Some(observer) = &self.observer {
                observer.reject(decoded_bytes);
            }
            return false;
        }
        self.entries.insert(key, entry);
        true
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{
        DecodedMetadataSegmentBlock, HeadStateCache, MetadataSegmentBlockKind,
        MetadataSegmentCache, MetadataSegmentCacheConfig, MetadataSegmentCacheKey,
        WalTailProjectionCacheKey,
    };
    use crate::metadata::{InodeRecord, MetadataState};
    use crate::wal::ProjectedWalTail;
    use loonfs_api::wire::sst_blocks::DecodedDataBlock;
    use loonfs_api::{ActorId, ChangeSeq, InodeId, InodeKind, ManifestNo, NamespaceId};
    use std::sync::Arc;

    fn block(decoded_bytes: usize) -> DecodedMetadataSegmentBlock {
        DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    fn key(identity: &str) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: identity.to_owned(),
            block_kind: MetadataSegmentBlockKind::Data,
            block_offset: 0,
        }
    }

    #[test]
    fn row_attribution_and_timestamps_never_enter_projection_cache_keys() {
        let cache = HeadStateCache::new(16 * 1024);
        let key = WalTailProjectionCacheKey {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            manifest_no: ManifestNo(7),
            head_seq: ChangeSeq(12),
        };
        let actors = [
            ActorId::parse("auth0|x").expect("actor id"),
            ActorId::parse("x".repeat(256)).expect("256-byte actor id"),
            ActorId::parse("external|actor").expect("external actor id"),
        ];

        for (offset, actor) in actors.into_iter().enumerate() {
            let rows = MetadataState::from_rows(
                vec![InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    committed_seq: ChangeSeq(3),
                    commit_id: loonfs_api::CommitId::parse("c_cache_row").expect("commit id"),
                    committed_by: actor.clone(),
                    committed_at_ms: 3_000 + offset as u64,
                }],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            let tail = Arc::new(ProjectedWalTail::from_rows(rows));
            cache.insert_tail(key.clone(), Arc::clone(&tail));
            assert_eq!(
                cache.stats().cached_decoded_bytes,
                tail.decoded_bytes(),
                "the same key replaces its entry"
            );
            assert_eq!(
                cache
                    .get_tail(&key)
                    .expect("same projection cache key should hit")
                    .rows
                    .inodes()[0]
                    .committed_by,
                actor
            );
        }
    }

    #[test]
    fn byte_budget_evicts_the_oldest_block() {
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
            max_decoded_bytes: 1000,
            ..MetadataSegmentCacheConfig::default()
        });
        cache.insert(key("a"), block(600));
        cache.insert(key("b"), block(600));
        assert!(
            cache.get(&key("a")).is_none(),
            "oldest block should evict once the byte budget is exceeded"
        );
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn replacing_a_block_reaccounts_its_decoded_bytes() {
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
            max_decoded_bytes: 1000,
            ..MetadataSegmentCacheConfig::default()
        });
        cache.insert(key("a"), block(600));
        cache.insert(key("a"), block(100));
        // 600 was released on replace: another 600 fits without eviction.
        cache.insert(key("b"), block(600));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 0);
    }

    #[test]
    fn cache_hits_share_the_decoded_row_allocation() {
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
        let inserted = block(64);
        let rows = match &inserted {
            DecodedMetadataSegmentBlock::Data { block: rows, .. } => Arc::clone(rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => {
                panic!("fixture builds a data block")
            }
        };
        cache.insert(key("a"), inserted);
        let hit = cache.get(&key("a")).expect("inserted block should hit");
        let shares_allocation = match &hit {
            DecodedMetadataSegmentBlock::Data {
                block: hit_rows, ..
            } => Arc::ptr_eq(hit_rows, &rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => false,
        };
        assert!(
            shares_allocation,
            "a cache hit should share the decoded rows, not clone them"
        );
    }

    #[tokio::test]
    async fn get_or_load_retries_a_failed_load_and_then_stops_loading() {
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
        let failed: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Err("transport".to_owned()) })
            .await;
        assert!(failed.is_err());
        let recovered: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Ok(block(1)) })
            .await;
        assert!(
            recovered.is_ok(),
            "a failed fetch should leave nothing behind for the next caller"
        );

        let cached: Result<_, String> = cache
            .get_or_load(&key("a"), || async {
                Err("a populated key must not re-fetch".to_owned())
            })
            .await;
        assert!(cached.is_ok(), "the cached block should answer the access");
    }
}
