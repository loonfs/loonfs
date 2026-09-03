//! Shared caches for decoded manifest state: SST blocks keyed by content
//! digest, validated manifests, and bounded WAL-tail projections.
//!
//! The decoded block cache also carries the handle to the optional
//! node-local cache of the same blocks in their encoded form; see
//! [`stored_block_cache`](super::stored_block_cache).

use super::runs::MetadataRunManifest;
use super::stored_block_cache::StoredMetadataBlockCache;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
    DecodedBlockWeight, DecodedSegmentBlock, SegmentBlockKind, SegmentCacheKey,
};
use crate::metadata::MetadataState;
use loonfs_api::wire::manifest::MetadataRow;
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default decoded-byte budget for metadata segment blocks. A value of zero
/// disables the cache.
pub(crate) const DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_OPEN_PREFETCH_MAX_STORED_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSegmentCacheConfig {
    pub max_decoded_bytes: usize,
    /// Maximum stored bytes selected for the open-time segment-tail prefetch.
    /// Zero disables the prefetch.
    pub open_prefetch_max_stored_bytes: usize,
}

impl Default for MetadataSegmentCacheConfig {
    fn default() -> Self {
        Self {
            max_decoded_bytes: DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES,
            open_prefetch_max_stored_bytes: DEFAULT_OPEN_PREFETCH_MAX_STORED_BYTES,
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
    ),
>;

pub struct MetadataSegmentCache {
    blocks: DecodedBlockCache<MetadataSegmentCacheKey, DecodedMetadataSegmentBlock>,
    open_prefetch_max_stored_bytes: usize,
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
            .field(
                "open_prefetch_max_stored_bytes",
                &self.open_prefetch_max_stored_bytes,
            )
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
        let open_prefetch_max_stored_bytes = config.open_prefetch_max_stored_bytes;
        Self {
            blocks: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes: config.max_decoded_bytes,
                max_rows: None,
                max_entries: None,
                observer: observer.clone(),
            }),
            open_prefetch_max_stored_bytes,
            stats: MetadataSegmentFilterStatsInner::default(),
            observer,
            stored_block_cache,
        }
    }

    /// Returns the node-local encoded-block cache, if one was configured.
    pub fn stored_block_cache(&self) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
        self.stored_block_cache.as_ref()
    }

    /// Returns the stored-byte budget for open-time segment-tail prefetching.
    pub fn open_prefetch_max_stored_bytes(&self) -> usize {
        self.open_prefetch_max_stored_bytes
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

    /// A probe that records no hit or miss, for deciding what to prefetch.
    pub(super) fn contains(&self, key: &MetadataSegmentCacheKey) -> bool {
        self.blocks.contains_key(key)
    }

    pub(super) fn insert(&self, key: MetadataSegmentCacheKey, block: DecodedMetadataSegmentBlock) {
        self.blocks.insert(key, block);
    }
}

/// Default bounds for WAL-tail projections, shared by the read-side
/// projection cache and the publish-side tail reuse check.
pub const DEFAULT_WAL_TAIL_PROJECTION_ROWS: usize = 1_000_000;
pub const DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES: usize = 256 * 1024 * 1024;

/// Zero entries disables the cache; the row and byte limits bound what one
/// entry may hold and what the cache may retain in total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTailProjectionCacheConfig {
    pub max_entries: usize,
    pub max_rows: usize,
    pub max_decoded_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalTailProjectionCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_rows: usize,
    pub evicted_decoded_bytes: usize,
    pub uncacheable_count: usize,
    pub uncacheable_rows: usize,
    pub uncacheable_decoded_bytes: usize,
    pub cached_rows: usize,
    pub cached_decoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailProjectionCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub manifest_head_seq: ChangeSeq,
    pub head_seq: ChangeSeq,
    pub head_etag: String,
}

impl DecodedBlock for Arc<MetadataState> {
    fn weight(&self) -> DecodedBlockWeight {
        DecodedBlockWeight {
            bytes: self.decoded_bytes(),
            rows: self.row_count(),
        }
    }
}

pub struct WalTailProjectionCache {
    config: WalTailProjectionCacheConfig,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    blocks: DecodedBlockCache<WalTailProjectionCacheKey, Arc<MetadataState>>,
    rejection_stats: WalTailProjectionCacheRejectionStats,
}

impl std::fmt::Debug for WalTailProjectionCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalTailProjectionCache")
            .field("config", &self.config)
            .field("blocks", &self.blocks)
            .field("rejection_stats", &self.rejection_stats)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct WalTailProjectionCacheRejectionStats {
    uncacheable_count: AtomicUsize,
    uncacheable_rows: AtomicUsize,
    uncacheable_decoded_bytes: AtomicUsize,
}

impl WalTailProjectionCache {
    pub fn new(
        config: WalTailProjectionCacheConfig,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        let blocks = DecodedBlockCache::new(DecodedBlockCacheConfig {
            max_decoded_bytes: config.max_decoded_bytes,
            max_rows: Some(config.max_rows),
            max_entries: Some(config.max_entries),
            observer: observer.clone(),
        });
        Self {
            config,
            observer,
            blocks,
            rejection_stats: WalTailProjectionCacheRejectionStats::default(),
        }
    }

    pub fn stats(&self) -> WalTailProjectionCacheStats {
        let blocks = self.blocks.stats();
        WalTailProjectionCacheStats {
            hits: blocks.hits,
            misses: blocks.misses,
            inserts: blocks.inserts,
            evictions: blocks.evictions,
            evicted_rows: blocks.evicted_rows,
            evicted_decoded_bytes: blocks.evicted_decoded_bytes,
            uncacheable_count: self
                .rejection_stats
                .uncacheable_count
                .load(Ordering::SeqCst),
            uncacheable_rows: self.rejection_stats.uncacheable_rows.load(Ordering::SeqCst),
            uncacheable_decoded_bytes: self
                .rejection_stats
                .uncacheable_decoded_bytes
                .load(Ordering::SeqCst),
            cached_rows: blocks.cached_rows,
            cached_decoded_bytes: blocks.cached_decoded_bytes,
        }
    }

    pub fn get(&self, key: &WalTailProjectionCacheKey) -> Option<Arc<MetadataState>> {
        self.blocks.get(key)
    }

    pub(crate) fn contains_head(
        &self,
        namespace_id: &NamespaceId,
        head_seq: ChangeSeq,
        head_etag: &str,
    ) -> bool {
        self.blocks.contains_key_matching(|key| {
            &key.namespace_id == namespace_id
                && key.head_seq == head_seq
                && key.head_etag == head_etag
        })
    }

    pub fn insert(&self, key: WalTailProjectionCacheKey, rows: Arc<MetadataState>) {
        if self.config.max_entries == 0 {
            return;
        }
        let weight = rows.weight();
        if weight.rows > self.config.max_rows || weight.bytes > self.config.max_decoded_bytes {
            self.rejection_stats
                .uncacheable_count
                .fetch_add(1, Ordering::SeqCst);
            self.rejection_stats
                .uncacheable_rows
                .fetch_add(weight.rows, Ordering::SeqCst);
            self.rejection_stats
                .uncacheable_decoded_bytes
                .fetch_add(weight.bytes, Ordering::SeqCst);
            if let Some(observer) = &self.observer {
                observer.reject(weight);
            }
            return;
        }
        self.blocks.insert(key, rows);
    }

    pub fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.blocks
            .invalidate(|key| &key.namespace_id == namespace_id);
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{
        DecodedMetadataSegmentBlock, MetadataSegmentBlockKind, MetadataSegmentCache,
        MetadataSegmentCacheConfig, MetadataSegmentCacheKey, WalTailProjectionCache,
        WalTailProjectionCacheConfig, WalTailProjectionCacheKey,
    };
    use crate::metadata::{InodeRecord, MetadataState};
    use loonfs_api::wire::sst_blocks::DecodedDataBlock;
    use loonfs_api::{ActorId, ActorRef, ChangeSeq, InodeId, InodeKind, ManifestNo, NamespaceId};
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

    fn key(digest: &str) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: digest.to_owned(),
            block_kind: MetadataSegmentBlockKind::Data,
            block_offset: 0,
        }
    }

    #[test]
    fn row_attribution_and_timestamps_never_enter_projection_cache_keys() {
        let cache = WalTailProjectionCache::new(
            WalTailProjectionCacheConfig {
                max_entries: 1,
                max_rows: 10,
                max_decoded_bytes: 16 * 1024,
            },
            None,
        );
        let key = WalTailProjectionCacheKey {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            manifest_no: ManifestNo(7),
            manifest_head_seq: ChangeSeq(11),
            head_seq: ChangeSeq(12),
            head_etag: "stable-head-etag".to_owned(),
        };
        let actors = [
            ActorRef::user(ActorId::parse("auth0|x").expect("actor id")),
            ActorRef::service(ActorId::parse("x".repeat(256)).expect("256-byte actor id")),
            ActorRef::system(ActorId::parse("雪-actor").expect("unicode actor id")),
        ];

        for (offset, actor) in actors.into_iter().enumerate() {
            let rows = MetadataState::from_rows(
                vec![InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(3),
                    commit_id: loonfs_api::CommitId::parse("c_cache_row").expect("commit id"),
                    created_by: actor.clone(),
                    created_at_ms: 3_000 + offset as u64,
                }],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            cache.insert(key.clone(), Arc::new(rows));
            assert_eq!(
                cache
                    .get(&key)
                    .expect("same projection cache key should hit")
                    .inodes()[0]
                    .created_by,
                actor
            );
        }
        assert_eq!(cache.stats().cached_rows, 1);
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
