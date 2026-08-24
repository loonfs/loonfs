//! Shared caches for decoded manifest state: SST blocks keyed by content
//! digest, validated manifests, and bounded WAL-tail projections.
//!
//! The decoded block cache also carries the handle to the optional
//! node-local cache of the same blocks in their encoded form; see
//! [`stored_block_cache`](super::stored_block_cache).

use super::runs::MetadataRunManifest;
use super::stored_block_cache::StoredMetadataBlockCache;
use crate::block_cache::{DecodedBlock, DecodedBlockCache, DecodedBlockCacheObserver};
use crate::metadata::MetadataState;
use crate::recency::Recency;
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Default decoded-byte budget for metadata segment blocks. A value of zero
/// disables the cache.
pub(crate) const DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSegmentCacheConfig {
    pub max_decoded_bytes: usize,
}

impl Default for MetadataSegmentCacheConfig {
    fn default() -> Self {
        Self {
            max_decoded_bytes: DEFAULT_METADATA_SEGMENT_CACHE_DECODED_BYTES,
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

/// Receives metadata segment cache events for metrics.
pub trait MetadataSegmentCacheObserver: Send + Sync + 'static {
    fn hit(&self);
    fn miss(&self);
    fn insert(&self);
    fn evict(&self);
    fn filter_skip(&self);
    fn filter_false_positive(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MetadataSegmentBlockKind {
    Index,
    Filter,
    Data,
    Manifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MetadataSegmentCacheKey {
    /// The cached object's identity: a segment's object checksum for block
    /// entries, or the manifest object key for manifest entries — both
    /// immutable, so entries can never go stale.
    pub(super) identity: String,
    pub(super) block_kind: MetadataSegmentBlockKind,
    pub(super) block_offset: u64,
}

/// One decoded, verified object the cache holds: a CRC-checked segment
/// section (index, filter, or data) or a validated namespace manifest.
/// Everything shares the cache and its byte budget; the key's `block_kind`
/// and `block_offset` make collisions between kinds impossible. Data blocks
/// hold metadata rows.
#[derive(Debug, Clone)]
pub(super) enum DecodedMetadataSegmentBlock {
    Index {
        entries: Arc<Vec<SegmentIndexEntry>>,
        decoded_bytes: usize,
    },
    Filter {
        filter: Arc<SegmentFilter>,
        decoded_bytes: usize,
    },
    Data {
        block: Arc<DecodedDataBlock>,
        decoded_bytes: usize,
    },
    Manifest {
        manifest: Arc<NamespaceManifestEnvelope>,
        /// The manifest's `segments` grouped in scan order during validation.
        /// Reusing this list avoids rebuilding it for every page.
        scan_runs: Arc<Vec<MetadataRunManifest>>,
        decoded_bytes: usize,
    },
}

impl DecodedBlock for DecodedMetadataSegmentBlock {
    fn decoded_bytes(&self) -> usize {
        match self {
            Self::Index { decoded_bytes, .. }
            | Self::Filter { decoded_bytes, .. }
            | Self::Data { decoded_bytes, .. }
            | Self::Manifest { decoded_bytes, .. } => *decoded_bytes,
        }
    }
}

pub struct MetadataSegmentCache {
    blocks: DecodedBlockCache<MetadataSegmentCacheKey, DecodedMetadataSegmentBlock>,
    stats: MetadataSegmentFilterStatsInner,
    observer: Option<Arc<dyn MetadataSegmentCacheObserver>>,
    /// Optional node-local cache for encoded blocks. Keeping it with the
    /// decoded cache ensures callers use both cache tiers or neither tier.
    stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
}

impl std::fmt::Debug for MetadataSegmentCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataSegmentCache")
            .field("blocks", &self.blocks)
            .field("stats", &self.stats)
            .field("stored_block_cache", &self.stored_block_cache)
            .finish_non_exhaustive()
    }
}

/// The two counters the shared cache knows nothing about: a metadata scan
/// records them from what the filter told it, not from a cache access.
#[derive(Debug, Default)]
struct MetadataSegmentFilterStatsInner {
    filter_skips: AtomicUsize,
    filter_false_positives: AtomicUsize,
}

/// Reports the shared cache's four events to a metadata observer that also
/// takes the two filter events.
struct MetadataSegmentCacheEvents(Arc<dyn MetadataSegmentCacheObserver>);

impl DecodedBlockCacheObserver for MetadataSegmentCacheEvents {
    fn hit(&self) {
        self.0.hit();
    }

    fn miss(&self) {
        self.0.miss();
    }

    fn insert(&self) {
        self.0.insert();
    }

    fn evict(&self) {
        self.0.evict();
    }
}

impl MetadataSegmentCache {
    pub fn new(config: MetadataSegmentCacheConfig) -> Self {
        Self::with_stored_block_cache_and_observer(config, None, None)
    }

    /// Creates a cache that reports activity to the optional `observer`.
    pub fn with_stored_block_cache_and_observer(
        config: MetadataSegmentCacheConfig,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        observer: Option<Arc<dyn MetadataSegmentCacheObserver>>,
    ) -> Self {
        let block_observer = observer.clone().map(|observer| {
            Arc::new(MetadataSegmentCacheEvents(observer)) as Arc<dyn DecodedBlockCacheObserver>
        });
        Self {
            blocks: DecodedBlockCache::with_observer(config.max_decoded_bytes, block_observer),
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

/// Receives WAL-tail projection cache events so they can be recorded as metrics.
pub trait WalTailProjectionCacheObserver: Send + Sync + 'static {
    fn hit(&self);
    fn miss(&self);
    fn insert(&self);
    fn evict(&self, rows: usize, decoded_bytes: usize);
    fn reject(&self, rows: usize, decoded_bytes: usize);
    fn retained(&self, rows: usize, decoded_bytes: usize);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailProjectionCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub manifest_head_seq: ChangeSeq,
    pub head_seq: ChangeSeq,
    pub head_etag: String,
}

/// Returns the row count and decoded size charged to the cache budgets.
fn projection_weight(rows: &MetadataState) -> (usize, usize) {
    (rows.row_count(), rows.decoded_bytes())
}

pub struct WalTailProjectionCache {
    config: WalTailProjectionCacheConfig,
    inner: Mutex<WalTailProjectionCacheInner>,
    stats: WalTailProjectionCacheStatsInner,
    observer: Option<Arc<dyn WalTailProjectionCacheObserver>>,
}

impl std::fmt::Debug for WalTailProjectionCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalTailProjectionCache")
            .field("config", &self.config)
            .field("inner", &self.inner)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct WalTailProjectionCacheInner {
    entries: HashMap<WalTailProjectionCacheKey, WalTailProjectionCacheSlot>,
    order: Recency<WalTailProjectionCacheKey>,
    cached_rows: usize,
    cached_decoded_bytes: usize,
}

#[derive(Debug)]
struct WalTailProjectionCacheSlot {
    rows: Arc<MetadataState>,
    /// Recency stamp assigned on the most recent access. Recency records with
    /// an older stamp for this key are ignored.
    last_touch: u64,
}

#[derive(Debug, Default)]
struct WalTailProjectionCacheStatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
    evicted_rows: AtomicUsize,
    evicted_decoded_bytes: AtomicUsize,
    uncacheable_count: AtomicUsize,
    uncacheable_rows: AtomicUsize,
    uncacheable_decoded_bytes: AtomicUsize,
}

impl WalTailProjectionCache {
    pub fn new(config: WalTailProjectionCacheConfig) -> Self {
        Self::with_observer(config, None)
    }

    /// Creates a cache that reports activity to the optional `observer`.
    pub fn with_observer(
        config: WalTailProjectionCacheConfig,
        observer: Option<Arc<dyn WalTailProjectionCacheObserver>>,
    ) -> Self {
        Self {
            config,
            inner: Mutex::new(WalTailProjectionCacheInner::default()),
            stats: WalTailProjectionCacheStatsInner::default(),
            observer,
        }
    }

    pub fn stats(&self) -> WalTailProjectionCacheStats {
        let inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock should not be poisoned");
        WalTailProjectionCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
            evicted_rows: self.stats.evicted_rows.load(Ordering::SeqCst),
            evicted_decoded_bytes: self.stats.evicted_decoded_bytes.load(Ordering::SeqCst),
            uncacheable_count: self.stats.uncacheable_count.load(Ordering::SeqCst),
            uncacheable_rows: self.stats.uncacheable_rows.load(Ordering::SeqCst),
            uncacheable_decoded_bytes: self.stats.uncacheable_decoded_bytes.load(Ordering::SeqCst),
            cached_rows: inner.cached_rows,
            cached_decoded_bytes: inner.cached_decoded_bytes,
        }
    }

    pub fn get(&self, key: &WalTailProjectionCacheKey) -> Option<Arc<MetadataState>> {
        if self.config.max_entries == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock should not be poisoned");
        let Some(rows) = inner.entries.get(key).map(|slot| Arc::clone(&slot.rows)) else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            if let Some(observer) = &self.observer {
                observer.miss();
            }
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.hit();
        }
        Some(rows)
    }

    pub fn insert(&self, key: WalTailProjectionCacheKey, rows: Arc<MetadataState>) {
        if self.config.max_entries == 0 {
            return;
        }
        let (row_count, decoded_bytes) = projection_weight(&rows);
        if row_count > self.config.max_rows || decoded_bytes > self.config.max_decoded_bytes {
            self.stats.uncacheable_count.fetch_add(1, Ordering::SeqCst);
            self.stats
                .uncacheable_rows
                .fetch_add(row_count, Ordering::SeqCst);
            self.stats
                .uncacheable_decoded_bytes
                .fetch_add(decoded_bytes, Ordering::SeqCst);
            if let Some(observer) = &self.observer {
                observer.reject(row_count, decoded_bytes);
            }
            return;
        }

        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock should not be poisoned");
        if let Some(previous) = inner.entries.insert(
            key.clone(),
            WalTailProjectionCacheSlot {
                rows,
                last_touch: 0,
            },
        ) {
            let (previous_rows, previous_bytes) = projection_weight(&previous.rows);
            inner.cached_rows = inner.cached_rows.saturating_sub(previous_rows);
            inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(previous_bytes);
        }
        inner.cached_rows = inner.cached_rows.saturating_add(row_count);
        inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_add(decoded_bytes);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.insert();
        }

        while inner.entries.len() > self.config.max_entries
            || inner.cached_rows > self.config.max_rows
            || inner.cached_decoded_bytes > self.config.max_decoded_bytes
        {
            let WalTailProjectionCacheInner { entries, order, .. } = &mut *inner;
            let Some(evicted) = order
                .pop_oldest(|key, stamp| wal_tail_projection_slot_is_live(entries, key, stamp))
            else {
                break;
            };
            if let Some(previous) = entries.remove(&evicted) {
                let (rows, bytes) = projection_weight(&previous.rows);
                inner.cached_rows = inner.cached_rows.saturating_sub(rows);
                inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(bytes);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                self.stats.evicted_rows.fetch_add(rows, Ordering::SeqCst);
                self.stats
                    .evicted_decoded_bytes
                    .fetch_add(bytes, Ordering::SeqCst);
                if let Some(observer) = &self.observer {
                    observer.evict(rows, bytes);
                }
            }
        }
        if let Some(observer) = &self.observer {
            observer.retained(inner.cached_rows, inner.cached_decoded_bytes);
        }
    }

    pub fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock should not be poisoned");
        let keys = inner
            .entries
            .keys()
            .filter(|key| &key.namespace_id == namespace_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(previous) = inner.entries.remove(&key) {
                let (rows, bytes) = projection_weight(&previous.rows);
                inner.cached_rows = inner.cached_rows.saturating_sub(rows);
                inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(bytes);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                self.stats.evicted_rows.fetch_add(rows, Ordering::SeqCst);
                self.stats
                    .evicted_decoded_bytes
                    .fetch_add(bytes, Ordering::SeqCst);
                if let Some(observer) = &self.observer {
                    observer.evict(rows, bytes);
                }
            }
        }
        if let Some(observer) = &self.observer {
            observer.retained(inner.cached_rows, inner.cached_decoded_bytes);
        }
    }
}

impl WalTailProjectionCacheInner {
    fn touch(&mut self, key: &WalTailProjectionCacheKey) {
        let stamp = self.order.touch(key);
        if let Some(slot) = self.entries.get_mut(key) {
            slot.last_touch = stamp;
        }
        let entries = &self.entries;
        self.order.compact(entries.len(), |key, stamp| {
            wal_tail_projection_slot_is_live(entries, key, stamp)
        });
    }
}

fn wal_tail_projection_slot_is_live(
    entries: &HashMap<WalTailProjectionCacheKey, WalTailProjectionCacheSlot>,
    key: &WalTailProjectionCacheKey,
    stamp: u64,
) -> bool {
    entries
        .get(key)
        .is_some_and(|slot| slot.last_touch == stamp)
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
        let cache = WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 1,
            max_rows: 10,
            max_decoded_bytes: 16 * 1024,
        });
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
