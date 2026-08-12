//! Shared caches for decoded manifest state: SST blocks keyed by content
//! digest, validated manifests, and bounded WAL-tail projections.
//!
//! The decoded block cache also carries the handle to the optional
//! node-local cache of the same blocks in their encoded form; see
//! [`stored_block_cache`](super::stored_block_cache).

use super::runs::MetadataRunManifest;
use super::stored_block_cache::StoredMetadataBlockCache;
use crate::metadata::MetadataState;
use crate::recency::Recency;
use loonfs_api::wire::manifest::NamespaceManifestEnvelope;
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// Default decoded-byte budget for cached metadata table blocks. The byte
/// budget is the only limit — one wide directory's segment working set must
/// fit, or warm listings re-fetch segments superlinearly — and zero
/// disables the cache.
pub const DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTableCacheConfig {
    pub max_decoded_bytes: usize,
}

impl Default for MetadataTableCacheConfig {
    fn default() -> Self {
        Self {
            max_decoded_bytes: DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataTableCacheStats {
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

/// Receives decoded metadata-table cache events for runtime metrics.
pub trait MetadataTableCacheObserver: Send + Sync + 'static {
    fn hit(&self);
    fn miss(&self);
    fn insert(&self);
    fn evict(&self);
    fn filter_skip(&self);
    fn filter_false_positive(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MetadataTableBlockKind {
    Index,
    Filter,
    Data,
    Manifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MetadataTableCacheKey {
    /// The cached object's identity: a segment's payload checksum for block
    /// entries, or the manifest object key for manifest entries — both
    /// immutable, so entries can never go stale.
    pub(super) identity: String,
    pub(super) block_kind: MetadataTableBlockKind,
    pub(super) block_offset: u64,
}

/// One decoded, verified object the cache holds: a CRC-checked segment
/// section (index, filter, or data) or a validated namespace manifest.
/// Everything shares the cache and its byte budget; the key's `block_kind`
/// and `block_offset` make collisions between kinds impossible. Data blocks
/// hold metadata rows.
#[derive(Debug, Clone)]
pub(super) enum DecodedMetadataTableBlock {
    Index {
        entries: Arc<Vec<SegmentIndexEntry>>,
        decoded_byte_len: usize,
    },
    Filter {
        filter: Arc<SegmentFilter>,
        decoded_byte_len: usize,
    },
    Data {
        block: Arc<DecodedDataBlock>,
        decoded_byte_len: usize,
    },
    Manifest {
        manifest: Arc<NamespaceManifestEnvelope>,
        /// The manifest's `metadata_files` regrouped into scan-order runs,
        /// derived once when the envelope is validated. Scans walk this
        /// list on every page, so it is far too hot to regroup per scan.
        scan_runs: Arc<Vec<MetadataRunManifest>>,
        decoded_byte_len: usize,
    },
}

impl DecodedMetadataTableBlock {
    pub(super) fn decoded_byte_len(&self) -> usize {
        match self {
            Self::Index {
                decoded_byte_len, ..
            }
            | Self::Filter {
                decoded_byte_len, ..
            }
            | Self::Data {
                decoded_byte_len, ..
            }
            | Self::Manifest {
                decoded_byte_len, ..
            } => *decoded_byte_len,
        }
    }
}

pub struct MetadataTableCache {
    config: MetadataTableCacheConfig,
    inner: Mutex<MetadataTableCacheInner>,
    stats: MetadataTableCacheStatsInner,
    observer: Option<Arc<dyn MetadataTableCacheObserver>>,
    /// One cell per in-flight block fetch, keyed by the block's cache key,
    /// so concurrent readers share a single ranged GET per block.
    in_flight: Mutex<HashMap<MetadataTableCacheKey, Arc<OnceCell<DecodedMetadataTableBlock>>>>,
    /// The node-local cache of encoded stored blocks this cache misses
    /// into, when the runtime was built with one. It lives here so the two
    /// tiers are available together: a path that carries no table cache —
    /// maintenance, reorganization, collection, and retention all pass
    /// none — reaches neither tier, and needs no separate wiring to say so.
    stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
}

impl std::fmt::Debug for MetadataTableCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataTableCache")
            .field("config", &self.config)
            .field("inner", &self.inner)
            .field("stats", &self.stats)
            .field("in_flight", &self.in_flight)
            .field("stored_block_cache", &self.stored_block_cache)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct MetadataTableCacheInner {
    entries: HashMap<MetadataTableCacheKey, CacheSlot>,
    order: Recency<MetadataTableCacheKey>,
    decoded_byte_len: usize,
}

#[derive(Debug)]
struct CacheSlot {
    block: DecodedMetadataTableBlock,
    /// Stamp of this entry's newest queue position; older positions for the
    /// same key are ghosts.
    last_touch: u64,
}

#[derive(Debug, Default)]
struct MetadataTableCacheStatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
    filter_skips: AtomicUsize,
    filter_false_positives: AtomicUsize,
}

impl MetadataTableCache {
    pub fn new(config: MetadataTableCacheConfig) -> Self {
        Self::with_stored_block_cache(config, None)
    }

    /// Sizes a cache that also carries a node-local cache of encoded stored
    /// blocks beneath it. Passing `None` is exactly [`Self::new`].
    pub fn with_stored_block_cache(
        config: MetadataTableCacheConfig,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    ) -> Self {
        Self::with_stored_block_cache_and_observer(config, stored_block_cache, None)
    }

    /// Sizes a cache and reports its activity to `observer`.
    pub fn with_stored_block_cache_and_observer(
        config: MetadataTableCacheConfig,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
        observer: Option<Arc<dyn MetadataTableCacheObserver>>,
    ) -> Self {
        Self {
            config,
            inner: Mutex::new(MetadataTableCacheInner::default()),
            stats: MetadataTableCacheStatsInner::default(),
            observer,
            in_flight: Mutex::new(HashMap::new()),
            stored_block_cache,
        }
    }

    /// The node-local stored-block cache this cache misses into, if the
    /// runtime was built with one.
    pub fn stored_block_cache(&self) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
        self.stored_block_cache.as_ref()
    }

    /// Resolves one block access through a single-flight cell.
    pub(super) async fn get_or_load<E, F, Fut>(
        &self,
        cache_key: &MetadataTableCacheKey,
        fetch: F,
    ) -> Result<DecodedMetadataTableBlock, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DecodedMetadataTableBlock, E>>,
    {
        let cell = {
            let mut in_flight = self
                .in_flight
                .lock()
                .expect("metadata table cache in-flight lock should not be poisoned");
            Arc::clone(
                in_flight
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };
        let result = cell
            .get_or_try_init(|| async {
                if let Some(block) = self.get(cache_key) {
                    return Ok(block);
                }
                let block = fetch().await?;
                self.insert(cache_key.clone(), block.clone());
                Ok(block)
            })
            .await
            .cloned();
        let mut in_flight = self
            .in_flight
            .lock()
            .expect("metadata table cache in-flight lock should not be poisoned");
        if in_flight
            .get(cache_key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            in_flight.remove(cache_key);
        }
        result
    }

    pub fn stats(&self) -> MetadataTableCacheStats {
        MetadataTableCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
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

    pub(super) fn get(&self, key: &MetadataTableCacheKey) -> Option<DecodedMetadataTableBlock> {
        if self.config.max_decoded_bytes == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock should not be poisoned");
        let Some(block) = inner.entries.get(key).map(|slot| slot.block.clone()) else {
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
        Some(block)
    }

    pub(super) fn insert(&self, key: MetadataTableCacheKey, block: DecodedMetadataTableBlock) {
        if self.config.max_decoded_bytes == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock should not be poisoned");
        let decoded_byte_len = block.decoded_byte_len();
        if let Some(previous) = inner.entries.insert(
            key.clone(),
            CacheSlot {
                block,
                last_touch: 0,
            },
        ) {
            inner.decoded_byte_len = inner
                .decoded_byte_len
                .saturating_sub(previous.block.decoded_byte_len());
        }
        inner.decoded_byte_len = inner.decoded_byte_len.saturating_add(decoded_byte_len);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.insert();
        }
        let MetadataTableCacheInner {
            entries,
            order,
            decoded_byte_len,
        } = &mut *inner;
        while *decoded_byte_len > self.config.max_decoded_bytes {
            let Some(candidate) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            if let Some(slot) = entries.remove(&candidate) {
                *decoded_byte_len = decoded_byte_len.saturating_sub(slot.block.decoded_byte_len());
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                if let Some(observer) = &self.observer {
                    observer.evict();
                }
            }
        }
    }
}

impl MetadataTableCacheInner {
    fn touch(&mut self, key: &MetadataTableCacheKey) {
        let stamp = self.order.touch(key);
        if let Some(slot) = self.entries.get_mut(key) {
            slot.last_touch = stamp;
        }
        let entries = &self.entries;
        self.order.compact(entries.len(), |key, stamp| {
            slot_is_live(entries, key, stamp)
        });
    }
}

/// Whether a queue position still names the entry's newest access. An entry
/// that was replaced, evicted, or re-touched leaves stale positions behind.
fn slot_is_live(
    entries: &HashMap<MetadataTableCacheKey, CacheSlot>,
    key: &MetadataTableCacheKey,
    stamp: u64,
) -> bool {
    entries
        .get(key)
        .is_some_and(|slot| slot.last_touch == stamp)
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

/// Receives WAL-tail projection cache events for runtime metrics.
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
    pub manifest_id: ManifestId,
    pub manifest_head_seq: ChangeSeq,
    pub head_seq: ChangeSeq,
    pub head_etag: String,
}

/// What one entry counts against the row and byte budgets. The projection
/// carries both totals, and a projection in the cache is immutable, so the
/// cache reads them rather than keeping its own copies.
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
    /// Stamp of this entry's newest queue position; older positions for the
    /// same key are ghosts.
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

    /// Sizes a cache and reports its activity to `observer`.
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
mod tests {
    use super::{
        DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache,
        MetadataTableCacheConfig, MetadataTableCacheKey,
    };
    use loonfs_api::wire::sst_blocks::DecodedDataBlock;
    use std::sync::Arc;

    fn block(decoded_byte_len: usize) -> DecodedMetadataTableBlock {
        DecodedMetadataTableBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_byte_len,
        }
    }

    fn key(digest: &str) -> MetadataTableCacheKey {
        MetadataTableCacheKey {
            identity: digest.to_owned(),
            block_kind: MetadataTableBlockKind::Data,
            block_offset: 0,
        }
    }

    #[test]
    fn byte_budget_evicts_the_oldest_block() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
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
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
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
    fn recency_queue_stays_bounded_under_repeated_hits() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
            max_decoded_bytes: 10_000,
        });
        for index in 0..8 {
            cache.insert(key(&format!("k{index}")), block(10));
        }
        for _ in 0..10_000 {
            cache.get(&key("k0"));
            cache.get(&key("k3"));
        }
        let inner = cache.inner.lock().expect("cache lock");
        assert_eq!(inner.entries.len(), 8);
        assert!(
            inner.order.positions() <= (inner.entries.len() * 2).max(16),
            "hits must not grow the recency queue unboundedly, queue = {}",
            inner.order.positions()
        );
    }

    #[test]
    fn cache_hits_share_the_decoded_row_allocation() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
        let inserted = block(64);
        let rows = match &inserted {
            DecodedMetadataTableBlock::Data { block: rows, .. } => Arc::clone(rows),
            DecodedMetadataTableBlock::Index { .. }
            | DecodedMetadataTableBlock::Filter { .. }
            | DecodedMetadataTableBlock::Manifest { .. } => {
                unreachable!("fixture builds a data block")
            }
        };
        cache.insert(key("a"), inserted);
        let hit = cache.get(&key("a")).expect("inserted block should hit");
        let shares_allocation = match &hit {
            DecodedMetadataTableBlock::Data {
                block: hit_rows, ..
            } => Arc::ptr_eq(hit_rows, &rows),
            DecodedMetadataTableBlock::Index { .. }
            | DecodedMetadataTableBlock::Filter { .. }
            | DecodedMetadataTableBlock::Manifest { .. } => false,
        };
        assert!(
            shares_allocation,
            "a cache hit should share the decoded rows, not clone them"
        );
    }

    #[tokio::test]
    async fn get_or_load_retries_after_a_failed_load() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
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
    }

    #[tokio::test]
    async fn get_or_load_counts_one_miss_and_populates_for_later_hits() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
        let fetched: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Ok(block(1)) })
            .await;
        assert!(fetched.is_ok());
        let cached: Result<_, String> = cache
            .get_or_load(&key("a"), || async {
                Err("a populated key must not re-fetch".to_owned())
            })
            .await;
        assert!(cached.is_ok(), "the cached block should answer the access");

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.hits, 1);
    }
}
