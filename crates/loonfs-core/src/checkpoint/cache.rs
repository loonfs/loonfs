use crate::metadata::MetadataState;
use loonfs_api::wire::manifest::{MetadataRow, MetadataSegmentKey, MetadataTableFamily};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// Default decoded-byte budget for cached metadata table blocks. The byte
/// budget is the primary limit: one wide directory's segment working set
/// must fit, or warm listings re-fetch segments superlinearly.
pub const DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES: usize = 256 * 1024 * 1024;
/// Secondary block-count bound behind the byte budget; it only binds under
/// pathological many-tiny-block shapes.
pub const DEFAULT_METADATA_TABLE_CACHE_MAX_BLOCKS: usize = 8192;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTableCacheConfig {
    pub enabled: bool,
    pub max_blocks: usize,
    pub max_decoded_bytes: Option<usize>,
}

impl Default for MetadataTableCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_blocks: DEFAULT_METADATA_TABLE_CACHE_MAX_BLOCKS,
            max_decoded_bytes: Some(DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataTableCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum MetadataTableBlockKind {
    SegmentPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MetadataTableCacheKey {
    pub(super) table_digest: String,
    pub(super) block_kind: MetadataTableBlockKind,
    pub(super) block_offset: u64,
}

#[derive(Debug, Clone)]
pub(super) struct DecodedMetadataTableBlock {
    pub(super) rows: Vec<MetadataRow>,
    pub(super) segment_seq: ChangeSeq,
    pub(super) family: MetadataTableFamily,
    pub(super) segment_index: u32,
    pub(super) segment_key: MetadataSegmentKey,
    pub(super) row_count: u64,
    pub(super) min_key: String,
    pub(super) max_key: String,
    pub(super) decoded_byte_len: usize,
}

#[derive(Debug)]
pub struct MetadataTableCache {
    config: MetadataTableCacheConfig,
    inner: Mutex<MetadataTableCacheInner>,
    stats: MetadataTableCacheStatsInner,
    /// One cell per in-flight segment fetch, keyed by object key, so
    /// concurrent readers share a single store GET per segment.
    in_flight: Mutex<HashMap<String, Arc<OnceCell<DecodedMetadataTableBlock>>>>,
}

#[derive(Debug, Default)]
struct MetadataTableCacheInner {
    entries: HashMap<MetadataTableCacheKey, DecodedMetadataTableBlock>,
    order: VecDeque<MetadataTableCacheKey>,
    decoded_byte_len: usize,
}

#[derive(Debug, Default)]
struct MetadataTableCacheStatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
}

impl MetadataTableCache {
    pub fn new(config: MetadataTableCacheConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(MetadataTableCacheInner::default()),
            stats: MetadataTableCacheStatsInner::default(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Runs `fetch` once per object key across concurrent callers: waiters
    /// share the winner's decoded block instead of issuing duplicate store
    /// GETs. A failed or cancelled fetch leaves the cell empty, so the next
    /// caller retries.
    pub(super) async fn fetch_deduplicated<E, F, Fut>(
        &self,
        object_key: &str,
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
                .expect("metadata table cache in-flight lock poisoned");
            Arc::clone(
                in_flight
                    .entry(object_key.to_owned())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };
        let result = cell.get_or_try_init(fetch).await.cloned();
        let mut in_flight = self
            .in_flight
            .lock()
            .expect("metadata table cache in-flight lock poisoned");
        if in_flight
            .get(object_key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            in_flight.remove(object_key);
        }
        result
    }

    pub fn stats(&self) -> MetadataTableCacheStats {
        MetadataTableCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
        }
    }

    pub(super) fn get(&self, key: &MetadataTableCacheKey) -> Option<DecodedMetadataTableBlock> {
        if !self.config.enabled || self.config.max_blocks == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock poisoned");
        let Some(block) = inner.entries.get(key).cloned() else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        Some(block)
    }

    pub(super) fn insert(&self, key: MetadataTableCacheKey, block: DecodedMetadataTableBlock) {
        if !self.config.enabled || self.config.max_blocks == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock poisoned");
        if let Some(previous) = inner.entries.insert(key.clone(), block.clone()) {
            inner.decoded_byte_len = inner
                .decoded_byte_len
                .saturating_sub(previous.decoded_byte_len);
        }
        inner.decoded_byte_len = inner
            .decoded_byte_len
            .saturating_add(block.decoded_byte_len);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        while inner.entries.len() > self.config.max_blocks
            || self
                .config
                .max_decoded_bytes
                .map(|max_decoded_bytes| inner.decoded_byte_len > max_decoded_bytes)
                .unwrap_or(false)
        {
            let Some(evicted) = inner.order.pop_front() else {
                break;
            };
            if let Some(block) = inner.entries.remove(&evicted) {
                inner.decoded_byte_len = inner
                    .decoded_byte_len
                    .saturating_sub(block.decoded_byte_len);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

impl MetadataTableCacheInner {
    fn touch(&mut self, key: &MetadataTableCacheKey) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTailProjectionCacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_rows: usize,
    pub max_decoded_bytes: Option<usize>,
}

impl Default for WalTailProjectionCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 64,
            max_rows: 1_000_000,
            max_decoded_bytes: Some(256 * 1024 * 1024),
        }
    }
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
    pub manifest_id: ManifestId,
    pub manifest_head_seq: ChangeSeq,
    pub head_seq: ChangeSeq,
    pub head_etag: String,
}

#[derive(Debug, Clone)]
struct CachedWalTailProjection {
    rows: Arc<MetadataState>,
    row_count: usize,
    decoded_bytes: usize,
}

impl CachedWalTailProjection {
    fn new(rows: Arc<MetadataState>) -> Self {
        Self {
            row_count: rows.row_count(),
            decoded_bytes: rows.decoded_bytes(),
            rows,
        }
    }

    fn rows(&self) -> Arc<MetadataState> {
        Arc::clone(&self.rows)
    }

    fn weight(&self) -> (usize, usize) {
        (self.row_count, self.decoded_bytes)
    }
}

#[derive(Debug)]
pub struct WalTailProjectionCache {
    config: WalTailProjectionCacheConfig,
    inner: Mutex<WalTailProjectionCacheInner>,
    stats: WalTailProjectionCacheStatsInner,
}

#[derive(Debug, Default)]
struct WalTailProjectionCacheInner {
    entries: HashMap<WalTailProjectionCacheKey, CachedWalTailProjection>,
    order: VecDeque<WalTailProjectionCacheKey>,
    cached_rows: usize,
    cached_decoded_bytes: usize,
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
        Self {
            config,
            inner: Mutex::new(WalTailProjectionCacheInner::default()),
            stats: WalTailProjectionCacheStatsInner::default(),
        }
    }

    pub fn stats(&self) -> WalTailProjectionCacheStats {
        let inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock poisoned");
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
        if !self.config.enabled || self.config.max_entries == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock poisoned");
        let Some(rows) = inner.entries.get(key).map(CachedWalTailProjection::rows) else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        Some(rows)
    }

    pub fn insert(&self, key: WalTailProjectionCacheKey, rows: Arc<MetadataState>) {
        if !self.config.enabled || self.config.max_entries == 0 {
            return;
        }
        let cached = CachedWalTailProjection::new(rows);
        let (row_count, decoded_bytes) = cached.weight();
        if row_count > self.config.max_rows
            || self
                .config
                .max_decoded_bytes
                .map(|max| decoded_bytes > max)
                .unwrap_or(false)
        {
            self.stats.uncacheable_count.fetch_add(1, Ordering::SeqCst);
            self.stats
                .uncacheable_rows
                .fetch_add(row_count, Ordering::SeqCst);
            self.stats
                .uncacheable_decoded_bytes
                .fetch_add(decoded_bytes, Ordering::SeqCst);
            return;
        }

        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock poisoned");
        if let Some(previous) = inner.entries.insert(key.clone(), cached) {
            let (rows, bytes) = previous.weight();
            inner.cached_rows = inner.cached_rows.saturating_sub(rows);
            inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(bytes);
        }
        inner.cached_rows = inner.cached_rows.saturating_add(row_count);
        inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_add(decoded_bytes);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);

        while inner.entries.len() > self.config.max_entries
            || inner.cached_rows > self.config.max_rows
            || self
                .config
                .max_decoded_bytes
                .map(|max| inner.cached_decoded_bytes > max)
                .unwrap_or(false)
        {
            let Some(evicted) = inner.order.pop_front() else {
                break;
            };
            if let Some(previous) = inner.entries.remove(&evicted) {
                let (rows, bytes) = previous.weight();
                inner.cached_rows = inner.cached_rows.saturating_sub(rows);
                inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(bytes);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                self.stats.evicted_rows.fetch_add(rows, Ordering::SeqCst);
                self.stats
                    .evicted_decoded_bytes
                    .fetch_add(bytes, Ordering::SeqCst);
            }
        }
    }

    pub fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        let mut inner = self
            .inner
            .lock()
            .expect("wal tail projection cache lock poisoned");
        let keys = inner
            .entries
            .keys()
            .filter(|key| &key.namespace_id == namespace_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(previous) = inner.entries.remove(&key) {
                let (rows, bytes) = previous.weight();
                inner.cached_rows = inner.cached_rows.saturating_sub(rows);
                inner.cached_decoded_bytes = inner.cached_decoded_bytes.saturating_sub(bytes);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                self.stats.evicted_rows.fetch_add(rows, Ordering::SeqCst);
                self.stats
                    .evicted_decoded_bytes
                    .fetch_add(bytes, Ordering::SeqCst);
            }
        }
        inner.order.retain(|key| &key.namespace_id != namespace_id);
    }
}

impl WalTailProjectionCacheInner {
    fn touch(&mut self, key: &WalTailProjectionCacheKey) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache,
        MetadataTableCacheConfig, MetadataTableCacheKey,
    };
    use loonfs_api::wire::manifest::{MetadataSegmentKey, MetadataTableFamily};
    use loonfs_api::ChangeSeq;

    fn block(decoded_byte_len: usize) -> DecodedMetadataTableBlock {
        DecodedMetadataTableBlock {
            rows: Vec::new(),
            segment_seq: ChangeSeq(1),
            family: MetadataTableFamily::Inodes,
            segment_index: 0,
            segment_key: MetadataSegmentKey::Full,
            row_count: 0,
            min_key: String::new(),
            max_key: String::new(),
            decoded_byte_len,
        }
    }

    fn key(digest: &str) -> MetadataTableCacheKey {
        MetadataTableCacheKey {
            table_digest: digest.to_owned(),
            block_kind: MetadataTableBlockKind::SegmentPayload,
            block_offset: 0,
        }
    }

    #[test]
    fn default_config_budgets_bytes_as_the_primary_limit() {
        let config = MetadataTableCacheConfig::default();
        assert_eq!(
            config.max_decoded_bytes,
            Some(super::DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES)
        );
        assert_eq!(
            config.max_blocks,
            super::DEFAULT_METADATA_TABLE_CACHE_MAX_BLOCKS
        );
    }

    #[test]
    fn byte_budget_evicts_before_the_block_bound() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
            enabled: true,
            max_blocks: 100,
            max_decoded_bytes: Some(1000),
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
            enabled: true,
            max_blocks: 100,
            max_decoded_bytes: Some(1000),
        });
        cache.insert(key("a"), block(600));
        cache.insert(key("a"), block(100));
        // 600 was released on replace: another 600 fits without eviction.
        cache.insert(key("b"), block(600));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 0);
    }

    #[tokio::test]
    async fn fetch_deduplicated_retries_after_a_failed_fetch() {
        let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
        let failed: Result<_, String> = cache
            .fetch_deduplicated("segment-key", || async { Err("transport".to_owned()) })
            .await;
        assert!(failed.is_err());
        let recovered: Result<_, String> = cache
            .fetch_deduplicated("segment-key", || async { Ok(block(1)) })
            .await;
        assert!(
            recovered.is_ok(),
            "a failed fetch should leave nothing behind for the next caller"
        );
    }
}
