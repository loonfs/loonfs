use crate::metadata::MetadataState;
use loonfs_api::wire::manifest::{MetadataRow, MetadataSegmentKey, MetadataTableFamily};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
            max_blocks: 256,
            max_decoded_bytes: None,
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
        }
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
