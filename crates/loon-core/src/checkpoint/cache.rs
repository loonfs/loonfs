use loon_api::wire::checkpoint::{CheckpointRow, CheckpointSegmentKey, CheckpointTableFamily};
use loon_api::ChangeSeq;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

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
    pub(super) rows: Vec<CheckpointRow>,
    pub(super) segment_seq: ChangeSeq,
    pub(super) family: CheckpointTableFamily,
    pub(super) segment_index: u32,
    pub(super) segment_key: CheckpointSegmentKey,
    pub(super) row_count: u64,
    pub(super) min_key: String,
    pub(super) max_key: String,
    pub(super) page_checksums_sha256: Vec<String>,
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
