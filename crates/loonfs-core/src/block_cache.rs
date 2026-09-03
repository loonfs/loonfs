//! Shared cache for decoded immutable objects.

use crate::checkpoint::ManifestLoadError;
use crate::recency::Recency;
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodedBlockWeight {
    pub bytes: usize,
    pub rows: usize,
}

impl DecodedBlockWeight {
    fn saturating_add(self, other: Self) -> Self {
        Self {
            bytes: self.bytes.saturating_add(other.bytes),
            rows: self.rows.saturating_add(other.rows),
        }
    }

    fn saturating_sub(self, other: Self) -> Self {
        Self {
            bytes: self.bytes.saturating_sub(other.bytes),
            rows: self.rows.saturating_sub(other.rows),
        }
    }
}

pub trait DecodedBlock: Clone {
    fn weight(&self) -> DecodedBlockWeight;
}

/// Receives decoded-block cache events for metrics.
pub trait DecodedBlockCacheObserver: Send + Sync + 'static {
    fn hit(&self) {}
    fn miss(&self) {}
    fn insert(&self) {}
    fn evict(&self, _weight: DecodedBlockWeight) {}
    fn reject(&self, _weight: DecodedBlockWeight) {}
    fn retained(&self, _weight: DecodedBlockWeight) {}
    fn filter_skip(&self) {}
    fn filter_false_positive(&self) {}
}

#[derive(Clone)]
pub struct DecodedBlockCacheConfig {
    pub max_decoded_bytes: usize,
    pub max_rows: Option<usize>,
    pub max_entries: Option<usize>,
    pub observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
}

impl std::fmt::Debug for DecodedBlockCacheConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodedBlockCacheConfig")
            .field("max_decoded_bytes", &self.max_decoded_bytes)
            .field("max_rows", &self.max_rows)
            .field("max_entries", &self.max_entries)
            .field("observer", &self.observer.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl DecodedBlockCacheConfig {
    /// A byte-bounded cache with no row bound, no entry bound, and no observer.
    pub fn with_max_decoded_bytes(max_decoded_bytes: usize) -> Self {
        Self {
            max_decoded_bytes,
            max_rows: None,
            max_entries: None,
            observer: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodedBlockCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_rows: usize,
    pub evicted_decoded_bytes: usize,
    pub cached_rows: usize,
    pub cached_decoded_bytes: usize,
}

pub struct DecodedBlockCache<K, V> {
    config: DecodedBlockCacheConfig,
    inner: Mutex<Inner<K, V>>,
    stats: StatsInner,
    in_flight: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for DecodedBlockCache<K, V> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodedBlockCache")
            .field("config", &self.config)
            .field("inner", &self.inner)
            .field("stats", &self.stats)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Inner<K, V> {
    entries: HashMap<K, CacheSlot<V>>,
    order: Recency<K>,
    weight: DecodedBlockWeight,
}

#[derive(Debug)]
struct CacheSlot<V> {
    block: V,
    last_touch: u64,
}

#[derive(Debug, Default)]
struct StatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
    evicted_rows: AtomicUsize,
    evicted_decoded_bytes: AtomicUsize,
}

impl<K: Clone + Eq + Hash, V: DecodedBlock> DecodedBlockCache<K, V> {
    pub fn new(config: DecodedBlockCacheConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: Recency::default(),
                weight: DecodedBlockWeight::default(),
            }),
            stats: StatsInner::default(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Loads a missing value once when callers request the same key concurrently.
    pub async fn get_or_load<E, F, Fut>(&self, cache_key: &K, fetch: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        let cell = {
            let mut in_flight = self
                .in_flight
                .lock()
                .expect("decoded block cache in-flight lock should not be poisoned");
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
            .expect("decoded block cache in-flight lock should not be poisoned");
        if in_flight
            .get(cache_key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            in_flight.remove(cache_key);
        }
        result
    }

    pub fn stats(&self) -> DecodedBlockCacheStats {
        let inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        DecodedBlockCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
            evicted_rows: self.stats.evicted_rows.load(Ordering::SeqCst),
            evicted_decoded_bytes: self.stats.evicted_decoded_bytes.load(Ordering::SeqCst),
            cached_rows: inner.weight.rows,
            cached_decoded_bytes: inner.weight.bytes,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if self.disabled() {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        let Some(block) = inner.entries.get(key).map(|slot| slot.block.clone()) else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            if let Some(observer) = &self.config.observer {
                observer.miss();
            }
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.config.observer {
            observer.hit();
        }
        Some(block)
    }

    /// Whether the cache holds `key`. Unlike `get`, this records no hit or
    /// miss and does not touch the entry's recency.
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.inner
            .lock()
            .expect("decoded block cache lock should not be poisoned")
            .entries
            .contains_key(key)
    }

    pub(crate) fn contains_key_matching(&self, matches: impl Fn(&K) -> bool) -> bool {
        self.inner
            .lock()
            .expect("decoded block cache lock should not be poisoned")
            .entries
            .keys()
            .any(matches)
    }

    pub fn insert(&self, key: K, block: V) {
        if self.disabled() {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        let weight = block.weight();
        if let Some(previous) = inner.entries.insert(
            key.clone(),
            CacheSlot {
                block,
                last_touch: 0,
            },
        ) {
            inner.weight = inner.weight.saturating_sub(previous.block.weight());
        }
        inner.weight = inner.weight.saturating_add(weight);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.config.observer {
            observer.insert();
        }
        self.evict_over_budget(&mut inner);
        self.record_retained(inner.weight);
    }

    pub fn invalidate(&self, matches: impl Fn(&K) -> bool) {
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        let keys = inner
            .entries
            .keys()
            .filter(|key| matches(key))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(slot) = inner.entries.remove(&key) {
                let weight = slot.block.weight();
                inner.weight = inner.weight.saturating_sub(weight);
                self.record_eviction(weight);
            }
        }
        self.record_retained(inner.weight);
    }

    fn disabled(&self) -> bool {
        self.config.max_decoded_bytes == 0 || self.config.max_entries == Some(0)
    }

    fn evict_over_budget(&self, inner: &mut Inner<K, V>) {
        while inner.weight.bytes > self.config.max_decoded_bytes
            || self
                .config
                .max_rows
                .is_some_and(|max_rows| inner.weight.rows > max_rows)
            || self
                .config
                .max_entries
                .is_some_and(|max_entries| inner.entries.len() > max_entries)
        {
            let Inner { entries, order, .. } = &mut *inner;
            let Some(candidate) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            if let Some(slot) = entries.remove(&candidate) {
                let weight = slot.block.weight();
                inner.weight = inner.weight.saturating_sub(weight);
                self.record_eviction(weight);
            }
        }
    }

    fn record_eviction(&self, weight: DecodedBlockWeight) {
        self.stats.evictions.fetch_add(1, Ordering::SeqCst);
        self.stats
            .evicted_rows
            .fetch_add(weight.rows, Ordering::SeqCst);
        self.stats
            .evicted_decoded_bytes
            .fetch_add(weight.bytes, Ordering::SeqCst);
        if let Some(observer) = &self.config.observer {
            observer.evict(weight);
        }
    }

    fn record_retained(&self, weight: DecodedBlockWeight) {
        if let Some(observer) = &self.config.observer {
            observer.retained(weight);
        }
    }
}

impl<K: Clone + Eq + Hash, V> Inner<K, V> {
    fn touch(&mut self, key: &K) {
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

fn slot_is_live<K: Eq + Hash, V>(entries: &HashMap<K, CacheSlot<V>>, key: &K, stamp: u64) -> bool {
    entries
        .get(key)
        .is_some_and(|slot| slot.last_touch == stamp)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentBlockKind {
    Index,
    Filter,
    Data,
    Manifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentCacheKey {
    /// The immutable checksum or object key that identifies the cached object.
    pub identity: String,
    pub block_kind: SegmentBlockKind,
    pub block_offset: u64,
}

#[derive(Debug, Clone)]
pub enum DecodedSegmentBlock<Row, Manifest> {
    Index {
        entries: Arc<Vec<SegmentIndexEntry>>,
        decoded_bytes: usize,
    },
    Filter {
        filter: Arc<SegmentFilter>,
        decoded_bytes: usize,
    },
    Data {
        block: Arc<DecodedDataBlock<Row>>,
        decoded_bytes: usize,
    },
    Manifest {
        manifest: Manifest,
        decoded_bytes: usize,
    },
}

impl<Row: Clone, Manifest: Clone> DecodedBlock for DecodedSegmentBlock<Row, Manifest> {
    fn weight(&self) -> DecodedBlockWeight {
        let bytes = match self {
            Self::Index { decoded_bytes, .. }
            | Self::Filter { decoded_bytes, .. }
            | Self::Data { decoded_bytes, .. }
            | Self::Manifest { decoded_bytes, .. } => *decoded_bytes,
        };
        DecodedBlockWeight { bytes, rows: 0 }
    }
}

impl<Row, Manifest> DecodedSegmentBlock<Row, Manifest> {
    pub fn index(self) -> Option<Arc<Vec<SegmentIndexEntry>>> {
        match self {
            Self::Index { entries, .. } => Some(entries),
            Self::Filter { .. } | Self::Data { .. } | Self::Manifest { .. } => None,
        }
    }

    pub fn filter(self) -> Option<Arc<SegmentFilter>> {
        match self {
            Self::Filter { filter, .. } => Some(filter),
            Self::Index { .. } | Self::Data { .. } | Self::Manifest { .. } => None,
        }
    }

    pub fn data(self) -> Option<Arc<DecodedDataBlock<Row>>> {
        match self {
            Self::Data { block, .. } => Some(block),
            Self::Index { .. } | Self::Filter { .. } | Self::Manifest { .. } => None,
        }
    }

    pub(crate) fn into_index(
        self,
        object_key: &str,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
        match self {
            Self::Index { entries, .. } => Ok(entries),
            _ => Err(wrong_kind(object_key, "non-index block for an index key")),
        }
    }

    pub(crate) fn into_filter(
        self,
        object_key: &str,
    ) -> Result<Arc<SegmentFilter>, ManifestLoadError> {
        match self {
            Self::Filter { filter, .. } => Ok(filter),
            _ => Err(wrong_kind(object_key, "non-filter block")),
        }
    }

    pub(crate) fn into_data(
        self,
        object_key: &str,
    ) -> Result<Arc<DecodedDataBlock<Row>>, ManifestLoadError> {
        match self {
            Self::Data { block, .. } => Ok(block),
            _ => Err(wrong_kind(object_key, "non-data block for a data key")),
        }
    }

    pub(crate) fn into_manifest(self, object_key: &str) -> Result<Manifest, ManifestLoadError> {
        match self {
            Self::Manifest { manifest, .. } => Ok(manifest),
            _ => Err(wrong_kind(
                object_key,
                "non-manifest entry for a manifest key",
            )),
        }
    }
}

fn wrong_kind(object_key: &str, message: &str) -> ManifestLoadError {
    ManifestLoadError::SegmentCodec {
        object_key: object_key.to_owned(),
        message: format!("cache returned a {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockWeight};

    #[derive(Clone)]
    struct Block(usize);

    impl DecodedBlock for Block {
        fn weight(&self) -> DecodedBlockWeight {
            DecodedBlockWeight {
                bytes: self.0,
                rows: 0,
            }
        }
    }

    #[test]
    fn recency_queue_stays_bounded_under_repeated_hits() {
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::with_max_decoded_bytes(10_000));
        for index in 0..8 {
            cache.insert(format!("k{index}"), Block(10));
        }
        let (first, second) = ("k0".to_owned(), "k3".to_owned());
        for _ in 0..10_000 {
            cache.get(&first);
            cache.get(&second);
        }
        let inner = cache.inner.lock().expect("cache lock");
        assert_eq!(inner.entries.len(), 8);
        assert!(
            inner.order.positions() <= (inner.entries.len() * 2).max(16),
            "hits must not grow the recency queue unboundedly, queue = {}",
            inner.order.positions()
        );
    }
}
