//! Shared cache for decoded immutable objects.

use crate::heap_bytes::{hash_map_table_bytes, HeapBytes};
use crate::manifest::ManifestLoadError;
use crate::recency::Recency;
use loonfs_types::format::segment_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::collections::HashMap;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

pub trait DecodedBlock: Clone {
    /// The decoded bytes this value holds, charged against the cache budget.
    fn weight(&self) -> usize;
}

/// A key of a [`DecodedBlockCache`].
///
/// The cache charges each entry the heap of its key as well as the weight of
/// its value, so that a cache of small values stays within its budget.
pub trait DecodedBlockKey: Clone + Eq + Hash {
    /// Returns the heap the key owns outside its inline size, which the cache
    /// charges with the entry.
    fn heap_bytes(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct DecodedBlockCacheConfig {
    pub max_decoded_bytes: usize,
}

impl DecodedBlockCacheConfig {
    /// Returns the config of a cache with a budget of `max_decoded_bytes`.
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self { max_decoded_bytes }
    }
}

/// Counters and occupancy of a [`DecodedBlockCache`].
///
/// Retained bytes include the values, keys, table, and recency queue.
/// Evicted bytes count the key and value released with an entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodedBlockCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    /// Entries the cache didn't keep, because each one's charge was more
    /// than the whole budget.
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    /// Retained allocations, including unused table and queue capacity.
    pub cached_decoded_bytes: usize,
}

/// Lookups and inserts of one kind of entry, in a cache that keeps more than
/// one kind of entry under one budget.
///
/// The cache records a lookup or an insert here only when it records one in
/// its own counters, so a disabled cache records nothing here either.
#[derive(Debug, Default)]
pub(crate) struct KindCounters {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
}

/// A snapshot of [`KindCounters`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct KindStats {
    pub(crate) hits: usize,
    pub(crate) misses: usize,
    pub(crate) inserts: usize,
}

impl KindCounters {
    pub(crate) fn stats(&self) -> KindStats {
        KindStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
        }
    }
}

/// Returns the initial table, queue, and queued key allocations.
fn initial_overhead<K: DecodedBlockKey, V>(key: &K) -> usize {
    let bucket_bytes = size_of::<(K, CacheSlot<V>)>() + 1;
    key.heap_bytes()
        .saturating_add(4 * bucket_bytes + 16 + 4 * size_of::<(K, u64)>())
}

/// Returns the bytes needed to cache an entry on its own.
///
/// The table and the entry's recency position each store a copy of the key,
/// so the key's heap is charged twice.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn charged_bytes<K: DecodedBlockKey, V: DecodedBlock>(key: &K, block: &V) -> usize {
    block
        .weight()
        .saturating_add(key.heap_bytes())
        .saturating_add(initial_overhead::<K, V>(key))
}

pub struct DecodedBlockCache<K, V> {
    config: DecodedBlockCacheConfig,
    inner: Mutex<Inner<K, V>>,
    stats: StatsInner,
    in_flight: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

struct LoadGuard<'a, K: Eq + Hash, V> {
    in_flight: &'a Mutex<HashMap<K, Arc<OnceCell<V>>>>,
    cache_key: &'a K,
    cell: Option<Arc<OnceCell<V>>>,
}

impl<K: Eq + Hash, V> Drop for LoadGuard<'_, K, V> {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .expect("decoded block cache in-flight lock should not be poisoned");
        // Release the reference under the lock so concurrent drops cannot both leave an entry.
        drop(self.cell.take());
        if in_flight
            .get(self.cache_key)
            .is_some_and(|cell| Arc::strong_count(cell) == 1)
        {
            in_flight.remove(self.cache_key);
        }
    }
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
    /// Key and value allocations owned by live entries.
    charged_bytes: usize,
    order_key_bytes: usize,
    /// Deletion can reduce `HashMap::capacity` without releasing its table.
    /// Keep the largest allocation until the table is explicitly shrunk.
    table_bytes: usize,
}

#[derive(Debug)]
struct CacheSlot<V> {
    block: V,
    /// The key and value allocations released when the entry leaves.
    charged_bytes: usize,
    last_touch: u64,
}

#[derive(Debug, Default)]
struct StatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
    evicted_decoded_bytes: AtomicUsize,
    rejections: AtomicUsize,
    rejected_decoded_bytes: AtomicUsize,
}

impl<K: DecodedBlockKey, V: DecodedBlock> DecodedBlockCache<K, V> {
    pub fn new(config: DecodedBlockCacheConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: Recency::default(),
                charged_bytes: 0,
                order_key_bytes: 0,
                table_bytes: 0,
            }),
            stats: StatsInner::default(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Loads a missing value once when callers request the same key concurrently.
    pub async fn get_or_load<E, F, Fut>(&self, cache_key: &K, load: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        self.load_once(cache_key, None, load).await
    }

    /// Loads like [`Self::get_or_load`], and records the lookup and the
    /// insert in `kind` too.
    pub(crate) async fn get_or_load_kind<E, F, Fut>(
        &self,
        cache_key: &K,
        kind: &KindCounters,
        load: F,
    ) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        self.load_once(cache_key, Some(kind), load).await
    }

    /// Runs one load for the concurrent callers of `cache_key`.
    ///
    /// A caller that receives the value of another caller's load records no
    /// lookup.
    async fn load_once<E, F, Fut>(
        &self,
        cache_key: &K,
        kind: Option<&KindCounters>,
        load: F,
    ) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        let guard = {
            let mut in_flight = self
                .in_flight
                .lock()
                .expect("decoded block cache in-flight lock should not be poisoned");
            let cell = Arc::clone(
                in_flight
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            );
            LoadGuard {
                in_flight: &self.in_flight,
                cache_key,
                cell: Some(cell),
            }
        };
        guard
            .cell
            .as_ref()
            .expect("in-flight load should hold its cell")
            .get_or_try_init(|| async {
                if let Some(block) = self.get_entry(cache_key, kind, Some) {
                    return Ok(block);
                }
                let block = load().await?;
                self.insert_entry(cache_key.clone(), block.clone(), kind);
                Ok(block)
            })
            .await
            .cloned()
    }

    /// Counts the entries that the cache keeps.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("decoded block cache lock should not be poisoned")
            .entries
            .len()
    }

    #[cfg(test)]
    fn in_flight_len(&self) -> usize {
        self.in_flight
            .lock()
            .expect("decoded block cache in-flight lock should not be poisoned")
            .len()
    }

    pub fn stats(&self) -> DecodedBlockCacheStats {
        let inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        DecodedBlockCacheStats {
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            inserts: self.stats.inserts.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            evicted_decoded_bytes: self.stats.evicted_decoded_bytes.load(Ordering::Relaxed),
            rejections: self.stats.rejections.load(Ordering::Relaxed),
            rejected_decoded_bytes: self.stats.rejected_decoded_bytes.load(Ordering::Relaxed),
            cached_decoded_bytes: inner.retained_bytes(),
        }
    }

    /// Returns the value under `key` and marks it recently used.
    ///
    /// A disabled cache returns `None` and records neither a hit nor a miss.
    pub fn get(&self, key: &K) -> Option<V> {
        self.get_entry(key, None, Some)
    }

    /// Returns the value under `key` if `accept` takes it, and records the
    /// lookup in `kind` too: a hit if `accept` took the value, and a miss
    /// otherwise.
    pub(crate) fn get_kind<T>(
        &self,
        key: &K,
        kind: &KindCounters,
        accept: impl FnOnce(V) -> Option<T>,
    ) -> Option<T> {
        self.get_entry(key, Some(kind), accept)
    }

    fn get_entry<T>(
        &self,
        key: &K,
        kind: Option<&KindCounters>,
        accept: impl FnOnce(V) -> Option<T>,
    ) -> Option<T> {
        if self.disabled() {
            return None;
        }
        let found = self.touch_entry(key).and_then(accept);
        if let Some(kind) = kind {
            let counter = match found {
                Some(_) => &kind.hits,
                None => &kind.misses,
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    /// Returns the value under `key`, marks it recently used, and records a
    /// hit or a miss.
    fn touch_entry(&self, key: &K) -> Option<V> {
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        let Some(block) = inner.entries.get(key).map(|slot| slot.block.clone()) else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        inner.touch(key);
        self.evict_over_budget(&mut inner);
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(block)
    }

    /// Returns a cached value without touching its recency or counting a
    /// hit or a miss.
    pub fn peek(&self, key: &K) -> Option<V> {
        self.inner
            .lock()
            .expect("decoded block cache lock should not be poisoned")
            .entries
            .get(key)
            .map(|slot| slot.block.clone())
    }

    /// Inserts `block` under `key`, then evicts the least recently used
    /// entries until the cache is within its budget again.
    ///
    /// Returns whether the cache kept the entry. A disabled cache keeps
    /// nothing. The cache also doesn't keep an entry whose charge is more
    /// than the whole budget, because inserting it would evict every other
    /// entry first and then the entry itself. When the cache doesn't keep
    /// the entry, the entry that the key already had stays.
    pub fn insert(&self, key: K, block: V) -> bool {
        self.insert_entry(key, block, None)
    }

    /// Inserts like [`Self::insert`], and records the insert in `kind` too
    /// if the cache kept the entry.
    pub(crate) fn insert_kind(&self, key: K, block: V, kind: &KindCounters) -> bool {
        self.insert_entry(key, block, Some(kind))
    }

    fn insert_entry(&self, key: K, block: V, kind: Option<&KindCounters>) -> bool {
        if self.disabled() {
            return false;
        }
        let stored_key = key.clone();
        let entry_charge = block.weight().saturating_add(stored_key.heap_bytes());
        let charged_bytes = entry_charge.saturating_add(initial_overhead::<K, V>(&stored_key));
        if charged_bytes > self.config.max_decoded_bytes {
            self.record_rejection(charged_bytes);
            return false;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        if let Some(previous) = inner.entries.remove(&key) {
            inner.charged_bytes = inner.charged_bytes.saturating_sub(previous.charged_bytes);
        }
        inner.entries.insert(
            stored_key,
            CacheSlot {
                block,
                charged_bytes: entry_charge,
                last_touch: 0,
            },
        );
        inner.table_bytes = inner.table_bytes.max(hash_map_table_bytes(&inner.entries));
        inner.charged_bytes = inner.charged_bytes.saturating_add(entry_charge);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
        self.evict_over_budget(&mut inner);
        let kept = inner.entries.contains_key(&key);
        if let Some(kind) = kind.filter(|_| kept) {
            kind.inserts.fetch_add(1, Ordering::Relaxed);
        }
        kept
    }

    /// Drops one entry without counting an eviction.
    pub fn invalidate(&self, key: &K) {
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        if let Some(slot) = inner.entries.remove(key) {
            inner.charged_bytes = inner.charged_bytes.saturating_sub(slot.charged_bytes);
            inner.shrink_if_sparse();
        }
    }

    fn disabled(&self) -> bool {
        self.config.max_decoded_bytes == 0
    }

    fn evict_over_budget(&self, inner: &mut Inner<K, V>) {
        while inner.retained_bytes() > self.config.max_decoded_bytes {
            let Some((candidate, stamp)) = inner.order.pop_front() else {
                break;
            };
            inner.order_key_bytes -= candidate.heap_bytes();
            if slot_is_live(&inner.entries, &candidate, stamp) {
                let slot = inner.entries.remove(&candidate).expect("live entry");
                inner.charged_bytes = inner.charged_bytes.saturating_sub(slot.charged_bytes);
                self.record_eviction(slot.charged_bytes);
            }
            inner.shrink_if_sparse();
        }
    }

    fn record_rejection(&self, charged_bytes: usize) {
        self.stats.rejections.fetch_add(1, Ordering::Relaxed);
        self.stats
            .rejected_decoded_bytes
            .fetch_add(charged_bytes, Ordering::Relaxed);
    }

    fn record_eviction(&self, decoded_bytes: usize) {
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        self.stats
            .evicted_decoded_bytes
            .fetch_add(decoded_bytes, Ordering::Relaxed);
    }
}

impl<K: DecodedBlockKey, V> Inner<K, V> {
    fn retained_bytes(&self) -> usize {
        self.charged_bytes
            .saturating_add(self.order_key_bytes)
            .saturating_add(self.table_bytes)
            .saturating_add(self.order.allocation_bytes())
    }

    fn touch(&mut self, key: &K) {
        let queued_key = key.clone();
        self.order_key_bytes += queued_key.heap_bytes();
        let stamp = self.order.touch(queued_key);
        if let Some(slot) = self.entries.get_mut(key) {
            slot.last_touch = stamp;
        }
        let entries = &self.entries;
        self.order.compact(entries.len(), |key, stamp| {
            let live = slot_is_live(entries, key, stamp);
            if !live {
                self.order_key_bytes -= key.heap_bytes();
            }
            live
        });
    }

    fn shrink_if_sparse(&mut self) {
        if self.entries.is_empty() {
            self.entries = HashMap::new();
            self.order = Recency::default();
            self.table_bytes = 0;
            self.order_key_bytes = 0;
            return;
        }
        // Shrink only after most entries have left, so eviction doesn't
        // rebuild the table on every removal.
        if self.entries.len() * size_of::<(K, CacheSlot<V>)>() < self.table_bytes / 4 {
            self.entries.shrink_to_fit();
            self.table_bytes = hash_map_table_bytes(&self.entries);
        }
        self.order.shrink_if_sparse();
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
    /// The immutable segment id or object key that identifies the cached object.
    pub identity: String,
    pub block_kind: SegmentBlockKind,
    pub block_offset: u64,
}

impl DecodedBlockKey for SegmentCacheKey {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(&self.identity)
    }
}

impl DecodedBlockKey for String {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(self)
    }
}

impl<A: DecodedBlockKey, B: DecodedBlockKey> DecodedBlockKey for (A, B) {
    fn heap_bytes(&self) -> usize {
        DecodedBlockKey::heap_bytes(&self.0) + DecodedBlockKey::heap_bytes(&self.1)
    }
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
    fn weight(&self) -> usize {
        match self {
            Self::Index { decoded_bytes, .. }
            | Self::Filter { decoded_bytes, .. }
            | Self::Data { decoded_bytes, .. }
            | Self::Manifest { decoded_bytes, .. } => *decoded_bytes,
        }
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
    use super::{
        charged_bytes, DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockKey,
    };
    use crate::heap_bytes::HeapBytes;
    use bytes::Bytes;
    use futures::poll;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{
        BlockingStore, KeyPredicate, OperationSelector, RecordingStore,
    };
    use loonfs_types::MetadataSegmentId;
    use std::pin::pin;

    #[derive(Clone)]
    struct Block(usize);

    impl DecodedBlock for Block {
        fn weight(&self) -> usize {
            self.0
        }
    }

    impl DecodedBlockKey for MetadataSegmentId {
        fn heap_bytes(&self) -> usize {
            HeapBytes::heap_bytes(self)
        }
    }

    #[tokio::test]
    async fn cancelled_loads_release_entries_after_the_last_waiter_leaves() {
        for max_decoded_bytes in [0, 1_024] {
            let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_decoded_bytes));
            let directory = tempfile::tempdir().expect("temporary directory");
            let store = RecordingStore::new(
                BlockingStore::new(
                    LocalFsStore::new(directory.path()).expect("local store"),
                    KeyPredicate::any(),
                    OperationSelector::Get,
                ),
                KeyPredicate::any(),
            );
            let keys: Vec<_> = (0..64)
                .map(|index| {
                    MetadataSegmentId::parse(format!("seg_{index:032x}")).expect("segment id")
                })
                .collect();
            let mut loads = Vec::new();
            for key in &keys {
                store.inner().arm();
                let mut load = Box::pin(cache.get_or_load(key, || async {
                    store.get(key.as_str(), None).await.map(|_| Block(1))
                }));
                assert!(poll!(load.as_mut()).is_pending());
                assert!(poll!(pin!(store.inner().wait_until_blocked())).is_ready());
                loads.push(load);
            }
            assert_eq!(cache.in_flight_len(), keys.len());
            drop(loads);
            assert_eq!(cache.in_flight_len(), 0);

            let key = &keys[0];
            store
                .put_overwrite(key.as_str(), Bytes::from_static(b"block"))
                .await
                .expect("write block");
            let load = || {
                cache.get_or_load(key, || async {
                    store
                        .get(key.as_str(), None)
                        .await
                        .map(|body| Block(body.expect("block should exist").len()))
                })
            };
            let mut driver = Box::pin(load());
            let mut waiter = Box::pin(load());
            assert!(poll!(driver.as_mut()).is_pending());
            assert!(poll!(waiter.as_mut()).is_pending());
            drop(waiter);
            assert_eq!(cache.in_flight_len(), 1);

            let mut waiter = Box::pin(load());
            assert!(poll!(waiter.as_mut()).is_pending());
            drop(driver);
            assert_eq!(cache.in_flight_len(), 1);
            assert!(poll!(waiter.as_mut()).is_pending());
            let mut newcomer = Box::pin(load());
            assert!(poll!(newcomer.as_mut()).is_pending());

            store.inner().release();
            let (waiter, newcomer) = futures::join!(waiter, newcomer);
            assert_eq!(waiter.expect("remaining waiter should finish").0, 5);
            assert_eq!(newcomer.expect("new waiter should finish").0, 5);
            assert_eq!(store.counts().gets, 1);
            assert_eq!(cache.in_flight_len(), 0);
        }
    }

    #[test]
    fn recency_queue_stays_bounded_under_repeated_hits() {
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(10_000));
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

    /// Checks that entries whose values weigh nothing are charged their keys
    /// and the cache's own allocation, so a budget that fits a few of them
    /// keeps a bounded number and evicts the rest.
    #[test]
    fn entries_with_weightless_values_are_charged_and_evicted() {
        let keys: Vec<String> = (0..32).map(|index| format!("key {index:02}")).collect();
        let budget = 1_024;
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(budget));
        let key_count = keys.len();
        for key in keys {
            assert!(cache.insert(key, Block(0)));
        }
        let live_entries = cache.inner.lock().expect("cache lock").entries.len();
        let stats = cache.stats();
        assert!(live_entries > 0 && live_entries < key_count);
        assert_eq!(stats.evictions, key_count - live_entries);
        assert!(stats.cached_decoded_bytes > 0 && stats.cached_decoded_bytes <= budget);
    }

    /// Checks that an entry whose charge is more than the whole budget isn't
    /// kept, that the entries already in the cache stay, and that the stats
    /// record the rejection and its charge.
    #[test]
    fn an_entry_heavier_than_the_budget_is_rejected_without_evictions() {
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(1_000));
        let (kept, heavy) = ("kept".to_owned(), "heavy".to_owned());
        assert!(cache.insert(kept.clone(), Block(100)));
        // The value alone fits the budget, but its key and the cache's own
        // allocation for the entry don't.
        assert!(!cache.insert(heavy.clone(), Block(1_000)));
        assert!(cache.peek(&kept).is_some());
        assert!(cache.peek(&heavy).is_none());

        let stats = cache.stats();
        let rejected_bytes = charged_bytes(&heavy, &Block(1_000));
        assert_eq!(
            (stats.inserts, stats.evictions, stats.rejections),
            (1, 0, 1)
        );
        assert_eq!(stats.rejected_decoded_bytes, rejected_bytes);
        assert_eq!(
            stats.cached_decoded_bytes,
            charged_bytes(&kept, &Block(100))
        );
    }
}
