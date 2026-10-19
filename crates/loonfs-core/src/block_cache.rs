//! Shared cache for decoded immutable objects.

use crate::error::CoreError;
use crate::heap_bytes::HeapBytes;
use crate::manifest::ManifestLoadError;
use crate::recency::Recency;
use crate::shared_check::SharedCheck;
use loonfs_types::format::segment_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::collections::HashMap;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

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
/// The byte fields report what the cache charges its entries. An entry's
/// charge includes its value's weight, its key's heap, and the cache's own
/// allocation for the entry.
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
    /// The bytes that every entry in the cache is charged, in total.
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

/// Returns the bytes the cache allocates for one entry, apart from the
/// key's heap and the value's weight.
///
/// Each entry takes one table bucket for its key and slot, with the bucket's
/// control byte, at eight buckets for every seven entries. It also takes one
/// recency position for its key and stamp. Spare table and queue capacity,
/// and ghost recency positions, stay outside the budget, as allocator
/// rounding does, so a table that doubles doesn't change any entry's charge
/// or evict an entry.
const fn entry_bytes<K, V>() -> usize {
    let bucket_bytes = size_of::<(K, CacheSlot<V>)>() + 1;
    (bucket_bytes * 8).div_ceil(7) + size_of::<(K, u64)>()
}

/// Returns the bytes the cache charges an entry of `block` under `key`.
///
/// The table and the entry's recency position each store a copy of the key,
/// so the key's heap is charged twice.
pub(crate) fn charged_bytes<K: DecodedBlockKey, V: DecodedBlock>(key: &K, block: &V) -> usize {
    block
        .weight()
        .saturating_add(key.heap_bytes().saturating_mul(2))
        .saturating_add(entry_bytes::<K, V>())
}

pub struct DecodedBlockCache<K, V> {
    config: DecodedBlockCacheConfig,
    inner: Mutex<Inner<K, V>>,
    stats: StatsInner,
}

/// The loads and checks of one key in flight.
#[derive(Debug)]
struct InFlight<V> {
    /// The join that the key's concurrent loads and checks share. A check
    /// shares its error with the callers that wait for it, and a load shares
    /// only a value.
    join: Arc<SharedCheck<Result<V, CoreError>>>,
    /// How many times an invalidation or an eviction dropped the key since
    /// the entry was created.
    drops: u64,
}

/// The drops of a key when a load or a check of that key started.
///
/// [`DecodedBlockCache::insert_if`] inserts nothing for a start that an
/// invalidation or an eviction of the key has passed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckStart {
    drops: u64,
}

/// One caller's reference to the in-flight entry of a key.
///
/// The entry goes when its last caller leaves, so the cache keeps an entry
/// only for a key with a caller.
struct InFlightGuard<'a, K: DecodedBlockKey, V: DecodedBlock> {
    cache: &'a DecodedBlockCache<K, V>,
    key: &'a K,
    join: Option<Arc<SharedCheck<Result<V, CoreError>>>>,
}

impl<K: DecodedBlockKey, V: DecodedBlock> InFlightGuard<'_, K, V> {
    fn join(&self) -> &SharedCheck<Result<V, CoreError>> {
        self.join
            .as_deref()
            .expect("an in-flight guard keeps its join until it drops")
    }
}

impl<K: DecodedBlockKey, V: DecodedBlock> Drop for InFlightGuard<'_, K, V> {
    fn drop(&mut self) {
        let mut inner = self.cache.lock();
        // Release the reference under the lock so concurrent drops cannot both leave an entry.
        drop(self.join.take());
        if inner
            .in_flight
            .get(self.key)
            .is_some_and(|entry| Arc::strong_count(&entry.join) == 1)
        {
            inner.in_flight.remove(self.key);
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
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Inner<K, V> {
    entries: HashMap<K, CacheSlot<V>>,
    order: Recency<K>,
    /// The charges of the entries, in total.
    charged_bytes: usize,
    /// The keys with a load or a check in flight. The engine's lock covers
    /// these with the entries, so an insert can compare a key's drops and
    /// insert in one step.
    in_flight: HashMap<K, InFlight<V>>,
}

#[derive(Debug)]
struct CacheSlot<V> {
    block: V,
    /// The bytes that the entry was charged when it was inserted.
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
                in_flight: HashMap::new(),
            }),
            stats: StatsInner::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner<K, V>> {
        self.inner
            .lock()
            .expect("decoded block cache lock should not be poisoned")
    }

    /// Returns the value under `cache_key`, or loads it with `load` and
    /// caches it.
    ///
    /// Concurrent callers of one key share one load, so a caller that
    /// arrives while a load is in flight takes the value of that load and
    /// records no lookup. A load that fails shares nothing, and the next
    /// caller loads again. A load during which an invalidation or an
    /// eviction dropped the key caches nothing, and returns its value to its
    /// callers uncached.
    pub async fn get_or_load<E, F, Fut>(&self, cache_key: &K, load: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
    {
        let guard = self.join(cache_key);
        guard
            .join()
            .load(
                || self.get(cache_key),
                || {
                    let start = self.start(cache_key);
                    async move {
                        let block = load().await?;
                        self.insert_entry(cache_key.clone(), block.clone(), None, Some(start));
                        Ok(block)
                    }
                },
            )
            .await
    }

    /// Returns `current()`, or the outcome of a check of `key` that the
    /// concurrent callers of `key` share, as [`SharedCheck::run`] describes.
    ///
    /// A caller waits for the check in flight, and then returns `current()`
    /// when it's `Some`, or else the outcome of a check that started after
    /// the caller arrived, or else the outcome of `check`, which it runs.
    /// So a wave of concurrent callers costs at most two checks. `check`
    /// receives the start of the check, which it passes to
    /// [`Self::insert_if`] with the value that it inserts.
    pub(crate) async fn check_or_join<F, Fut>(
        &self,
        key: &K,
        current: impl FnOnce() -> Option<V>,
        check: F,
    ) -> Result<V, CoreError>
    where
        F: FnOnce(CheckStart) -> Fut,
        Fut: std::future::Future<Output = Result<V, CoreError>>,
    {
        let guard = self.join(key);
        guard
            .join()
            .run(|| current().map(Ok), || check(self.start(key)))
            .await
    }

    /// Returns a reference to the in-flight entry of `key`, creating the
    /// entry when the key has none.
    fn join<'a>(&'a self, key: &'a K) -> InFlightGuard<'a, K, V> {
        let mut inner = self.lock();
        let entry = inner
            .in_flight
            .entry(key.clone())
            .or_insert_with(|| InFlight {
                join: Arc::default(),
                drops: 0,
            });
        InFlightGuard {
            cache: self,
            key,
            join: Some(Arc::clone(&entry.join)),
        }
    }

    /// Returns the start of a load or a check of `key`, whose caller keeps
    /// the key's in-flight entry.
    fn start(&self, key: &K) -> CheckStart {
        let drops = self
            .lock()
            .in_flight
            .get(key)
            .expect("a load or a check keeps the in-flight entry of its key")
            .drops;
        CheckStart { drops }
    }

    /// Counts the entries that the cache keeps.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Counts the keys with a load or a check in flight.
    #[cfg(test)]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.lock().in_flight.len()
    }

    pub fn stats(&self) -> DecodedBlockCacheStats {
        let inner = self.lock();
        DecodedBlockCacheStats {
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            inserts: self.stats.inserts.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            evicted_decoded_bytes: self.stats.evicted_decoded_bytes.load(Ordering::Relaxed),
            rejections: self.stats.rejections.load(Ordering::Relaxed),
            rejected_decoded_bytes: self.stats.rejected_decoded_bytes.load(Ordering::Relaxed),
            cached_decoded_bytes: inner.charged_bytes,
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
        let mut inner = self.lock();
        let Some(block) = inner.entries.get(key).map(|slot| slot.block.clone()) else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(block)
    }

    /// Returns a cached value without touching its recency or counting a
    /// hit or a miss.
    pub fn peek(&self, key: &K) -> Option<V> {
        self.lock().entries.get(key).map(|slot| slot.block.clone())
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
        self.insert_entry(key, block, None, None)
    }

    /// Inserts like [`Self::insert`], and records the insert in `kind` too
    /// if the cache kept the entry.
    pub(crate) fn insert_kind(&self, key: K, block: V, kind: &KindCounters) -> bool {
        self.insert_entry(key, block, Some(kind), None)
    }

    /// Inserts like [`Self::insert`], unless `start` is the start of a load
    /// that an invalidation or an eviction of `key` has passed.
    fn insert_entry(
        &self,
        key: K,
        block: V,
        kind: Option<&KindCounters>,
        start: Option<CheckStart>,
    ) -> bool {
        if self.disabled() {
            return false;
        }
        let entry_charge = match self.entry_charge(&key, &block) {
            Ok(entry_charge) => entry_charge,
            Err(charged_bytes) => {
                self.record_rejection(charged_bytes);
                return false;
            }
        };
        let mut inner = self.lock();
        if start.is_some_and(|start| inner.dropped_since(&key, start)) {
            return false;
        }
        let kept = self.put(&mut inner, key, block, entry_charge);
        if let Some(kind) = kind.filter(|_| kept) {
            kind.inserts.fetch_add(1, Ordering::Relaxed);
        }
        kept
    }

    /// Inserts `value` under `key` if the key has no entry or `newer`
    /// returns true for the key's entry, and returns the key's entry when
    /// `newer` returns false.
    ///
    /// The cache's lock covers the comparison and the insert, so values that
    /// replace each other in an order (such as a namespace's heads) never
    /// replace a newer value. (`newer` runs under that lock, so it compares
    /// the two values and does nothing else.)
    ///
    /// `start` is the start of the load or the check that produced `value`,
    /// if one did. If an invalidation or an eviction dropped the key since
    /// then, `value` might be older than the value that was dropped, so
    /// nothing is inserted.
    ///
    /// A disabled cache keeps nothing. If `newer` accepts `value` but
    /// `value` is heavier than the whole budget, the key's older entry goes
    /// too, and the key is dropped for the loads and checks in flight, as
    /// [`Self::invalidate`] drops it. So no read takes the older value, and
    /// no load that started before this insert installs an older value
    /// afterwards.
    pub(crate) fn insert_if(
        &self,
        key: K,
        value: V,
        start: Option<CheckStart>,
        newer: impl FnOnce(&V) -> bool,
    ) -> Option<V> {
        if self.disabled() {
            return None;
        }
        let entry_charge = self.entry_charge(&key, &value);
        let mut inner = self.lock();
        if start.is_some_and(|start| inner.dropped_since(&key, start)) {
            return None;
        }
        if let Some(cached) = inner.entries.get(&key).map(|slot| &slot.block) {
            if !newer(cached) {
                return Some(cached.clone());
            }
        }
        match entry_charge {
            Ok(entry_charge) => {
                self.put(&mut inner, key, value, entry_charge);
            }
            Err(charged_bytes) => {
                inner.drop_key(&key);
                self.record_rejection(charged_bytes);
            }
        }
        None
    }

    /// Returns the charge of an entry of `block` under `key`, or the bytes
    /// that the entry would have been charged if that's more than the whole
    /// budget.
    fn entry_charge(&self, key: &K, block: &V) -> Result<usize, usize> {
        let entry_charge = charged_bytes(key, block);
        if entry_charge > self.config.max_decoded_bytes {
            return Err(entry_charge);
        }
        Ok(entry_charge)
    }

    /// Puts `block` under `key` in place of the key's entry, evicts the
    /// least recently used entries until the cache is within its budget
    /// again, and returns whether the cache kept the new entry.
    fn put(&self, inner: &mut Inner<K, V>, key: K, block: V, entry_charge: usize) -> bool {
        let stored_key = key.clone();
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
        inner.charged_bytes = inner.charged_bytes.saturating_add(entry_charge);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
        self.evict_over_budget(inner);
        inner.entries.contains_key(&key)
    }

    /// Drops one entry without counting an eviction.
    ///
    /// A load or a check of the key in flight inserts nothing afterwards,
    /// even when the cache had no entry for the key, because it might have
    /// started before the change that the invalidation is for.
    pub fn invalidate(&self, key: &K) {
        self.lock().drop_key(key);
    }

    fn disabled(&self) -> bool {
        self.config.max_decoded_bytes == 0
    }

    fn evict_over_budget(&self, inner: &mut Inner<K, V>) {
        while inner.charged_bytes > self.config.max_decoded_bytes {
            let Inner { entries, order, .. } = &mut *inner;
            let Some(candidate) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            let slot = entries.remove(&candidate).expect("live entry");
            inner.charged_bytes = inner.charged_bytes.saturating_sub(slot.charged_bytes);
            inner.count_drop(&candidate);
            self.record_eviction(slot.charged_bytes);
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
    /// Returns whether an invalidation or an eviction dropped `key` since
    /// `start`, or whether the key's in-flight entry is gone.
    fn dropped_since(&self, key: &K, start: CheckStart) -> bool {
        self.in_flight
            .get(key)
            .is_none_or(|entry| entry.drops != start.drops)
    }

    /// Records that `key` was dropped, for the loads and checks of the key
    /// in flight.
    fn count_drop(&mut self, key: &K) {
        if let Some(entry) = self.in_flight.get_mut(key) {
            entry.drops += 1;
        }
    }

    /// Removes the key's entry, if it has one, and records the drop for the
    /// loads and checks of the key in flight.
    fn drop_key(&mut self, key: &K) {
        self.count_drop(key);
        if let Some(slot) = self.entries.remove(key) {
            self.charged_bytes = self.charged_bytes.saturating_sub(slot.charged_bytes);
        }
    }

    fn touch(&mut self, key: &K) {
        let stamp = self.order.touch(key.clone());
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Checks that a load during which an invalidation or an eviction drops
    /// its key returns its value to its callers uncached, and that the next
    /// call loads the key afresh and caches it.
    #[tokio::test]
    async fn a_load_whose_key_is_dropped_while_it_runs_caches_nothing() {
        let (key, other) = ("a".to_owned(), "b".to_owned());
        let invalidate = |cache: &DecodedBlockCache<String, Block>| cache.invalidate(&key);
        let evict = |cache: &DecodedBlockCache<String, Block>| {
            assert!(cache.insert(key.clone(), Block(10)));
            assert!(cache.insert(other.clone(), Block(10)));
            assert!(
                cache.peek(&key).is_none(),
                "the second insert evicts the key"
            );
        };
        for drop_key in [
            &invalidate as &dyn Fn(&DecodedBlockCache<String, Block>),
            &evict,
        ] {
            let budget = charged_bytes(&key, &Block(10));
            let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(budget));
            let loads = AtomicUsize::new(0);
            let (release, released) = tokio::sync::watch::channel(false);
            let load = || {
                cache.get_or_load(&key, || async {
                    loads.fetch_add(1, Ordering::SeqCst);
                    released
                        .clone()
                        .wait_for(|released| *released)
                        .await
                        .expect("the test keeps the sender");
                    Ok::<_, String>(Block(7))
                })
            };
            let mut first = pin!(load());
            assert!(poll!(first.as_mut()).is_pending());
            let mut waiter = pin!(load());
            assert!(poll!(waiter.as_mut()).is_pending());
            drop_key(&cache);
            release.send(true).expect("the load waits for the release");
            let (first, waiter) = futures::join!(first, waiter);
            assert_eq!((first.expect("first").0, waiter.expect("waiter").0), (7, 7));
            assert_eq!(
                loads.load(Ordering::SeqCst),
                1,
                "the waiter takes the value"
            );
            assert!(cache.peek(&key).is_none(), "the load caches nothing");
            load().await.expect("the next load");
            assert_eq!(loads.load(Ordering::SeqCst), 2);
            assert_eq!(cache.peek(&key).map(|block| block.0), Some(7));
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
    /// keeps that many and evicts the rest.
    #[test]
    fn entries_with_weightless_values_are_charged_and_evicted() {
        let keys: Vec<String> = (0..32).map(|index| format!("key {index:02}")).collect();
        let charge = charged_bytes(&keys[0], &Block(0));
        let budget = 1_024;
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(budget));
        let key_count = keys.len();
        for key in keys {
            assert!(cache.insert(key, Block(0)));
        }
        let live_entries = cache.len();
        let stats = cache.stats();
        assert_eq!(live_entries, budget / charge);
        assert_eq!(stats.evictions, key_count - live_entries);
        assert_eq!(stats.cached_decoded_bytes, live_entries * charge);
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

    /// Checks that a cache of small entries, whose table grows through
    /// several resizes, evicts nothing until its entries' charges reach the
    /// budget, and then evicts one entry for each insert.
    #[test]
    fn small_entries_are_evicted_one_per_insert_from_the_budget_on() {
        const ENTRIES: usize = 1_000;
        let keys: Vec<_> = (0..4 * ENTRIES)
            .map(|index| MetadataSegmentId::parse(format!("seg_{index:032x}")).expect("segment id"))
            .collect();
        // Every key is 36 characters long, so every entry is charged the same.
        let budget = ENTRIES * charged_bytes(&keys[0], &Block(32));
        let cache = DecodedBlockCache::new(DecodedBlockCacheConfig::new(budget));
        let (mut capacity, mut resizes) = (0, 0);
        for (inserted, key) in (1..).zip(keys) {
            let evictions = cache.stats().evictions;
            assert!(cache.insert(key, Block(32)));
            let evicted = cache.stats().evictions - evictions;
            assert!(
                evicted <= usize::from(inserted > ENTRIES),
                "insert {inserted} evicted {evicted} entries"
            );
            let grown = cache.inner.lock().expect("cache lock").entries.capacity();
            if grown > capacity {
                capacity = grown;
                resizes += 1;
            }
        }
        assert!(resizes >= 8, "the table grew {resizes} times");
        assert_eq!(cache.stats().evictions, 3 * ENTRIES);
        assert_eq!(cache.len(), ENTRIES);
    }
}
