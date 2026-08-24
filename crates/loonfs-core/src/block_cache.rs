//! [`DecodedBlockCache`]: the byte-budgeted, single-flight cache of decoded
//! immutable objects that metadata reads and derived projections share.

use crate::recency::Recency;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// A decoded value the cache charges against its byte budget.
pub trait DecodedBlock: Clone {
    /// Bytes this value occupies in its decoded form.
    fn decoded_bytes(&self) -> usize;
}

/// Receives decoded-block cache events for metrics.
pub trait DecodedBlockCacheObserver: Send + Sync + 'static {
    fn hit(&self);
    fn miss(&self);
    fn insert(&self);
    fn evict(&self);
}

/// Cumulative activity counters for one decoded-block cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodedBlockCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
}

/// A process-shareable cache of decoded immutable objects, keyed by an
/// identity that can never go stale, evicted in recency order once the
/// decoded bytes it holds exceed its budget. A budget of zero disables it.
pub struct DecodedBlockCache<K, V> {
    max_decoded_bytes: usize,
    inner: Mutex<Inner<K, V>>,
    stats: StatsInner,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    /// One cell per in-flight fetch, keyed by the entry's cache key, so
    /// concurrent readers share a single object-store read per entry.
    in_flight: Mutex<HashMap<K, Arc<OnceCell<V>>>>,
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for DecodedBlockCache<K, V> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodedBlockCache")
            .field("max_decoded_bytes", &self.max_decoded_bytes)
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
    decoded_bytes: usize,
}

#[derive(Debug)]
struct CacheSlot<V> {
    block: V,
    /// Recency stamp assigned on the most recent access. Recency records with
    /// an older stamp for this key are ignored.
    last_touch: u64,
}

#[derive(Debug, Default)]
struct StatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
}

impl<K: Clone + Eq + Hash, V: DecodedBlock> DecodedBlockCache<K, V> {
    /// Creates a cache with the supplied decoded-byte budget.
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self::with_observer(max_decoded_bytes, None)
    }

    /// Creates a cache that reports activity to the optional `observer`.
    pub fn with_observer(
        max_decoded_bytes: usize,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            max_decoded_bytes,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: Recency::default(),
                decoded_bytes: 0,
            }),
            stats: StatsInner::default(),
            observer,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Resolves one access through a per-key single-flight cell.
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

    /// Returns cumulative hit, miss, insert, and eviction counters.
    pub fn stats(&self) -> DecodedBlockCacheStats {
        DecodedBlockCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if self.max_decoded_bytes == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
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

    pub fn insert(&self, key: K, block: V) {
        if self.max_decoded_bytes == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("decoded block cache lock should not be poisoned");
        let decoded_bytes = block.decoded_bytes();
        if let Some(previous) = inner.entries.insert(
            key.clone(),
            CacheSlot {
                block,
                last_touch: 0,
            },
        ) {
            inner.decoded_bytes = inner
                .decoded_bytes
                .saturating_sub(previous.block.decoded_bytes());
        }
        inner.decoded_bytes = inner.decoded_bytes.saturating_add(decoded_bytes);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.observer {
            observer.insert();
        }
        let Inner {
            entries,
            order,
            decoded_bytes,
        } = &mut *inner;
        while *decoded_bytes > self.max_decoded_bytes {
            let Some(candidate) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            if let Some(slot) = entries.remove(&candidate) {
                *decoded_bytes = decoded_bytes.saturating_sub(slot.block.decoded_bytes());
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
                if let Some(observer) = &self.observer {
                    observer.evict();
                }
            }
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

/// Returns `true` when `stamp` matches the key's most recent access.
/// Replacing, evicting, or accessing an entry can leave older recency records.
fn slot_is_live<K: Eq + Hash, V>(entries: &HashMap<K, CacheSlot<V>>, key: &K, stamp: u64) -> bool {
    entries
        .get(key)
        .is_some_and(|slot| slot.last_touch == stamp)
}

#[cfg(test)]
mod tests {
    use super::{DecodedBlock, DecodedBlockCache};

    #[derive(Clone)]
    struct Block(usize);

    impl DecodedBlock for Block {
        fn decoded_bytes(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn recency_queue_stays_bounded_under_repeated_hits() {
        let cache = DecodedBlockCache::new(10_000);
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
