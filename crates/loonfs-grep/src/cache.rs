//! Grep-private cache for immutable manifests and decoded segment blocks.

use crate::codec::IndexRow;
use crate::root::GrepManifestState;
use loonfs::Recency;
use loonfs_api::wire::sst_blocks::{
    DecodedDataBlock, SegmentFilter, SegmentIndexEntry, DEFAULT_TARGET_BLOCK_BYTES,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// Default decoded-byte budget for cached grep manifests and segment blocks.
///
/// This preserves the former 4,096-entry cache's intended capacity at the
/// usual 64 KiB decoded data-block target while preventing unusually large
/// decoded blocks from making the cache effectively unbounded. Zero disables
/// the cache.
pub const DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES: usize = 4_096 * DEFAULT_TARGET_BLOCK_BYTES;

/// Cumulative activity counters for one grep block cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrepBlockCacheStats {
    /// Successful cache lookups.
    pub hits: usize,
    /// Cache lookups that required a load.
    pub misses: usize,
    /// Blocks inserted after successful loads.
    pub inserts: usize,
    /// Blocks removed to enforce the decoded-byte budget.
    pub evictions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GrepBlockKind {
    Manifest,
    Filter,
    Index,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct GrepBlockCacheKey {
    pub(crate) payload_checksum: String,
    pub(crate) block_kind: GrepBlockKind,
    pub(crate) block_offset: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedGrepBlock {
    Manifest {
        state: Arc<GrepManifestState>,
        decoded_byte_len: usize,
    },
    Filter {
        filter: Arc<SegmentFilter>,
        decoded_byte_len: usize,
    },
    Index {
        entries: Arc<Vec<SegmentIndexEntry>>,
        decoded_byte_len: usize,
    },
    Data {
        block: Arc<DecodedDataBlock<IndexRow>>,
        decoded_byte_len: usize,
    },
}

impl DecodedGrepBlock {
    fn decoded_byte_len(&self) -> usize {
        match self {
            Self::Manifest {
                decoded_byte_len, ..
            }
            | Self::Filter {
                decoded_byte_len, ..
            }
            | Self::Index {
                decoded_byte_len, ..
            }
            | Self::Data {
                decoded_byte_len, ..
            } => *decoded_byte_len,
        }
    }
}

/// A process-shareable cache of immutable decoded grep blocks.
#[derive(Debug)]
pub struct GrepBlockCache {
    max_decoded_bytes: usize,
    inner: Mutex<GrepBlockCacheInner>,
    stats: GrepBlockCacheStatsInner,
    /// One cell per in-flight block fetch, keyed by immutable block identity,
    /// so concurrent readers share a single object-store read per block.
    in_flight: Mutex<HashMap<GrepBlockCacheKey, Arc<OnceCell<DecodedGrepBlock>>>>,
}

#[derive(Debug, Default)]
struct GrepBlockCacheInner {
    entries: HashMap<GrepBlockCacheKey, CacheSlot>,
    order: Recency<GrepBlockCacheKey>,
    decoded_byte_len: usize,
}

#[derive(Debug)]
struct CacheSlot {
    block: DecodedGrepBlock,
    /// Stamp of this entry's newest queue position; older positions for the
    /// same key are ghosts.
    last_touch: u64,
}

#[derive(Debug, Default)]
struct GrepBlockCacheStatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
}

impl GrepBlockCache {
    /// Creates a cache with the supplied decoded-byte budget.
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self {
            max_decoded_bytes,
            inner: Mutex::new(GrepBlockCacheInner::default()),
            stats: GrepBlockCacheStatsInner::default(),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Resolves one block access through a per-key single-flight cell.
    pub(crate) async fn get_or_load<E, F, Fut>(
        &self,
        cache_key: &GrepBlockCacheKey,
        fetch: F,
    ) -> Result<DecodedGrepBlock, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DecodedGrepBlock, E>>,
    {
        let cell = {
            let mut in_flight = self
                .in_flight
                .lock()
                .expect("grep block cache in-flight lock should not be poisoned");
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
            .expect("grep block cache in-flight lock should not be poisoned");
        if in_flight
            .get(cache_key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            in_flight.remove(cache_key);
        }
        result
    }

    /// Returns cumulative hit, miss, insert, and eviction counters.
    pub fn stats(&self) -> GrepBlockCacheStats {
        GrepBlockCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
        }
    }

    fn get(&self, key: &GrepBlockCacheKey) -> Option<DecodedGrepBlock> {
        if self.max_decoded_bytes == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("grep block cache lock should not be poisoned");
        let Some(block) = inner.entries.get(key).map(|slot| slot.block.clone()) else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        Some(block)
    }

    fn insert(&self, key: GrepBlockCacheKey, block: DecodedGrepBlock) {
        if self.max_decoded_bytes == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("grep block cache lock should not be poisoned");
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
        let GrepBlockCacheInner {
            entries,
            order,
            decoded_byte_len,
        } = &mut *inner;
        while *decoded_byte_len > self.max_decoded_bytes {
            let Some(candidate) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            if let Some(slot) = entries.remove(&candidate) {
                *decoded_byte_len = decoded_byte_len.saturating_sub(slot.block.decoded_byte_len());
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

impl GrepBlockCacheInner {
    fn touch(&mut self, key: &GrepBlockCacheKey) {
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
    entries: &HashMap<GrepBlockCacheKey, CacheSlot>,
    key: &GrepBlockCacheKey,
    stamp: u64,
) -> bool {
    entries
        .get(key)
        .is_some_and(|slot| slot.last_touch == stamp)
}

#[cfg(test)]
mod tests {
    use super::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind};
    use loonfs_api::wire::sst_blocks::DecodedDataBlock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn block(decoded_byte_len: usize) -> DecodedGrepBlock {
        DecodedGrepBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_byte_len,
        }
    }

    fn key(checksum: &str) -> GrepBlockCacheKey {
        GrepBlockCacheKey {
            payload_checksum: checksum.to_owned(),
            block_kind: GrepBlockKind::Data,
            block_offset: 0,
        }
    }

    #[tokio::test]
    async fn concurrent_loads_of_one_key_run_one_underlying_load() {
        let cache = GrepBlockCache::new(1_000);
        let loads = AtomicUsize::new(0);
        let cache_key = key("a");
        let first = cache.get_or_load(&cache_key, || async {
            loads.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok::<_, String>(block(100))
        });
        let second = cache.get_or_load(&cache_key, || async {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(block(100))
        });

        let (first, second) = tokio::join!(first, second);
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().inserts, 1);
    }

    #[test]
    fn decoded_byte_budget_evicts_the_oldest_block() {
        let cache = GrepBlockCache::new(1_000);
        cache.insert(key("a"), block(600));
        cache.insert(key("b"), block(600));

        assert!(cache.get(&key("a")).is_none());
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 1);
    }
}
