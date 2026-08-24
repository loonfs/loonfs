//! Grep-private cache for immutable manifests and decoded segment blocks.

use crate::codec::IndexRow;
use crate::root::GrepManifestState;
use loonfs::metrics::{CounterHandle, MetricsRecorder, RESULT_HIT, RESULT_MISS};
use loonfs::{DecodedBlock, DecodedBlockCache, DecodedBlockCacheObserver};
use loonfs_api::wire::sst_blocks::{
    DecodedDataBlock, SegmentFilter, SegmentIndexEntry, DEFAULT_TARGET_BLOCK_BYTES,
};
use std::sync::Arc;

/// Default decoded-byte budget for cached grep manifests and segment blocks.
pub const DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES: usize = 4_096 * DEFAULT_TARGET_BLOCK_BYTES;

/// Grep's cache for decoded manifests and segment blocks.
pub type GrepBlockCache = DecodedBlockCache<GrepBlockCacheKey, DecodedGrepBlock>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GrepBlockKind {
    Manifest,
    Filter,
    Index,
    Data,
}

/// Identifies a cached grep block.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrepBlockCacheKey {
    /// Immutable cache identity. Manifest entries use `payload_checksum`;
    /// segment blocks use `object_checksum`.
    pub(crate) identity: String,
    pub(crate) block_kind: GrepBlockKind,
    pub(crate) block_offset: u64,
}

/// A decoded grep block.
#[derive(Debug, Clone)]
pub enum DecodedGrepBlock {
    Manifest {
        state: Arc<GrepManifestState>,
        decoded_bytes: usize,
    },
    Filter {
        filter: Arc<SegmentFilter>,
        decoded_bytes: usize,
    },
    Index {
        entries: Arc<Vec<SegmentIndexEntry>>,
        decoded_bytes: usize,
    },
    Data {
        block: Arc<DecodedDataBlock<IndexRow>>,
        decoded_bytes: usize,
    },
}

impl DecodedBlock for DecodedGrepBlock {
    fn decoded_bytes(&self) -> usize {
        match self {
            Self::Manifest { decoded_bytes, .. }
            | Self::Filter { decoded_bytes, .. }
            | Self::Index { decoded_bytes, .. }
            | Self::Data { decoded_bytes, .. } => *decoded_bytes,
        }
    }
}

struct GrepBlockCacheMetrics {
    hits: Arc<dyn CounterHandle>,
    misses: Arc<dyn CounterHandle>,
    inserts: Arc<dyn CounterHandle>,
    evictions: Arc<dyn CounterHandle>,
}

impl GrepBlockCacheMetrics {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let get = |result| {
            recorder.register_counter(
                "loonfs.grep_block_cache.gets",
                "Decoded grep block cache lookups, by outcome",
                &[("result", result)],
            )
        };
        Self {
            hits: get(RESULT_HIT),
            misses: get(RESULT_MISS),
            inserts: recorder.register_counter(
                "loonfs.grep_block_cache.inserts",
                "Blocks inserted into the decoded grep block cache",
                &[],
            ),
            evictions: recorder.register_counter(
                "loonfs.grep_block_cache.evictions",
                "Blocks evicted from the decoded grep block cache",
                &[],
            ),
        }
    }
}

/// Creates a grep block cache that reports metrics to `recorder`.
pub fn new_grep_block_cache(
    max_decoded_bytes: usize,
    recorder: &dyn MetricsRecorder,
) -> GrepBlockCache {
    GrepBlockCache::with_observer(
        max_decoded_bytes,
        Some(Arc::new(GrepBlockCacheMetrics::register(recorder))),
    )
}

impl DecodedBlockCacheObserver for GrepBlockCacheMetrics {
    fn hit(&self) {
        self.hits.increment(1);
    }

    fn miss(&self) {
        self.misses.increment(1);
    }

    fn insert(&self) {
        self.inserts.increment(1);
    }

    fn evict(&self) {
        self.evictions.increment(1);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // The metric helper below expects counters and panics if a test requests
    // a metric of another type.

    use super::{
        DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockCacheMetrics, GrepBlockKind,
    };
    use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
    use loonfs_api::wire::sst_blocks::DecodedDataBlock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn block(decoded_bytes: usize) -> DecodedGrepBlock {
        DecodedGrepBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    fn key(checksum: &str) -> GrepBlockCacheKey {
        GrepBlockCacheKey {
            identity: checksum.to_owned(),
            block_kind: GrepBlockKind::Data,
            block_offset: 0,
        }
    }

    fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
        let entry = snapshot
            .by_name(name)
            .find(|entry| entry.labels == labels)
            .expect("grep cache counter should be registered");
        match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("expected a counter, found {other:?}"),
        }
    }

    #[test]
    fn cache_activity_reaches_the_metrics_recorder() {
        let recorder = DefaultMetricsRecorder::new();
        let cache = GrepBlockCache::with_observer(
            1_000,
            Some(Arc::new(GrepBlockCacheMetrics::register(&recorder))),
        );
        cache.insert(key("a"), block(600));
        cache.insert(key("b"), block(600));
        assert!(cache.get(&key("a")).is_none());
        assert!(cache.get(&key("b")).is_some());

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.grep_block_cache.gets",
                &[("result", "hit")]
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.grep_block_cache.gets",
                &[("result", "miss")]
            ),
            1
        );
        assert_eq!(
            counter(&snapshot, "loonfs.grep_block_cache.inserts", &[]),
            2
        );
        assert_eq!(
            counter(&snapshot, "loonfs.grep_block_cache.evictions", &[]),
            1
        );
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
}
