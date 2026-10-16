//! Grep-private cache for immutable manifests and decoded segment blocks.

use crate::codec::IndexRow;
use crate::manifest::GrepManifestState;
use loonfs::engine::{
    DecodedBlockCache, DecodedBlockCacheConfig, DecodedSegmentBlock, SegmentCacheKey,
};
use loonfs_types::format::segment_blocks::DEFAULT_TARGET_BLOCK_BYTES;
use std::sync::Arc;

/// Default decoded-byte budget for cached grep manifests and segment blocks.
pub const DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES: usize = 4_096 * DEFAULT_TARGET_BLOCK_BYTES;

/// Grep's cache for decoded manifests and segment blocks.
pub type GrepBlockCache = DecodedBlockCache<SegmentCacheKey, DecodedGrepBlock>;

/// A decoded grep block.
pub type DecodedGrepBlock = DecodedSegmentBlock<IndexRow, Arc<GrepManifestState>>;
pub type GrepBlockCacheKey = SegmentCacheKey;

/// Creates a grep block cache with a budget of `max_decoded_bytes`.
pub fn new_grep_block_cache(max_decoded_bytes: usize) -> GrepBlockCache {
    GrepBlockCache::new(DecodedBlockCacheConfig::new(max_decoded_bytes))
}

#[cfg(test)]
mod tests {

    use super::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey};
    use loonfs::engine::SegmentBlockKind;
    use loonfs_types::format::segment_blocks::DecodedDataBlock;
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

    fn key(identity: &str) -> GrepBlockCacheKey {
        GrepBlockCacheKey {
            identity: identity.to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: 0,
        }
    }

    #[tokio::test]
    async fn concurrent_loads_of_one_key_run_one_underlying_load() {
        let cache = GrepBlockCache::new(loonfs::engine::DecodedBlockCacheConfig::new(1_000));
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
