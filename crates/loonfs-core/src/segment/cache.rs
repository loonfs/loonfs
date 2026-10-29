//! The shared cache of decoded segment blocks and validated manifests, and the
//! per-object-store views that reads go through.
//!
//! One or more runtimes may read through one cache, each through its own
//! view. A view adds the [`ObjectStoreScope`] of the runtime's object store to
//! every key, so runtimes over the same object store share decoded blocks and
//! manifests, and runtimes over different object stores never read each
//! other's blocks. Eviction runs across every scope.

use super::runs::MetadataRunManifest;
use super::segment_block_cache::{MemorySegmentBlocks, SegmentBlockCache};
use crate::block_cache::ObjectStoreScope;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey,
};
use crate::heap_bytes::HeapBytes;
use crate::limits::READAHEAD_WINDOW_BYTES;
use crate::manifest::ManifestLoadError;
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_types::format::manifest::{MetadataRow, NamespaceManifestEnvelope};
use loonfs_types::format::segment_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

impl SegmentCacheKey {
    /// Returns the key of a decoded manifest, which is cached under its
    /// object key.
    pub(crate) fn manifest(object_key: &str) -> Self {
        Self {
            identity: object_key.to_owned(),
            block_kind: SegmentBlockKind::Manifest,
            block_offset: 0,
        }
    }
}

impl DecodedBlockKey for SegmentCacheKey {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(&self.identity)
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

/// Counters and occupancy of the shared segment blocks.
///
/// The first fields are the counters that every cache reports (see
/// [`DecodedBlockCacheStats`])
/// for the data, index, and filter blocks, and the rest count what the
/// segment filters did. [`SharedSegmentBlocks::manifest_stats`] returns the
/// counters of the manifests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataSegmentCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    pub cached_decoded_bytes: usize,
    /// Segments a scan skipped because their bloom filter ruled the lookup
    /// key out before any index or data fetch.
    pub filter_skips: usize,
    /// Segments whose filter admitted a lookup that then matched no rows.
    /// Approximate: a lookup narrower than the filter key (an exact unbind,
    /// a single revision) can count a true admission here.
    pub filter_false_positives: usize,
}

/// The key of a metadata segment block in the segment block cache.
pub type MetadataSegmentCacheKey = SegmentCacheKey;
/// A decoded metadata segment block or manifest, as the shared segment
/// blocks keep it.
pub type DecodedMetadataSegmentBlock = DecodedSegmentBlock<
    MetadataRow,
    (
        Arc<NamespaceManifestEnvelope>,
        Arc<Vec<MetadataRunManifest>>,
        u64,
    ),
>;

/// Most bytes that the block memo of a read without a shared read working
/// memory keeps, in a pool of the read's own.
const MAX_UNSHARED_READ_WORKING_BYTES: usize = 64 * 1024 * 1024;

/// Returns a read working memory of [`MAX_UNSHARED_READ_WORKING_BYTES`] that
/// no other read shares.
pub(crate) fn unshared_read_working_memory() -> Arc<ReadWorkingMemory> {
    Arc::new(ReadWorkingMemory::new(
        MAX_UNSHARED_READ_WORKING_BYTES,
        None,
    ))
}

pub(crate) fn read_working_memory(
    segment_cache: Option<&MetadataSegmentCache>,
) -> Arc<ReadWorkingMemory> {
    segment_cache.map_or_else(unshared_read_working_memory, |cache| {
        Arc::clone(&cache.read_working_memory)
    })
}

/// Returns the stored bytes that a read through `segment_cache` fetches ahead
/// when it misses a data block, or the default for a read without a cache.
pub(crate) fn read_ahead_window_bytes(segment_cache: Option<&MetadataSegmentCache>) -> usize {
    segment_cache.map_or(READAHEAD_WINDOW_BYTES, |cache| {
        cache.read_ahead_window_bytes
    })
}

/// A cache of decoded segment entries (blocks or manifests) under one byte
/// budget.
pub(super) type SegmentEntries =
    DecodedBlockCache<(ObjectStoreScope, MetadataSegmentCacheKey), DecodedMetadataSegmentBlock>;

/// Decoded segment blocks and manifests for every object store, with the
/// blocks and the manifests under separate byte budgets and recency orders,
/// so a burst of blocks can't evict a manifest.
pub struct SharedSegmentBlocks {
    /// The data, index, and filter blocks.
    blocks: SegmentEntries,
    manifests: SegmentEntries,
    filter_stats: MetadataSegmentFilterStatsInner,
}

impl std::fmt::Debug for SharedSegmentBlocks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedSegmentBlocks")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct MetadataSegmentFilterStatsInner {
    filter_skips: AtomicUsize,
    filter_false_positives: AtomicUsize,
}

impl SharedSegmentBlocks {
    /// Returns a cache that keeps up to `max_segment_bytes` of data, index,
    /// and filter blocks and up to `max_manifest_bytes` of manifests.
    pub fn new(max_segment_bytes: usize, max_manifest_bytes: usize) -> Self {
        Self {
            blocks: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_segment_bytes)),
            manifests: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_manifest_bytes)),
            filter_stats: MetadataSegmentFilterStatsInner::default(),
        }
    }

    /// Returns the cache that keeps entries of `kind`.
    pub(super) fn entries(&self, kind: SegmentBlockKind) -> &SegmentEntries {
        match kind {
            SegmentBlockKind::Data | SegmentBlockKind::Index | SegmentBlockKind::Filter => {
                &self.blocks
            }
            SegmentBlockKind::Manifest => &self.manifests,
        }
    }

    /// Returns the counters and occupancy of the manifests.
    pub fn manifest_stats(&self) -> DecodedBlockCacheStats {
        self.manifests.stats()
    }

    /// Returns the counters and occupancy of the data, index, and filter
    /// blocks, and the counters of the segment filters.
    pub fn stats(&self) -> MetadataSegmentCacheStats {
        let blocks = self.blocks.stats();
        MetadataSegmentCacheStats {
            hits: blocks.hits,
            misses: blocks.misses,
            inserts: blocks.inserts,
            evictions: blocks.evictions,
            evicted_decoded_bytes: blocks.evicted_decoded_bytes,
            rejections: blocks.rejections,
            rejected_decoded_bytes: blocks.rejected_decoded_bytes,
            cached_decoded_bytes: blocks.cached_decoded_bytes,
            filter_skips: self.filter_stats.filter_skips.load(Ordering::Relaxed),
            filter_false_positives: self
                .filter_stats
                .filter_false_positives
                .load(Ordering::Relaxed),
        }
    }
}

/// One runtime's view of shared segment blocks, under the scope of the
/// runtime's object store, with the runtime's shared read working memory.
pub struct MetadataSegmentCache {
    /// The block cache that every read of a data, index, or filter block
    /// goes through.
    blocks: Arc<dyn SegmentBlockCache>,
    /// The shared segment blocks, which keep this view's manifests under
    /// `scope`.
    shared: Arc<SharedSegmentBlocks>,
    scope: ObjectStoreScope,
    read_working_memory: Arc<ReadWorkingMemory>,
    /// The stored bytes that a read through this view fetches ahead when it
    /// misses a data block (see [`READAHEAD_WINDOW_BYTES`]).
    read_ahead_window_bytes: usize,
}

impl std::fmt::Debug for MetadataSegmentCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataSegmentCache")
            .field("scope", &self.scope)
            .field("read_working_memory", &self.read_working_memory)
            .field("read_ahead_window_bytes", &self.read_ahead_window_bytes)
            .finish_non_exhaustive()
    }
}

impl MetadataSegmentCache {
    /// Returns a view whose block cache is the memory implementation over
    /// `shared`.
    pub fn new(
        shared: Arc<SharedSegmentBlocks>,
        scope: ObjectStoreScope,
        read_working_memory: Arc<ReadWorkingMemory>,
        read_ahead_window_bytes: usize,
    ) -> Self {
        Self {
            blocks: Arc::new(MemorySegmentBlocks::new(Arc::clone(&shared), scope)),
            shared,
            scope,
            read_working_memory,
            read_ahead_window_bytes,
        }
    }

    /// Returns a view whose data, index, and filter blocks go through
    /// `blocks`, and whose manifests are kept in `shared` under `scope`.
    ///
    /// The `loonfs` crate passes its implementation with an SSD tier here.
    pub fn with_blocks(
        blocks: Arc<dyn SegmentBlockCache>,
        shared: Arc<SharedSegmentBlocks>,
        scope: ObjectStoreScope,
        read_working_memory: Arc<ReadWorkingMemory>,
        read_ahead_window_bytes: usize,
    ) -> Self {
        Self {
            blocks,
            shared,
            scope,
            read_working_memory,
            read_ahead_window_bytes,
        }
    }

    /// Returns a view over blocks that no other view reads, with a budget of
    /// `max_decoded_bytes` for blocks, a second budget of the same size for
    /// manifests, and the default block memo budget and read-ahead window.
    pub fn unshared(max_decoded_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedSegmentBlocks::new(
                max_decoded_bytes,
                max_decoded_bytes,
            )),
            ObjectStoreScope::new(0),
            unshared_read_working_memory(),
            READAHEAD_WINDOW_BYTES,
        )
    }

    /// Returns the block cache that every read of a data, index, or filter
    /// block goes through.
    pub(super) fn blocks(&self) -> &dyn SegmentBlockCache {
        self.blocks.as_ref()
    }

    /// Resolves one access to the shared segment blocks through a
    /// single-flight cell. Manifests are loaded this way, and blocks are
    /// loaded through [`Self::blocks`].
    pub(crate) async fn get_or_load<E, F, Fut>(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        load: F,
    ) -> Result<DecodedMetadataSegmentBlock, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DecodedMetadataSegmentBlock, E>>,
    {
        self.shared
            .entries(cache_key.block_kind)
            .get_or_load(&(self.scope, cache_key.clone()), load)
            .await
    }

    /// Returns the counters of the blocks and the segment filters in the
    /// whole shared cache, across every scope.
    pub fn stats(&self) -> MetadataSegmentCacheStats {
        self.shared.stats()
    }

    /// Returns the counters of the manifests in the whole shared cache,
    /// across every scope.
    pub fn manifest_stats(&self) -> DecodedBlockCacheStats {
        self.shared.manifest_stats()
    }

    pub(super) fn record_filter_skip(&self) {
        self.shared
            .filter_stats
            .filter_skips
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_filter_false_positive(&self) {
        self.shared
            .filter_stats
            .filter_false_positives
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn get(&self, key: &MetadataSegmentCacheKey) -> Option<DecodedMetadataSegmentBlock> {
        self.shared
            .entries(key.block_kind)
            .get(&(self.scope, key.clone()))
    }

    /// Inserts an entry into the shared segment blocks. Manifests are
    /// inserted this way, and blocks are inserted through [`Self::blocks`].
    pub(crate) fn insert(&self, key: MetadataSegmentCacheKey, block: DecodedMetadataSegmentBlock) {
        self.shared
            .entries(key.block_kind)
            .insert((self.scope, key), block);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache::{charged_bytes, DecodedBlock};

    use super::SegmentBlockKind;

    use loonfs_types::format::segment_blocks::DecodedDataBlock;

    use std::sync::Arc;

    fn block(decoded_bytes: usize) -> DecodedMetadataSegmentBlock {
        DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    fn key(identity: &str) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: identity.to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: 0,
        }
    }

    /// Checks that views over two object stores read their own blocks under
    /// the same key, and that two views over one object store read the same
    /// blocks.
    #[tokio::test]
    async fn views_read_only_the_blocks_of_their_own_object_store() {
        let shared = Arc::new(SharedSegmentBlocks::new(usize::MAX, usize::MAX));
        let view = |scope| {
            MetadataSegmentCache::new(
                Arc::clone(&shared),
                ObjectStoreScope::new(scope),
                super::unshared_read_working_memory(),
                crate::limits::READAHEAD_WINDOW_BYTES,
            )
        };
        let (first, second, first_again) = (view(1), view(2), view(1));
        first.insert(key("a"), block(100));
        second.insert(key("a"), block(200));
        for (view, decoded_bytes) in [(&first, 100), (&second, 200), (&first_again, 100)] {
            let hit = view
                .get(&key("a"))
                .expect("each view reads the block of its own object store");
            assert_eq!(hit.weight(), decoded_bytes);
        }

        // Each load waits for the other to start. One in-flight cell shared
        // across scopes would run a single load, and this would time out.
        let (started, shared_key) = (tokio::sync::Barrier::new(2), key("b"));
        let first_load = first.get_or_load(&shared_key, || async {
            started.wait().await;
            Ok::<_, String>(block(300))
        });
        let second_load = second.get_or_load(&shared_key, || async {
            started.wait().await;
            Ok::<_, String>(block(400))
        });
        let (from_first, from_second) =
            loonfs_test_support::clock::bounded(std::time::Duration::from_secs(10), async {
                tokio::join!(first_load, second_load)
            })
            .await
            .expect("loads in two scopes run side by side");
        assert_eq!(from_first.expect("first load").weight(), 300);
        assert_eq!(from_second.expect("second load").weight(), 400);
        for (view, decoded_bytes) in [(&first, 300), (&second, 400)] {
            let hit = view.get(&key("b")).expect("each view caches its own load");
            assert_eq!(hit.weight(), decoded_bytes);
        }
    }

    #[test]
    fn byte_budget_evicts_the_oldest_block() {
        let budget = charged_bytes(&(ObjectStoreScope::new(0), key("a")), &block(600));
        let cache = MetadataSegmentCache::unshared(budget);
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
        let charge = |identity, decoded_bytes| {
            charged_bytes(
                &(ObjectStoreScope::new(0), key(identity)),
                &block(decoded_bytes),
            )
        };
        let cache = MetadataSegmentCache::unshared(charge("a", 100) + charge("b", 600));
        cache.insert(key("a"), block(600));
        cache.insert(key("a"), block(100));
        // 600 was released on replace: another 600 fits without eviction.
        cache.insert(key("b"), block(600));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 0);
    }

    #[test]
    fn cache_hits_share_the_decoded_row_allocation() {
        let cache = MetadataSegmentCache::unshared(usize::MAX);
        let inserted = block(64);
        let rows = match &inserted {
            DecodedMetadataSegmentBlock::Data { block: rows, .. } => Arc::clone(rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => {
                panic!("fixture builds a data block")
            }
        };
        cache.insert(key("a"), inserted);
        let hit = cache.get(&key("a")).expect("inserted block should hit");
        let shares_allocation = match &hit {
            DecodedMetadataSegmentBlock::Data {
                block: hit_rows, ..
            } => Arc::ptr_eq(hit_rows, &rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => false,
        };
        assert!(
            shares_allocation,
            "a cache hit should share the decoded rows, not clone them"
        );
    }

    #[tokio::test]
    async fn get_or_load_retries_a_failed_load_and_then_stops_loading() {
        let cache = MetadataSegmentCache::unshared(usize::MAX);
        let failed: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Err("transport".to_owned()) })
            .await;
        assert_eq!(
            failed.expect_err("the failed load should return its error"),
            "transport"
        );
        let recovered: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Ok(block(1)) })
            .await;
        assert!(
            recovered.is_ok(),
            "a failed fetch should leave nothing behind for the next caller"
        );

        let cached: Result<_, String> = cache
            .get_or_load(&key("a"), || async {
                Err("a populated key must not re-fetch".to_owned())
            })
            .await;
        assert!(cached.is_ok(), "the cached block should answer the access");
    }
}
