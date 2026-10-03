//! SST block-range selection and per-view block memoization.

use super::block_fetch::load_segment_index;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey};
use super::data_block_load::{
    load_segment_data_block_span, load_segment_data_block_with_readahead,
};
use super::error::ManifestLoadError;
use super::scan::Readahead;
use super::validate::validate_manifest_row_seq_range;
use crate::block_cache::DecodedBlock;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes};
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::sst_blocks::{index_blocks_for_key_range, DecodedDataBlock};
use loonfs_types::ChangeSeq;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The data blocks of one segment that can hold keys in
/// `[lower_bound, upper_bound)`, shared straight from the decoded-block
/// memo and caches. Row access borrows from the blocks rather than building
/// an owned row set, which would clone every row and key of every touched
/// block on every scan.
pub(crate) struct SegmentKeyRangeBlocks {
    blocks: Vec<Arc<DecodedDataBlock>>,
}

impl SegmentKeyRangeBlocks {
    /// Rows whose keys fall in `[lower_bound, upper_bound)`, in row order,
    /// found by binary search over each block's decode-validated key order.
    /// Express a prefix scan as `[prefix, string_prefix_upper_bound(prefix))`.
    pub(super) fn rows_in_key_range<'a>(
        &'a self,
        lower_bound: &'a str,
        upper_bound: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a MetadataRow)> + 'a {
        self.blocks.iter().flat_map(move |block| {
            let start = block
                .row_keys
                .partition_point(|key| key.as_str() < lower_bound);
            let end = upper_bound.map_or(block.row_keys.len(), |upper_bound| {
                block
                    .row_keys
                    .partition_point(|key| key.as_str() < upper_bound)
            });
            let range = start..end.max(start);
            block.row_keys[range.clone()]
                .iter()
                .zip(&block.rows[range])
                .map(|(key, row)| (key.as_str(), row))
        })
    }

    #[cfg(test)]
    pub(super) fn rows(&self) -> impl Iterator<Item = &MetadataRow> {
        self.blocks.iter().flat_map(|block| block.rows.iter())
    }

    #[cfg(test)]
    pub(super) fn row_keys(&self) -> impl Iterator<Item = &String> {
        self.blocks.iter().flat_map(|block| block.row_keys.iter())
    }
}

/// This memo stays separate because it uses FIFO eviction for one operation.
#[derive(Debug)]
pub(super) struct SessionBlockMemo {
    pool: Arc<ReadWorkingMemory>,
    inner: Mutex<SessionBlockMemoInner>,
}

impl Default for SessionBlockMemo {
    fn default() -> Self {
        Self::new(Arc::default())
    }
}

#[derive(Debug, Default)]
struct SessionBlockMemoInner {
    blocks: HashMap<Arc<MetadataSegmentCacheKey>, MemoBlock>,
    insertion_order: VecDeque<Arc<MetadataSegmentCacheKey>>,
    entry_bytes: usize,
    reserved_bytes: usize,
}

#[derive(Debug, Clone)]
enum MemoBlock {
    Decoded(DecodedMetadataSegmentBlock),
    Stored(Arc<[u8]>),
}

impl MemoBlock {
    fn bytes(&self) -> usize {
        match self {
            Self::Decoded(block) => block.weight(),
            Self::Stored(bytes) => arc_bytes::<()>() + bytes.len(),
        }
    }
}

impl SessionBlockMemoInner {
    fn bytes(&self) -> usize {
        self.entry_bytes
            + hash_map_table_bytes(&self.blocks)
            + self.insertion_order.capacity() * std::mem::size_of::<Arc<MetadataSegmentCacheKey>>()
    }
}

fn key_bytes(key: &MetadataSegmentCacheKey) -> usize {
    arc_bytes::<MetadataSegmentCacheKey>() + key.identity.capacity()
}

impl SessionBlockMemo {
    pub(super) fn new(pool: Arc<ReadWorkingMemory>) -> Self {
        Self {
            pool,
            inner: Mutex::default(),
        }
    }

    pub(super) fn get(
        &self,
        cache_key: &MetadataSegmentCacheKey,
    ) -> Option<DecodedMetadataSegmentBlock> {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .blocks
            .get(cache_key)
            .and_then(|block| match block {
                MemoBlock::Decoded(block) => Some(block.clone()),
                MemoBlock::Stored(_) => None,
            })
    }

    pub(super) fn stored(&self, cache_key: &MetadataSegmentCacheKey) -> Option<Arc<[u8]>> {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .blocks
            .get(cache_key)
            .and_then(|block| match block {
                MemoBlock::Stored(bytes) => Some(bytes.clone()),
                MemoBlock::Decoded(_) => None,
            })
    }

    pub(super) fn record_stored(&self, cache_key: &MetadataSegmentCacheKey, bytes: &[u8]) {
        self.record_block(cache_key, MemoBlock::Stored(Arc::from(bytes)));
    }

    pub(super) fn record(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        block: &DecodedMetadataSegmentBlock,
    ) {
        self.record_block(cache_key, MemoBlock::Decoded(block.clone()));
    }

    fn record_block(&self, cache_key: &MetadataSegmentCacheKey, block: MemoBlock) {
        let mut inner = self
            .inner
            .lock()
            .expect("session block memo lock should not be poisoned");
        inner.entry_bytes += block.bytes();
        if let Some(previous) = inner.blocks.get_mut(cache_key) {
            let previous = std::mem::replace(previous, block);
            inner.entry_bytes -= previous.bytes();
        } else {
            let cache_key = Arc::new(cache_key.clone());
            inner.entry_bytes += key_bytes(&cache_key);
            inner.insertion_order.push_back(Arc::clone(&cache_key));
            inner.blocks.insert(cache_key, block);
        }
        loop {
            let bytes = inner.bytes();
            if bytes <= inner.reserved_bytes {
                self.pool.release(inner.reserved_bytes - bytes);
                inner.reserved_bytes = bytes;
                break;
            }
            if self.pool.try_reserve(bytes - inner.reserved_bytes) {
                inner.reserved_bytes = bytes;
                break;
            }
            let oldest = inner
                .insertion_order
                .pop_front()
                .expect("accounted blocks should have an insertion-order entry");
            let evicted = inner
                .blocks
                .remove(&oldest)
                .expect("session block memo queue and map should stay one-to-one");
            inner.entry_bytes -= evicted.bytes() + key_bytes(&oldest);
            inner.blocks.shrink_to_fit();
            inner.insertion_order.shrink_to_fit();
        }
    }
}

impl Drop for SessionBlockMemo {
    fn drop(&mut self) {
        let inner = self
            .inner
            .get_mut()
            .expect("session block memo lock should not be poisoned");
        self.pool.release(inner.reserved_bytes);
    }
}

// Adjacent lookups share fetched bytes within one aligned window.
const RANGE_SCAN_READAHEAD_BLOCKS: usize = 32;
/// Loads the rows of one segment whose keys can fall in
/// `[lower_bound, upper_bound)`: index first, then only the data blocks the
/// index says can match. Callers trim edge blocks with
/// [`SegmentKeyRangeBlocks::rows_in_key_range`].
#[allow(
    clippy::too_many_arguments,
    reason = "the segment scan inputs stay explicit at the shared load boundary"
)]
pub(super) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    max_seq: ChangeSeq,
    lower_bound: &str,
    upper_bound: Option<&str>,
    row_limit: usize,
    readahead: Readahead,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let index = load_segment_index(store, segment_cache, memo, descriptor).await?;
    let needed = index_blocks_for_key_range(&index, lower_bound, upper_bound);

    let mut result = SegmentKeyRangeBlocks { blocks: Vec::new() };
    let mut remaining_rows = row_limit;
    let mut start = needed.start;
    while start < needed.end && remaining_rows > 0 {
        // A page only needs the first `row_limit` matching rows from each
        // segment before the global merge truncates it. Count actual decoded
        // matches one aligned window at a time; byte size is not a row count.
        // Unbounded scans retain their existing coalesced range fetch.
        let required_end = if readahead == Readahead::Stored {
            start + 1
        } else if row_limit == usize::MAX {
            needed.end
        } else {
            (start + 1)
                .div_ceil(RANGE_SCAN_READAHEAD_BLOCKS)
                .saturating_mul(RANGE_SCAN_READAHEAD_BLOCKS)
                .min(needed.end)
        };
        // Preserve the aligned read-ahead policy for subsequent pages.
        let extended_end = if readahead != Readahead::Disabled {
            required_end
                .div_ceil(RANGE_SCAN_READAHEAD_BLOCKS)
                .saturating_mul(RANGE_SCAN_READAHEAD_BLOCKS)
                .min(index.len())
        } else {
            required_end
        };
        let blocks = if readahead == Readahead::Stored {
            vec![
                load_segment_data_block_with_readahead(
                    store,
                    segment_cache,
                    Some(memo),
                    descriptor,
                    &index[start..extended_end],
                )
                .await?,
            ]
        } else {
            load_segment_data_block_span(
                store,
                segment_cache,
                Some(memo),
                descriptor,
                &index[start..extended_end],
            )
            .await?
            .into_iter()
            .take(required_end - start)
            .collect()
        };
        let batch = SegmentKeyRangeBlocks { blocks };
        if row_limit != usize::MAX {
            remaining_rows = remaining_rows
                .saturating_sub(batch.rows_in_key_range(lower_bound, upper_bound).count());
        }
        result.blocks.extend(batch.blocks);
        start = required_end;
    }

    validate_manifest_row_seq_range(
        &metadata_segment_object_key(descriptor),
        result.blocks.iter().flat_map(|block| block.rows.iter()),
        max_seq,
    )?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::cache::MetadataSegmentBlockKind;
    use super::*;

    fn key(offset: u64) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: "segment".to_owned(),
            block_kind: MetadataSegmentBlockKind::Data,
            block_offset: offset,
        }
    }

    #[test]
    fn replacement_and_eviction_release_reservations_without_invalidating_borrows() {
        let pool = Arc::new(ReadWorkingMemory::new(4096, None));
        let memo = SessionBlockMemo::new(Arc::clone(&pool));
        memo.record_stored(&key(1), &[1; 1024]);
        let stored = memo.stored(&key(1)).expect("stored block");
        let stored_bytes = pool.in_use();
        let block = DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                rows: Vec::new(),
                row_keys: Vec::new(),
            }),
            decoded_bytes: 2048,
        };
        memo.record(&key(1), &block);
        assert_eq!(pool.in_use(), stored_bytes + 1024 - arc_bytes::<()>());
        let borrowed = memo.get(&key(1)).expect("decoded block");
        memo.record(&key(1), &block);
        assert_eq!(pool.in_use(), stored_bytes + 1024 - arc_bytes::<()>());
        memo.record(&key(2), &block);
        assert!(memo.get(&key(1)).is_none());
        assert!(memo.get(&key(2)).is_some());
        assert!(pool.in_use() <= 4096);
        memo.record_stored(&key(3), &[3; 4096]);
        assert_eq!(pool.in_use(), 0);
        assert_eq!(stored.as_ref(), &[1; 1024]);
        assert_eq!(borrowed.weight(), 2048);
        drop(memo);
        assert_eq!(pool.in_use(), 0);
    }
}
