//! The per-view memo of decoded segment blocks.

use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey};
use crate::block_cache::DecodedBlock;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes};
use crate::limits::MAX_BLOCK_MEMO_BYTES;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The decoded index, filter, and lookup blocks that one read has loaded,
/// kept until the read ends. The memo has a budget of its own, and past
/// that budget it evicts its oldest blocks first.
#[derive(Debug)]
pub(crate) struct SessionBlockMemo {
    max_bytes: usize,
    inner: Mutex<SessionBlockMemoInner>,
}

impl Default for SessionBlockMemo {
    fn default() -> Self {
        Self::with_max_bytes(MAX_BLOCK_MEMO_BYTES)
    }
}

#[derive(Debug, Default)]
struct SessionBlockMemoInner {
    blocks: HashMap<Arc<MetadataSegmentCacheKey>, DecodedMetadataSegmentBlock>,
    insertion_order: VecDeque<Arc<MetadataSegmentCacheKey>>,
    entry_bytes: usize,
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
    /// Returns an empty memo that keeps at most `max_bytes`.
    pub(crate) fn with_max_bytes(max_bytes: usize) -> Self {
        Self {
            max_bytes,
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
            .cloned()
    }

    pub(super) fn record(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        block: &DecodedMetadataSegmentBlock,
    ) {
        let block = block.clone();
        let mut inner = self
            .inner
            .lock()
            .expect("session block memo lock should not be poisoned");
        inner.entry_bytes += block.weight();
        if let Some(previous) = inner.blocks.get_mut(cache_key) {
            let previous = std::mem::replace(previous, block);
            inner.entry_bytes -= previous.weight();
        } else {
            let cache_key = Arc::new(cache_key.clone());
            inner.entry_bytes += key_bytes(&cache_key);
            inner.insertion_order.push_back(Arc::clone(&cache_key));
            inner.blocks.insert(cache_key, block);
        }
        // The memo keeps its newest block even when that block alone exceeds
        // the budget, so repeated lookups in one block larger than the budget
        // (a block of one large row) decode it once. The containers keep
        // their capacity as blocks leave, so a memo at its budget doesn't
        // rebuild its table on every eviction.
        while inner.bytes() > self.max_bytes && inner.insertion_order.len() > 1 {
            let oldest = inner
                .insertion_order
                .pop_front()
                .expect("a memo with more than one block has an oldest block");
            let evicted = inner
                .blocks
                .remove(&oldest)
                .expect("session block memo queue and map should stay one-to-one");
            inner.entry_bytes -= evicted.weight() + key_bytes(&oldest);
        }
    }

    /// Returns the bytes that the memo keeps, counted as its budget counts
    /// them.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::SegmentBlockKind;
    use super::*;
    use loonfs_types::format::segment_blocks::DecodedDataBlock;

    fn key(offset: u64) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: "segment".to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: offset,
        }
    }

    fn data_block(decoded_bytes: usize) -> DecodedMetadataSegmentBlock {
        DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                rows: Vec::new(),
                row_keys: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    #[test]
    fn a_memo_evicts_its_oldest_blocks_at_its_budget_and_keeps_its_newest_one() {
        let memo = SessionBlockMemo::with_max_bytes(4096);
        let block = data_block(2048);
        memo.record(&key(1), &block);
        let recorded_bytes = memo.bytes();
        assert!(recorded_bytes > 2048);
        let borrowed = memo.get(&key(1)).expect("decoded block");
        memo.record(&key(1), &block);
        assert_eq!(
            memo.bytes(),
            recorded_bytes,
            "a replacement keeps the bytes"
        );
        memo.record(&key(2), &block);
        assert!(memo.get(&key(1)).is_none(), "the oldest block left first");
        assert!(memo.get(&key(2)).is_some());
        assert!(memo.bytes() <= 4096);
        memo.record(&key(3), &data_block(8192));
        assert!(
            memo.get(&key(3)).is_some(),
            "the newest block stays, even above the whole budget"
        );
        assert!(memo.get(&key(2)).is_none(), "every older block left");
        memo.record(&key(4), &block);
        assert!(memo.get(&key(3)).is_none());
        assert!(memo.get(&key(4)).is_some());
        assert!(memo.bytes() <= 4096);
        assert_eq!(borrowed.weight(), 2048);
    }

    #[test]
    fn the_default_memo_keeps_blocks_up_to_its_fixed_budget() {
        let memo = SessionBlockMemo::default();
        let block_bytes = 64 * 1024;
        let blocks = MAX_BLOCK_MEMO_BYTES / block_bytes;
        for offset in 0..2 * blocks as u64 {
            memo.record(&key(offset), &data_block(block_bytes));
            assert!(memo.bytes() <= MAX_BLOCK_MEMO_BYTES);
        }
        assert!(memo.get(&key(0)).is_none());
        assert!(memo.get(&key(2 * blocks as u64 - 1)).is_some());
    }
}
