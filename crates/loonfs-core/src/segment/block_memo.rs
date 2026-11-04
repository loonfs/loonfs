//! The per-view memo of decoded segment blocks.

use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey};
use crate::block_cache::DecodedBlock;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes};
use crate::read_working_memory::ReadWorkingMemory;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The decoded index, filter, and lookup blocks that one read has loaded,
/// charged to the read working memory and evicted oldest first when the pool
/// is full, until the read ends.
#[derive(Debug)]
pub(crate) struct SessionBlockMemo {
    pool: Arc<ReadWorkingMemory>,
    inner: Mutex<SessionBlockMemoInner>,
}

impl Default for SessionBlockMemo {
    fn default() -> Self {
        Self::new(super::cache::unshared_read_working_memory())
    }
}

#[derive(Debug, Default)]
struct SessionBlockMemoInner {
    blocks: HashMap<Arc<MetadataSegmentCacheKey>, DecodedMetadataSegmentBlock>,
    insertion_order: VecDeque<Arc<MetadataSegmentCacheKey>>,
    entry_bytes: usize,
    reserved_bytes: usize,
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
    pub(crate) fn new(pool: Arc<ReadWorkingMemory>) -> Self {
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
            .cloned()
    }

    /// Returns the read working memory that the memo is charged to.
    pub(super) fn pool(&self) -> &Arc<ReadWorkingMemory> {
        &self.pool
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
            inner.entry_bytes -= evicted.weight() + key_bytes(&oldest);
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
    fn replacement_and_eviction_release_reservations_without_invalidating_borrows() {
        let pool = Arc::new(ReadWorkingMemory::new(4096, None));
        let memo = SessionBlockMemo::new(Arc::clone(&pool));
        let block = data_block(2048);
        memo.record(&key(1), &block);
        let recorded_bytes = pool.in_use();
        assert!(recorded_bytes > 2048);
        let borrowed = memo.get(&key(1)).expect("decoded block");
        memo.record(&key(1), &block);
        assert_eq!(pool.in_use(), recorded_bytes);
        memo.record(&key(2), &block);
        assert!(memo.get(&key(1)).is_none());
        assert!(memo.get(&key(2)).is_some());
        assert!(pool.in_use() <= 4096);
        memo.record(&key(3), &data_block(4096));
        assert!(memo.get(&key(3)).is_none());
        assert_eq!(pool.in_use(), 0);
        assert_eq!(borrowed.weight(), 2048);
        memo.record(&key(2), &block);
        drop(memo);
        assert_eq!(pool.in_use(), 0);
    }
}
