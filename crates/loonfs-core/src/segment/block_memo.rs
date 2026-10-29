//! The per-view memo of decoded and stored segment blocks, and the loads of
//! a range of data blocks that read through it.

use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey};
use super::data_block_load::{
    load_segment_data_block_span, load_segment_data_block_with_readahead,
};
use super::scan::Readahead;
use crate::block_cache::DecodedBlock;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes};
use crate::manifest::error::ManifestLoadError;
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::segment_blocks::DecodedDataBlock;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// This memo stays separate because it uses FIFO eviction for one operation.
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

    /// Returns the read working memory that the memo's pool has free.
    pub(super) fn available_bytes(&self) -> usize {
        self.pool.available_bytes()
    }

    /// Evicts the memo's decoded blocks, and keeps the stored bytes of the
    /// blocks that the read fetched ahead and hasn't used yet.
    pub(super) fn release_decoded(&self) {
        let mut inner = self
            .inner
            .lock()
            .expect("session block memo lock should not be poisoned");
        let SessionBlockMemoInner {
            blocks,
            insertion_order,
            entry_bytes,
            ..
        } = &mut *inner;
        blocks.retain(|key, block| {
            let stored = matches!(block, MemoBlock::Stored(_));
            if !stored {
                *entry_bytes -= block.bytes() + key_bytes(key);
            }
            stored
        });
        insertion_order.retain(|key| blocks.contains_key(key));
        blocks.shrink_to_fit();
        insertion_order.shrink_to_fit();
        let bytes = inner.bytes();
        self.pool.release(inner.reserved_bytes - bytes);
        inner.reserved_bytes = bytes;
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

/// Loads the data blocks of `index[range]`, and reads ahead as `readahead`
/// says. A read-ahead window starts at a block that the read misses and
/// stops at the segment's last block at the latest.
pub(crate) async fn load_segment_blocks_with_readahead<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    index: &[loonfs_types::format::segment_blocks::SegmentIndexEntry],
    range: std::ops::Range<usize>,
    readahead: Readahead,
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    match readahead {
        Readahead::Enabled => {
            let mut blocks = Vec::with_capacity(range.len());
            for position in range {
                blocks.push(
                    load_segment_data_block_with_readahead(
                        store,
                        segment_cache,
                        Some(memo),
                        descriptor,
                        &index[position..],
                    )
                    .await?,
                );
            }
            Ok(blocks)
        }
        Readahead::Disabled => {
            load_segment_data_block_span(
                store,
                segment_cache,
                Some(memo),
                descriptor,
                &index[range],
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::SegmentBlockKind;
    use super::*;

    fn key(offset: u64) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: "segment".to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: offset,
        }
    }

    #[test]
    fn releasing_decoded_blocks_keeps_the_stored_bytes_and_their_charge() {
        let pool = Arc::new(ReadWorkingMemory::new(1024 * 1024, None));
        let memo = SessionBlockMemo::new(Arc::clone(&pool));
        memo.record_stored(&key(1), &[1; 1024]);
        let stored_bytes = pool.in_use();
        memo.record(
            &key(2),
            &DecodedMetadataSegmentBlock::Data {
                block: Arc::new(DecodedDataBlock {
                    rows: Vec::new(),
                    row_keys: Vec::new(),
                }),
                decoded_bytes: 2048,
            },
        );
        assert!(pool.in_use() > stored_bytes + 2048);
        memo.release_decoded();
        assert!(memo.get(&key(2)).is_none());
        assert_eq!(memo.stored(&key(1)).as_deref(), Some(&[1; 1024][..]));
        assert!((1024..=stored_bytes).contains(&pool.in_use()));
        drop(memo);
        assert_eq!(pool.in_use(), 0);
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
