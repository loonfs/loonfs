//! Grep-private cache for decoded immutable index-segment blocks.

use crate::codec::IndexRow;
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Maximum decoded segment sections retained by one grep service.
pub(crate) const MAX_CACHED_GREP_BLOCKS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GrepBlockKind {
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
    Filter(Arc<SegmentFilter>),
    Index(Arc<Vec<SegmentIndexEntry>>),
    Data(Arc<DecodedDataBlock<IndexRow>>),
}

#[derive(Debug)]
pub(crate) struct GrepBlockCache {
    max_blocks: usize,
    inner: Mutex<GrepBlockCacheInner>,
}

#[derive(Debug, Default)]
struct GrepBlockCacheInner {
    entries: HashMap<GrepBlockCacheKey, CacheSlot>,
    order: VecDeque<(GrepBlockCacheKey, u64)>,
    touch_counter: u64,
}

#[derive(Debug)]
struct CacheSlot {
    block: DecodedGrepBlock,
    last_touch: u64,
}

impl GrepBlockCache {
    pub(crate) fn new(max_blocks: usize) -> Self {
        Self {
            max_blocks,
            inner: Mutex::new(GrepBlockCacheInner::default()),
        }
    }

    pub(crate) fn get(&self, key: &GrepBlockCacheKey) -> Option<DecodedGrepBlock> {
        if self.max_blocks == 0 {
            return None;
        }
        let mut inner = self.inner.lock().expect("grep block cache lock poisoned");
        let block = inner.entries.get(key)?.block.clone();
        inner.touch(key);
        inner.compact_if_needed(self.max_blocks);
        Some(block)
    }

    pub(crate) fn insert(&self, key: GrepBlockCacheKey, block: DecodedGrepBlock) {
        if self.max_blocks == 0 {
            return;
        }
        let mut inner = self.inner.lock().expect("grep block cache lock poisoned");
        let stamp = inner.next_stamp();
        inner.entries.insert(
            key.clone(),
            CacheSlot {
                block,
                last_touch: stamp,
            },
        );
        inner.order.push_back((key, stamp));
        while inner.entries.len() > self.max_blocks {
            inner.evict_oldest();
        }
        inner.compact_if_needed(self.max_blocks);
    }
}

impl GrepBlockCacheInner {
    fn next_stamp(&mut self) -> u64 {
        self.touch_counter = self.touch_counter.saturating_add(1);
        self.touch_counter
    }

    fn touch(&mut self, key: &GrepBlockCacheKey) {
        let stamp = self.next_stamp();
        let Some(slot) = self.entries.get_mut(key) else {
            return;
        };
        slot.last_touch = stamp;
        self.order.push_back((key.clone(), stamp));
    }

    fn evict_oldest(&mut self) {
        while let Some((key, stamp)) = self.order.pop_front() {
            if self
                .entries
                .get(&key)
                .is_some_and(|slot| slot.last_touch == stamp)
            {
                self.entries.remove(&key);
                return;
            }
        }
    }

    fn compact_if_needed(&mut self, max_blocks: usize) {
        let queue_limit = max_blocks.saturating_mul(4).max(1);
        if self.order.len() <= queue_limit {
            return;
        }
        let mut live = self
            .entries
            .iter()
            .map(|(key, slot)| (key.clone(), slot.last_touch))
            .collect::<Vec<_>>();
        live.sort_by_key(|(_, stamp)| *stamp);
        self.order = live.into();
    }
}
