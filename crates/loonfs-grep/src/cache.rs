//! Grep-private cache for immutable manifests and decoded segment blocks.

use crate::codec::IndexRow;
use crate::root::GrepManifestState;
use loonfs::Recency;
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentFilter, SegmentIndexEntry};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Maximum decoded manifests and segment sections retained by one grep service or worker.
pub(crate) const MAX_CACHED_GREP_BLOCKS: usize = 4_096;

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
    Manifest(Arc<GrepManifestState>),
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
    order: Recency<GrepBlockCacheKey>,
}

#[derive(Debug)]
struct CacheSlot {
    block: DecodedGrepBlock,
    /// Stamp of this entry's newest queue position; older positions for the
    /// same key are ghosts.
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
        let mut inner = self
            .inner
            .lock()
            .expect("grep block cache lock should not be poisoned");
        let block = inner.entries.get(key)?.block.clone();
        inner.touch(key);
        Some(block)
    }

    pub(crate) fn insert(&self, key: GrepBlockCacheKey, block: DecodedGrepBlock) {
        if self.max_blocks == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("grep block cache lock should not be poisoned");
        inner.entries.insert(
            key.clone(),
            CacheSlot {
                block,
                last_touch: 0,
            },
        );
        inner.touch(&key);
        let GrepBlockCacheInner { entries, order } = &mut *inner;
        while entries.len() > self.max_blocks {
            let Some(evicted) = order.pop_oldest(|key, stamp| slot_is_live(entries, key, stamp))
            else {
                break;
            };
            entries.remove(&evicted);
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
