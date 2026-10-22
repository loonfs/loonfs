//! The tiered implementation of the segment block cache, which keeps every
//! block in memory and in the SSD tier.

use super::ssd_segment_blocks::{SsdBlockKey, SsdSegmentBlocks};
use async_trait::async_trait;
use loonfs_core::cache::{
    decode_stored_block, DecodedMetadataSegmentBlock, MemorySegmentBlocks, MetadataSegmentCacheKey,
    SegmentBlockCache, SegmentBlockLoad,
};
use loonfs_core::ManifestLoadError;
use loonfs_objectstore::StoreIdentity;
use loonfs_types::format::segment_blocks::BlockHandle;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// The tiered implementation of [`SegmentBlockCache`], which keeps every
/// block that's inserted in the shared segment blocks and on local SSD.
///
/// A get looks in memory first. A hit there marks the block as the most
/// recently used block on disk too, so the disk doesn't evict a block that
/// memory still keeps. A block that's found on disk is checked against the
/// caller's handle and decoded by the same code as a block loaded from the
/// store, on the runtime's blocking pool, and then inserted into memory.
///
/// On disk, a block's key identifies its object store by the store's
/// identity, not by the scope that the process assigned to the store, so
/// the blocks stay readable after a restart.
pub(crate) struct TieredSegmentBlocks {
    memory: MemorySegmentBlocks,
    ssd: Arc<SsdSegmentBlocks>,
    store: StoreIdentity,
}

impl TieredSegmentBlocks {
    pub(crate) fn new(
        memory: MemorySegmentBlocks,
        ssd: Arc<SsdSegmentBlocks>,
        store: StoreIdentity,
    ) -> Self {
        Self { memory, ssd, store }
    }

    /// Reads the block under `key` from disk, and checks and decodes it
    /// against `handle`. Returns the block with its stored bytes.
    async fn read_disk(
        &self,
        disk_key: &SsdBlockKey,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
    ) -> Option<(DecodedMetadataSegmentBlock, Vec<u8>)> {
        let (object_key, kind, handle) = (key.identity.clone(), key.block_kind, *handle);
        self.ssd
            .read(disk_key, move |stored| {
                decode_stored_block(&object_key, kind, &handle, &stored)
                    .map(|block| (block, stored))
            })
            .await
    }
}

#[async_trait]
impl SegmentBlockCache for TieredSegmentBlocks {
    async fn get_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
    ) -> Option<DecodedMetadataSegmentBlock> {
        let disk_key = SsdBlockKey::new(&self.store, key);
        if let Some(block) = self.memory.get_block(key, handle).await {
            self.ssd.touch(&disk_key);
            return Some(block);
        }
        let (block, stored) = self.read_disk(&disk_key, key, handle).await?;
        self.memory
            .insert_block(key.clone(), block.clone(), &stored);
        Some(block)
    }

    fn insert_block(
        &self,
        key: MetadataSegmentCacheKey,
        block: DecodedMetadataSegmentBlock,
        stored: &[u8],
    ) {
        let disk_key = SsdBlockKey::new(&self.store, &key);
        self.memory.insert_block(key, block, stored);
        self.ssd.insert(disk_key, stored);
    }

    /// Returns the block under `key` like the memory implementation, with a
    /// load that reads the block from disk if the disk keeps it, and loads
    /// it from the store and writes it to disk if not. Concurrent callers of
    /// one key share that load, so they read the disk once.
    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
        let disk_key = SsdBlockKey::new(&self.store, key);
        self.memory
            .get_or_load_with_hit(
                key,
                |_| self.ssd.touch(&disk_key),
                move || async move {
                    if let Some((block, _)) = self.read_disk(&disk_key, key, handle).await {
                        return Ok(block);
                    }
                    let loaded = load.await?;
                    self.ssd.insert(disk_key, &loaded.stored);
                    Ok(loaded.block)
                },
            )
            .await
    }
}
