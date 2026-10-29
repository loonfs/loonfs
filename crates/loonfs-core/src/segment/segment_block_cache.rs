//! The block cache that every read of a metadata segment's data, index, and
//! filter blocks goes through, and its memory implementation.
//!
//! The `loonfs` crate has a second implementation, which also keeps blocks
//! on local SSD, so the interface and the memory implementation are public.

use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey, SharedSegmentBlocks};
use crate::block_cache::ObjectStoreScope;
use crate::manifest::error::ManifestLoadError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use loonfs_types::format::segment_blocks::BlockHandle;
use std::future::Future;
use std::sync::Arc;

/// A block that a read loaded from the store, with its stored bytes (the
/// bytes of the block's section, exactly as the segment stores them).
pub struct LoadedSegmentBlock {
    pub block: DecodedMetadataSegmentBlock,
    pub stored: Bytes,
}

/// The load of one block from the store, which a block cache runs on a miss.
pub type SegmentBlockLoad<'a> = BoxFuture<'a, Result<LoadedSegmentBlock, ManifestLoadError>>;

/// The cache of metadata segment blocks, which every read of a data, index,
/// or filter block goes through.
///
/// A block's key is its segment's object key, its kind, and its offset in the
/// segment. A get also passes the block's handle, which records the block's
/// stored length and the CRC32C of its stored bytes, and an insert also
/// passes the block's stored bytes. So an implementation that keeps blocks in
/// their stored form can write the bytes that the store returned, and check
/// them against the handle when it reads them back. Manifests, which the
/// shared segment blocks also keep, don't go through this cache.
#[async_trait]
pub trait SegmentBlockCache: Send + Sync {
    /// Returns the block under `key` and marks it recently used, or returns
    /// `None` if the cache doesn't keep it.
    async fn get_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
    ) -> Option<DecodedMetadataSegmentBlock>;

    /// Inserts `block` under `key`, with `stored`, the block's stored bytes.
    fn insert_block(
        &self,
        key: MetadataSegmentCacheKey,
        block: DecodedMetadataSegmentBlock,
        stored: &[u8],
    );

    /// Returns the block under `key`, or awaits `load` and inserts the block
    /// that it loads, with the block's stored bytes.
    ///
    /// Concurrent callers of one key share one load, so a caller that
    /// arrives while a load is in flight takes the block of that load. A
    /// load that fails shares nothing, and the next caller loads again.
    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError>;
}

/// The memory implementation of [`SegmentBlockCache`], which keeps blocks in
/// the shared segment blocks under the scope of one runtime's object store.
///
/// It keeps decoded blocks, which LoonFS checked against their handles when
/// it loaded them, so it doesn't use the handle on a get or the stored bytes
/// on an insert.
pub struct MemorySegmentBlocks {
    shared: Arc<SharedSegmentBlocks>,
    scope: ObjectStoreScope,
}

impl MemorySegmentBlocks {
    /// Returns the memory implementation over `shared`, under `scope`.
    pub fn new(shared: Arc<SharedSegmentBlocks>, scope: ObjectStoreScope) -> Self {
        Self { shared, scope }
    }

    /// Returns the block under `key`, or awaits `load` and inserts the block
    /// that it returns, like [`SegmentBlockCache::get_or_load_block`], and
    /// calls `hit` with the block when it's already in memory.
    ///
    /// Concurrent callers of one key share one run of `load`. An
    /// implementation with a second tier under this one runs its own `load`
    /// (from that tier, and then from the store) inside that single flight,
    /// and uses `hit` to mark the block recently used in its tier too.
    pub async fn get_or_load_with_hit<H, F, Fut>(
        &self,
        key: &MetadataSegmentCacheKey,
        hit: H,
        load: F,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError>
    where
        H: FnOnce(&DecodedMetadataSegmentBlock),
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<DecodedMetadataSegmentBlock, ManifestLoadError>>,
    {
        self.shared
            .entries(key.block_kind)
            .get_or_load_with_hit(&(self.scope, key.clone()), hit, load)
            .await
    }
}

#[async_trait]
impl SegmentBlockCache for MemorySegmentBlocks {
    async fn get_block(
        &self,
        key: &MetadataSegmentCacheKey,
        _handle: &BlockHandle,
    ) -> Option<DecodedMetadataSegmentBlock> {
        self.shared
            .entries(key.block_kind)
            .get(&(self.scope, key.clone()))
    }

    fn insert_block(
        &self,
        key: MetadataSegmentCacheKey,
        block: DecodedMetadataSegmentBlock,
        _stored: &[u8],
    ) {
        self.shared
            .entries(key.block_kind)
            .insert((self.scope, key), block);
    }

    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        _handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
        self.get_or_load_with_hit(key, |_| {}, move || async move { Ok(load.await?.block) })
            .await
    }
}
