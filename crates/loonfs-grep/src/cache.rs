//! Grep-private cache for decoded segment blocks.

use crate::codec::IndexRow;
use loonfs::engine::{DecodedBlockCache, DecodedSegmentBlock, SegmentCacheKey};
use loonfs_types::format::segment_blocks::DEFAULT_TARGET_BLOCK_BYTES;
use std::convert::Infallible;

/// Default decoded-byte budget for cached grep segment blocks.
pub const DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES: usize = 4_096 * DEFAULT_TARGET_BLOCK_BYTES.get();

/// Grep's cache for decoded segment blocks.
pub type GrepBlockCache = DecodedBlockCache<SegmentCacheKey, DecodedGrepBlock>;

/// A decoded grep block. Grep's entry is in the namespace manifest, which
/// the runtime caches, so the cache has no manifest blocks.
pub type DecodedGrepBlock = DecodedSegmentBlock<IndexRow, Infallible>;
pub type GrepBlockCacheKey = SegmentCacheKey;
