//! Metadata segments: building and writing them, loading and caching their
//! blocks, and scanning their rows.
//!
//! # Segment cache policy
//!
//! LoonFS decodes segment blocks into a [`MetadataSegmentCache`], and it uses
//! that cache the same way for each kind of segment traffic:
//!
//! - Reads and folds read segments through the cache.
//! - A compaction reads its input segments without the cache. The merge reads
//!   each input block once, and the manifest it publishes no longer lists the
//!   input segments, so caching their blocks would evict blocks that reads of
//!   the current manifest still use.
//! - LoonFS inserts each segment that a fold or a compaction writes into the
//!   cache after the segment's PUT succeeds, and before the manifest that
//!   lists the segment is published. These are the blocks that the first
//!   fetch of the segment would insert: the index and filter blocks, and the
//!   data blocks of a segment of 256 KiB or less. The first read after a
//!   fold or a compaction then finds those blocks in the cache instead of
//!   reading them from object storage.
//!
//! A runtime can read blocks through the `loonfs` crate's implementation of
//! [`SegmentBlockCache`] instead, which also writes every block that it
//! inserts to local disk. A block that memory has evicted is then read back
//! from disk, checked against its handle, and decoded by
//! [`decode_stored_block`]. Manifests stay in memory.

pub(crate) mod block_memo;
pub(crate) mod build;
pub(crate) mod cache;
pub(crate) mod data_block_load;
pub(crate) mod index_block_load;
pub(crate) mod row;
pub(crate) mod runs;
pub(crate) mod scan;
pub(crate) mod scan_load;
pub(crate) mod segment_block_cache;

pub use self::cache::{
    DecodedMetadataSegmentBlock, DecodedSegmentBlock, MetadataSegmentCache,
    MetadataSegmentCacheKey, MetadataSegmentCacheStats, SegmentBlockKind, SegmentCacheKey,
    SharedSegmentBlocks,
};
pub use self::index_block_load::decode_stored_block;
pub use self::runs::{MetadataFamilyGroup, MetadataRunManifest, MetadataRunPolicy};
pub use self::segment_block_cache::{
    LoadedSegmentBlock, MemorySegmentBlocks, SegmentBlockCache, SegmentBlockLoad,
};

pub(crate) use self::scan::{Readahead, VerifiedMetadataSegments};
pub(crate) use self::scan_load::{visit_segment_rows, ScanDescriptor};
