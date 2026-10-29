//! Namespace manifests and metadata segments.
//!
//! A namespace manifest references the immutable metadata segment runs for one
//! namespace state. Folding, compaction, and retention publish manifests.
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

mod block_load;
mod build;
pub(crate) mod cache;
mod compaction_merge;
mod compaction_retention;
mod compaction_step;
mod compactor;
mod data_block_load;
mod error;
mod fold;
mod index_block_load;
mod kept_inodes;
mod load;
pub(crate) mod publish;
mod published;
mod retention;
mod row;
mod runs;
mod scan;
mod scan_load;
mod segment_block_cache;
mod statistics;
mod streaming_compaction;
#[cfg(test)]
pub(crate) mod tests;
mod validate;

pub use self::cache::{
    CacheScope, CachedHead, DecodedMetadataSegmentBlock, HeadAndWalTailCache, MetadataSegmentCache,
    MetadataSegmentCacheKey, MetadataSegmentCacheStats, ObjectStoreScope, SharedHeadsAndWalTails,
    SharedSegmentBlocks, WalTailCacheKey,
};
pub use self::compaction_merge::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::compaction_step::{
    CompactionAdmission, CompactionUnitOutcome, MetadataCompactionPolicy,
};
pub use self::error::ManifestLoadError;
pub use self::fold::{
    fold_wal_tail, next_run_no_after, FoldWalOutcome, FoldWalResponse, FoldedWalTail,
};
pub use self::index_block_load::decode_stored_block;
pub use self::load::load_position_basis;
pub(crate) use self::published::same_head;
pub use self::published::{PublicationReport, PublishedManifest};
pub use self::retention::RetentionTarget;
pub use self::runs::{MetadataFamilyGroup, MetadataRunManifest, MetadataRunPolicy};
pub use self::segment_block_cache::{
    LoadedSegmentBlock, MemorySegmentBlocks, SegmentBlockCache, SegmentBlockLoad,
};
pub use self::statistics::{
    load_checkpoint_statistics, load_namespace_statistics, NamespaceStatistics,
};
pub use self::streaming_compaction::MetadataCompactionCancellation;

pub(crate) use self::compaction_step::compact_unit;
pub use self::compaction_step::metadata_compaction_due;
pub(crate) use self::compactor::claim_compactor;
#[cfg(test)]
pub(crate) use self::fold::fold_wal;
pub(crate) use self::load::{
    ensure_manifest_reference_matches, head_from_manifest, load_basis_metadata_segments,
    load_manifest_segments, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, metadata_basis_from_manifest, LoadedMetadataBasis,
};
pub(crate) use self::retention::{advance_retention_floor, set_retention_window};
pub(crate) use self::scan::{Readahead, VerifiedMetadataSegments};
pub(crate) use self::scan_load::{visit_segment_rows, ScanDescriptor};
