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
//!   data blocks of a segment of 256 KiB or less. LoonFS also offers the same
//!   blocks to the node-local block cache when one is configured. The first
//!   read after a fold or a compaction then finds those blocks in the cache
//!   instead of reading them from object storage.

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
mod load;
pub(crate) mod publish;
mod published;
mod retention;
mod row;
mod runs;
mod scan;
mod scan_load;
mod statistics;
mod stored_block_cache;
mod streaming_compaction;
#[cfg(test)]
pub(crate) mod tests;
mod validate;

pub use self::cache::{
    CacheScope, CachedHead, HeadStateCache, HeadStateCacheStats, MetadataSegmentCache,
    MetadataSegmentCacheStats, NamespaceHead, ObjectStoreScope, SharedHeadState,
    SharedSegmentBlocks, WalTailProjectionCacheKey,
};
pub use self::compaction_merge::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::compaction_step::{CompactionStepOutcome, MetadataCompactionPolicy};
pub use self::error::ManifestLoadError;
pub use self::fold::{
    fold_wal_tail, next_run_no_after, FoldWalOutcome, FoldWalResponse, FoldedWalTail,
};
pub use self::published::{PublicationReport, PublishedManifest};
pub use self::retention::RetentionTarget;
pub use self::runs::{MetadataFamilyGroup, MetadataRunPolicy};
pub use self::statistics::{
    load_checkpoint_statistics, load_namespace_statistics, NamespaceStatistics,
};
pub use self::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
pub use self::streaming_compaction::{
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionSpec,
};

pub(crate) use self::compaction_step::compaction_step;
pub use self::compaction_step::metadata_compaction_due;
pub(crate) use self::compactor::claim_compactor;
#[cfg(test)]
pub(crate) use self::fold::fold_wal;
pub(crate) use self::fold::{fold_wal_with_deadline, try_fold_wal, TryFoldWalOutcome};
pub(crate) use self::load::{
    ensure_manifest_reference_matches, head_from_manifest, load_basis_metadata_segments,
    load_manifest_segments, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, metadata_basis_from_manifest, LoadedMetadataBasis,
};
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataSegments};
pub(crate) use self::streaming_compaction::run_metadata_compaction_job;
