//! Namespace manifests and metadata segments.
//!
//! A namespace manifest references the immutable metadata segment runs for one
//! namespace state. Folding, compaction, and retention publish manifests.

mod block_fetch;
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
mod load;
pub(crate) mod publish;
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
    CacheScope, CachedReadAnchor, HeadStateCache, HeadStateCacheStats, MetadataSegmentCache,
    MetadataSegmentCacheStats, NamespaceValidation, SharedHeadState, SharedSegmentBlocks,
    WalTailProjectionCacheKey,
};
pub use self::compaction_merge::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::compaction_step::{CompactionStepOutcome, MetadataCompactionPolicy};
pub use self::error::{ManifestLoadError, ManifestLoadFailureClass};
pub use self::fold::{fold_wal_tail, next_run_no_after, FoldedWalTail};
pub use self::runs::{MetadataFamilyGroup, MetadataLsmPolicy};
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

pub(crate) use self::cache::read_working_memory;
pub(crate) use self::compaction_step::compaction_step;
pub use self::compaction_step::metadata_compaction_due;
pub(crate) use self::compactor::claim_compactor;
#[cfg(test)]
pub(crate) use self::fold::fold_wal;
pub(crate) use self::fold::{fold_wal_with_deadline, try_fold_wal, TryFoldWal};
pub(crate) use self::load::{
    ensure_manifest_reference_matches, head_from_manifest, load_basis_metadata_segments,
    load_manifest_segments, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, metadata_basis_from_manifest, LoadedMetadataBasis,
};
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataSegments};
pub(crate) use self::streaming_compaction::run_metadata_compaction_job;
