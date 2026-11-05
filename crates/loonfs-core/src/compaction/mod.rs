//! Metadata compaction: rewriting a window of one family group's runs into one
//! run, and publishing the manifest that lists the new run in place of
//! its inputs.

pub(crate) mod group_compaction;
pub(crate) mod kept_inodes;
pub(crate) mod plan;
pub(crate) mod publication;
pub(crate) mod retention;
pub(crate) mod run_iterator;
pub(crate) mod step;

pub use self::run_iterator::{
    refill_iterators, select_next_iterator, LoadedBlocks, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::step::{
    CompactionUnitOutcome, MetadataCompactionCancellation, MetadataCompactionPolicy,
};
