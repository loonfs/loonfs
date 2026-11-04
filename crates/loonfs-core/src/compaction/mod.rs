//! Metadata compaction: rewriting a window of one family group's runs into one
//! run, and publishing the manifest that lists the new run in place of
//! its inputs.

pub(crate) mod group_compaction;
pub(crate) mod kept_inodes;
pub(crate) mod merge_iterator;
pub(crate) mod plan;
pub(crate) mod publication;
pub(crate) mod retention;
pub(crate) mod step;

pub use self::merge_iterator::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::step::{
    CompactionUnitOutcome, MetadataCompactionCancellation, MetadataCompactionPolicy,
};
