//! Metadata compaction: merging a window of one family group's runs into one
//! run, and publishing the manifest that lists the merged run in place of
//! its inputs.

pub(crate) mod group_merge;
pub(crate) mod kept_inodes;
pub(crate) mod merge_iterator;
pub(crate) mod plan;
pub(crate) mod publication;
pub(crate) mod retention;
pub(crate) mod step;

pub use self::merge_iterator::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::plan::metadata_compaction_due;
pub use self::step::{
    CompactionAdmission, CompactionUnitOutcome, MetadataCompactionCancellation,
    MetadataCompactionPolicy,
};
