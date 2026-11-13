//! Namespace metadata in memory.
//!
//! `MetadataState` stores append-only rows at a WAL sequence. Derived indexes
//! accelerate current reads, while historical reads scan the retained rows.
//! The `visibility` module applies shared direntry rules to every storage view.

pub(crate) mod access;
mod apply;
mod children_page;
mod durable_cache;
mod indexes;
pub(crate) mod manifest_index;
mod manifest_tail_combiner;
mod queries;
pub(crate) mod row_decode;
mod rows;
mod tail_rows;
#[cfg(test)]
mod tests;
mod view;
mod view_bindings;
mod view_revisions;
mod view_session;
mod visibility;

pub use self::queries::{ResolvedVisiblePath, VisiblePathError};
pub use self::rows::MetadataState;
pub use loonfs_types::format::manifest::{
    AccessRevisionRecord, AttributesRevisionRecord, CommitReceiptRecord, ContentLayoutRecord,
    DirentryBindingRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord, TombstoneRowAction,
};

pub(crate) use self::children_page::VisibleChildEntry;
pub(crate) use self::durable_cache::DurableVisibilityCache;
/// Newest-event-wins tombstone selection used by differential tests.
#[cfg(test)]
pub(crate) use self::rows::hiding_tombstone_from_records;
pub(crate) use self::rows::{
    listed_trash_entry_from_record, trash_record_from_tombstone, ListedTrashEntry, TrashRange,
};
pub(crate) use self::tail_rows::RowOrders;
pub(crate) use self::view::MetadataView;
pub(crate) use self::view_session::{
    preload_inode_groups, LeafRevisionPreload, MetadataViewSession,
    METADATA_VIEW_SESSION_COUNTER_FIELDS,
};
pub(crate) use loonfs_types::format::manifest::TrashRecord;

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;

pub(crate) use visibility::{
    ancestry_allows_read, visible_child, AncestorWalk, MetadataVisibilityReads,
};
