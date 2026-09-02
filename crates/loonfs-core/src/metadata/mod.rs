//! Materialized namespace metadata.
//!
//! `MetadataState` stores append-only rows at a WAL sequence. Derived indexes
//! accelerate current reads, while historical reads scan the retained rows.
//! The `visibility` module applies shared direntry rules to every storage view.

mod apply;
mod durable_cache;
mod indexes;
pub(crate) mod manifest_index;
mod queries;
pub(crate) mod row_decode;
mod rows;
#[cfg(test)]
mod tests;
mod view;
mod view_session;
mod visibility;

pub use self::queries::{ResolvedVisiblePath, VisiblePathError};
pub use self::rows::MetadataState;
pub use loonfs_api::wire::manifest::{
    AttributesRevisionRecord, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, RevisionRecord, SubtreeTombstoneRecord, TombstoneRowAction,
};

pub(crate) use self::durable_cache::DurableVisibilityCache;
/// Newest-event-wins tombstone selection used by differential tests.
#[cfg(test)]
pub(crate) use self::rows::active_tombstone_from_records;
pub(crate) use self::rows::{
    active_deletion_from_tombstone, recoverable_deletion_from_active_record, RecoverableDeletion,
};
#[cfg(test)]
pub(crate) use self::view::InMemoryMetadataView;
pub(crate) use self::view::{AttributesProjection, MetadataView};
pub(crate) use self::view_session::{
    LeafRevisionPrefetch, MetadataViewSession, VisibleChildEntry,
    METADATA_VIEW_SESSION_COUNTER_FIELDS,
};
pub(crate) use self::visibility::{binding_generation, unbind_matches_binding, BindingIdentity};
pub(crate) use loonfs_api::wire::manifest::ActiveDeletionRecord;

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;
