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
pub use self::rows::{
    AttributesRevisionRecord, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneAction, SubtreeTombstoneRecord,
};

pub(crate) use self::durable_cache::DurableVisibilityCache;
/// Newest-event-wins tombstone selection used by differential tests.
#[cfg(test)]
pub(crate) use self::rows::active_tombstone_from_records;
pub(crate) use self::rows::content_ref_evidence_bytes;
pub(crate) use self::rows::{
    active_deletion_from_tombstone, ActiveDeletionAction, ActiveDeletionRecord, RecoverableDeletion,
};
#[cfg(test)]
pub(crate) use self::view::InMemoryMetadataView;
pub(crate) use self::view::MetadataView;
pub(crate) use self::view_session::{
    LeafRevisionPrefetch, MetadataViewSession, VisibleChildEntry,
    METADATA_VIEW_SESSION_COUNTER_FIELDS,
};
pub(crate) use self::visibility::{unbind_matches_binding, BindingIdentity};

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;
