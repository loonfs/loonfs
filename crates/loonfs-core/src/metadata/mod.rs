//! Materialized namespace metadata: append-only row families plus derived
//! indexes for current-head reads.

mod apply;
mod indexes;
pub(crate) mod manifest_index;
mod queries;
mod row_decode;
mod rows;
#[cfg(test)]
mod tests;
mod view;
mod visibility;

pub use self::apply::{AppliedMetadataState, MetadataApplyError};
pub use self::queries::{ResolvedVisiblePath, VisiblePathError};
pub use self::rows::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    RevisionRecord, SubtreeTombstoneRecord,
};

pub(crate) use self::rows::record_name_key;
pub(crate) use self::view::{
    InMemoryMetadataView, MetadataView, MetadataViewSession, VisibleChildEntry,
};
pub(crate) use self::visibility::{unbind_matches_binding, BindingIdentity};

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;
