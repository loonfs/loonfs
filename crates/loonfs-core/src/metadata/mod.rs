//! Materialized namespace metadata: append-only row families plus derived
//! indexes that answer reads over them.
//!
//! [`MetadataState`] is one namespace's metadata materialized at one WAL
//! sequence: committed WAL deltas append immutable rows, and every read is
//! answered from those rows. Submodules follow that lifecycle:
//!
//! - `rows` defines the row records and the append/accounting plumbing that
//!   keeps the derived indexes and decoded-size totals in step with every
//!   appended row.
//! - `apply` maps committed WAL deltas onto row appends — the only WAL-delta
//!   to metadata-row mapping in the crate, shared by durable replay and
//!   commit-validation previews.
//! - `queries` answers seq-gated reads: record lookups, visibility checks,
//!   and path resolution, routed to the indexes at head and to historical
//!   row scans below it.
//! - `indexes` maintains the at-head lookup structures behind those fast
//!   paths.

mod apply;
mod indexes;
pub(crate) mod manifest_index;
mod queries;
mod row_decode;
mod rows;
#[cfg(test)]
mod tests;
mod view;

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

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;
