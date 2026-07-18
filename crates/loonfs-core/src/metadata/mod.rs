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
//! - `visibility` holds the single authoritative statement of the
//!   direntry-visibility rules (binding identity, active bindings, tombstone
//!   coverage, path resolution) that every storage shape above decides
//!   through.

mod apply;
mod indexes;
pub(crate) mod manifest_index;
mod queries;
pub(crate) mod row_decode;
mod rows;
#[cfg(test)]
mod tests;
mod view;
mod visibility;

pub use self::apply::AppliedMetadataState;
pub use self::queries::{ResolvedVisiblePath, VisiblePathError};
pub use self::rows::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    RevisionRecord, SubtreeTombstoneAction, SubtreeTombstoneRecord,
};

pub(crate) use self::view::{
    DurableVisibilityCache, InMemoryMetadataView, MetadataView, MetadataViewSession,
    VisibleChildEntry,
};
pub(crate) use self::visibility::{unbind_matches_binding, BindingIdentity};

#[cfg(test)]
pub(crate) use self::rows::MetadataStateBuilder;
