//! Snapshots: durable pin records that each keep one manifest.
//!
//! A snapshot keeps a manifest for retention, forks, stable reads, or
//! restore. Each snapshot has an owner: `user` for a snapshot that a caller
//! created, or `fork` for the snapshot that keeps a fork's base manifest.

mod create;
mod files;
mod lifecycle;
mod list;
mod read_basis;
pub(crate) mod record;
mod summary;
#[cfg(test)]
mod tests;

pub use self::files::{
    ListSnapshotFilesOptions, SnapshotFile, SnapshotFilesPage, SnapshotFilesPageCursor,
};
pub use self::list::{CheckpointPageCursor, SnapshotPageCursor};

pub(crate) use self::lifecycle::ensure_unexpired;
pub(crate) use self::read_basis::load_pin_basis;
pub(crate) use self::summary::{checkpoint_summary, snapshot_summary, user_pin_snapshot};
