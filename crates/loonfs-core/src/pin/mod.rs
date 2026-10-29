//! Pins: durable records that each keep one manifest.
//!
//! A pin keeps a manifest for retention, forks, stable reads, or restore. A
//! user pin is what the API calls a snapshot, and the maintenance inventory
//! lists every pin as a checkpoint.

mod create;
mod files;
mod list;
mod read_basis;
pub(crate) mod record;
mod snapshot;
mod summary;
#[cfg(test)]
mod tests;

pub use self::files::{
    CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor, ListCheckpointFilesOptions,
};
pub use self::list::{CheckpointPageCursor, SnapshotPageCursor};
pub use self::read_basis::SnapshotReadBasis;

pub(crate) use self::read_basis::load_pin_basis;
pub(crate) use self::snapshot::ensure_unexpired;
pub(crate) use self::summary::{checkpoint_summary, snapshot_summary, user_pin_snapshot};
