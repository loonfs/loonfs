//! Pins: durable records that each hold one manifest.
//!
//! A pin holds a manifest for retention, forks, stable reads, or restore. The
//! API calls a user pin a checkpoint.

mod create;
mod delete;
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
pub use self::list::CheckpointPageCursor;
pub use self::read_basis::{load_checkpoint_read_basis, CheckpointReadBasis};
pub use self::snapshot::load_snapshot_read_basis;

pub(crate) use self::create::create_pin;
#[cfg(test)]
pub(crate) use self::create::create_pin_at_basis;
pub(crate) use self::delete::delete_checkpoint;
pub(crate) use self::files::list_checkpoint_files_page;
pub(crate) use self::list::list_checkpoints_page;
pub(crate) use self::read_basis::load_pin_basis;
pub(crate) use self::snapshot::{classify_live_snapshot, delete_snapshot, extend_snapshot_expiry};
pub(crate) use self::summary::{checkpoint_summary, snapshot_summary};
