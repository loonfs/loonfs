//! Filesystem operations and shared handle state.

mod core;
mod maintenance;
mod namespaces;
mod read_result;
mod reads;
mod snapshots;
mod uploads;
mod writes;
mod writes_by_inode;

pub use maintenance::{CheckpointsPager, MetadataMaintenanceOutcome};
pub use reads::{
    ChangesPager, CheckpointFilesPager, FileRevisionsPager, InodeChildrenPager, PathEntriesPager,
    ReadView, TrashPager,
};
pub use snapshots::{SnapshotPolicy, SnapshotsPager};

pub(crate) use core::{should_invalidate_after_result, RuntimeCore, WriterBits, WriterIdentity};
pub(crate) use namespaces::delete_namespace_with_engine;
