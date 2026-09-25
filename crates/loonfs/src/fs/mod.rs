//! Filesystem operations and shared handle state.

mod core;
mod maintenance;
mod namespaces;
mod read_result;
mod reads;
mod snapshots;
mod speculative_read;
mod uploads;
mod writes;

pub use maintenance::CheckpointsPager;
pub use reads::{
    ChangesPager, FileRevisionsPager, FsReadSnapshot, InodeChildrenPager, PathEntriesPager,
    TrashPager,
};
pub use snapshots::{SnapshotPolicy, SnapshotsPager};

pub(crate) use core::{should_invalidate_after_result, ReadCore, WriterBits, WriterIdentity};
pub(crate) use namespaces::delete_namespace_with_engine;
pub(crate) use writes::publish_batch_with_engine;
