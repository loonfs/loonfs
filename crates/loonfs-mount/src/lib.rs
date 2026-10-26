//! The engine of `loonfs mount`, which shows a namespace directory as a
//! directory on a Linux host.
//!
//! [`Engine`] keeps a copy of the mounted tree in a local database
//! (`tree.db`) and in memory, serves metadata from that copy, fetches content
//! by range into a local cache, and follows the namespace's change feed to
//! apply other hosts' commits. It's a client of the public LoonFS API through
//! `loonfs-client`, and it depends on no server crate. The [`daemon`] module
//! serves a host user's mounts over a Unix socket, the FUSE adapter builds on
//! both, and `docs/design/mount.md` describes the design.
//!
//! This version serves read-only mounts. The write path's methods exist, and
//! they refuse every change.
//!
//! The engine and the daemon are built on Unix only, because they use Unix
//! file and socket interfaces. On every other platform the crate is empty, so
//! the CLI, which depends on it everywhere, still builds there.

#![cfg(unix)]

mod admission;
mod attributes;
mod cache;
mod cache_file;
mod clock;
mod config;
mod control;
pub mod daemon;
mod database;
mod engine;
mod error;
mod fetch;
mod host;
mod invalidation;
mod item;
mod item_table;
mod progress;
mod ranges;
mod read_handle;
mod records;
mod scan;
mod status;
mod tree;
mod verify;
mod window;
mod workers;
mod write;

pub use config::{ConflictPolicy, MountAccess, MountConfig, CONTROL_DIRECTORY_NAME};
pub use engine::Engine;
pub use error::{MountError, Result};
pub use invalidation::{CachedPages, Invalidation};
pub use item::{Attr, DirEntry, Entry, ItemId, ItemKind, LocalChanges};
pub use progress::MountProgress;
pub use read_handle::{ContentState, ReadHandle};
pub use status::{
    CacheStatus, ConflictReport, MountMethod, PendingBatchStatus, PublicationStop, Readiness,
    ReflinkSupport, Status, Transports, Warning,
};
pub use write::{Mkdir, OpenWrite, Rename, SetAttr, Symlink, Truncation, WriteHandle, WriteTarget};
