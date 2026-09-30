//! Purpose-specific filesystem handles.
//!
//! [`FsWriter`] is the write-capable runtime. It owns the store client, the
//! caches, the admission budgets, and shutdown. It creates and forks
//! namespaces, and it opens a [`NamespaceWriter`] for each namespace the host
//! writes. A `NamespaceWriter` carries one namespace's mutations, uploads,
//! and snapshots, and its commits go through that namespace's writer session.
//! The host owns each session: it lives while the host holds a handle for it.
//! [`FsReader`] serves reads, and [`FsMaintenance`] runs explicit
//! maintenance.
//!
//! Each handle must be opened in the Tokio runtime where it will be used.
//! Prefer builders that accept [`StoreConfig`](crate::StoreConfig); use
//! `builder_with_store` only when the supplied store is safe to use from that
//! runtime.

mod builder_core;
mod maintenance;
mod namespace_writer;
mod reader;
mod writer;

pub use maintenance::{FsMaintenance, FsMaintenanceBuilder};
pub use namespace_writer::NamespaceWriter;
pub use reader::{FsReader, FsReaderBuilder};
pub use writer::{FsWriter, FsWriterBuilder};

use builder_core::{owning_runtime, HandleBuilderCore};
