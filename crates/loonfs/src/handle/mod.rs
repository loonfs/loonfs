//! Purpose-specific filesystem handles.
//!
//! The API has two nouns: a runtime and a namespace. The runtime is
//! [`FsWriter`] or [`FsReader`]. It owns the store client, the caches, and,
//! for a writer, the admission budgets and shutdown. A [`Namespace`] handle
//! acts on one namespace, and its methods take no namespace id.
//!
//! A namespace handle has one of two modes. A `Namespace<ReadOnly>` comes
//! from [`FsReader::namespace`]. It reads, owns no writer session, and costs
//! nothing to create. A `Namespace<Writable>` comes from
//! [`FsWriter::open_namespace`]. It reads the same way, and it is also the
//! namespace's writer session, so its mutations, uploads, and snapshots go
//! through one publication queue. The host owns each session: it lives while
//! the host holds a handle for it. [`FsWriter`] also creates and forks
//! namespaces, and [`FsMaintenance`] runs explicit maintenance.
//!
//! Each runtime must be opened in the Tokio runtime where it will be used.
//! Prefer builders that accept [`StoreConfig`](crate::StoreConfig); use
//! `builder_with_store` only when the supplied store is safe to use from that
//! runtime.

mod builder_core;
mod maintenance;
mod namespace;
mod reader;
mod writer;

pub use maintenance::{FsMaintenance, FsMaintenanceBuilder};
pub use namespace::{Namespace, ReadOnly, Writable};
pub use reader::{FsReader, FsReaderBuilder};
pub use writer::{FsWriter, FsWriterBuilder};

use builder_core::{owning_runtime, HandleBuilderCore};
