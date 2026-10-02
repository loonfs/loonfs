//! The runtime and the namespace handle.
//!
//! The API has two nouns. A [`LoonFs`] is the runtime. It owns the store
//! client, the caches, and the read budgets. A [`Namespace`] acts on one
//! namespace, and its methods take no namespace id.
//!
//! Both have one of two modes. [`ReadOnly`] reads. [`Writable`] reads and
//! writes. A `LoonFs<ReadOnly>` returns only `Namespace<ReadOnly>` handles,
//! from [`LoonFs::namespace`]; they own no writer session and cost nothing
//! to create. A `LoonFs<Writable>` also owns the writer identity, the
//! publication service, the per-namespace admission limits, and shutdown,
//! and runs its work under an execution budget it may share. It creates and
//! forks namespaces, and [`LoonFs::open_namespace`] returns a
//! `Namespace<Writable>`, which is that namespace's writer session. The host
//! owns each session: it lives while the host holds a handle for it.
//!
//! Maintenance is a capability of a writable runtime.
//! [`LoonFs::maintenance`] returns a [`Maintenance`] value that runs folds,
//! compaction, checkpoints, retention, and garbage collection. A process that
//! only maintains builds a writable runtime and never opens a namespace.
//!
//! Build each runtime inside the Tokio runtime where it will be used. Prefer
//! the builder that takes a [`StoreConfig`](crate::StoreConfig); use
//! `builder_with_store` only when the supplied store is safe to use from
//! that Tokio runtime.

mod builder;
mod loonfs;
mod maintenance;
mod namespace;

pub use builder::LoonFsBuilder;
pub use loonfs::{LoonFs, ReadOnly, Writable};
pub use maintenance::{Maintenance, MaintenanceCancellation};
pub use namespace::Namespace;
