//! Purpose-specific runtime handles.
//!
//! One handle per job, each opened asynchronously inside the Tokio runtime
//! that will drive it:
//!
//! - [`FsWriter`] mutates namespaces and optionally schedules non-destructive
//!   maintenance after writes, controlled by
//!   [`FsBackgroundWork`](crate::FsBackgroundWork).
//! - [`FsReader`] serves latest-view reads. It owns no writer session and
//!   starts no maintenance.
//! - [`FsAdmin`] runs explicit maintenance: status, checkpoints, retention
//!   advancement, and garbage collection, always as one-shot calls in the
//!   caller's task.
//!
//! Builders prefer [`StoreConfig`](crate::StoreConfig) so the object-store
//! client is constructed inside the handle's runtime ownership domain. The
//! `builder_with_store` escape hatches are for callers who know the store is
//! safe in that domain — do not use them to share one provider client across
//! unrelated runtimes; open another handle from config instead.

mod admin;
mod builder_core;
mod reader;
mod writer;

pub use admin::{FsAdmin, FsAdminBuilder};
pub use reader::{FsReader, FsReaderBuilder};
pub use writer::{FsWriter, FsWriterBuilder};

use builder_core::{owning_runtime, HandleBuilderCore};
