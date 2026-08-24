//! Purpose-specific filesystem handles.
//!
//! [`FsWriter`] mutates namespaces, [`FsReader`] serves reads, and [`FsAdmin`]
//! runs explicit maintenance. Each handle must be opened in the Tokio runtime
//! where it will be used. Prefer builders that accept
//! [`StoreConfig`](crate::StoreConfig); use `builder_with_store` only when the
//! supplied store is safe to use from that runtime.

mod admin;
mod builder_core;
mod reader;
mod writer;

pub use admin::{FsAdmin, FsAdminBuilder};
pub use reader::{FsReader, FsReaderBuilder};
pub use writer::{FsWriter, FsWriterBuilder};

use builder_core::{owning_runtime, HandleBuilderCore};
