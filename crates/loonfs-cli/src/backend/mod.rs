//! Unified CLI operations for embedded and remote profiles.
//!
//! Commands call [`crate::resolve::ResolvedTarget`] without depending on the selected
//! transport. Embedded profiles use the in-process `loonfs` runtime; remote
//! profiles use `loonfs-client` over HTTP. This private abstraction is for
//! CLI parity, not application extension. All methods are async and normalize
//! transport errors as [`crate::backend_error::BackendError`].

mod dispatch;
mod download;
mod embedded;
mod progress;

pub(crate) use download::FileDownload;
pub(crate) use embedded::EmbeddedBackend;
pub(crate) use progress::{
    GrepWaitProgress, MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget,
};
