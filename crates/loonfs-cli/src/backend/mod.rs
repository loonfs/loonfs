//! Unified CLI operations for embedded and remote profiles.
//!
//! [`crate::resolve::ResolvedTarget`] gives commands the same interface for
//! both profile types. Embedded profiles call the in-process runtime, while
//! remote profiles use the HTTP client. Both map failures to
//! [`crate::backend_error::BackendError`].

mod dispatch;
mod download;
mod embedded;
mod progress;

pub(crate) use download::FileDownload;
pub(crate) use embedded::EmbeddedBackend;
pub(crate) use progress::{
    GrepWaitProgress, MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget,
};
