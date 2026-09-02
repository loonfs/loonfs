//! Unified CLI operations for embedded and remote profiles.
//!
//! [`crate::resolve::ResolvedTarget`] gives commands the same interface for
//! both profile types. Embedded profiles call the in-process runtime, while
//! remote profiles use the HTTP client. Both map failures to
//! [`crate::error::CliError`].

mod dispatch;
mod download;
mod embedded;
mod step_budget;

pub(crate) use download::FileDownload;
pub(crate) use embedded::EmbeddedBackend;
pub(crate) use step_budget::{
    GrepWaitProgress, MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget,
};
