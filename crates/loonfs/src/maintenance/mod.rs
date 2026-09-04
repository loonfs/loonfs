//! Runtime maintenance jobs, hint delivery, registry execution, and optional scheduling.

mod admission;
mod gc;
mod hints;
mod job;
mod metadata;
mod metadata_compaction;
mod registry;
mod runner;
#[cfg(test)]
mod tests;

pub use gc::GarbageCollectionJob;
pub(crate) use gc::{completed_upload_reclaim_at_ms, upload_session_reclaim_at_ms};
pub use hints::{
    maintenance_hint_relay, MaintenanceHint, MaintenanceHintObserver, MaintenanceHintReceiver,
};
pub use job::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport, NamespacePublication,
};
pub use metadata::MetadataMaintenanceJob;
pub use metadata_compaction::MetadataCompactionJob;
pub use registry::{MaintenanceAssignment, MaintenanceRegistry};
pub use runner::{
    MaintenanceHandle, MaintenanceRunner, MaintenanceRunnerBuilder, MaintenanceRunnerStats,
};
