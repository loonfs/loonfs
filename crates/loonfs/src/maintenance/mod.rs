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
    MaintenanceHint, MaintenanceHintObserver, MaintenanceHintReceiver, MaintenanceHintRelay,
};
pub use job::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport, NamespacePublication,
};
pub use metadata::MetadataMaintenanceJob;
pub use metadata_compaction::MetadataCompactionJob;
pub use registry::MaintenanceRegistry;
pub use runner::{
    MaintenanceHandle, MaintenanceRunner, MaintenanceRunnerBuilder, MaintenanceRunnerStats,
};

/// One maintenance job assigned to one namespace.
pub struct MaintenanceAssignment {
    /// Namespace to maintain.
    pub namespace_id: crate::NamespaceId,
    /// Registered job to run.
    pub job: MaintenanceJobId,
    /// Process-local resume position.
    pub continuation: Option<String>,
}
