//! Registration and direct execution for maintenance jobs.

use super::{
    MaintenanceCancellation, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceRunReport,
};
use crate::{NamespaceId, Result, RuntimeError};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Thread-safe set of maintenance jobs.
#[derive(Clone, Default)]
pub struct MaintenanceRegistry {
    jobs: Arc<Mutex<BTreeMap<MaintenanceJobId, Arc<dyn MaintenanceJob>>>>,
}

/// One maintenance job assigned to one namespace.
pub struct MaintenanceAssignment {
    /// Namespace to maintain.
    pub namespace_id: NamespaceId,
    /// Registered job to run.
    pub job: MaintenanceJobId,
    /// Process-local resume position.
    pub continuation: Option<String>,
}

impl MaintenanceRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one job, rejecting duplicate identifiers.
    pub fn register(&self, job: Arc<dyn MaintenanceJob>) -> Result<MaintenanceJobId> {
        let id = job.id();
        let mut jobs = self.lock();
        if jobs.contains_key(&id) {
            return Err(RuntimeError::Config(format!(
                "maintenance job `{id}` is already registered"
            )));
        }
        jobs.insert(id, job);
        Ok(id)
    }

    /// Gets a registered job.
    pub fn get(&self, id: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.lock().get(&id).cloned()
    }

    /// Returns registered identifiers in sorted order.
    pub fn job_ids(&self) -> Vec<MaintenanceJobId> {
        self.lock().keys().copied().collect()
    }

    /// Probes one registered job.
    pub async fn probe(
        &self,
        id: MaintenanceJobId,
        namespace_id: &NamespaceId,
    ) -> Result<MaintenanceProbe> {
        self.require(id)?.probe(namespace_id).await
    }

    /// Runs one registered job with a fresh cancellation token.
    pub async fn run(
        &self,
        id: MaintenanceJobId,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceRunReport> {
        self.require(id)?
            .run(namespace_id, continuation, &MaintenanceCancellation::new())
            .await
    }

    /// Runs one assignment with a fresh cancellation token.
    pub async fn execute(&self, assignment: MaintenanceAssignment) -> Result<MaintenanceRunReport> {
        self.run(
            assignment.job,
            &assignment.namespace_id,
            assignment.continuation.as_deref(),
        )
        .await
    }

    fn require(&self, id: MaintenanceJobId) -> Result<Arc<dyn MaintenanceJob>> {
        self.get(id).ok_or_else(|| {
            RuntimeError::Config(format!("maintenance job `{id}` is not registered"))
        })
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<MaintenanceJobId, Arc<dyn MaintenanceJob>>> {
        self.jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
