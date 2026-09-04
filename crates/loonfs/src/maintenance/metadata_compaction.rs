use super::runner::RECONCILE_INTERVAL_MS;
use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport,
};
use crate::{FsMaintenance, MetadataCompactionOutcome, NamespaceId, Result};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(super) const MAX_CONCURRENT_COMPACTIONS: usize = 2;

/// Runs streaming metadata compaction under a process-local permit limit.
pub struct MetadataCompactionJob {
    maintenance: FsMaintenance,
    permits: Arc<Semaphore>,
}

impl MetadataCompactionJob {
    /// Creates a compaction job over a maintenance handle.
    pub fn new(maintenance: FsMaintenance) -> Self {
        Self {
            maintenance,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_COMPACTIONS)),
        }
    }

    /// Sets the maximum compactions this job runs concurrently.
    pub fn max_concurrent(mut self, max_concurrent: NonZeroUsize) -> Self {
        self.permits = Arc::new(Semaphore::new(max_concurrent.get()));
        self
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for MetadataCompactionJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::METADATA_COMPACTION
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        let Ok(_permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            return Ok(MaintenanceRunReport {
                conclusion: MaintenanceConclusion::Blocked,
                continuation: None,
                not_before_ms: Some(
                    loonfs_core::time::current_time_ms()?.saturating_add(RECONCILE_INTERVAL_MS),
                ),
                follow_up: None,
            });
        };
        let response = self
            .maintenance
            .compact_metadata_with(namespace_id, cancellation)
            .await?;
        let (conclusion, follow_up) = match response.outcome {
            MetadataCompactionOutcome::Published { .. }
            | MetadataCompactionOutcome::BoundedMergePublished => (
                MaintenanceConclusion::Progressed,
                Some(MaintenanceJobId::METADATA),
            ),
            MetadataCompactionOutcome::NotNeeded => (MaintenanceConclusion::Idle, None),
            MetadataCompactionOutcome::Superseded
            | MetadataCompactionOutcome::Abandoned
            | MetadataCompactionOutcome::Fenced => (MaintenanceConclusion::Superseded, None),
            MetadataCompactionOutcome::Cancelled => (MaintenanceConclusion::Blocked, None),
        };
        Ok(MaintenanceRunReport {
            conclusion,
            continuation: None,
            not_before_ms: None,
            follow_up,
        })
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}
