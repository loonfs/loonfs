//! Garbage-collection scheduling over the runtime maintenance handle.

use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport,
};
use crate::{
    ErrorCode, FsMaintenance, GcConfig, GcResponse, NamespaceId, Result, RunMaintenanceRequest,
    RunMaintenanceResponse, RuntimeError,
};
use async_trait::async_trait;
use loonfs_api::GcRequest;
use loonfs_core::limits::{
    CONTENT_RECLAMATION_GRACE_MS, GC_SAFETY_MARGIN_MS, UPLOAD_SESSION_LEASE_MS,
};

pub(crate) fn upload_session_reclaim_at_ms(session_durable_at_ms: u64) -> u64 {
    session_durable_at_ms
        .saturating_add(UPLOAD_SESSION_LEASE_MS)
        .saturating_add(GcConfig::default().grace_window_ms)
        .saturating_add(GC_SAFETY_MARGIN_MS)
}

pub(crate) fn completed_upload_reclaim_at_ms(completion_observed_at_ms: u64) -> u64 {
    completion_observed_at_ms
        .saturating_add(CONTENT_RECLAMATION_GRACE_MS)
        .saturating_add(GC_SAFETY_MARGIN_MS)
}

/// Runs one complete garbage-collection pass.
pub struct GarbageCollectionJob {
    maintenance: FsMaintenance,
}

impl GarbageCollectionJob {
    /// Creates a garbage-collection job over a maintenance handle.
    pub fn new(maintenance: FsMaintenance) -> Self {
        Self { maintenance }
    }
}

#[async_trait]
impl MaintenanceJob for GarbageCollectionJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::GC
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        let response = match self
            .maintenance
            .run_maintenance(
                namespace_id,
                RunMaintenanceRequest::Gc(GcRequest::default()),
            )
            .await
        {
            Ok(response) => response,
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                return Ok(MaintenanceRunReport::concluded(
                    MaintenanceConclusion::NotEnabled,
                ));
            }
            Err(error) => return Err(error),
        };
        let RunMaintenanceResponse::Gc(gc) = response else {
            return Err(RuntimeError::Core(loonfs_core::Error::Internal(
                "maintenance GC returned a non-GC response".to_owned(),
            )));
        };
        let follow_up = if gc.reclaim_after_ms.is_some() {
            loonfs_core::control::load_namespace_read_state(
                self.maintenance.core.store(),
                namespace_id,
            )
            .await
            .map_err(loonfs_core::Error::ControlObjectLoad)?
            .fork_basis
            .map(|basis| (MaintenanceJobId::GC, basis.manifest.owner_namespace_id))
        } else {
            None
        };
        let mut report = gc_run_result(gc);
        report.follow_up = follow_up;
        Ok(report)
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

fn gc_run_result(gc: GcResponse) -> MaintenanceRunReport {
    MaintenanceRunReport {
        conclusion: gc_conclusion(&gc),
        not_before_ms: gc.next_reclamation_at_ms,
        follow_up: None,
    }
}

fn gc_conclusion(gc: &GcResponse) -> MaintenanceConclusion {
    if reclaimed_anything(gc) {
        MaintenanceConclusion::Progressed
    } else {
        MaintenanceConclusion::Idle
    }
}

fn reclaimed_anything(gc: &GcResponse) -> bool {
    gc.deleted.wal_segments > 0
        || gc.deleted.metadata_segments > 0
        || gc.deleted.manifests > 0
        || gc.deleted.content_objects > 0
        || gc.deleted.upload_sessions > 0
        || gc.deleted.retired_content_objects > 0
        || gc.deleted_checkpoints_by_owner.fork > 0
        || gc.deleted_checkpoints_by_owner.expired > 0
        || gc.deleted_checkpoints_by_owner.snapshot > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_is_idle_until_it_reclaims_content() {
        let mut report = GcResponse::empty(NamespaceId::parse("demo").expect("namespace"));
        assert_eq!(gc_conclusion(&report), MaintenanceConclusion::Idle);
        report.deleted.content_objects = 1;
        assert_eq!(gc_conclusion(&report), MaintenanceConclusion::Progressed);
    }
}
