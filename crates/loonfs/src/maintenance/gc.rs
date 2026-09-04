//! Garbage-collection scheduling over the runtime maintenance handle.

use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport,
};
use crate::{
    ErrorCode, FsMaintenance, GcConfig, GcResponse, MaintenanceRunRequest, MaintenanceRunResponse,
    NamespaceId, Result, RuntimeError,
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

/// Runs one bounded garbage-collection pass.
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
        continuation: Option<&str>,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        let response = match self
            .maintenance
            .run_maintenance(
                namespace_id,
                MaintenanceRunRequest::Gc(GcRequest {
                    cursor: continuation.map(str::to_owned),
                    ..GcRequest::default()
                }),
            )
            .await
        {
            Ok(response) => response,
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                return Ok(MaintenanceRunReport::concluded(
                    MaintenanceConclusion::NotEnabled,
                ));
            }
            Err(error) if continuation.is_some() && error.code() == ErrorCode::InvalidRequest => {
                tracing::warn!(
                    namespace_id = %namespace_id,
                    error = %error.public_message(),
                    "collection rejected its resume position; restarting the pass"
                );
                return Ok(MaintenanceRunReport::concluded(
                    MaintenanceConclusion::Superseded,
                ));
            }
            Err(error) => return Err(error),
        };
        let MaintenanceRunResponse::Gc(gc) = response else {
            return Err(RuntimeError::Core(loonfs_core::Error::Internal(
                "maintenance GC returned a non-GC response".to_owned(),
            )));
        };
        Ok(gc_run_result(gc, continuation))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

fn gc_run_result(gc: GcResponse, submitted_cursor: Option<&str>) -> MaintenanceRunReport {
    MaintenanceRunReport {
        conclusion: gc_conclusion(&gc, submitted_cursor),
        continuation: gc.next_cursor,
        not_before_ms: gc.next_reclamation_at_ms,
        follow_up: None,
    }
}

fn gc_conclusion(gc: &GcResponse, submitted_cursor: Option<&str>) -> MaintenanceConclusion {
    match gc.next_cursor.as_deref() {
        Some(next_cursor) if Some(next_cursor) == submitted_cursor => {
            MaintenanceConclusion::Blocked
        }
        Some(_) => MaintenanceConclusion::Progressed,
        None if reclaimed_anything(gc) => MaintenanceConclusion::Progressed,
        None if gc.budget_exhausted || gc.retention_degraded => MaintenanceConclusion::Blocked,
        None => MaintenanceConclusion::Idle,
    }
}

fn reclaimed_anything(gc: &GcResponse) -> bool {
    gc.deleted.wal_segments > 0
        || gc.deleted.metadata_segments > 0
        || gc.deleted.manifests > 0
        || gc.deleted.checkpoint_records > 0
        || gc.deleted.upload_sessions > 0
        || gc.released_checkpoints.fork > 0
        || gc.released_checkpoints.expired > 0
        || gc.released_checkpoints.missing_basis > 0
        || gc.released_checkpoints.snapshot > 0
}
