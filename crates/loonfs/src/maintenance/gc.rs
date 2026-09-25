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
    CONTENT_RECLAMATION_GRACE_MS, GC_SAFETY_MARGIN_MS, NAMESPACE_RETIREMENT_GRACE_MS,
    UPLOAD_SESSION_LEASE_MS,
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

pub(crate) fn namespace_reclaim_at_ms(deleted_at_ms: u64) -> u64 {
    deleted_at_ms
        .saturating_add(
            GcConfig::default()
                .grace_window_ms
                .max(NAMESPACE_RETIREMENT_GRACE_MS),
        )
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
        let follow_up = if gc.reclaimable_at_ms.is_some() {
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
        Ok(MaintenanceRunReport {
            conclusion: gc_conclusion(&gc),
            not_before_ms: gc.next_reclamation_at_ms,
            follow_up,
        })
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
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
        || gc.deleted.retired_content_objects > 0
        || gc.deleted.upload_sessions > 0
        || gc.deleted_checkpoints_by_owner.fork > 0
        || gc.deleted_checkpoints_by_owner.user > 0
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

    #[tokio::test]
    async fn repeated_retirement_lists_once_without_deletes_and_reports_idle() {
        use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
        use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};
        use std::sync::Arc;

        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix("namespaces/retired/content/"),
        ));
        let namespace_id = NamespaceId::parse("retired").expect("namespace");
        let writer = crate::FsWriter::builder_with_store(store.clone())
            .writer_id("retirement-test")
            .build()
            .await
            .expect("writer");
        writer
            .create_namespace(
                &namespace_id,
                crate::CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("namespace");
        for _ in 0..3 {
            let key = loonfs_objectstore::keys::content_blob(
                &namespace_id,
                &loonfs_api::ContentId::generate(),
            );
            store
                .put_if_absent(&key, bytes::Bytes::from_static(b"content"))
                .await
                .expect("content");
        }
        let mut context = loonfs_core::MutationContext {
            writer_id: loonfs_api::WriterId::parse("deleter").expect("writer id"),
            now_ms: 1_000,
        };
        loonfs_core::publish::NamespaceCommitEngine::new(namespace_id.clone())
            .delete_namespace(store.as_ref(), Default::default(), &context)
            .await
            .expect("delete");
        context.now_ms = namespace_reclaim_at_ms(context.now_ms);
        store.reset();
        let first = loonfs_core::gc_namespace(
            store.as_ref(),
            &namespace_id,
            &GcConfig::default(),
            &context,
        )
        .await
        .expect("first pass");
        assert_eq!(first.deleted.retired_content_objects, 3);
        assert_eq!(gc_conclusion(&first), MaintenanceConclusion::Progressed);
        assert_eq!(
            store.counts(),
            StoreCounts {
                lists: 1,
                deletes: 3,
                ..Default::default()
            }
        );
        store.reset();
        let repeated = loonfs_core::gc_namespace(
            store.as_ref(),
            &namespace_id,
            &GcConfig::default(),
            &context,
        )
        .await
        .expect("repeat pass");
        assert_eq!(repeated.deleted, loonfs_api::DeletedObjectCounts::default());
        assert_eq!(gc_conclusion(&repeated), MaintenanceConclusion::Idle);
        assert_eq!(
            store.counts(),
            StoreCounts {
                lists: 1,
                ..Default::default()
            }
        );
        writer.shutdown().await.expect("shutdown");
    }
}
