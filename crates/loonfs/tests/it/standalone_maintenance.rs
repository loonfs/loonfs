//! Standalone execution of every core maintenance job through a registry.

use loonfs::{
    CreateNamespaceOptions, FsMaintenance, FsWriter, GarbageCollectionJob, MaintenanceAssignment,
    MaintenanceJobId, MaintenanceRegistry, MetadataCompactionJob, MetadataMaintenanceJob,
    MetadataMaintenanceOptions, PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;

#[tokio::test]
async fn a_registry_runs_every_core_job_without_a_writer() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = namespace_id("standalone-maintenance");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("departing-writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("namespace");
    let threshold = MetadataMaintenanceOptions::default()
        .max_wal_tail_segments
        .get();
    for index in 0..threshold {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/file-{index}.txt"),
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write file");
    }
    writer.shutdown().await.expect("writer shutdown");
    drop(writer);

    let maintenance = FsMaintenance::builder_with_store(store)
        .actor_id("standalone-worker")
        .build()
        .await
        .expect("maintenance");
    let registry = MaintenanceRegistry::new();
    registry
        .register(Arc::new(MetadataMaintenanceJob::new(maintenance.clone())))
        .expect("metadata job");
    registry
        .register(Arc::new(MetadataCompactionJob::new(maintenance.clone())))
        .expect("metadata compaction job");
    registry
        .register(Arc::new(GarbageCollectionJob::new(maintenance.clone())))
        .expect("garbage collection job");

    for job in [
        MaintenanceJobId::METADATA,
        MaintenanceJobId::METADATA_COMPACTION,
        MaintenanceJobId::GC,
    ] {
        let result = registry
            .execute(MaintenanceAssignment {
                namespace_id: namespace_id.clone(),
                job,
                continuation: None,
            })
            .await;
        assert!(result.is_ok(), "{job} failed: {:?}", result.err());
    }
    let diagnostics = maintenance
        .get_namespace_diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert!(diagnostics.wal_tail_segments < threshold, "{diagnostics:?}");
}
