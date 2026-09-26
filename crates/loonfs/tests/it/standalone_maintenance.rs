//! Standalone execution of every core maintenance job through a registry.

use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, FsMaintenance, FsWriter, GarbageCollectionJob,
    MaintenanceAssignment, MaintenanceJobId, MaintenanceRegistry, MetadataCompactionJob,
    MetadataMaintenanceJob, MetadataMaintenanceOptions, PutFileOptions, SharedObjectStore,
    WallClock,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use std::sync::Arc;

#[derive(Debug)]
struct FixedWallClock(u64);

impl WallClock for FixedWallClock {
    fn now_ms(&self) -> Result<u64, loonfs::CoreError> {
        Ok(self.0)
    }
}

#[tokio::test]
async fn injected_wall_time_collects_objects_the_system_clock_keeps() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(FixedWallClock(u64::MAX / 2));
    let namespace_id = namespace_id("wall-clock");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("writer")
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    assert_eq!(
        writer
            .create_directory(
                &namespace_id,
                "/directory",
                CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("directory")
            .committed_at_ms,
        clock.0
    );
    let system = FsMaintenance::builder_with_store(store.clone())
        .actor_id("system")
        .build()
        .await
        .expect("system maintenance");
    let future = FsMaintenance::builder_with_store(store.clone())
        .actor_id("future")
        .wall_clock(clock)
        .build()
        .await
        .expect("future maintenance");
    let derived = writer.maintenance_handle("derived").expect("maintenance");
    for maintenance in [future, derived] {
        let object_key = loonfs_objectstore::keys::metadata_segment(
            &namespace_id,
            &loonfs_api::MetadataSegmentId::generate(),
        );
        store
            .put_if_absent(&object_key, b"unreferenced".as_slice().into())
            .await
            .expect("unreferenced segment");
        store.reset();
        let kept = system
            .gc_namespace(&namespace_id, &Default::default())
            .await
            .expect("system collection");
        assert_eq!(kept.deleted.metadata_segments, 0);
        assert!(!store
            .take()
            .iter()
            .any(|operation| matches!(operation, RecordedOperation::Delete { .. })));
        assert!(store
            .get(&object_key, None)
            .await
            .expect("segment")
            .is_some());
        let collected = maintenance
            .gc_namespace(&namespace_id, &Default::default())
            .await
            .expect("future collection");
        assert_eq!(collected.deleted.metadata_segments, 1);
        assert!(store.take().iter().any(|operation| {
            matches!(operation, RecordedOperation::Delete { key, .. } if key == &object_key)
        }));
        assert!(store
            .get(&object_key, None)
            .await
            .expect("segment")
            .is_none());
    }
    writer.shutdown().await.expect("shutdown");
}

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
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
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
