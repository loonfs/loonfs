//! Standalone execution of every core maintenance job through a registry.

use crate::common::SettableWallClock;
use loonfs::{
    CreateCheckpointOptions, GarbageCollectionJob, LoonFs, MaintenanceAssignment,
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRegistry, MetadataCompactionJob, MetadataMaintenanceJob,
    MetadataMaintenanceOptions, SharedObjectStore, WallClock,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct FixedWallClock(u64);

impl WallClock for FixedWallClock {
    fn now_ms(&self) -> Result<u64, loonfs::CoreError> {
        Ok(self.0)
    }
}

#[tokio::test]
async fn a_fresh_runtime_folds_a_short_tail_once_its_newest_commit_is_idle() {
    const COMMITTED_AT_MS: u64 = 1_750_000_000_000;
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(directory.path()).expect("local store"));
    let clock = Arc::new(SettableWallClock(AtomicU64::new(COMMITTED_AT_MS)));
    let namespace_id = namespace_id("idle-tail");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("departed-writer")
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file("/file.txt", b"body", &loonfs_test_support::test_actor())
        .await
        .expect("write file");
    writer.shutdown().await.expect("writer shutdown");
    drop(writer);

    let maintenance = LoonFs::builder_with_store(store)
        .writer_id("fresh-worker")
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("fresh-worker"));
    let job = MetadataMaintenanceJob::new(maintenance.clone());
    let disabled =
        MetadataMaintenanceJob::new(maintenance.clone()).options(MetadataMaintenanceOptions {
            idle_fold_after_ms: 0,
            ..MetadataMaintenanceOptions::default()
        });
    let idle_ms = MetadataMaintenanceOptions::default().idle_fold_after_ms;

    clock
        .0
        .store(COMMITTED_AT_MS + idle_ms - 1, Ordering::SeqCst);
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe"),
        MaintenanceProbe::Idle,
        "a tail younger than the idle period waits"
    );
    clock.0.store(COMMITTED_AT_MS + idle_ms, Ordering::SeqCst);
    assert_eq!(
        disabled.probe(&namespace_id).await.expect("probe"),
        MaintenanceProbe::Idle,
        "zero turns the idle rule off"
    );
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe"),
        MaintenanceProbe::Due
    );
    let report = job
        .run(&namespace_id, &MaintenanceCancellation::new())
        .await
        .expect("idle fold");
    assert_eq!(report.conclusion, MaintenanceConclusion::Progressed);
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.wal_tail_objects, 0, "{diagnostics:?}");
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe"),
        MaintenanceProbe::Idle
    );
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    assert_eq!(
        namespace
            .create_directory("/directory", &loonfs_test_support::test_actor())
            .await
            .expect("directory")
            .committed_at_ms,
        clock.0
    );
    let system = LoonFs::builder_with_store(store.clone())
        .writer_id("system")
        .build()
        .await
        .expect("system maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("system"));
    let future = LoonFs::builder_with_store(store.clone())
        .writer_id("future")
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("future maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("future"));
    let checkpoint = future
        .create_checkpoint_with_options(
            &namespace_id,
            "pinned",
            &CreateCheckpointOptions {
                ttl_ms: Some(1_000),
            },
        )
        .await
        .expect("checkpoint");
    assert_eq!(checkpoint.created_at_ms, clock.0);
    assert_eq!(checkpoint.expires_at_ms, Some(clock.0 + 1_000));
    let derived = writer.maintenance(loonfs_test_support::ids::writer_id("derived"));
    for maintenance in [future, derived] {
        let object_key = loonfs_objectstore::keys::metadata_segment(
            &namespace_id,
            &loonfs_types::MetadataSegmentId::generate(),
        );
        store
            .put_if_absent(&object_key, b"unreferenced".as_slice().into())
            .await
            .expect("unreferenced segment");
        store.reset();
        let kept = system.gc(&namespace_id).await.expect("system collection");
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
            .gc(&namespace_id)
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("departing-writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let threshold = MetadataMaintenanceOptions::default()
        .max_wal_tail_objects
        .get();
    for index in 0..threshold {
        namespace
            .put_file(
                &format!("/file-{index}.txt"),
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write file");
    }
    writer.shutdown().await.expect("writer shutdown");
    drop(writer);

    let maintenance = LoonFs::builder_with_store(store)
        .writer_id("standalone-worker")
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("standalone-worker"));
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
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert!(diagnostics.wal_tail_objects < threshold, "{diagnostics:?}");
}
