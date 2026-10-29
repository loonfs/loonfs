//! Maintenance from a runtime that holds no writer session for the
//! namespace.

use loonfs::{
    CreateSnapshotOptions, LoonFs, MaintainMetadataOptions, SharedObjectStore, WalFoldStepOutcome,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use std::assert_matches;
use std::sync::Arc;

#[tokio::test]
async fn a_fresh_runtime_folds_a_short_tail_once_its_newest_commit_is_idle() {
    const COMMITTED_AT_MS: u64 = 1_750_000_000_000;
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(directory.path()).expect("local store"));
    let clock = Arc::new(ManualClock::new(COMMITTED_AT_MS));
    let namespace_id = namespace_id("idle-tail");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("departed-writer"))
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

    let disabled = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("idle-off-worker"))
        .wall_clock(clock.clone())
        .config(loonfs::RuntimeConfig {
            idle_fold_after_ms: None,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("maintenance without idle folds")
        .maintenance(loonfs_test_support::ids::writer_id("idle-off-worker"));
    let maintenance = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("fresh-worker"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("fresh-worker"));
    let idle_ms = loonfs::DEFAULT_IDLE_FOLD_AFTER_MS;

    clock.set_ms(COMMITTED_AT_MS + idle_ms - 1);
    assert_eq!(
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("young tail")
            .wal_fold,
        WalFoldStepOutcome::NotNeeded,
        "a tail younger than the idle period waits"
    );
    clock.set_ms(COMMITTED_AT_MS + idle_ms);
    assert_eq!(
        disabled
            .maintain_metadata(&namespace_id)
            .await
            .expect("idle rule off")
            .wal_fold,
        WalFoldStepOutcome::NotNeeded,
        "a runtime without an idle fold age leaves a short tail"
    );
    assert_matches!(
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("idle fold")
            .wal_fold,
        WalFoldStepOutcome::Folded { .. }
    );
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.wal_tail_objects, 0, "{diagnostics:?}");
    assert_eq!(
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("folded tail")
            .wal_fold,
        WalFoldStepOutcome::NotNeeded
    );
}

#[tokio::test]
async fn injected_wall_time_collects_objects_the_system_clock_keeps() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(ManualClock::new(u64::MAX / 2));
    let namespace_id = namespace_id("wall-clock");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
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
        clock.now_ms()
    );
    let system = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("system"))
        .build()
        .await
        .expect("system maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("system"));
    let future_runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("future"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("future maintenance");
    let future = future_runtime.maintenance(loonfs_test_support::ids::writer_id("future"));
    let checkpoint = future_runtime
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .create_snapshot_with_options(
            "pinned",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(1_000),
            },
        )
        .await
        .expect("snapshot");
    assert_eq!(checkpoint.created_at_ms, clock.now_ms());
    assert_eq!(checkpoint.expires_at_ms, Some(clock.now_ms() + 1_000));
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
async fn every_core_maintenance_operation_runs_without_a_writer() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = namespace_id("standalone-maintenance");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("departing-writer"))
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
    let threshold = MaintainMetadataOptions::default()
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
        .writer_id(loonfs_test_support::ids::writer_id("standalone-worker"))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("standalone-worker"));
    maintenance
        .maintain_metadata(&namespace_id)
        .await
        .expect("metadata maintenance");
    maintenance
        .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        .await
        .expect("metadata compaction");
    maintenance
        .gc(&namespace_id)
        .await
        .expect("garbage collection");
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert!(diagnostics.wal_tail_objects < threshold, "{diagnostics:?}");
}
