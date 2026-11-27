//! Maintenance from a runtime that holds no writer session for the
//! namespace.

use loonfs::{CreateSnapshotOptions, FoldWalOutcome, FoldWalResponse, LoonFs, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use std::assert_matches;
use std::sync::Arc;

/// Checks that a runtime without a session folds a short tail once the
/// tail's oldest commit reaches the runtime's fold age, while a newer commit
/// in the tail is younger, and that a runtime with a longer fold age leaves
/// the tail.
#[tokio::test]
async fn a_fresh_runtime_folds_a_short_tail_once_its_oldest_commit_reaches_the_fold_age() {
    const COMMITTED_AT_MS: u64 = 1_750_000_000_000;
    const NEWER_COMMIT_AFTER_MS: u64 = 10 * 60 * 1_000;
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(directory.path()).expect("local store"));
    let clock = Arc::new(ManualClock::new(COMMITTED_AT_MS));
    let namespace_id = namespace_id("aged-tail");
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
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
    clock.set_ms(COMMITTED_AT_MS + NEWER_COMMIT_AFTER_MS);
    namespace
        .put_file("/newer.txt", b"newer", &loonfs_test_support::test_actor())
        .await
        .expect("write a newer file");
    writer.shutdown().await.expect("writer shutdown");
    drop(writer);

    let fold_age_ms = loonfs::DEFAULT_FOLD_AGE_MS.get();
    let patient = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("patient-worker"))
        .wall_clock(clock.clone())
        .config(loonfs::RuntimeConfig {
            fold_age_ms: std::num::NonZeroU64::new(fold_age_ms + 1).expect("a nonzero fold age"),
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("maintenance with a longer fold age")
        .maintenance(loonfs_test_support::ids::writer_id("patient-worker"));
    let maintenance = LoonFs::builder_with_store(store, loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("fresh-worker"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("fresh-worker"));

    clock.set_ms(COMMITTED_AT_MS + fold_age_ms - 1);
    assert_eq!(
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("young tail")
            .wal_fold,
        None,
        "a tail whose oldest commit is younger than the fold age waits"
    );
    clock.set_ms(COMMITTED_AT_MS + fold_age_ms);
    assert_eq!(
        patient
            .maintain_metadata(&namespace_id)
            .await
            .expect("longer fold age")
            .wal_fold,
        None,
        "a runtime with a longer fold age leaves a short tail"
    );
    assert_matches!(
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("fold at the fold age")
            .wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            ..
        })
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
        None
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
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
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
    let system = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("system"))
        .build()
        .await
        .expect("system maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("system"));
    let future_runtime = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("future"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("future maintenance");
    let future = future_runtime.maintenance(loonfs_test_support::ids::writer_id("future"));
    let snapshot = future_runtime
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .create_snapshot_with_options(&CreateSnapshotOptions {
            name: Some("pinned".to_owned()),
            ttl_ms: std::num::NonZeroU64::new(1_000),
        })
        .await
        .expect("snapshot");
    assert_eq!(snapshot.created_at_ms, clock.now_ms());
    assert_eq!(snapshot.expires_at_ms, Some(clock.now_ms() + 1_000));
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
