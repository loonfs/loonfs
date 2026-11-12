//! Snapshot publication races and verification request counts.

use super::*;
use loonfs_objectstore::keys::snapshot_prefix;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::control::SnapshotOwner;
use std::assert_matches;

/// Checks that a snapshot of a manifest that compaction, or a fold of a newer
/// head, replaces before the snapshot's verification returns
/// `snapshot_unavailable` and deletes the snapshot, so the caller folds and takes a
/// snapshot again.
#[tokio::test]
async fn a_snapshot_of_a_manifest_replaced_before_verification_is_refused_and_deleted() {
    for advance_head in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = BlockingStore::new(
            MetadataMapStore::aged(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::metadata_segment(),
            ),
            KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        );
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context)
            .await
            .expect("bootstrap");
        for path in ["/one", "/two"] {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .write_file_bytes(path, b"data", None)
                .await
                .expect("write");
            crate::test_support::namespace_store(&store, &namespace_id)
                .fold_wal()
                .await
                .expect("fold");
        }
        let selected = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("selected manifest");
        store.block_next();
        let (created, ()) = tokio::join!(create_snapshot(&store, &namespace_id, &context), async {
            store.wait_until_blocked().await;
            if advance_head {
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                    .write_file_bytes("/three", b"data", None)
                    .await
                    .expect("advance head");
                crate::test_support::namespace_store(&store, &namespace_id)
                    .fold_wal()
                    .await
                    .expect("fold newer head");
            }
            let current =
                compact_and_collect_replaced_segments(&store, &namespace_id, &selected).await;
            assert_eq!(
                selected.manifest().head_seq != current.head_seq,
                advance_head
            );
            store.release();
        });
        let error = created.expect_err("the selected manifest is no longer current");
        assert_eq!(error.code(), ErrorCode::SnapshotUnavailable);
        assert!(store
            .list_prefix(&snapshot_prefix(&namespace_id))
            .await
            .expect("snapshots")
            .is_empty());
    }
}

pub(super) async fn compact_and_collect_replaced_segments<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    selected: &crate::namespace::control::LoadedManifest,
) -> loonfs_types::format::control::ManifestRef {
    let report = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .with_run_policy(crate::test_support::one_delta_run_policy())
    .engine()
    .compact_metadata_unit(
        None,
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("compact");
    assert_matches!(report, CompactionUnitOutcome::Published { .. });
    // The runtime that published the compaction raises the hint.
    crate::test_support::namespace_store(store, namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_ne!(selected.manifest(), current.manifest());
    let current_segments: BTreeSet<_> = current
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect();
    let replaced: Vec<_> = selected
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .filter(|key| !current_segments.contains(key))
        .collect();
    assert!(!replaced.is_empty());
    assert!(store
        .list_prefix(&snapshot_prefix(namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
    let options = crate::gc::GcOptions::default();
    // The replaced segments report an ancient age, and the collection runs
    // after the selected manifest has aged past the collector's grace window,
    // so the collection deletes the segments that only the selected manifest
    // lists.
    let selected_modified_ms = store
        .head(&selected.object_key)
        .await
        .expect("selected manifest metadata")
        .expect("selected manifest exists")
        .last_modified_at_ms
        .expect("test store provides a timestamp");
    let collection = crate::test_support::namespace_store(store, namespace_id)
        .gc(
            &options,
            &writer_at(
                "collector",
                (selected_modified_ms + options.grace_window_ms + 1)
                    .max(crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1),
            )
            .clock(),
        )
        .await
        .expect("collect before snapshot write");
    assert!(collection.deleted.metadata_segments >= replaced.len() as u64);
    for key in replaced {
        assert!(store.head(&key).await.expect("replaced segment").is_none());
    }
    current.manifest()
}

#[tokio::test]
async fn namespace_deletion_during_snapshot_verification_deletes_the_snapshot() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationSelector::Read,
    );
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let writer = crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("writer");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = context.clock();
    let (result, ()) = tokio::join!(
        namespace.create_snapshot_record(
            SnapshotOwner::User {
                name: "racing".to_owned(),
                expires_at_ms: None
            },
            current.manifest(),
            &clock
        ),
        async {
            store.wait_until_blocked().await;
            assert_eq!(
                store
                    .inner()
                    .list_prefix(&snapshot_prefix(&namespace_id))
                    .await
                    .expect("durable snapshot")
                    .len(),
                1
            );
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .delete_namespace(Default::default(), writer, None, &context.clock())
                .await
                .expect("delete namespace during verification");
            store.release();
        }
    );
    assert_matches!(result, Err(CoreError::SnapshotUnavailable(_)));
    assert!(store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
}

#[tokio::test]
async fn snapshot_verification_checks_manifest_identity_with_only_the_current_manifest_load() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let snapshot = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot");
    let record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("load snapshot")
        .expect("snapshot exists")
        .state;
    store.reset();
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    let mut expected = store.take();
    loonfs_test_support::stores::sort_head_runs(&mut expected);
    let mut changed_number = record.clone();
    changed_number.snapshot_id = SnapshotId::generate(
        record
            .snapshot_id
            .manifest_no()
            .successor()
            .expect("next number"),
    );
    let mut changed_checksum = record.clone();
    changed_checksum.payload_checksum = loonfs_types::sha256_digest(b"different");
    for (record, expected_verification) in [
        (record, record::SnapshotBasisVerification::Verified),
        (changed_number, record::SnapshotBasisVerification::Invalid),
        (changed_checksum, record::SnapshotBasisVerification::Invalid),
    ] {
        assert_eq!(
            crate::test_support::namespace_store(&store, &record.namespace_id)
                .verify_snapshot_basis(&record)
                .await
                .expect("verification"),
            expected_verification
        );
        let mut requests = store.take();
        loonfs_test_support::stores::sort_head_runs(&mut requests);
        assert_eq!(requests, expected);
    }
}

#[tokio::test]
async fn a_snapshot_write_that_is_stored_and_fails_its_read_back_leaves_no_snapshot() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::Any,
        InjectedError::Transport("lost snapshot acknowledgement".to_owned()),
    )
    .apply_then_fail();
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    store.fail_next(2);
    let error = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect_err("an unconfirmed snapshot write fails creation");
    assert_matches!(error, CoreError::Store { .. });
    assert_eq!(store.attempts(), 3, "put, read-back, and cleanup delete");
    assert!(store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
}

/// Checks that a snapshot read accepts a user snapshot with or without an
/// expiry and a fork snapshot, and that every other snapshot operation refuses a
/// fork snapshot's ID with `snapshot_not_found` and leaves the snapshot in place.
#[tokio::test]
async fn a_fork_snapshot_id_is_read_like_a_user_snapshot_and_refused_by_every_other_snapshot_operation(
) {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = writer_at("gc-test", 1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let mut snapshots = Vec::new();
    for owner in [
        SnapshotOwner::User {
            name: "user".to_owned(),
            expires_at_ms: None,
        },
        SnapshotOwner::User {
            name: "snapshot".to_owned(),
            expires_at_ms: Some(u64::MAX),
        },
        SnapshotOwner::Fork {
            target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
        },
    ] {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let created = namespace
            .create_snapshot_record(
                owner,
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &setup.clock(),
            )
            .await
            .expect("snapshot");
        snapshots.push(created.snapshot_id);
    }
    let [user, snapshot, fork] = &snapshots[..] else {
        panic!("expected three snapshots, got {snapshots:?}");
    };
    let head = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    let namespace =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup).namespace;
    let snapshot_read = |id| namespace.load_snapshot_read_basis(&head, id);
    snapshot_read(user)
        .await
        .expect("read a snapshot without an expiry");
    snapshot_read(snapshot)
        .await
        .expect("read a snapshot with an expiry");
    snapshot_read(fork).await.expect("read a fork snapshot");
    let cases = [
        (
            "retrieve of a fork snapshot",
            crate::NamespaceEngine::reader(crate::test_support::namespace_store(
                &store,
                &namespace_id,
            ))
            .get_snapshot(fork)
            .await
            .map(drop),
        ),
        (
            "delete of a fork snapshot",
            crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            )
            .engine()
            .delete_snapshot(fork)
            .await
            .map(drop),
        ),
    ];
    for (label, result) in cases {
        assert_eq!(
            result.expect_err(label).code(),
            ErrorCode::SnapshotNotFound,
            "{label}"
        );
    }
    assert_eq!(
        store
            .list_prefix(&snapshot_prefix(&namespace_id))
            .await
            .expect("snapshots")
            .len(),
        3
    );
}
