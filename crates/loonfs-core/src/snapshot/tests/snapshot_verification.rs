//! Snapshot publication races and verification request counts.

use super::*;
use crate::namespace::basis::PublicationBasis;
use loonfs_objectstore::keys::snapshot_prefix;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::control::SnapshotOwner;
use std::assert_matches;

/// Checks that a snapshot of a manifest that compaction, or a fold of a newer
/// head, replaces before the snapshot's verification returns
/// `stale_head` and deletes the snapshot, so the caller folds and takes a
/// snapshot again. The deleted record stays at the snapshot's key.
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
        assert_eq!(error.code(), ErrorCode::StaleHead);
        assert_eq!(
            crate::test_support::deleted_snapshot_records(&store, &namespace_id)
                .await
                .len(),
            1
        );
    }
}

/// Checks that a snapshot without a fold keeps the manifest that the caller
/// read and leaves the WAL tail unfolded, and that a fold which replaces the
/// manifest while the snapshot record is written makes the call delete the
/// snapshot record and return `stale_head`, so the caller reads a
/// fresh basis.
#[tokio::test]
async fn a_snapshot_without_a_fold_is_verified_on_a_current_manifest_and_replaced_by_a_fold() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/one", b"data", None)
        .await
        .expect("write");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = context.clock();
    let basis = namespace.load_current_basis().await.expect("discovery");
    let (created, verified) = namespace
        .create_verified_snapshot_record(None, None, basis.clone(), &clock)
        .await
        .expect("snapshot of the current manifest");
    assert_eq!(verified.manifest.manifest(), basis.manifest.manifest());
    assert_eq!(created.record.head_seq, basis.manifest.manifest().head_seq);
    let head = namespace.load_read_state().await.expect("head");
    assert!(
        head.seq > created.record.head_seq,
        "the tail stays unfolded"
    );
    assert_eq!(
        namespace
            .load_current_basis()
            .await
            .expect("current manifest")
            .manifest
            .manifest(),
        basis.manifest.manifest()
    );

    store.block_next();
    let (outcome, ()) = tokio::join!(
        namespace.create_verified_snapshot_record(None, None, basis, &clock),
        async {
            store.wait_until_blocked().await;
            crate::test_support::namespace_store(&store, &namespace_id)
                .fold_wal()
                .await
                .expect("fold");
            store.release();
        }
    );
    assert_matches!(outcome, Err(CoreError::HeadMoved(_)));
    let snapshots = store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots");
    assert_eq!(snapshots.len(), 2);
    for key in snapshots {
        let record = record::load_snapshot_record_at_key(&store, &key)
            .await
            .expect("snapshot record")
            .state;
        assert_eq!(
            record.owner.deleted_at_ms().is_none(),
            record.snapshot_id == created.record.snapshot_id,
            "only the verified snapshot isn't deleted"
        );
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
        selected.compactor_epoch(),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("compact");
    assert_matches!(report, CompactionUnitOutcome::Published { .. });
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_ne!(selected.manifest(), current.manifest());
    let replaced = crate::manifest::tests::dropped_segments(selected, &current);
    assert!(store
        .list_prefix(&snapshot_prefix(namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
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
        .gc(&writer_at(
            "collector",
            (selected_modified_ms + crate::limits::GC_GRACE_WINDOW_MS + 1)
                .max(crate::limits::GC_GRACE_WINDOW_MS + 1),
        )
        .clock())
        .await
        .expect("collect before snapshot write");
    assert!(collection.deleted.metadata_segments >= replaced.len() as u64);
    for key in replaced {
        assert!(store.head(&key).await.expect("replaced segment").is_none());
    }
    current.manifest()
}

/// Checks that a delete of the namespace during a snapshot's verification
/// fails the verification and deletes the snapshot. The verification checks
/// that the snapshot's manifest is still the newest with a HEAD request for
/// its successor, so the test parks that request and publishes the tombstone
/// as the successor while the request waits.
#[tokio::test]
async fn namespace_deletion_during_snapshot_verification_deletes_the_snapshot() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Head,
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
                name: Some("racing".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            PublicationBasis {
                manifest: current,
                boundary: None,
            },
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
    assert_matches!(result, Err(CoreError::HeadMoved(_)));
    assert_eq!(
        crate::test_support::deleted_snapshot_records(&store, &namespace_id)
            .await
            .len(),
        1
    );
}

/// Checks that a snapshot's verification reads no manifest. It checks the
/// manifest that the fold returned with a HEAD request for the successor and
/// one read of the manifest boundary, conditional on the boundary that the
/// fold read. A manifest that gained a successor, or whose successor a
/// collection deleted below a raised boundary, is refused.
#[tokio::test]
async fn snapshot_verification_sends_a_successor_head_and_a_boundary_read_and_reads_no_manifest() {
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
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    namespace
        .raise_boundary(ManifestNo(1))
        .await
        .expect("raise the boundary");
    let write_and_fold = |path: &'static str| {
        let store = &store;
        let namespace_id = &namespace_id;
        let context = &context;
        async move {
            crate::test_support::NamespaceFixture::new(store, namespace_id, context)
                .write_file_bytes(path, b"data", None)
                .await
                .expect("write");
            crate::test_support::folded_manifest(&crate::test_support::namespace_store(
                store,
                namespace_id,
            ))
            .await
            .expect("fold the WAL tail")
        }
    };
    let basis = write_and_fold("/one").await;
    assert!(
        basis.boundary.is_some(),
        "the fold read the boundary object"
    );
    let manifest_no = basis.manifest.payload().manifest_no;
    let successor = manifest_no.successor().expect("next number");
    let successor_key = metadata_manifest_object(&namespace_id, &successor);

    store.reset();
    let verification = namespace
        .verify_snapshot_basis(basis.clone())
        .await
        .expect("verification");
    let record::SnapshotBasisVerification::Verified(verified) = verification else {
        panic!("the fold's manifest is current: {verification:?}");
    };
    assert_eq!(verified.manifest.payload().manifest_no, manifest_no);
    let requests = store.take();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(
        control_requests(&namespace_id, &requests),
        vec![
            ("head", successor_key.clone()),
            ("get", boundary(&namespace_id)),
        ]
    );

    // A successor published after the fold makes the HEAD request find it.
    write_and_fold("/two").await;
    store.reset();
    let verification = namespace
        .verify_snapshot_basis(basis.clone())
        .await
        .expect("verification");
    assert_matches!(verification, record::SnapshotBasisVerification::Invalid);
    assert_eq!(
        control_requests(&namespace_id, &store.take()).first(),
        Some(&("head", successor_key.clone()))
    );

    // A collection raised the boundary to the successor and deleted it, so
    // the HEAD request finds nothing and the boundary read refuses the
    // manifest.
    write_and_fold("/three").await;
    namespace
        .raise_boundary(successor)
        .await
        .expect("raise the boundary");
    store
        .delete(&successor_key)
        .await
        .expect("delete the successor");
    store.reset();
    let verification = namespace
        .verify_snapshot_basis(basis)
        .await
        .expect("verification");
    assert_matches!(verification, record::SnapshotBasisVerification::Invalid);
    let requests = control_requests(&namespace_id, &store.take());
    assert_eq!(
        requests[..2],
        [("head", successor_key), ("get", boundary(&namespace_id))]
    );
}

/// Checks that a creation whose put of the snapshot record was applied and
/// whose read-back failed marks the creation's own record deleted, so a later
/// put of the creation can't leave an active snapshot, with or without an
/// expiry.
#[tokio::test]
async fn a_snapshot_write_that_is_stored_and_fails_its_read_back_leaves_a_deleted_snapshot() {
    for expires_at_ms in [None, Some(10 * crate::limits::GC_GRACE_WINDOW_MS)] {
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
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let basis = crate::test_support::folded_manifest(&namespace)
            .await
            .expect("fold the WAL tail");
        let owner = SnapshotOwner::User {
            name: Some("unconfirmed".to_owned()),
            expires_at_ms,
            deleted_at_ms: None,
        };
        let clock = context.clock();
        store.fail_next(2);
        let error = namespace
            .create_snapshot_record(owner.clone(), basis.clone(), &clock)
            .await
            .expect_err("an unconfirmed snapshot write fails creation");
        assert_eq!(
            store.attempts(),
            4,
            "the put, its read-back, the cleanup's read, and its compare-and-swap"
        );
        let put = super::snapshot_cleanup::record_of_failed_creation(
            &error,
            &basis,
            owner,
            clock.request_at_ms(),
        );
        super::snapshot_cleanup::assert_a_late_put_is_refused_by_a_deleted_snapshot(&store, &put)
            .await;
    }
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
            name: Some("user".to_owned()),
            expires_at_ms: None,
            deleted_at_ms: None,
        },
        SnapshotOwner::User {
            name: Some("snapshot".to_owned()),
            expires_at_ms: Some(u64::MAX),
            deleted_at_ms: None,
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
            .map(|(record, _)| record)
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
            .get_snapshot(&head, fork)
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
