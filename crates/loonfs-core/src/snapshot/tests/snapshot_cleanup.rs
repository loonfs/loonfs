//! Collection recovery after failed or concurrent snapshot cleanup.

use super::*;
use loonfs_objectstore::keys::{self, snapshot_prefix};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::{replace_object, MetadataMapStore};
use loonfs_types::format::control::SnapshotOwner;

/// Checks the schedule where a quiet namespace's current manifest is replaced
/// and collected between a snapshot's fold and its record put, and the
/// creator's delete of the unverified record fails: collections keep working,
/// and the record, which roots nothing, is deleted once its provider age
/// reaches the grace window, although it has no expiry.
#[tokio::test]
async fn an_unverified_snapshot_left_without_its_manifest_is_deleted_at_the_grace_window() {
    let owner = SnapshotOwner::User {
        name: Some("abandoned".to_owned()),
        expires_at_ms: None,
        deleted_at_ms: None,
    };
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    // Every object is old, as in a quiet namespace, except the snapshot
    // record, whose provider timestamp is the creator's clock.
    let store = BlockingStore::new(
        FailStore::new(
            MetadataMapStore::new(
                MetadataMapStore::aged(
                    LocalFsStore::new(directory.path()).expect("store"),
                    KeyPredicate::any(),
                ),
                KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
                |mut metadata| {
                    metadata.last_modified_at_ms = Some(1_000);
                    metadata
                },
            ),
            KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
            OperationSelector::Delete,
            InjectedError::Transport("cleanup unavailable".to_owned()),
        ),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let setup = writer_at("creator", 1_000);
    bootstrap_namespace(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    for path in ["/one", "/two"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
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
    store.inner().fail_next(1);
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = setup.clock();
    let (creation, ()) = tokio::join!(
        namespace.create_snapshot_record(
            owner,
            crate::namespace::basis::PublicationBasis {
                manifest: selected.clone(),
                boundary: None,
            },
            &clock
        ),
        async {
            store.wait_until_blocked().await;
            super::snapshot_verification::compact_and_collect_replaced_segments(
                &store,
                &namespace_id,
                &selected,
            )
            .await;
            assert!(store
                .head(&selected.object_key)
                .await
                .expect("old manifest")
                .is_none());
            store.release();
        }
    );
    assert_eq!(
        creation.expect_err("verification failed").code(),
        ErrorCode::StaleHead,
        "a failed cleanup keeps the verification error"
    );
    assert_eq!(store.inner().attempts(), 1);
    store.inner().clear();
    let snapshots = store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots");
    assert_eq!(
        snapshots.len(),
        1,
        "failed cleanup left its unverified snapshot"
    );

    let grace_window_ms = crate::limits::GC_GRACE_WINDOW_MS;
    for now_ms in [1_000, 1_000 + grace_window_ms - 1] {
        let young = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&writer_at("collector", now_ms).clock())
            .await
            .expect("a record without its manifest doesn't fail the collection");
        assert_eq!(young.deleted_snapshots_by_owner.user, 0);
        assert!(store.head(&snapshots[0]).await.expect("snapshot").is_some());
    }
    let collected = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&writer_at("collector", 1_000 + grace_window_ms).clock())
        .await
        .expect("delete the record once its provider age reaches the grace window");
    assert_eq!(collected.deleted_snapshots_by_owner.user, 1);
    assert!(store.head(&snapshots[0]).await.expect("snapshot").is_none());
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&writer_at("collector", 1_001 + grace_window_ms).clock())
        .await
        .expect("next collection also succeeds");
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("current view remains readable");
}

#[tokio::test]
async fn snapshot_removed_after_listing_does_not_make_a_collected_basis_corruption() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let base = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = test_context();
    bootstrap_namespace(&base, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let snapshot = create_snapshot(&base, &namespace_id, &setup)
        .await
        .expect("snapshot");
    crate::test_support::NamespaceFixture::new(&base, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&base, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let old_manifest = metadata_manifest_object(&namespace_id, &snapshot.manifest_no);
    let store = BlockingStore::new(
        base,
        KeyPredicate::exact(&old_manifest),
        OperationSelector::Read,
    );
    let aged = writer_at("collector", crate::limits::GC_GRACE_WINDOW_MS + 1);
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = aged.clock();
    let (collection, ()) = tokio::join!(namespace.gc(&clock), async {
        store.wait_until_blocked().await;
        store
            .inner()
            .delete(&keys::snapshot_record(&namespace_id, &snapshot.snapshot_id))
            .await
            .expect("release snapshot");
        crate::test_support::namespace_store(store.inner(), &namespace_id)
            .gc(&aged.clock())
            .await
            .expect("other collector");
        assert!(store
            .inner()
            .head(&old_manifest)
            .await
            .expect("manifest")
            .is_none());
        store.release();
    });
    collection.expect("concurrent snapshot release is not corruption");
}

#[tokio::test]
async fn missing_basis_checks_each_snapshot_and_propagates_snapshot_read_errors() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("snapshot unavailable".to_owned()),
    );
    let setup = writer_at("creator", 1_000);
    bootstrap_namespace(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let mut ids = [
        SnapshotId::generate(manifest.manifest().manifest_no),
        SnapshotId::generate(manifest.manifest().manifest_no),
    ];
    ids.sort();
    for (index, id) in ids.iter().enumerate() {
        record::write_snapshot_record(
            &store,
            &loonfs_types::format::control::SnapshotPayload {
                snapshot_id: id.clone(),
                namespace_id: namespace_id.clone(),
                head_seq: manifest.manifest().head_seq,
                payload_checksum: manifest.manifest().payload_checksum.clone(),
                created_at_ms: setup.now_ms,
                owner: if index == 0 {
                    SnapshotOwner::User {
                        name: Some(format!("snapshot-{index}")),
                        expires_at_ms: Some(2_000),
                        deleted_at_ms: None,
                    }
                } else {
                    SnapshotOwner::Fork {
                        target_namespace_id: NamespaceId::parse("fork").expect("namespace"),
                    }
                },
            },
        )
        .await
        .expect("write snapshot");
    }
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // A fork snapshot's manifest is protected from its creation, so losing it
    // is corruption, even when the first listed snapshot for the same
    // manifest is independently eligible for deletion.
    store
        .delete(&manifest.object_key)
        .await
        .expect("remove snapshot's manifest");
    let aged = writer_at("collector", 2_000 + crate::limits::GC_GRACE_WINDOW_MS);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("the fork snapshot still requires the missing basis");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(error.to_string().contains(ids[1].as_str()));
    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("unreadable snapshot is not absent");
    assert_eq!(error.code(), ErrorCode::ServerError);
    store.clear();
    let key = keys::snapshot_record(&namespace_id, &ids[0]);
    replace_object(&store, &key, Bytes::from_static(b"not json"))
        .await
        .expect("malformed snapshot");
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("malformed snapshot is not deletable");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    for id in ids {
        assert!(store
            .head(&keys::snapshot_record(&namespace_id, &id))
            .await
            .expect("snapshot")
            .is_some());
    }
}
