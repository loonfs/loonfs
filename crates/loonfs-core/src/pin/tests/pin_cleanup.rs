//! Collection recovery after failed or concurrent pin cleanup.

use super::*;
use loonfs_objectstore::keys::{self, pin_prefix};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::{replace_object, MetadataMapStore};
use loonfs_types::format::control::PinOwner;

#[tokio::test]
async fn abandoned_pins_with_collected_bases_can_be_reaped_after_failed_cleanup() {
    for owner in [
        PinOwner::User {
            name: "abandoned".to_owned(),
            expires_at_ms: Some(2_000),
        },
        PinOwner::Fork {
            target_namespace_id: NamespaceId::parse("never-published").expect("target"),
        },
    ] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = BlockingStore::new(
            FailStore::new(
                MetadataMapStore::aged(
                    LocalFsStore::new(directory.path()).expect("store"),
                    KeyPredicate::any(),
                ),
                KeyPredicate::prefix(pin_prefix(&namespace_id)),
                OperationSelector::Delete,
                InjectedError::Transport("cleanup unavailable".to_owned()),
            ),
            KeyPredicate::prefix(pin_prefix(&namespace_id)),
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
            namespace.create_pin_at_basis(owner, selected.manifest(), &clock),
            async {
                store.wait_until_blocked().await;
                super::pin_verification::compact_and_collect_replaced_segments(
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
            ErrorCode::CheckpointUnavailable,
            "a failed cleanup keeps the verification error"
        );
        assert_eq!(store.inner().attempts(), 1);
        store.inner().clear();
        let pins = store
            .list_prefix(&pin_prefix(&namespace_id))
            .await
            .expect("pins");
        assert_eq!(pins.len(), 1, "failed cleanup left its unverified pin");

        // A missing basis is still an error while the record must be retained.
        let young = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(None, &crate::gc::GcOptions::default(), &setup.clock())
            .await
            .expect_err("young pin remains protected");
        assert_eq!(young.code(), ErrorCode::NamespaceCorrupt);

        let collected = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(
                None,
                &crate::gc::GcOptions::default(),
                &writer_at(
                    "collector",
                    crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 2_001,
                )
                .clock(),
            )
            .await
            .expect("reap abandoned pin once owner rules permit deletion");
        assert_eq!(
            collected.deleted_checkpoints_by_owner.user
                + collected.deleted_checkpoints_by_owner.fork,
            1
        );
        assert!(store.head(&pins[0]).await.expect("pin").is_none());
        crate::test_support::namespace_store(&store, &namespace_id)
            .gc(
                None,
                &crate::gc::GcOptions::default(),
                &writer_at(
                    "collector",
                    crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 2_002,
                )
                .clock(),
            )
            .await
            .expect("next pass also succeeds");
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_metadata_view()
            .await
            .expect("current view remains readable");
    }
}

#[tokio::test]
async fn pin_removed_after_listing_does_not_make_a_collected_basis_corruption() {
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
    let pin = create_checkpoint(&base, &namespace_id, &setup)
        .await
        .expect("pin");
    crate::test_support::NamespaceFixture::new(&base, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&base, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // The runtime that published the fold raises the hint.
    crate::test_support::namespace_store(&base, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let old_manifest = metadata_manifest_object(&namespace_id, &pin.manifest_no);
    let store = BlockingStore::new(
        base,
        KeyPredicate::exact(&old_manifest),
        OperationSelector::Read,
    );
    let aged = writer_at(
        "collector",
        crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
    );
    let options = crate::gc::GcOptions::default();
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = aged.clock();
    let (collection, ()) = tokio::join!(namespace.gc(None, &options, &clock), async {
        store.wait_until_blocked().await;
        store
            .inner()
            .delete(&keys::pin(&namespace_id, &pin.checkpoint_id))
            .await
            .expect("release pin");
        crate::test_support::namespace_store(store.inner(), &namespace_id)
            .gc(None, &options, &aged.clock())
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
    collection.expect("concurrent pin release is not corruption");
}

#[tokio::test]
async fn missing_basis_checks_each_pin_and_propagates_pin_read_errors() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(pin_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("pin unavailable".to_owned()),
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
        PinId::generate(manifest.manifest().manifest_no),
        PinId::generate(manifest.manifest().manifest_no),
    ];
    ids.sort();
    for (index, id) in ids.iter().enumerate() {
        record::write_pin(
            &store,
            &loonfs_types::format::control::PinPayload {
                pin_id: id.clone(),
                namespace_id: namespace_id.clone(),
                head_seq: manifest.manifest().head_seq,
                payload_checksum: manifest.manifest().payload_checksum.clone(),
                created_at_ms: setup.now_ms,
                owner: PinOwner::User {
                    name: format!("pin-{index}"),
                    expires_at_ms: (index == 0).then_some(2_000),
                },
            },
        )
        .await
        .expect("write pin");
    }
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // The runtime that published the fold raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    // Deliberate corruption checks must stay fail-closed, even when the first
    // listed pin for this same manifest is independently eligible for deletion.
    store
        .delete(&manifest.object_key)
        .await
        .expect("remove pinned manifest");
    let aged = writer_at(
        "collector",
        crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
    );
    let options = crate::gc::GcOptions::default();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &aged.clock())
        .await
        .expect_err("second pin still requires the missing basis");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(error.to_string().contains(ids[1].as_str()));
    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &aged.clock())
        .await
        .expect_err("unreadable pin is not absent");
    assert_eq!(error.code(), ErrorCode::ServerError);
    store.clear();
    let key = keys::pin(&namespace_id, &ids[0]);
    replace_object(&store, &key, Bytes::from_static(b"not json"))
        .await
        .expect("malformed pin");
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &aged.clock())
        .await
        .expect_err("malformed pin is not deletable");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    for id in ids {
        assert!(store
            .head(&keys::pin(&namespace_id, &id))
            .await
            .expect("pin")
            .is_some());
    }
}
