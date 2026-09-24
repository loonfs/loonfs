//! Collection recovery after failed or concurrent pin cleanup.

use super::*;
use loonfs_api::wire::control::PinOwner;
use loonfs_objectstore::keys::{checkpoint_prefix, checkpoint_record};
use loonfs_test_support::stores::MetadataMapStore;

#[tokio::test]
async fn abandoned_pins_with_collected_bases_can_be_reaped_after_failed_cleanup() {
    for owner in [
        PinOwner::Snapshot {
            name: "abandoned".to_owned(),
            expires_at_ms: 2_000,
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
                KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
                OperationClass::Delete,
                InjectedError::Transport("cleanup unavailable".to_owned()),
            ),
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::PutCreateIfAbsent,
        );
        let setup = mutation_context("creator", 1_000);
        bootstrap_namespace(&store, &namespace_id, &setup)
            .await
            .expect("bootstrap");
        for path in ["/one", "/two"] {
            write_file_bytes(&store, &namespace_id, path, b"data", &setup, None)
                .await
                .expect("write");
            flush::flush_wal(&store, &namespace_id)
                .await
                .expect("flush");
        }
        let selected = load_current_manifest(&store, &namespace_id)
            .await
            .expect("selected manifest");
        store.inner().fail_next(1);
        store.block_next();
        let (creation, ()) = tokio::join!(
            create::create_checkpoint_at_basis(
                &store,
                &namespace_id,
                owner,
                selected.state.manifest.clone(),
                &setup,
            ),
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
            creation.expect_err("cleanup failed").code(),
            ErrorCode::ServerError
        );
        assert_eq!(store.inner().attempts(), 1);
        store.inner().clear();
        let pins = store
            .list_prefix(&checkpoint_prefix(&namespace_id))
            .await
            .expect("pins");
        assert_eq!(pins.len(), 1, "failed cleanup left its unverified pin");

        // A missing basis is still an error while the record must be retained.
        let young = crate::gc::gc_namespace(
            &store,
            &namespace_id,
            &crate::gc::GcConfig::default(),
            &setup,
        )
        .await
        .expect_err("young pin remains protected");
        assert_eq!(young.code(), ErrorCode::NamespaceCorrupt);

        let collected = crate::gc::gc_namespace(
            &store,
            &namespace_id,
            &crate::gc::GcConfig::default(),
            &mutation_context(
                "collector",
                crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 2_001,
            ),
        )
        .await
        .expect("reap abandoned pin once owner rules permit deletion");
        assert_eq!(
            collected.deleted_checkpoints_by_owner.snapshot
                + collected.deleted_checkpoints_by_owner.fork,
            1
        );
        assert!(store.head(&pins[0]).await.expect("pin").is_none());
        crate::gc::gc_namespace(
            &store,
            &namespace_id,
            &crate::gc::GcConfig::default(),
            &mutation_context(
                "collector",
                crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 2_002,
            ),
        )
        .await
        .expect("next pass also succeeds");
        load_current_metadata_view(&store, &namespace_id)
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
    write_file_bytes(&base, &namespace_id, "/new", b"data", &setup, None)
        .await
        .expect("write");
    flush::flush_wal(&base, &namespace_id).await.expect("flush");
    let old_manifest = metadata_manifest_object(&namespace_id, &pin.manifest_no);
    let store = BlockingStore::new(
        base,
        KeyPredicate::exact(&old_manifest),
        OperationClass::Read,
    );
    let aged = mutation_context(
        "collector",
        crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1,
    );
    let config = crate::gc::GcConfig::default();
    store.block_next();
    let (collection, ()) = tokio::join!(
        crate::gc::gc_namespace(&store, &namespace_id, &config, &aged),
        async {
            store.wait_until_blocked().await;
            store
                .inner()
                .delete(&checkpoint_record(&namespace_id, &pin.checkpoint_id))
                .await
                .expect("release pin");
            crate::gc::gc_namespace(store.inner(), &namespace_id, &config, &aged)
                .await
                .expect("other collector");
            assert!(store
                .inner()
                .head(&old_manifest)
                .await
                .expect("manifest")
                .is_none());
            store.release();
        }
    );
    collection.expect("concurrent pin release is not corruption");
}

#[tokio::test]
async fn missing_basis_checks_each_pin_and_propagates_pin_read_errors() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("pin unavailable".to_owned()),
    );
    let setup = mutation_context("creator", 1_000);
    bootstrap_namespace(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let manifest = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let mut ids = [
        PinId::generate(manifest.state.manifest.manifest_no),
        PinId::generate(manifest.state.manifest.manifest_no),
    ];
    ids.sort();
    for (index, id) in ids.iter().enumerate() {
        record::write_checkpoint_record(
            &store,
            &loonfs_api::wire::control::PinPayload {
                pin_id: id.clone(),
                namespace_id: namespace_id.clone(),
                head_seq: manifest.state.manifest.head_seq,
                payload_checksum: manifest.state.manifest.payload_checksum.clone(),
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
    write_file_bytes(&store, &namespace_id, "/new", b"data", &setup, None)
        .await
        .expect("write");
    flush::flush_wal(&store, &namespace_id)
        .await
        .expect("flush");
    // Deliberate corruption checks must stay fail-closed, even when the first
    // listed pin for this same manifest is independently eligible for deletion.
    store
        .delete(&manifest.object_key)
        .await
        .expect("remove pinned manifest");
    let aged = mutation_context(
        "collector",
        crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1,
    );
    let config = crate::gc::GcConfig::default();
    let error = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect_err("second pin still requires the missing basis");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(ids[1].as_str()));
    store.fail_all();
    let error = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect_err("unreadable pin is not absent");
    assert_eq!(error.code(), ErrorCode::ServerError);
    store.clear();
    let key = checkpoint_record(&namespace_id, &ids[0]);
    store
        .put_overwrite(&key, Bytes::from_static(b"not json"))
        .await
        .expect("malformed pin");
    let error = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect_err("malformed pin is not deletable");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    for id in ids {
        assert!(store
            .head(&checkpoint_record(&namespace_id, &id))
            .await
            .expect("pin")
            .is_some());
    }
}
