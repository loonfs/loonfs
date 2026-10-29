//! Pin publication races and verification request counts.

use super::*;
use loonfs_objectstore::keys::pin_prefix;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::control::PinOwner;
use std::assert_matches;

#[tokio::test]
async fn pin_creation_retries_after_compaction_and_collection() {
    for advance_head in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = BlockingStore::new(
            MetadataMapStore::aged(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::metadata_segment(),
            ),
            KeyPredicate::prefix(pin_prefix(&namespace_id)),
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
        let (created, current) =
            tokio::join!(create_checkpoint(&store, &namespace_id, &context), async {
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
                current
            });
        let checkpoint = created.expect("retry against the current manifest");
        let page = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .namespace
            .list_checkpoint_files_page(
                &checkpoint.checkpoint_id,
                loonfs_types::PageRequest {
                    cursor: None,
                    limit: loonfs_test_support::ids::page_limit(10),
                },
                crate::pin::ListCheckpointFilesOptions::default(),
            )
            .await
            .expect("acknowledged checkpoint remains readable");
        assert_eq!(page.files.len(), if advance_head { 3 } else { 2 });
        assert_eq!(checkpoint.manifest_no, current.manifest_no);
        assert_eq!(checkpoint.captured_seq, current.head_seq);
        assert_eq!(
            store
                .list_prefix(&pin_prefix(&namespace_id))
                .await
                .expect("pins"),
            [loonfs_objectstore::keys::pin(
                &namespace_id,
                &checkpoint.checkpoint_id
            )]
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
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::CompactImmediately,
        crate::manifest::CompactionAdmission::AnySize,
        selected.compactor_epoch(),
        &crate::manifest::MetadataCompactionCancellation::default(),
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
        .list_prefix(&pin_prefix(namespace_id))
        .await
        .expect("pins")
        .is_empty());
    let options = crate::gc::GcOptions::default();
    // A superseded manifest roots its replaced segments until its successor
    // has aged past the collector's grace window.
    let successor_modified_ms = store
        .head(&current.object_key)
        .await
        .expect("successor metadata")
        .expect("successor exists")
        .last_modified_at_ms
        .expect("test store provides a timestamp");
    let collection = crate::test_support::namespace_store(store, namespace_id)
        .gc(
            None,
            &options,
            &writer_at(
                "collector",
                (successor_modified_ms + options.grace_window_ms + 1)
                    .max(crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1),
            )
            .clock(),
        )
        .await
        .expect("collect before pin write");
    assert!(collection.deleted.metadata_segments >= replaced.len() as u64);
    for key in replaced {
        assert!(store.head(&key).await.expect("replaced segment").is_none());
    }
    current.manifest()
}

#[tokio::test]
async fn namespace_deletion_during_pin_verification_deletes_the_pin() {
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
        namespace.create_pin_at_basis(
            PinOwner::User {
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
                    .list_prefix(&pin_prefix(&namespace_id))
                    .await
                    .expect("durable pin")
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
    assert_matches!(result, Err(CoreError::CheckpointUnavailable(_)));
    assert!(store
        .list_prefix(&pin_prefix(&namespace_id))
        .await
        .expect("pins")
        .is_empty());
}

#[tokio::test]
async fn pin_verification_checks_manifest_identity_with_only_the_current_manifest_load() {
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
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&checkpoint.checkpoint_id)
        .await
        .expect("load pin")
        .expect("pin exists")
        .state;
    store.reset();
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    let mut expected = store.take();
    loonfs_test_support::stores::sort_head_runs(&mut expected);
    let mut changed_number = record.clone();
    changed_number.pin_id = PinId::generate(
        record
            .pin_id
            .manifest_no()
            .successor()
            .expect("next number"),
    );
    let mut changed_checksum = record.clone();
    changed_checksum.payload_checksum = loonfs_types::sha256_digest(b"different");
    for (record, expected_verification) in [
        (record, record::PinBasisVerification::Verified),
        (changed_number, record::PinBasisVerification::Invalid),
        (changed_checksum, record::PinBasisVerification::Invalid),
    ] {
        assert_eq!(
            crate::test_support::namespace_store(&store, &record.namespace_id)
                .verify_pin_basis(&record)
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
async fn a_pin_write_that_lands_and_fails_its_read_back_leaves_no_pin() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(pin_prefix(&namespace_id)),
        OperationSelector::Any,
        InjectedError::Transport("lost pin acknowledgement".to_owned()),
    )
    .apply_then_fail();
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    store.fail_next(2);
    let error = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect_err("an unconfirmed pin write fails creation");
    assert_matches!(error, CoreError::Store { .. });
    assert_eq!(store.attempts(), 3, "put, read-back, and cleanup delete");
    assert!(store
        .list_prefix(&pin_prefix(&namespace_id))
        .await
        .expect("pins")
        .is_empty());
}

/// Checks that a snapshot read accepts a user pin with or without an
/// expiry, and that every snapshot operation refuses a fork pin's ID with
/// `snapshot_not_found` and leaves the pin in place.
#[tokio::test]
async fn a_fork_pin_id_is_refused_by_every_snapshot_operation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = writer_at("gc-test", 1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let mut pins = Vec::new();
    for owner in [
        PinOwner::User {
            name: "user".to_owned(),
            expires_at_ms: None,
        },
        PinOwner::User {
            name: "snapshot".to_owned(),
            expires_at_ms: Some(u64::MAX),
        },
        PinOwner::Fork {
            target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
        },
    ] {
        let pin = crate::test_support::namespace_store(&store, &namespace_id)
            .create_pin(owner, &setup.clock())
            .await
            .expect("pin")
            .response;
        pins.push(pin.pin_id);
    }
    let [user, snapshot, fork] = &pins[..] else {
        panic!("expected three pins, got {pins:?}");
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
        .expect("read a pin without an expiry");
    snapshot_read(snapshot)
        .await
        .expect("read a pin with an expiry");
    let cases = [
        ("read of a fork pin", snapshot_read(fork).await.map(drop)),
        (
            "retrieve of a fork pin",
            crate::NamespaceEngine::reader(crate::test_support::namespace_store(
                &store,
                &namespace_id,
            ))
            .get_snapshot(fork)
            .await
            .map(drop),
        ),
        (
            "delete of a fork pin",
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
            .list_prefix(&pin_prefix(&namespace_id))
            .await
            .expect("pins")
            .len(),
        3
    );
}
