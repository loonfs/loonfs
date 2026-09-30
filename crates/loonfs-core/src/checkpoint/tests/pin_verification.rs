//! Pin publication races and verification request counts.

use super::*;
use loonfs_api::wire::control::PinOwner;
use loonfs_objectstore::keys::checkpoint_prefix;
use loonfs_test_support::stores::MetadataMapStore;

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
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::PutCreateIfAbsent,
        );
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context)
            .await
            .expect("bootstrap");
        for path in ["/one", "/two"] {
            write_file_bytes(&store, &namespace_id, path, b"data", &context, None)
                .await
                .expect("write");
            flush::flush_wal(&store, &namespace_id)
                .await
                .expect("flush");
        }
        let selected = load_current_manifest(&store, &namespace_id)
            .await
            .expect("selected manifest");
        store.block_next();
        let (created, current) =
            tokio::join!(create_checkpoint(&store, &namespace_id, &context), async {
                store.wait_until_blocked().await;
                if advance_head {
                    write_file_bytes(&store, &namespace_id, "/three", b"data", &context, None)
                        .await
                        .expect("advance head");
                    flush::flush_wal(&store, &namespace_id)
                        .await
                        .expect("flush newer head");
                }
                let current =
                    compact_and_collect_replaced_segments(&store, &namespace_id, &selected).await;
                assert_eq!(
                    selected.state.manifest().head_seq != current.head_seq,
                    advance_head
                );
                store.release();
                current
            });
        let checkpoint = created.expect("retry against the current manifest");
        let page = crate::checkpoint::list_checkpoint_files_page(
            &store,
            None,
            &crate::namespace::control::load_namespace_read_state(&store, &namespace_id)
                .await
                .expect("head"),
            &checkpoint.checkpoint_id,
            loonfs_api::PageRequest {
                cursor: None,
                limit: loonfs_test_support::ids::page_limit(10),
            },
            crate::checkpoint::ListCheckpointFilesOptions::default(),
        )
        .await
        .expect("acknowledged checkpoint remains readable");
        assert_eq!(page.files.len(), if advance_head { 3 } else { 2 });
        assert_eq!(checkpoint.manifest_no, current.manifest_no);
        assert_eq!(checkpoint.captured_seq, current.head_seq);
        assert_eq!(
            store
                .list_prefix(&checkpoint_prefix(&namespace_id))
                .await
                .expect("pins"),
            [loonfs_objectstore::keys::checkpoint_record(
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
) -> loonfs_api::wire::control::ManifestRef {
    let report = reorganize_metadata_step(
        store,
        namespace_id,
        selected.state.compactor_epoch(),
        MetadataLsmPolicy::default(),
        MetadataCompactionPolicy::CompactImmediately,
    )
    .await
    .expect("compact");
    assert!(matches!(
        report,
        MetadataReorganizeOutcome::UnitPublished { .. }
    ));
    let current = load_current_manifest(store, namespace_id)
        .await
        .expect("current manifest");
    assert_ne!(selected.state.manifest(), current.state.manifest());
    let current_segments: BTreeSet<_> = current
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect();
    let replaced: Vec<_> = selected
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .filter(|key| !current_segments.contains(key))
        .collect();
    assert!(!replaced.is_empty());
    assert!(store
        .list_prefix(&checkpoint_prefix(namespace_id))
        .await
        .expect("pins")
        .is_empty());
    let config = crate::gc::GcConfig::default();
    // A superseded manifest roots its replaced segments until its successor
    // has aged past the collector's grace window.
    let successor_modified_ms = store
        .head(&current.object_key)
        .await
        .expect("successor metadata")
        .expect("successor exists")
        .last_modified_ms
        .expect("test store provides a timestamp");
    let collection = crate::gc::gc_namespace(
        store,
        namespace_id,
        &config,
        &mutation_context(
            "collector",
            (successor_modified_ms + config.grace_window_ms + 1)
                .max(crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1),
        ),
    )
    .await
    .expect("collect before pin write");
    assert!(collection.deleted.metadata_segments as usize >= replaced.len());
    for key in replaced {
        assert!(store.head(&key).await.expect("replaced segment").is_none());
    }
    current.state.manifest()
}

#[tokio::test]
async fn namespace_deletion_during_pin_verification_deletes_the_pin() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationClass::Read,
    );
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let writer = acquire_writer_epoch(&store, &namespace_id, &context)
        .await
        .expect("writer");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest");
    store.block_next();
    let (result, ()) = tokio::join!(
        create::create_checkpoint_at_basis(
            &store,
            &namespace_id,
            PinOwner::User {
                name: "racing".to_owned(),
                expires_at_ms: None
            },
            current.state.manifest(),
            &context,
        ),
        async {
            store.wait_until_blocked().await;
            assert_eq!(
                store
                    .inner()
                    .list_prefix(&checkpoint_prefix(&namespace_id))
                    .await
                    .expect("durable pin")
                    .len(),
                1
            );
            crate::namespace::delete::delete_namespace(
                store.inner(),
                &namespace_id,
                Default::default(),
                writer,
                &context,
                &crate::time::Deadline::start(Arc::new(crate::time::StdMonotonicTimer::default())),
                MetadataLsmPolicy::default(),
            )
            .await
            .expect("delete namespace during verification");
            store.release();
        }
    );
    assert!(matches!(result, Err(CoreError::CheckpointUnavailable(_))));
    assert!(store
        .list_prefix(&checkpoint_prefix(&namespace_id))
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
    let record = load_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("load pin")
        .expect("pin exists")
        .state;
    store.reset();
    load_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest");
    let expected = store.take();
    let mut changed_number = record.clone();
    changed_number.pin_id = PinId::generate(
        record
            .pin_id
            .manifest_no()
            .successor()
            .expect("next number"),
    );
    let mut changed_checksum = record.clone();
    changed_checksum.payload_checksum = "sha256:different".to_owned();
    for (record, expected_verification) in [
        (record, record::CheckpointBasisVerification::Verified),
        (changed_number, record::CheckpointBasisVerification::Invalid),
        (
            changed_checksum,
            record::CheckpointBasisVerification::Invalid,
        ),
    ] {
        assert_eq!(
            record::verify_checkpoint_basis(&store, &record)
                .await
                .expect("verification"),
            expected_verification
        );
        assert_eq!(store.take(), expected);
    }
}

#[tokio::test]
async fn a_pin_write_that_lands_and_fails_its_read_back_leaves_no_pin() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
        OperationClass::Any,
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
    assert!(matches!(error, CoreError::Store { .. }), "{error:?}");
    assert_eq!(store.attempts(), 3, "put, read-back, and cleanup delete");
    assert!(store
        .list_prefix(&checkpoint_prefix(&namespace_id))
        .await
        .expect("pins")
        .is_empty());
}

#[tokio::test]
async fn a_pin_id_answers_only_operations_of_its_owner_kind() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = mutation_context("gc-test", 1_000);
    create(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let mut pins = Vec::new();
    for owner in [
        PinOwner::User {
            name: "user".to_owned(),
            expires_at_ms: None,
        },
        PinOwner::Snapshot {
            name: "snapshot".to_owned(),
            expires_at_ms: u64::MAX,
        },
        PinOwner::Fork {
            target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
        },
    ] {
        let pin = crate::checkpoint::create_checkpoint(&store, &namespace_id, owner, &setup)
            .await
            .expect("pin");
        pins.push(pin.pin_id);
    }
    let [user, snapshot, fork] = &pins[..] else {
        panic!("expected three pins, got {pins:?}");
    };
    let head = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    let checkpoint_read =
        |id| crate::checkpoint::load_checkpoint_read_basis(&store, None, &head, id);
    let snapshot_read =
        |id| crate::checkpoint::load_snapshot_read_basis(&store, None, &head, id, setup.now_ms);
    let cases = [
        (
            "checkpoint read of a snapshot",
            checkpoint_read(snapshot).await.map(drop),
            ErrorCode::CheckpointNotFound,
        ),
        (
            "checkpoint read of a fork pin",
            checkpoint_read(fork).await.map(drop),
            ErrorCode::CheckpointNotFound,
        ),
        (
            "checkpoint delete of a snapshot",
            crate::checkpoint::delete_checkpoint(&store, &namespace_id, snapshot)
                .await
                .map(drop),
            ErrorCode::CheckpointNotFound,
        ),
        (
            "checkpoint delete of a fork pin",
            crate::checkpoint::delete_checkpoint(&store, &namespace_id, fork)
                .await
                .map(drop),
            ErrorCode::CheckpointNotFound,
        ),
        (
            "snapshot read of a user pin",
            snapshot_read(user).await.map(drop),
            ErrorCode::SnapshotNotFound,
        ),
        (
            "snapshot delete of a user pin",
            crate::checkpoint::delete_snapshot(&store, &namespace_id, user)
                .await
                .map(drop),
            ErrorCode::SnapshotNotFound,
        ),
    ];
    for (label, result, expected) in cases {
        assert_eq!(result.expect_err(label).code(), expected, "{label}");
    }
    assert_eq!(
        store
            .list_prefix(&checkpoint_prefix(&namespace_id))
            .await
            .expect("pins")
            .len(),
        3
    );
}
