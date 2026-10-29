//! Pin publication races and verification request counts.

use super::*;
use loonfs_objectstore::keys::pin_prefix;
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
            write_file_bytes(&store, &namespace_id, path, b"data", &context, None)
                .await
                .expect("write");
            fold_wal(&store, &namespace_id).await.expect("fold");
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
                    fold_wal(&store, &namespace_id)
                        .await
                        .expect("fold newer head");
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
        let page = crate::pin::list_checkpoint_files_page(
            &store,
            None,
            &crate::namespace::control::load_namespace_read_state(
                &store,
                &namespace_id,
                crate::test_support::std_timer(),
            )
            .await
            .expect("head"),
            &checkpoint.checkpoint_id,
            loonfs_types::PageRequest {
                cursor: None,
                limit: loonfs_test_support::ids::page_limit(10),
            },
            crate::pin::ListCheckpointFilesOptions::default(),
            context.now_ms,
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
    let report = compact_unit(
        store,
        None,
        namespace_id,
        None,
        selected.state.compactor_epoch(),
        MetadataRunPolicy::default(),
        MetadataCompactionPolicy::CompactImmediately,
        crate::manifest::CompactionAdmission::AnySize,
        &crate::manifest::MetadataCompactionCancellation::default(),
        crate::test_support::std_timer(),
    )
    .await
    .expect("compact");
    assert_matches!(report, CompactionUnitOutcome::Published { .. });
    // The runtime that published the compaction raises the hint.
    crate::test_support::raise_hint_to_current_manifest(store, namespace_id).await;
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
    let collection = crate::gc::gc_namespace(
        store,
        None,
        None,
        namespace_id,
        &options,
        &mutation_context(
            "collector",
            (successor_modified_ms + options.grace_window_ms + 1)
                .max(crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1),
        ),
        crate::test_support::std_timer(),
    )
    .await
    .expect("collect before pin write");
    assert!(collection.deleted.metadata_segments >= replaced.len() as u64);
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
        OperationSelector::Read,
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
        create::create_pin_at_basis(
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
                    .list_prefix(&pin_prefix(&namespace_id))
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
                None,
                &context,
                &crate::time::Deadline::start(Arc::new(crate::time::StdMonotonicTimer::default())),
                MetadataRunPolicy::default(),
                None,
                &crate::test_support::content_writes(),
            )
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
    let record = load_pin(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("load pin")
        .expect("pin exists")
        .state;
    store.reset();
    load_current_manifest(&store, &namespace_id)
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
            record::verify_pin_basis(&store, &record)
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
        PinOwner::User {
            name: "snapshot".to_owned(),
            expires_at_ms: Some(u64::MAX),
        },
        PinOwner::Fork {
            target_namespace_id: NamespaceId::parse("clone").expect("namespace id"),
        },
    ] {
        let pin = crate::pin::create_pin(
            &store,
            &namespace_id,
            owner,
            &setup,
            Default::default(),
            None,
            &crate::test_support::content_writes(),
            crate::test_support::std_timer(),
        )
        .await
        .expect("pin")
        .response;
        pins.push(pin.pin_id);
    }
    let [user, snapshot, fork] = &pins[..] else {
        panic!("expected three pins, got {pins:?}");
    };
    let head = load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
        .await
        .expect("head");
    let snapshot_read =
        |id| crate::pin::load_snapshot_read_basis(&store, None, &head, id, setup.now_ms);
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
            crate::pin::get_snapshot(&store, &namespace_id, fork)
                .await
                .map(drop),
        ),
        (
            "delete of a fork pin",
            crate::pin::delete_snapshot(&store, &namespace_id, fork)
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
