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
    let collection = crate::gc::gc_namespace(
        store,
        namespace_id,
        &crate::gc::GcConfig::default(),
        &mutation_context(
            "collector",
            crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1,
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
async fn fork_owned_checkpoints_reject_user_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = mutation_context("gc-test", 1_000);
    crate::namespace::bootstrap::bootstrap_namespace(
        &store,
        &source,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    crate::namespace::fork::fork_namespace(
        &store,
        &source,
        &clone,
        &loonfs_test_support::test_actor(),
        None,
        &setup,
    )
    .await
    .expect("fork");

    let keys = store
        .list_prefix(&checkpoint_prefix(&source))
        .await
        .expect("list checkpoints");
    assert_eq!(keys.len(), 1);
    let bytes = store
        .get(&keys[0], None)
        .await
        .expect("get record")
        .expect("record exists");
    let fork_record = loonfs_api::wire::control::decode_control_object::<
        loonfs_api::wire::control::PinPayload,
    >(&bytes, loonfs_api::wire::control::ControlObjectKind::Pin)
    .expect("decode record")
    .into_payload();

    let error = crate::checkpoint::delete_checkpoint(&store, &source, &fork_record.pin_id)
        .await
        .expect_err("fork-owned release must fail");
    assert!(
        matches!(
            &error,
            CoreError::InvalidCheckpointRequest(message)
                if message.contains("owned by fork target")
        ),
        "expected invalid checkpoint request, got {error:?}"
    );
}

#[tokio::test]
async fn snapshot_owned_checkpoints_reject_user_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = mutation_context("gc-test", 1_000);
    crate::namespace::bootstrap::bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let snapshot = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        PinOwner::Snapshot {
            name: "report-run".to_owned(),
            expires_at_ms: u64::MAX,
        },
        &setup,
    )
    .await
    .map(crate::checkpoint::checkpoint_summary)
    .expect("snapshot checkpoint");

    let error =
        crate::checkpoint::delete_checkpoint(&store, &namespace_id, &snapshot.checkpoint_id)
            .await
            .expect_err("snapshot-owned release must fail");
    assert!(
        matches!(
            &error,
            CoreError::InvalidCheckpointRequest(message)
                if message.contains("is a snapshot")
        ),
        "expected invalid checkpoint request, got {error:?}"
    );
    assert!(crate::checkpoint::load_checkpoint_record(
        &store,
        &namespace_id,
        &snapshot.checkpoint_id
    )
    .await
    .expect("read pin")
    .is_some());
}
