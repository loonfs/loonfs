//! Namespace creation, fork installation, and terminal lifecycle guards.
//!
//! Tests cover conditional manifest installation and
//! namespace state before the first metadata publication.

use crate::common::*;
use bytes::Bytes;
use loonfs_core::publish::FilesystemOperation;
use loonfs_core::test_support::WriterAt;
use loonfs_core::test_support::{namespace_store, NamespaceFixture};
use loonfs_core::{CoreError, ErrorCode};
use loonfs_objectstore::keys::{content_blob, hint, metadata_manifest_object, pin};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::{commit_id, namespace_id};
use loonfs_test_support::stores::{
    replace_object, BlockingStore, FailStore, InjectedError, KeyPredicate, LostCreateStore,
    MetadataMapStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{
    format::control::{decode_control_object, ControlObjectKind, PinOwner, PinPayload},
    format::manifest::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataRowFamily,
    },
    AbsolutePath, ChangeSeq, CommitId, DestinationBehavior, ManifestNo, NamespaceId, PinId,
};
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &WriterAt,
) -> Result<loonfs_types::NamespaceMetadata, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id, &loonfs_test_support::test_actor(), None)
        .await
        .map(|report| report.response)
}

async fn load_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
) -> PinPayload {
    let bytes = store
        .get(&pin(namespace_id, pin_id), None)
        .await
        .expect("read pin")
        .expect("pin exists");
    decode_control_object::<PinPayload>(&bytes, ControlObjectKind::Pin)
        .expect("decode pin")
        .into_payload()
}

async fn listed_names<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    let context = crate::common::read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &default_writer())
        .list_path_page(
            "/docs",
            loonfs_types::PageRequest {
                limit: loonfs_test_support::ids::page_limit(10),
                cursor: None,
            },
            Default::default(),
            &context,
        )
        .await
        .expect("list files")
        .items
        .into_iter()
        .map(|entry| entry.display_name.expect("file name").to_string())
        .collect()
}

#[tokio::test]
async fn snapshot_fork_keeps_its_view_after_source_compaction_collection_and_snapshot_delete() {
    let directory = tempdir().expect("tempdir");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    let context = default_writer();
    let source = namespace_id("source");
    let target = namespace_id("target");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let snapshot = engine
        .create_snapshot("basis".to_owned(), Some(u64::MAX / 2))
        .await
        .expect("snapshot")
        .response;
    let snapshot_record = load_pin(&store, &source, &snapshot.snapshot_id).await;
    NamespaceFixture::new(&store, &source, &context)
        .write_file_bytes("/docs/later.txt", b"later", Some(&commit_id("later")))
        .await
        .expect("advance source");
    engine.fold_wal().await.expect("fold current source");
    let floor = engine
        .advance_retention_floor(loonfs_core::RetentionTarget::Head)
        .await
        .expect("advance floor")
        .response;
    assert!(floor.retention_floor_seq > snapshot.captured_seq);
    let mut compacted = false;
    for _ in 0..16 {
        let report = engine
            .compact_metadata_unit(
                None,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                loonfs_types::CompactorEpoch(0),
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("compact source");
        match report {
            loonfs_core::CompactionUnitOutcome::NotNeeded => break,
            loonfs_core::CompactionUnitOutcome::Published { .. } => compacted = true,
            other => panic!("expected bounded compaction, got {other:?}"),
        }
    }
    assert!(compacted);
    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 4;
    let collected = namespace_store(&store, &source)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("collect source before fork");
    assert!(collected.deleted.metadata_segments > 0);
    let fork = engine
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            Some(&snapshot.snapshot_id),
        )
        .await
        .expect("fork snapshot");
    assert_eq!(fork.response.head_seq, snapshot.captured_seq);
    assert_eq!(fork.published, None, "a fork of a snapshot doesn't fold");
    let head = head_state(&store, &target).await;
    assert_eq!(
        head.fork_basis.as_ref().expect("fork basis").manifest,
        snapshot_record.manifest()
    );
    assert_eq!(listed_names(&store, &target).await, ["shared.txt"]);
    assert_eq!(
        listed_names(&store, &source).await,
        ["later.txt", "shared.txt"]
    );
    assert_eq!(
        load_pin(&store, &source, &snapshot.snapshot_id).await,
        snapshot_record
    );
    engine
        .delete_snapshot(&snapshot.snapshot_id)
        .await
        .expect("release snapshot");
    aged.now_ms = u64::MAX / 2;
    namespace_store(&store, &source)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("collect source past grace window");
    assert_eq!(listed_names(&store, &target).await, ["shared.txt"]);
    assert_eq!(
        read_file_bytes(&store, &target, "/docs/shared.txt")
            .await
            .expect("read fork")
            .bytes,
        b"base"
    );
}

#[tokio::test]
async fn invalid_snapshot_forks_write_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = default_writer();
    let source = namespace_id("source");
    let other = namespace_id("other");
    let target = namespace_id("target");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    seed_source_namespace_for_fork(&store, &other, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let expired = engine
        .create_snapshot("expired".to_owned(), Some(1))
        .await
        .expect("expired snapshot")
        .response;
    let foreign = namespace_engine(&store, &other, &context)
        .create_snapshot("foreign".to_owned(), Some(u64::MAX / 2))
        .await
        .expect("foreign snapshot")
        .response;
    for (snapshot_id, expected_code) in [
        (expired.snapshot_id, ErrorCode::SnapshotNotFound),
        (foreign.snapshot_id, ErrorCode::SnapshotNotFound),
    ] {
        store.take();
        let error = engine
            .fork_namespace(
                &target,
                &loonfs_test_support::test_actor(),
                Some(&snapshot_id),
            )
            .await
            .expect_err("invalid snapshot");
        assert_eq!(error.code(), expected_code);
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
        assert!(namespace_keys(&store, &target).await.is_empty());
    }
}

#[tokio::test]
async fn snapshot_deletion_during_fork_deletes_the_attempt_without_installing_a_target() {
    let directory = tempdir().expect("tempdir");
    let source = namespace_id("source");
    let target = namespace_id("target");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = default_writer();
    seed_source_namespace_for_fork(&store, &source, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let snapshot = engine
        .create_snapshot("basis".to_owned(), Some(u64::MAX / 2))
        .await
        .expect("snapshot")
        .response;
    let store = BlockingStore::new(
        BlockingStore::new(
            store,
            KeyPredicate::exact(loonfs_objectstore::keys::pin(
                &source,
                &snapshot.snapshot_id,
            )),
            OperationSelector::Read,
        ),
        KeyPredicate::prefix(loonfs_objectstore::keys::pin_prefix(&source)),
        OperationSelector::PutCreateIfAbsent,
    );
    let engine = namespace_engine(&store, &source, &context);
    store.block_next();
    let actor_id = loonfs_test_support::test_actor();
    let forking = engine.fork_namespace(&target, &actor_id, Some(&snapshot.snapshot_id));
    let deleting = async {
        store.wait_until_blocked().await;
        store.inner().block_next();
        store.release();
        store.inner().wait_until_blocked().await;
        assert_eq!(
            store
                .list_prefix(&loonfs_objectstore::keys::pin_prefix(&source))
                .await
                .expect("durable pins")
                .len(),
            2
        );
        namespace_engine(store.inner().inner(), &source, &context)
            .delete_snapshot(&snapshot.snapshot_id)
            .await
            .expect("delete snapshot after fork pin is durable");
        store.inner().release();
    };
    let (result, ()) = tokio::join!(forking, deleting);
    let error = result.expect_err("snapshot deleted");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert!(namespace_keys(&store, &target).await.is_empty());
    let records = store
        .list_prefix(&format!("namespaces/{source}/pins/"))
        .await
        .expect("list records");
    assert!(records.is_empty());
}

async fn seed_source_namespace_for_fork<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    context: &WriterAt,
) {
    NamespaceFixture::new(store, source_namespace_id, context)
        .create()
        .await
        .expect("bootstrap source namespace");
    NamespaceFixture::new(store, source_namespace_id, context)
        .write_file_bytes("/docs/shared.txt", b"base", Some(&commit_id("seed-shared")))
        .await
        .expect("seed shared file");
}

async fn head_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> loonfs_core::control::NamespaceReadState {
    namespace_store(store, namespace_id)
        .load_read_state()
        .await
        .expect("load head")
}

async fn namespace_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    store
        .list_prefix(&format!("namespaces/{}/", namespace_id.as_str()))
        .await
        .expect("list namespace prefix")
}

#[tokio::test]
async fn a_created_namespace_reads_manifest_one_before_its_first_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    let created = NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap namespace");
    assert_eq!(created.namespace_id, namespace_id);
    assert_eq!(created.head_seq, ChangeSeq(0));
    assert_eq!(created.retention_floor_seq, ChangeSeq(0));
    assert_eq!(
        namespace_keys(&store, &namespace_id).await,
        vec![
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ],
        "creation installs manifest one"
    );

    let root_entry = resolve_path(&store, &namespace_id, "/")
        .await
        .expect("a fresh namespace serves reads");
    assert_eq!(
        root_entry.inode_kind(),
        loonfs_types::EntryInodeKind::Directory
    );
    let status = namespace_store(&store, &namespace_id)
        .load_diagnostics()
        .await
        .expect("status");
    assert_eq!(status.current_manifest_no, ManifestNo(1));
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
    assert_eq!(status.acknowledged_floor_seq, ChangeSeq(0));

    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/first.txt", b"hello", Some(&commit_id("first-write")))
        .await
        .expect("a fresh namespace accepts writes");
    let file = read_file_bytes(&store, &namespace_id, "/docs/first.txt")
        .await
        .expect("read back");
    assert_eq!(file.bytes, b"hello");
    let content_ref = file.entry.content_ref().expect("content reference");
    let content_key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
    let keys = namespace_keys(&store, &namespace_id).await;
    assert!(
        keys.iter().all(|key| key == &hint(&namespace_id)
            || key == &content_key
            || key.starts_with(&format!("namespaces/{namespace_id}/manifests/"))
            || key.starts_with(&format!("namespaces/{}/wal/", namespace_id.as_str()))),
        "only control, WAL, and the uploaded content exist before the first fold: {keys:?}"
    );

    namespace_engine(&store, &namespace_id, &context)
        .fold_wal()
        .await
        .expect("fold");
    assert!(
        store
            .head(&metadata_manifest_object(&namespace_id, &ManifestNo(3)))
            .await
            .expect("probe root")
            .is_some(),
        "the fold follows the writer acquisitions"
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/first.txt")
            .await
            .expect("read after fold")
            .bytes,
        b"hello"
    );
    let status = namespace_store(&store, &namespace_id)
        .load_diagnostics()
        .await
        .expect("status after fold");
    assert_eq!(status.current_manifest_no, ManifestNo(3));
}

#[tokio::test]
async fn concurrent_installs_of_one_target_leave_exactly_one_winner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = default_writer();
    let source = namespace_id("source");
    let target = NamespaceId::parse("target").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let mut second = context.clone();
    second.writer_id = loonfs_types::WriterId::parse("writer-second").expect("writer id");
    let (left, right) = tokio::join!(
        fork_namespace(store.as_ref(), &source, &target, &context,),
        fork_namespace(store.as_ref(), &source, &target, &second,),
    );
    let outcomes = [left, right];
    assert_eq!(
        outcomes.iter().filter(|result| result.is_ok()).count(),
        1,
        "exactly one fork may install the target: {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .into_iter()
            .find_map(|result| result.err())
            .expect("one loser")
            .code(),
        ErrorCode::NamespaceExists
    );

    // A create racing a fork for a fresh id: same rule, and the loser never
    // sees a half-installed namespace.
    let contested = NamespaceId::parse("contested").expect("valid namespace id");
    let contested_fixture = NamespaceFixture::new(store.as_ref(), &contested, &context);
    let (created, forked) = tokio::join!(
        contested_fixture.create(),
        fork_namespace(store.as_ref(), &source, &contested, &second),
    );
    assert_eq!(
        usize::from(created.is_ok()) + usize::from(forked.is_ok()),
        1,
        "exactly one of create and fork may win: {created:?} {forked:?}"
    );
    let head = head_state(store.as_ref(), &contested).await;
    assert_eq!(
        head.status,
        loonfs_types::format::control::NamespaceStatus::Active {}
    );
    if created.is_ok() {
        assert!(head.fork_basis.is_none(), "the create won");
    } else {
        assert!(head.fork_basis.is_some(), "the fork won");
    }
}

#[tokio::test]
async fn fork_namespace_reads_inherited_content_and_isolates_metadata() {
    async fn upload_content<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        bytes: &[u8],
        context: &WriterAt,
    ) -> loonfs_types::ContentRef {
        let engine = namespace_engine(store, namespace_id, context);
        let upload = engine.begin_upload().await.expect("begin upload");
        let staged = engine
            .upload_content(&upload.upload_id, bytes)
            .await
            .expect("upload bytes");
        let catalog = namespace_store(store, namespace_id)
            .load_catalog_entry()
            .await
            .expect("catalog");
        let completed = engine
            .complete_upload(
                &catalog,
                &upload.upload_id,
                loonfs_types::api::v0::CompleteUploadBody::ServiceProxied {},
            )
            .await
            .expect("complete upload");
        assert_eq!(completed.response.content_ref(), staged.content_ref());
        staged.content_ref().expect("staged content").clone()
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
    )));
    let context = default_writer();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    NamespaceFixture::new(&store, &source_namespace_id, &context)
        .create()
        .await
        .expect("bootstrap source");
    let source_ref = upload_content(&store, &source_namespace_id, b"base", &context).await;
    assert_eq!(source_ref.owner_namespace_id, source_namespace_id);
    submit_operation(
        &store,
        &source_namespace_id,
        commit_id("seed-shared"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/shared.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(source_ref),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("seed shared file");
    namespace_engine(&store, &source_namespace_id, &context)
        .create_snapshot("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint before fork");

    let source_head = head_state(&store, &source_namespace_id).await;
    assert_eq!(source_head.seq, ChangeSeq(1));
    store.reset();
    store.inner().reset();
    let forked = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect("fork namespace");
    assert_eq!(forked.namespace_id, clone_namespace_id);
    assert_eq!(forked.head_seq, ChangeSeq(1));
    assert_eq!(forked.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "fork should validate manifest descriptors without loading metadata segment payloads"
    );

    assert_eq!(
        namespace_keys(&store, &clone_namespace_id).await,
        vec![
            hint(&clone_namespace_id),
            metadata_manifest_object(&clone_namespace_id, &ManifestNo(1))
        ],
        "a fork target installs its own manifest"
    );

    let clone_head = head_state(&store, &clone_namespace_id).await;

    assert_eq!(clone_head.seq, ChangeSeq(1));
    let clone_status = namespace_store(&store, &clone_namespace_id)
        .load_diagnostics()
        .await
        .expect("clone status");
    assert_eq!(
        (
            clone_status.retention_floor_seq,
            clone_status.acknowledged_floor_seq
        ),
        (ChangeSeq(1), ChangeSeq(1)),
        "a fork's first manifest acknowledges the fork point"
    );
    let fork_basis = clone_head.fork_basis.clone().expect("fork basis");
    assert_eq!(fork_basis.manifest.owner_namespace_id, source_namespace_id);
    assert_eq!(fork_basis.manifest.head_seq, ChangeSeq(1));
    assert_eq!(
        fork_basis.source_pin_id.manifest_no(),
        fork_basis.manifest.manifest_no
    );

    let source_record = load_pin(&store, &source_namespace_id, &fork_basis.source_pin_id).await;
    assert_eq!(source_record.head_seq, ChangeSeq(1));
    // The fork basis and pin must use the same manifest.
    assert_eq!(source_record.manifest(), fork_basis.manifest);
    assert_matches!(
        &source_record.owner,
        PinOwner::Fork {
            target_namespace_id,
            ..
        }
            if *target_namespace_id == clone_namespace_id,
        "fork record is owned by its target namespace"
    );

    let duplicate_error =
        fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
            .await
            .expect_err("duplicate fork target");
    assert_eq!(duplicate_error.code(), ErrorCode::NamespaceExists);

    let source_entry = resolve_path(&store, &source_namespace_id, "/docs/shared.txt")
        .await
        .expect("source stat");
    let clone_entry = resolve_path(&store, &clone_namespace_id, "/docs/shared.txt")
        .await
        .expect("clone stat");
    assert_eq!(source_entry.content_ref(), clone_entry.content_ref());
    let inherited_ref = source_entry.content_ref().expect("file content").clone();
    assert_eq!(inherited_ref.owner_namespace_id, source_namespace_id);
    let source_hint = hint(&source_namespace_id);
    let source_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&source_namespace_id);
    let guarded = FailStore::new(
        store.clone(),
        KeyPredicate::new(move |key| key == source_hint || key.starts_with(&source_manifests)),
        OperationSelector::Read,
        InjectedError::Transport("source head must not be read".to_owned()),
    );
    guarded.fail_all();
    assert_eq!(
        read_file_bytes(&guarded, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"base"
    );
    assert_eq!(guarded.attempts(), 0);
    assert_eq!(store.inner().counts().puts, 0);
    assert_eq!(
        store.inner().take_get_keys(),
        vec![format!(
            "namespaces/demo/content/{}",
            inherited_ref.content_id
        )]
    );
    let stale_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(0))
        .await
        .expect_err("old cursor");
    assert_eq!(stale_clone_changes.code(), ErrorCode::RebootstrapRequired);
    let empty_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("empty changes");
    assert!(empty_clone_changes.changes.is_empty());

    NamespaceFixture::new(&store, &source_namespace_id, &context)
        .write_file_bytes(
            "/docs/shared.txt",
            b"source-after-fork",
            Some(&commit_id("source-after-fork")),
        )
        .await
        .expect("source replace");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone after source write")
            .bytes,
        b"base"
    );

    let uploaded_ref =
        upload_content(&store, &clone_namespace_id, b"clone-after-fork", &context).await;
    assert_eq!(uploaded_ref.owner_namespace_id, clone_namespace_id);
    let clone_write = submit_operation(
        &store,
        &clone_namespace_id,
        commit_id("clone-after-fork"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/shared.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(uploaded_ref.clone()),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert!(store
        .head(&content_blob(&clone_namespace_id, &uploaded_ref.content_id))
        .await
        .expect("owner content")
        .is_some());
    assert_eq!(
        read_file_bytes(&store, &source_namespace_id, "/docs/shared.txt")
            .await
            .expect("read source")
            .bytes,
        b"source-after-fork"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"clone-after-fork"
    );

    let clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].committed_seq, ChangeSeq(2));

    // The target's own first fold inherits the source's segments by
    // reference and adds only its own delta run.
    namespace_engine(&store, &clone_namespace_id, &context)
        .fold_wal()
        .await
        .expect("fold clone");
    let clone_root = namespace_store(&store, &clone_namespace_id)
        .load_current_manifest()
        .await
        .expect("clone metadata root");
    let clone_manifest_bytes = store
        .get(
            &metadata_manifest_object(&clone_namespace_id, &clone_root.manifest().manifest_no),
            None,
        )
        .await
        .expect("read clone manifest")
        .expect("clone manifest exists");
    let clone_manifest =
        decode_namespace_manifest_json(&clone_manifest_bytes).expect("decode clone manifest");
    assert!(
        clone_manifest
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .any(|descriptor| descriptor.owner_namespace_id == source_namespace_id),
        "the target keeps referencing source-owned metadata segments"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone after its own fold")
            .bytes,
        b"clone-after-fork"
    );
    let engine = namespace_engine(&store, &clone_namespace_id, &context);
    let mut revisions_compacted = false;
    for _ in 0..16 {
        let report = engine
            .compact_metadata_unit(
                None,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                loonfs_types::CompactorEpoch(0),
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("compact clone");
        match report {
            loonfs_core::CompactionUnitOutcome::NotNeeded => break,
            loonfs_core::CompactionUnitOutcome::Published { group, .. } => {
                revisions_compacted |= group == loonfs_types::MetadataFamilyGroup::Revisions;
            }
            other => panic!("expected bounded compaction, got {other:?}"),
        }
    }
    assert!(revisions_compacted);
    let read_context = crate::common::read_context(&store, &clone_namespace_id).await;
    let revisions = engine
        .list_file_revisions_by_inode_page(
            clone_entry.inode_id,
            loonfs_types::PageRequest {
                limit: loonfs_test_support::ids::page_limit(10),
                cursor: None,
            },
            &read_context,
        )
        .await
        .expect("compacted history");
    assert_eq!(revisions.items.len(), 2);
    assert_eq!(revisions.items[1].content_ref, inherited_ref);
}

/// The inode rows that the current manifest's segments store.
async fn inode_rows<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> u64 {
    namespace_store(store, namespace_id)
        .load_statistics()
        .await
        .expect("load the namespace's statistics")
        .inode_record_count
}

/// Checks that a fork's base compactions remove the rows of a deletion that
/// the fork inherited with no writer session ever opened in the fork,
/// because the fork's retention floor and acknowledged floor start at its
/// fork point, so every inherited deletion has expired. The source keeps
/// its rows, and the fork's first write commits right after the fork point.
#[tokio::test]
async fn a_fork_removes_an_inherited_deletions_rows_without_a_writer_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let source_namespace_id = namespace_id("source");
    let fork_namespace_id = NamespaceId::parse("fork").expect("valid namespace id");
    NamespaceFixture::new(&store, &source_namespace_id, &context)
        .create()
        .await
        .expect("bootstrap source");
    // Each file is folded into a run of its own, so the fork inherits more
    // than one run of the inode and revision groups, and its compaction of
    // those groups has a window to merge.
    let source = namespace_engine(&store, &source_namespace_id, &context);
    for (path, commit) in [("/old/a.txt", "seed-old"), ("/kept.txt", "seed-kept")] {
        NamespaceFixture::new(&store, &source_namespace_id, &context)
            .write_file_bytes(path, b"body", Some(&commit_id(commit)))
            .await
            .expect("seed a file");
        source.fold_wal().await.expect("fold the file");
    }
    submit_operation(
        &store,
        &source_namespace_id,
        commit_id("delete-old"),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/old").expect("path"),
            behavior: loonfs_types::DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete /old");
    source.fold_wal().await.expect("fold the deletion");
    fork_namespace(&store, &source_namespace_id, &fork_namespace_id, &context)
        .await
        .expect("fork");
    let fork_head = head_state(&store, &fork_namespace_id).await;
    let source_rows = inode_rows(&store, &source_namespace_id).await;
    let fork_rows = inode_rows(&store, &fork_namespace_id).await;
    assert_eq!(fork_rows, source_rows);

    let fork = namespace_engine(&store, &fork_namespace_id, &context);
    for _ in 0..16 {
        let outcome = fork
            .compact_metadata_unit(
                None,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                loonfs_types::CompactorEpoch(0),
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("compact the fork");
        if matches!(outcome, loonfs_core::CompactionUnitOutcome::NotNeeded) {
            break;
        }
    }
    assert_eq!(
        inode_rows(&store, &fork_namespace_id).await,
        fork_rows - 2,
        "the fork removes the rows of /old and /old/a.txt"
    );
    assert_eq!(
        inode_rows(&store, &source_namespace_id).await,
        source_rows,
        "the source keeps the rows of its recoverable deletion"
    );
    let after_compaction = head_state(&store, &fork_namespace_id).await;
    assert_eq!(after_compaction.seq, fork_head.seq);
    assert_eq!(
        after_compaction.writer_epoch, fork_head.writer_epoch,
        "no writer session ran in the fork"
    );

    let first_write = NamespaceFixture::new(&store, &fork_namespace_id, &context)
        .write_file_bytes("/new.txt", b"new", Some(&commit_id("fork-first-write")))
        .await
        .expect("the fork's first write");
    assert_eq!(first_write.committed_seq.0, fork_head.seq.0 + 1);
}

#[tokio::test]
async fn fork_clone_survives_source_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("valid namespace id");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    let context = default_writer();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");

    namespace_engine(&store, &source, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete source");

    let clone_head = head_state(&store, &clone).await;
    assert_eq!(clone_head.seq, ChangeSeq(1));
    assert_eq!(
        read_file_bytes(&store, &clone, "/docs/shared.txt")
            .await
            .expect("clone reads forked file")
            .bytes,
        b"base"
    );
}

#[tokio::test]
async fn nested_fork_survives_ancestor_and_parent_delete_and_collection() {
    let temp_dir = tempdir().expect("tempdir");
    let ancestor = NamespaceId::parse("ancestor").expect("valid namespace id");
    let parent = NamespaceId::parse("parent").expect("valid namespace id");
    let descendant = NamespaceId::parse("descendant").expect("valid namespace id");
    let context = default_writer();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&store, &ancestor, &context).await;
    fork_namespace(&store, &ancestor, &parent, &context)
        .await
        .expect("fork parent");
    fork_namespace(&store, &parent, &descendant, &context)
        .await
        .expect("fork descendant");

    NamespaceFixture::new(&store, &descendant, &context)
        .write_file_bytes(
            "/docs/own.txt",
            b"own",
            Some(&commit_id("descendant-write")),
        )
        .await
        .expect("write descendant");
    let engine = namespace_engine(&store, &descendant, &context);
    engine.fold_wal().await.expect("fold descendant");
    let mut compacted = false;
    for _ in 0..16 {
        match engine
            .compact_metadata_unit(
                None,
                loonfs_core::MetadataCompactionPolicy::CompactImmediately,
                loonfs_core::CompactionAdmission::AnySize,
                loonfs_types::CompactorEpoch(0),
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("compact descendant")
        {
            loonfs_core::CompactionUnitOutcome::NotNeeded => break,
            loonfs_core::CompactionUnitOutcome::Published { .. } => compacted = true,
            other => panic!("expected compaction, got {other:?}"),
        }
    }
    assert!(compacted);

    namespace_engine(&store, &parent, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete parent");
    namespace_engine(&store, &ancestor, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete ancestor");

    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    namespace_store(&store, &ancestor)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("collect ancestor");

    assert_eq!(
        read_file_bytes(&store, &descendant, "/docs/shared.txt")
            .await
            .expect("descendant reads through its retained ancestry")
            .bytes,
        b"base"
    );
    let options = loonfs_core::GcOptions::default();
    let parent_deadline = namespace_store(&store, &parent)
        .load_read_state()
        .await
        .expect("parent tombstone")
        .status
        .deleted_at_ms()
        .expect("deletion stamp")
        + options
            .grace_window_ms
            .max(loonfs_core::limits::NAMESPACE_RETIREMENT_GRACE_WINDOW_MS);
    let parent_waiting = namespace_store(&store, &parent)
        .gc(None, &options, &aged.clock())
        .await
        .expect("parent waits");
    assert_eq!(parent_waiting.reclaimable_at_ms, Some(parent_deadline));
    let ancestor_waiting = namespace_store(&store, &ancestor)
        .gc(None, &options, &aged.clock())
        .await
        .expect("ancestor waits");
    assert_eq!(ancestor_waiting.deleted_checkpoints_by_owner.fork, 0);
    namespace_engine(&store, &descendant, &aged)
        .delete_namespace(Default::default())
        .await
        .expect("delete descendant");
    aged.now_ms = namespace_store(&store, &descendant)
        .load_read_state()
        .await
        .expect("descendant tombstone")
        .status
        .deleted_at_ms()
        .expect("deletion stamp");
    let retired = namespace_store(&store, &descendant)
        .gc(None, &options, &aged.clock())
        .await
        .expect("retire descendant");
    let descendant_deadline = retired
        .reclaimable_at_ms
        .expect("descendant retired despite compaction");
    assert_eq!(retired.deleted.content_objects, 0);
    let waiting = namespace_store(&store, &parent)
        .gc(None, &options, &aged.clock())
        .await
        .expect("wait for descendant grace");
    assert_eq!(waiting.deleted_checkpoints_by_owner.fork, 0);
    aged.now_ms = descendant_deadline;
    namespace_store(&store, &descendant)
        .gc(None, &options, &aged.clock())
        .await
        .expect("descendant releases its source pin");
    let released = namespace_store(&store, &parent)
        .gc(None, &options, &aged.clock())
        .await
        .expect("parent releases its source pin");
    assert_eq!(released.deleted_checkpoints_by_owner.fork, 1);
    assert_eq!(released.reclaimable_at_ms, Some(parent_deadline));
    let released = namespace_store(&store, &ancestor)
        .gc(None, &options, &aged.clock())
        .await
        .expect("collect ancestor after parent releases its pin");
    assert_eq!(released.deleted_checkpoints_by_owner.fork, 0);
    assert!(released.reclaimable_at_ms.is_some());
}

#[tokio::test]
async fn a_fork_survives_a_concurrent_collection_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = default_writer();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let gc_options = loonfs_core::GcOptions::default();
    let forking = fork_namespace(store.as_ref(), &source, &clone, &context);
    let source_store = namespace_store(store.as_ref(), &source);
    let clock = context.clock();
    let collecting = source_store.gc(None, &gc_options, &clock);
    let (forked, collected) = tokio::join!(forking, collecting);
    forked.expect("creation grace protects the fork");
    collected.expect("the pass finishes");
    assert_eq!(
        head_state(store.as_ref(), &clone).await.status,
        loonfs_types::format::control::NamespaceStatus::Active {}
    );
    assert_eq!(
        read_file_bytes(store.as_ref(), &clone, "/docs/shared.txt")
            .await
            .expect("the target reads its inherited file")
            .bytes,
        b"base"
    );
}

#[tokio::test]
async fn fork_namespace_rejects_corrupt_source_manifest_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;
    let checkpoint = namespace_engine(&store, &source_namespace_id, &context)
        .create_snapshot("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint")
        .response;

    let source_record = load_pin(&store, &source_namespace_id, &checkpoint.snapshot_id).await;
    let manifest_key =
        metadata_manifest_object(&source_namespace_id, &source_record.pin_id.manifest_no());
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read source manifest")
        .expect("source manifest exists");
    let mut manifest = decode_namespace_manifest_json(&manifest_bytes)
        .expect("decode source manifest")
        .into_payload();
    manifest.runs.iter_mut().for_each(|run| {
        run.segments
            .retain(|descriptor| descriptor.family != MetadataRowFamily::DirentryChildBinds);
    });
    let manifest = loonfs_types::format::manifest::encode_namespace_manifest_json(manifest)
        .expect("rebuild manifest checksum")
        .into_envelope();
    let corrupted = encode_namespace_manifest_json(manifest.payload().clone())
        .expect("encode corrupt manifest")
        .into_bytes();
    replace_object(&store, &manifest_key, Bytes::from(corrupted))
        .await
        .expect("overwrite source manifest");

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("corrupt source manifest should block fork");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(
        namespace_keys(&store, &clone_namespace_id).await.is_empty(),
        "a failed fork leaves the target absent"
    );
}

#[tokio::test]
async fn fork_source_checkpoint_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = default_writer();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(format!("namespaces/{}/pins/", source_namespace_id.as_str())),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::PermissionDenied("injected source checkpoint failure".to_owned()),
    );
    store.fail_next(1);
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("source checkpoint failure should abort fork before target publication");
    assert_eq!(error.code(), ErrorCode::StoragePermissionDenied);
    assert!(
        namespace_keys(&store, &clone_namespace_id).await.is_empty(),
        "the target must not be installed before the source basis is pinned"
    );
    assert!(
        store
            .head(&hint(&source_namespace_id))
            .await
            .expect("head source head")
            .is_some(),
        "the source is untouched"
    );
}

#[tokio::test]
async fn a_create_losing_to_a_foreign_head_reports_the_id_as_taken() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = default_writer();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    // Another writer's complete head for the same id, already durable.
    let foreign = loonfs_types::format::manifest::NamespaceManifestPayload::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
        loonfs_types::NamespaceAccess::Unrestricted {},
    );
    let foreign_bytes =
        loonfs_types::format::manifest::encode_namespace_manifest_json(foreign.clone())
            .map(|encoded| encoded.into_bytes())
            .expect("encode foreign head");
    let store = LostCreateStore::new(
        inner,
        KeyPredicate::exact(metadata_manifest_object(
            &namespace_id,
            &loonfs_types::ManifestNo(1),
        )),
        foreign_bytes.clone(),
    );

    let error = NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect_err("another namespace owns the id");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        store
            .get(
                &metadata_manifest_object(&namespace_id, &loonfs_types::ManifestNo(1)),
                None
            )
            .await
            .expect("read head")
            .expect("head exists")
            .to_vec(),
        foreign_bytes,
        "the losing create must not touch the winner's head"
    );
}

#[tokio::test]
async fn namespace_delete_blocks_reads_writes_and_forks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = default_writer();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let content = namespace_store(&store, &namespace_id)
        .store_bytes_as_content(b"will vanish")
        .await
        .expect("stage content");
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("before-delete").expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/keep.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("commit before delete");

    // A stale precondition deletes nothing.
    let engine = namespace_engine(&store, &namespace_id, &context);
    let stale = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(0)),
        })
        .await
        .expect_err("stale precondition");
    assert_eq!(stale.code(), ErrorCode::StaleHead);

    let response = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    assert_eq!(response.head_seq, ChangeSeq(1));

    // Reads, commits, status, repeat deletes, and forks observe the deleted head.
    let read = resolve_path(&store, &namespace_id, "/")
        .await
        .expect_err("read after delete");
    assert_eq!(read.code(), ErrorCode::NamespaceDeleted);
    let commit = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("after-delete").expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/late.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("commit after delete");
    assert_eq!(commit.code(), ErrorCode::NamespaceDeleted);
    let again = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect_err("repeat delete");
    assert_eq!(again.code(), ErrorCode::NamespaceDeleted);
    let fork_target = NamespaceId::parse("fork-of-deleted").expect("valid namespace id");
    let fork = fork_namespace(&store, &namespace_id, &fork_target, &context).await;
    assert_eq!(
        fork.expect_err("fork of deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
}

#[tokio::test]
async fn gc_preserves_unfolded_data_then_the_current_manifest_tombstone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/keep.txt", b"keep me", Some(&commit_id("keep")))
        .await
        .expect("write");

    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    let report = namespace_store(&store, &namespace_id)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("gc a namespace with no root");
    assert_eq!(report.deleted.wal_objects, 0, "{report:?}");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/keep.txt")
            .await
            .expect("still readable after gc")
            .bytes,
        b"keep me"
    );

    namespace_engine(&store, &namespace_id, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete");
    // The runtime that deleted the namespace raises its hint to the tombstone.
    let tombstone = namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("tombstone")
        .manifest()
        .manifest_no;
    namespace_store(&store, &namespace_id)
        .raise_hint(tombstone, None)
        .await
        .expect("raise the hint");
    let report = namespace_store(&store, &namespace_id)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("gc the tombstone");
    assert!(report.deleted.wal_objects >= 1, "{report:?}");
    namespace_store(&store, &namespace_id)
        .gc(None, &loonfs_core::GcOptions::default(), &aged.clock())
        .await
        .expect("collect obsolete tombstone version");
    let current = namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current tombstone");
    let mut expected = vec![hint(&namespace_id), current.object_key.clone()];
    expected.extend(
        current
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .map(loonfs_objectstore::keys::metadata_segment_object_key),
    );
    expected.sort();
    assert_eq!(namespace_keys(&store, &namespace_id).await, expected);
    assert!(store
        .head(&hint(&namespace_id))
        .await
        .expect("probe hint")
        .is_some());
}

#[tokio::test]
async fn creation_and_fork_install_hint_and_manifest_in_order() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = default_writer();
    let source = namespace_id("source");
    NamespaceFixture::new(&store, &source, &context)
        .create()
        .await
        .expect("bootstrap");
    let puts: Vec<_> = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, mode, .. } => Some((key, mode)),
            _ => None,
        })
        .collect();
    assert_eq!(
        puts,
        vec![
            (hint(&source), PutMode::CreateIfAbsent),
            (
                metadata_manifest_object(&source, &ManifestNo(1)),
                PutMode::CreateIfAbsent
            ),
        ]
    );
    store.reset();
    let target = namespace_id("target");
    fork_namespace(&store, &source, &target, &context)
        .await
        .expect("fork");
    let puts: Vec<_> = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put {
                key,
                mode: PutMode::CreateIfAbsent,
                ..
            } if key.starts_with(&format!("namespaces/{target}/")) => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(
        puts,
        vec![
            hint(&target),
            metadata_manifest_object(&target, &ManifestNo(1))
        ]
    );

    store.reset();
    let error = fork_namespace(&store, &source, &target, &context)
        .await
        .expect_err("target exists");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 0);

    for allow_existing in [false, true] {
        store.reset();
        let result = if allow_existing {
            NamespaceFixture::new(&store, &source, &context)
                .create_with_options(&loonfs_core::CreateNamespaceOptions {
                    allow_existing: true,
                    ..Default::default()
                })
                .await
        } else {
            NamespaceFixture::new(&store, &source, &context)
                .create()
                .await
        };
        if allow_existing {
            assert_eq!(result.expect("adopt source").namespace_id, source);
        } else {
            assert_eq!(
                result.expect_err("source exists").code(),
                ErrorCode::NamespaceExists
            );
        }
        let counts = store.counts();
        assert_eq!(
            (counts.puts, counts.compare_and_swaps, counts.deletes),
            (0, 0, 0)
        );
    }
}

#[tokio::test]
async fn bootstrap_hint_read_failures_write_nothing() {
    for injected in [
        InjectedError::Transport("hint read failed".to_owned()),
        InjectedError::PermissionDenied("hint read denied".to_owned()),
    ] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = namespace_id("demo");
        let failing = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::exact(hint(&namespace_id)),
            OperationSelector::Read,
            injected,
        );
        failing.fail_all();
        let store = RecordingStore::new(failing, KeyPredicate::any());
        for allow_existing in [false, true] {
            let error = (if allow_existing {
                NamespaceFixture::new(&store, &namespace_id, &default_writer())
                    .create_with_options(&loonfs_core::CreateNamespaceOptions {
                        allow_existing: true,
                        ..Default::default()
                    })
                    .await
            } else {
                NamespaceFixture::new(&store, &namespace_id, &default_writer())
                    .create()
                    .await
            })
            .expect_err("hint read failed");
            assert_matches!(error, CoreError::ControlObjectLoad(_));
        }
        let counts = store.counts();
        assert_eq!(
            (counts.puts, counts.compare_and_swaps, counts.deletes),
            (0, 0, 0)
        );
    }
}

#[tokio::test]
async fn bootstrap_of_a_corrupt_hint_writes_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    store
        .put_if_absent(&hint(&namespace_id), Bytes::from_static(b"invalid hint"))
        .await
        .expect("corrupt hint");
    store.reset();
    for allow_existing in [false, true] {
        let error = (if allow_existing {
            NamespaceFixture::new(&store, &namespace_id, &default_writer())
                .create_with_options(&loonfs_core::CreateNamespaceOptions {
                    allow_existing: true,
                    ..Default::default()
                })
                .await
        } else {
            NamespaceFixture::new(&store, &namespace_id, &default_writer())
                .create()
                .await
        })
        .expect_err("corrupt hint");
        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }
    let counts = store.counts();
    assert_eq!(
        (counts.puts, counts.compare_and_swaps, counts.deletes),
        (0, 0, 0)
    );
}

#[tokio::test]
async fn a_creator_losing_after_namespace_discovery_leaves_no_extra_objects() {
    let directory = tempdir().expect("tempdir");
    let store = loonfs_test_support::stores::BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id("demo"))),
        OperationSelector::PutCreateIfAbsent,
    );
    let namespace_id = namespace_id("demo");
    let context = default_writer();
    store.block_next();
    let delayed_fixture = NamespaceFixture::new(&store, &namespace_id, &context);
    let delayed = delayed_fixture.create();
    let winner = async {
        store.wait_until_blocked().await;
        let result = NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await;
        store.release();
        result.expect("concurrent creator wins")
    };
    let (delayed, winner) = tokio::join!(delayed, winner);
    assert_eq!(
        delayed.expect_err("lost conditional write").code(),
        ErrorCode::NamespaceExists
    );
    assert_eq!(winner.namespace_id, namespace_id);
    assert_eq!(
        namespace_keys(&store, &namespace_id).await,
        vec![
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ]
    );
}

#[tokio::test]
async fn retired_leaf_content_is_reclaimed_while_live_workspaces_keep_their_content() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = default_writer();
    let source = namespace_id("template");
    let sibling = namespace_id("sibling");
    seed_source_namespace_for_fork(&store, &source, &setup).await;
    fork_namespace(&store, &source, &sibling, &setup)
        .await
        .expect("fork sibling");
    NamespaceFixture::new(&store, &sibling, &setup)
        .write_file_bytes(
            "/docs/sibling.txt",
            b"sibling",
            Some(&commit_id("sibling-write")),
        )
        .await
        .expect("write sibling");
    let source_file = read_file_bytes(&store, &source, "/docs/shared.txt")
        .await
        .expect("source");
    let source_ref = source_file.entry.content_ref().expect("source content");
    let source_key = content_blob(&source_ref.owner_namespace_id, &source_ref.content_id);
    let sibling_file = read_file_bytes(&store, &sibling, "/docs/sibling.txt")
        .await
        .expect("sibling");
    let sibling_ref = sibling_file.entry.content_ref().expect("sibling content");
    let sibling_key = content_blob(&sibling_ref.owner_namespace_id, &sibling_ref.content_id);
    let options = loonfs_core::GcOptions::default();
    let mut aged = setup.clone();
    aged.now_ms = u64::MAX / 4;
    for number in 0..4 {
        let leaf = namespace_id(&format!("leaf-{number}"));
        fork_namespace(&store, &source, &leaf, &setup)
            .await
            .expect("fork leaf");
        NamespaceFixture::new(&store, &leaf, &setup)
            .write_file_bytes("/docs/own.txt", b"leaf", Some(&commit_id("leaf-write")))
            .await
            .expect("write leaf");
        submit_operation(
            &store,
            &leaf,
            CommitId::generate(),
            FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse("/docs/shared.txt").expect("source path"),
                destination_path: AbsolutePath::parse("/docs/copied.txt")
                    .expect("destination path"),
                precondition: Default::default(),
            },
            &setup,
        )
        .await
        .expect("publish inherited content in target");
        let leaf_file = read_file_bytes(&store, &leaf, "/docs/own.txt")
            .await
            .expect("leaf");
        let leaf_ref = leaf_file.entry.content_ref().expect("leaf content");
        let leaf_key = content_blob(&leaf_ref.owner_namespace_id, &leaf_ref.content_id);
        namespace_engine(&store, &leaf, &setup)
            .delete_namespace(Default::default())
            .await
            .expect("delete leaf");
        store.reset();
        let report = namespace_store(&store, &leaf)
            .gc(None, &options, &aged.clock())
            .await
            .expect("collect leaf");
        assert_eq!(report.deleted.retired_content_objects, 1);
        let content_lists = store
            .snapshot()
            .into_iter()
            .filter_map(|operation| match operation {
                RecordedOperation::List { prefix, .. } if prefix.contains("/content/") => {
                    Some(prefix)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(content_lists, [format!("namespaces/{leaf}/content/")]);
        assert!(store.head(&leaf_key).await.expect("leaf content").is_none());
        for key in [&source_key, &sibling_key] {
            assert!(store.head(key).await.expect("retained content").is_some());
        }
        assert_eq!(
            read_file_bytes(&store, &source, "/docs/shared.txt")
                .await
                .expect("source read")
                .bytes,
            b"base"
        );
        assert_eq!(
            read_file_bytes(&store, &sibling, "/docs/shared.txt")
                .await
                .expect("inherited read")
                .bytes,
            b"base"
        );
        assert_eq!(
            read_file_bytes(&store, &sibling, "/docs/sibling.txt")
                .await
                .expect("sibling read")
                .bytes,
            b"sibling"
        );
    }
    namespace_engine(&store, &source, &aged)
        .delete_namespace(Default::default())
        .await
        .expect("delete source");
    let source_deadline = namespace_store(&store, &source)
        .load_read_state()
        .await
        .expect("source tombstone")
        .status
        .deleted_at_ms()
        .expect("deletion stamp")
        + options
            .grace_window_ms
            .max(loonfs_core::limits::NAMESPACE_RETIREMENT_GRACE_WINDOW_MS);
    for _ in 0..3 {
        aged.now_ms += options.grace_window_ms;
        let report = namespace_store(&store, &source)
            .gc(None, &options, &aged.clock())
            .await
            .expect("collect deleted source");
        assert_eq!(report.reclaimable_at_ms, Some(source_deadline));
        assert_eq!(report.deleted.retired_content_objects, 0);
        assert!(store
            .head(&source_key)
            .await
            .expect("source content")
            .is_some());
    }
    assert_eq!(
        read_file_bytes(&store, &sibling, "/docs/shared.txt")
            .await
            .expect("read deleted ancestor")
            .bytes,
        b"base"
    );
}
