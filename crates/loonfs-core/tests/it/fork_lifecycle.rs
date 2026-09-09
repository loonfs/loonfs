//! Namespace creation, fork installation, and terminal lifecycle guards.
//!
//! Tests cover descriptor creation, conditional head installation, and
//! namespace state before the first metadata publication.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::{
    wire::control::{decode_control_object, CheckpointOwner, ContentStoreState, ControlObjectKind},
    wire::manifest::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataRowFamily,
    },
    AbsolutePath, ChangeSeq, CommitId, DestinationBehavior, ManifestNo, NamespaceId,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::control::load_namespace_head_control;
use loonfs_core::publish::FilesystemOperation;
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::keys::{
    content_blob, content_owner_prefix, content_store, hint, metadata_manifest_object,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use std::sync::Arc;
use tempfile::tempdir;

async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::Namespace, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id, None)
        .await
}

async fn listed_names<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    let context = crate::common::read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .list_path_page(
            "/docs",
            loonfs_api::PageRequest {
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
async fn snapshot_fork_keeps_its_tree_after_snapshot_release_and_source_gc() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = mutation_context();
    let source = namespace_id("source");
    let target = namespace_id("target");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let snapshot = engine
        .create_snapshot("basis".to_owned(), u64::MAX / 2)
        .await
        .expect("snapshot");
    let snapshot_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source,
        &snapshot.checkpoint_id,
    )
    .await
    .expect("load snapshot")
    .expect("snapshot exists");
    write_file_bytes(
        &store,
        &source,
        "/docs/later.txt",
        b"later",
        &context,
        Some("later"),
    )
    .await
    .expect("advance source");
    let fork = engine
        .fork_namespace(&target, Some(&snapshot.checkpoint_id))
        .await
        .expect("fork snapshot");
    assert_eq!(fork.head_seq, snapshot.checkpoint_seq);
    let head = head_state(&store, &target).await;
    assert_eq!(head.head_commit_id, snapshot_record.head_commit_id);
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
        loonfs_core::control::load_namespace_checkpoint_record_control(
            &store,
            &source,
            &snapshot.checkpoint_id,
        )
        .await
        .expect("load snapshot")
        .expect("snapshot exists"),
        snapshot_record
    );
    engine
        .release_snapshot(&snapshot.checkpoint_id)
        .await
        .expect("release snapshot");
    engine
        .create_checkpoint("current".to_owned(), None)
        .await
        .expect("flush current source");
    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    loonfs_core::gc_namespace(&store, &source, &loonfs_core::GcConfig::default(), &aged)
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
    let context = mutation_context();
    let source = namespace_id("source");
    let other = namespace_id("other");
    let target = namespace_id("target");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    seed_source_namespace_for_fork(&store, &other, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let expired = engine
        .create_snapshot("expired".to_owned(), 1)
        .await
        .expect("expired snapshot");
    let foreign = namespace_engine(&store, &other, &context)
        .create_snapshot("foreign".to_owned(), u64::MAX / 2)
        .await
        .expect("foreign snapshot");
    let checkpoint = engine
        .create_checkpoint("user".to_owned(), None)
        .await
        .expect("user checkpoint");
    for (snapshot_id, expected_code) in [
        (expired.checkpoint_id, ErrorCode::SnapshotGone),
        (foreign.checkpoint_id, ErrorCode::SnapshotNotFound),
        (checkpoint.checkpoint_id, ErrorCode::SnapshotNotFound),
    ] {
        store.take();
        let error = engine
            .fork_namespace(&target, Some(&snapshot_id))
            .await
            .expect_err("invalid snapshot");
        assert_eq!(error.code(), expected_code);
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
        assert!(namespace_keys(&store, &target).await.is_empty());
    }
}

#[tokio::test]
async fn snapshot_release_during_fork_releases_the_attempt_without_installing_a_target() {
    let directory = tempdir().expect("tempdir");
    let source = namespace_id("source");
    let target = namespace_id("target");
    let store = loonfs_test_support::stores::BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(format!("namespaces/{source}/pins/")),
        OperationClass::PutCreateIfAbsent,
    );
    let context = mutation_context();
    seed_source_namespace_for_fork(&store, &source, &context).await;
    let engine = namespace_engine(&store, &source, &context);
    let snapshot = engine
        .create_snapshot("basis".to_owned(), u64::MAX / 2)
        .await
        .expect("snapshot");
    store.block_next();
    let forking = engine.fork_namespace(&target, Some(&snapshot.checkpoint_id));
    let releasing = async {
        store.wait_until_blocked().await;
        engine
            .release_snapshot(&snapshot.checkpoint_id)
            .await
            .expect("release snapshot during fork write");
        store.release();
    };
    let (result, ()) = tokio::join!(forking, releasing);
    assert_eq!(
        result.expect_err("snapshot gone").code(),
        ErrorCode::SnapshotGone
    );
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
    context: &MutationContext,
) {
    bootstrap_namespace(store, source_namespace_id, context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        store,
        source_namespace_id,
        "/docs/shared.txt",
        b"base",
        context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
}

async fn head_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> loonfs_core::control::NamespaceReadState {
    load_namespace_head_control(store, namespace_id)
        .await
        .expect("load head")
        .state
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
async fn a_created_namespace_reads_manifest_one_before_its_first_flush() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    let created = bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(
        store
            .list_prefix("content-stores/")
            .await
            .expect("list content stores"),
        vec![content_store(&head.state.content_store_id)],
    );

    let root_entry = resolve_path(&store, &namespace_id, "/")
        .await
        .expect("a fresh namespace serves reads");
    assert_eq!(root_entry.inode_kind(), loonfs_api::InodeKind::Directory);
    let status = loonfs_core::cache::load_namespace_diagnostics(&store, &namespace_id)
        .await
        .expect("status");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(1)));
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/first.txt",
        b"hello",
        &context,
        Some("first-write"),
    )
    .await
    .expect("a fresh namespace accepts writes");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/first.txt")
            .await
            .expect("read back")
            .bytes,
        b"hello"
    );
    let keys = namespace_keys(&store, &namespace_id).await;
    assert!(
        keys.iter().all(|key| key == &hint(&namespace_id)
            || key.starts_with(&format!("namespaces/{namespace_id}/manifests/"))
            || key.starts_with(&format!("namespaces/{}/wal/", namespace_id.as_str()))),
        "only control and WAL objects exist before the first flush: {keys:?}"
    );

    namespace_engine(&store, &namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush");
    assert!(
        store
            .head(&metadata_manifest_object(&namespace_id, &ManifestNo(3)))
            .await
            .expect("probe root")
            .is_some(),
        "the flush follows the writer acquisitions"
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/first.txt")
            .await
            .expect("read after flush")
            .bytes,
        b"hello"
    );
    let status = loonfs_core::cache::load_namespace_diagnostics(&store, &namespace_id)
        .await
        .expect("status after flush");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(3)));
}

#[tokio::test]
async fn namespace_create_recovers_when_manifest_one_lands_ambiguously() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(metadata_manifest_object(&namespace_id, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost namespace-head acknowledgment".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);

    let created = bootstrap_namespace(&store, &namespace_id, &mutation_context(), false)
        .await
        .expect("head identity reconciles the landed create");

    assert_eq!(created.namespace_id, namespace_id);
    assert_eq!(store.attempts(), 1);
    assert_eq!(
        head_state(&store, &namespace_id).await.status,
        loonfs_api::wire::control::NamespaceStatus::Active {}
    );
}

#[tokio::test]
async fn concurrent_creates_of_one_id_leave_exactly_one_winner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = namespace_id("demo");
    let first = mutation_context();
    let mut second = mutation_context();
    second.writer_id = loonfs_api::WriterId::parse("writer-second").expect("writer id");

    let (left, right) = tokio::join!(
        bootstrap_namespace(store.as_ref(), &namespace_id, &first, false),
        bootstrap_namespace(store.as_ref(), &namespace_id, &second, false),
    );
    let outcomes = [left, right];
    let winners = outcomes.iter().filter(|result| result.is_ok()).count();
    assert_eq!(winners, 1, "exactly one create may win: {outcomes:?}");
    let loser = outcomes
        .into_iter()
        .find_map(|result| result.err())
        .expect("one loser");
    assert_eq!(loser.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        namespace_keys(store.as_ref(), &namespace_id).await,
        vec![
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ]
    );
}

#[tokio::test]
async fn a_create_retry_after_a_lost_acknowledgment_reports_the_id_as_taken() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("first create lands");
    let head_before = head_state(&store, &namespace_id).await;

    let mut retry_context = context.clone();
    retry_context.now_ms += 5_000;
    let conflict = bootstrap_namespace(&store, &namespace_id, &retry_context, false)
        .await
        .expect_err("the id is taken, whoever took it");
    assert_eq!(conflict.code(), ErrorCode::NamespaceExists);

    let mut other_writer = context.clone();
    other_writer.writer_id = loonfs_api::WriterId::parse("writer-other").expect("writer id");
    let conflict = bootstrap_namespace(&store, &namespace_id, &other_writer, false)
        .await
        .expect_err("another writer may not adopt this namespace either");
    assert_eq!(conflict.code(), ErrorCode::NamespaceExists);

    // Opting in makes the retry succeed, and the namespace it returns is
    // the one that landed.
    let adopted = bootstrap_namespace(&store, &namespace_id, &retry_context, true)
        .await
        .expect("allow_existing adopts the landed namespace");
    assert_eq!(adopted.namespace_id, namespace_id);
    assert_eq!(
        head_state(&store, &namespace_id).await,
        head_before,
        "no retry may rewrite the landed head"
    );
}

#[tokio::test]
async fn concurrent_installs_of_one_target_leave_exactly_one_winner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = mutation_context();
    let source = namespace_id("source");
    let target = NamespaceId::parse("target").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let mut second = context.clone();
    second.writer_id = loonfs_api::WriterId::parse("writer-second").expect("writer id");
    let (left, right) = tokio::join!(
        fork_namespace(store.as_ref(), &source, &target, &context),
        fork_namespace(store.as_ref(), &source, &target, &second),
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
    let (created, forked) = tokio::join!(
        bootstrap_namespace(store.as_ref(), &contested, &context, false),
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
        loonfs_api::wire::control::NamespaceStatus::Active {}
    );
    if created.is_ok() {
        assert!(head.fork_basis.is_none(), "the create won");
    } else {
        assert!(head.fork_basis.is_some(), "the fork won");
    }
}

#[tokio::test]
async fn fork_install_recovers_when_target_manifest_one_lands_ambiguously() {
    let temp_dir = tempdir().expect("tempdir");
    let context = mutation_context();
    let source = namespace_id("source");
    let target = namespace_id("target");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(metadata_manifest_object(&target, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost fork-head acknowledgment".to_owned()),
    )
    .apply_then_fail();
    seed_source_namespace_for_fork(&store, &source, &context).await;
    store.fail_next(1);

    let forked = fork_namespace(&store, &source, &target, &context)
        .await
        .expect("fork basis identity reconciles the landed target");

    assert_eq!(forked.namespace_id, target);
    assert_eq!(store.attempts(), 1);
    let target_head = head_state(&store, &target).await;
    let basis = target_head.fork_basis.expect("fork target basis");
    assert_eq!(basis.manifest.owner_namespace_id, source);
}

#[tokio::test]
async fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    async fn upload_content<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        bytes: &[u8],
        context: &MutationContext,
    ) -> loonfs_api::ContentRef {
        let engine = namespace_engine(store, namespace_id, context);
        let upload = engine.begin_upload().await.expect("begin upload");
        let staged = engine
            .upload_content(upload.upload_id(), bytes)
            .await
            .expect("upload bytes");
        let catalog = loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
            .await
            .expect("catalog");
        let completed = engine
            .complete_upload(
                &catalog,
                upload.upload_id(),
                loonfs_core::ResolvedUploadCompletion::KnownContent,
            )
            .await
            .expect("complete upload");
        assert_eq!(completed.response.content_ref(), Some(&staged.content_ref));
        staged.content_ref
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .await
        .expect("bootstrap source");
    let source_ref = upload_content(&store, &source_namespace_id, b"base", &context).await;
    assert_eq!(source_ref.owner_namespace_id, source_namespace_id);
    submit_operation(
        &store,
        &source_namespace_id,
        test_commit_id(Some("seed-shared")),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/shared.txt").expect("path"),
            content_ref: source_ref,
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("seed shared file");
    namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint before fork");

    let source_head = head_state(&store, &source_namespace_id).await;
    assert_eq!(source_head.seq, ChangeSeq(1));
    let content_store_id = source_head.content_store_id.clone();
    store.reset();
    store.inner().reset();
    let forked = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect("fork namespace");
    assert_eq!(forked.namespace_id, clone_namespace_id);
    assert_eq!(forked.head_seq, ChangeSeq(1));
    assert_eq!(forked.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        store.count(OperationClass::Read),
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
    assert_eq!(clone_head.content_store_id, content_store_id);
    assert_eq!(clone_head.seq, ChangeSeq(1));
    let fork_basis = clone_head.fork_basis.clone().expect("fork basis");
    assert_eq!(fork_basis.manifest.owner_namespace_id, source_namespace_id);
    assert_eq!(fork_basis.manifest.manifest_head_seq, ChangeSeq(1));
    assert_eq!(
        fork_basis.source_checkpoint_id.manifest_no(),
        fork_basis.manifest.manifest_no
    );

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &fork_basis.source_checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    assert_eq!(source_record.manifest_head_seq, ChangeSeq(1));
    // The fork basis and checkpoint record must use the same manifest.
    assert_eq!(source_record.manifest(), fork_basis.manifest);
    assert!(
        matches!(
            &source_record.owner,
            CheckpointOwner::Fork {
                target_namespace_id,
                ..
            }
                if *target_namespace_id == clone_namespace_id
        ),
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
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"base"
    );
    assert_eq!(store.inner().counts().puts, 0);
    assert_eq!(
        store.inner().take_get_keys(),
        vec![content_blob(
            &content_store_id,
            &source_namespace_id,
            &inherited_ref.content_id
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

    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"source-after-fork",
        &context,
        Some("source-after-fork"),
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
        test_commit_id(Some("clone-after-fork")),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/shared.txt").expect("path"),
            content_ref: uploaded_ref.clone(),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    let owner_keys = store
        .list_prefix(&content_owner_prefix(
            &content_store_id,
            &clone_namespace_id,
        ))
        .await
        .expect("owner content");
    assert_eq!(
        owner_keys,
        vec![content_blob(
            &content_store_id,
            &clone_namespace_id,
            &uploaded_ref.content_id
        )]
    );
    assert_eq!(
        head_state(&store, &clone_namespace_id)
            .await
            .content_store_id,
        content_store_id
    );
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

    // The target's own first flush inherits the source's segments by
    // reference and adds only its own delta run.
    namespace_engine(&store, &clone_namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush clone");
    let clone_root =
        loonfs_core::control::load_namespace_current_manifest(&store, &clone_namespace_id)
            .await
            .expect("clone metadata root");
    let clone_manifest_bytes = store
        .get(
            &metadata_manifest_object(&clone_namespace_id, &clone_root.state.manifest.manifest_no),
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
            .expect("read clone after its own flush")
            .bytes,
        b"clone-after-fork"
    );
    let engine = namespace_engine(&store, &clone_namespace_id, &context);
    let mut revisions_compacted = false;
    for _ in 0..16 {
        let report = engine
            .reorganize_metadata(loonfs_core::MetadataCompactionPolicy::CompactImmediately, 0)
            .await
            .expect("compact clone");
        match report.outcome {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            loonfs_core::MetadataReorganizeOutcome::UnitPublished { group, .. } => {
                revisions_compacted |= group == loonfs_api::MetadataFamilyGroup::Revisions;
            }
            other => panic!("expected bounded compaction, got {other:?}"),
        }
    }
    assert!(revisions_compacted);
    let read_context = crate::common::read_context(&store, &clone_namespace_id).await;
    let revisions = engine
        .list_file_revisions_for_inode_page(
            clone_entry.inode_id,
            loonfs_api::PageRequest {
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

#[tokio::test]
async fn fork_clone_survives_source_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("valid namespace id");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
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
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&store, &ancestor, &context).await;
    fork_namespace(&store, &ancestor, &parent, &context)
        .await
        .expect("fork parent");
    fork_namespace(&store, &parent, &descendant, &context)
        .await
        .expect("fork descendant");

    write_file_bytes(
        &store,
        &descendant,
        "/docs/own.txt",
        b"own",
        &context,
        Some("descendant-write"),
    )
    .await
    .expect("write descendant");
    let engine = namespace_engine(&store, &descendant, &context);
    engine.flush_wal().await.expect("flush descendant");
    let mut compacted = false;
    for _ in 0..16 {
        match engine
            .reorganize_metadata(loonfs_core::MetadataCompactionPolicy::CompactImmediately, 0)
            .await
            .expect("compact descendant")
            .outcome
        {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. } => compacted = true,
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
    loonfs_core::gc_namespace(&store, &ancestor, &loonfs_core::GcConfig::default(), &aged)
        .await
        .expect("collect ancestor");

    assert_eq!(
        read_file_bytes(&store, &descendant, "/docs/shared.txt")
            .await
            .expect("descendant reads through its retained ancestry")
            .bytes,
        b"base"
    );
    let config = loonfs_core::GcConfig::default();
    let parent_waiting = loonfs_core::gc_namespace(&store, &parent, &config, &aged)
        .await
        .expect("parent waits");
    assert_eq!(parent_waiting.reclaim_after_ms, None);
    let ancestor_waiting = loonfs_core::gc_namespace(&store, &ancestor, &config, &aged)
        .await
        .expect("ancestor waits");
    assert_eq!(ancestor_waiting.released_checkpoints.fork, 0);
    namespace_engine(&store, &descendant, &aged)
        .delete_namespace(Default::default())
        .await
        .expect("delete descendant");
    let retired = loonfs_core::gc_namespace(&store, &descendant, &config, &aged)
        .await
        .expect("retire descendant");
    let descendant_deadline = retired
        .reclaim_after_ms
        .expect("descendant retired despite compaction");
    assert_eq!(retired.deleted.content_objects, 0);
    let waiting = loonfs_core::gc_namespace(&store, &parent, &config, &aged)
        .await
        .expect("wait for descendant grace");
    assert_eq!(waiting.released_checkpoints.fork, 0);
    aged.now_ms = descendant_deadline;
    loonfs_core::gc_namespace(&store, &descendant, &config, &aged)
        .await
        .expect("descendant releases its source pin");
    let released = loonfs_core::gc_namespace(&store, &parent, &config, &aged)
        .await
        .expect("release descendant record");
    assert_eq!(released.released_checkpoints.fork, 0);
    assert_eq!(released.deleted.checkpoint_records, 0);
    let parent_deadline = released.reclaim_after_ms.expect("parent retired");
    let waiting = loonfs_core::gc_namespace(&store, &ancestor, &config, &aged)
        .await
        .expect("wait for parent grace");
    assert_eq!(waiting.released_checkpoints.fork, 0);
    aged.now_ms = parent_deadline;
    loonfs_core::gc_namespace(&store, &parent, &config, &aged)
        .await
        .expect("parent releases its source pin");
    let released = loonfs_core::gc_namespace(&store, &ancestor, &config, &aged)
        .await
        .expect("release parent record");
    assert_eq!(released.released_checkpoints.fork, 0);
    assert!(released.reclaim_after_ms.is_some());
}

#[tokio::test]
async fn a_fork_survives_a_concurrent_collection_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = mutation_context();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let gc_config = loonfs_core::GcConfig::default();
    let forking = fork_namespace(store.as_ref(), &source, &clone, &context);
    let collecting = loonfs_core::gc_namespace(store.as_ref(), &source, &gc_config, &context);
    let (forked, collected) = tokio::join!(forking, collecting);
    forked.expect("creation grace protects the fork");
    collected.expect("the pass finishes");
    assert_eq!(
        head_state(store.as_ref(), &clone).await.status,
        loonfs_api::wire::control::NamespaceStatus::Active {}
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
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;
    let checkpoint = namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint");

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    let manifest_key = metadata_manifest_object(&source_namespace_id, &source_record.manifest_no);
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
    let manifest = loonfs_api::wire::manifest::encode_namespace_manifest_json(manifest)
        .expect("rebuild manifest checksum")
        .into_envelope();
    let corrupted = encode_namespace_manifest_json(manifest.payload().clone())
        .expect("encode corrupt manifest")
        .into_bytes();
    store
        .put_overwrite(&manifest_key, Bytes::from(corrupted))
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
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!("namespaces/{}/pins/", source_namespace_id.as_str())),
        InjectedCreateFailure::PermissionDenied {
            message: "injected source checkpoint failure",
        },
    );
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
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    // Another writer's complete head for the same id, already durable.
    let foreign = loonfs_api::wire::manifest::NamespaceManifestPayload::initial(
        namespace_id.clone(),
        loonfs_api::ContentStoreId::generate(),
        1_000,
    );
    let foreign_bytes = loonfs_api::wire::manifest::encode_namespace_manifest_json(foreign.clone())
        .map(|encoded| encoded.into_bytes())
        .expect("encode foreign head");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(metadata_manifest_object(
            &namespace_id,
            &loonfs_api::ManifestNo(1),
        )),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: false,
            additional_writes: vec![(
                metadata_manifest_object(&namespace_id, &loonfs_api::ManifestNo(1)),
                foreign_bytes.clone(),
            )],
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("another namespace owns the id");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        store
            .get(
                &metadata_manifest_object(&namespace_id, &loonfs_api::ManifestNo(1)),
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
async fn namespace_delete_is_terminal_for_reads_writes_creation_and_forks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"will vanish")
        .await
        .expect("stage content");
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("before-delete").expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/keep.txt").expect("path"),
            content_ref: content.content_ref().clone(),
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

    // Terminal: reads, commits, status, repeat deletes, re-creation, and forks
    // all observe the deleted head.
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
            content_ref: content.content_ref().clone(),
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
    let recreate = bootstrap_namespace(&store, &namespace_id, &context, false).await;
    assert!(matches!(
        recreate,
        Err(loonfs_core::BootstrapNamespaceError::NamespaceDeleted { .. })
    ));
    // Even `allow_existing` cannot revive a retired id.
    let adopt = bootstrap_namespace(&store, &namespace_id, &context, true).await;
    assert!(matches!(
        adopt,
        Err(loonfs_core::BootstrapNamespaceError::NamespaceDeleted { .. })
    ));
    let fork_target = NamespaceId::parse("fork-of-deleted").expect("valid namespace id");
    let fork = fork_namespace(&store, &namespace_id, &fork_target, &context).await;
    assert_eq!(
        fork.expect_err("fork of deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
}

#[tokio::test]
async fn gc_preserves_unflushed_data_then_the_current_manifest_tombstone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/keep.txt",
        b"keep me",
        &context,
        Some("keep"),
    )
    .await
    .expect("write");

    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    let report = loonfs_core::gc_namespace(
        &store,
        &namespace_id,
        &loonfs_core::GcConfig::default(),
        &aged,
    )
    .await
    .expect("gc a namespace with no root");
    assert_eq!(report.deleted.wal_segments, 0, "{report:?}");
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
    let report = loonfs_core::gc_namespace(
        &store,
        &namespace_id,
        &loonfs_core::GcConfig::default(),
        &aged,
    )
    .await
    .expect("gc the tombstone");
    assert!(report.deleted.wal_segments >= 1, "{report:?}");
    loonfs_core::gc_namespace(
        &store,
        &namespace_id,
        &loonfs_core::GcConfig::default(),
        &aged,
    )
    .await
    .expect("collect obsolete tombstone version");
    let current = loonfs_core::control::load_namespace_current_manifest(&store, &namespace_id)
        .await
        .expect("current tombstone");
    assert_eq!(
        namespace_keys(&store, &namespace_id).await,
        vec![
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &current.state.manifest.manifest_no)
        ]
    );
    assert!(store
        .head(&hint(&namespace_id))
        .await
        .expect("probe hint")
        .is_some());
}

#[tokio::test]
async fn creation_and_fork_install_descriptor_hint_and_manifest_in_order() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = mutation_context();
    let source = namespace_id("source");
    bootstrap_namespace(&store, &source, &context, false)
        .await
        .expect("bootstrap");
    let head = load_namespace_head_control(&store, &source)
        .await
        .expect("head");
    let descriptor_key = content_store(&head.state.content_store_id);
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
            (descriptor_key.clone(), PutMode::CreateIfAbsent),
            (hint(&source), PutMode::CreateIfAbsent),
            (
                metadata_manifest_object(&source, &ManifestNo(1)),
                PutMode::CreateIfAbsent
            ),
        ]
    );
    let bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("get descriptor")
        .expect("descriptor");
    let descriptor =
        decode_control_object::<ContentStoreState>(&bytes, ControlObjectKind::ContentStore)
            .expect("decode descriptor")
            .into_payload();
    assert_eq!(
        descriptor,
        ContentStoreState {
            content_store_id: head.state.content_store_id.clone(),
            created_at_ms: head.state.created_at_ms,
        }
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
            } if key == descriptor_key || key.starts_with(&format!("namespaces/{target}/")) => {
                Some(key)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        puts,
        vec![
            descriptor_key,
            hint(&target),
            metadata_manifest_object(&target, &ManifestNo(1))
        ]
    );
    let fork_head = load_namespace_head_control(&store, &target)
        .await
        .expect("fork head");
    assert_eq!(
        fork_head.state.content_store_id,
        descriptor.content_store_id
    );
    for allow_existing in [false, true] {
        store.reset();
        let result = bootstrap_namespace(&store, &source, &context, allow_existing).await;
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
async fn bootstrap_of_a_deleted_namespace_writes_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    namespace_engine(&store, &namespace_id, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete");
    for allow_existing in [false, true] {
        store.reset();
        let error = bootstrap_namespace(&store, &namespace_id, &context, allow_existing)
            .await
            .expect_err("retired id");
        assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
        let counts = store.counts();
        assert_eq!(
            (counts.puts, counts.compare_and_swaps, counts.deletes),
            (0, 0, 0)
        );
    }
}

#[tokio::test]
async fn bootstrap_head_read_failures_never_create_a_descriptor() {
    for injected in [
        InjectedError::Transport("head read failed".to_owned()),
        InjectedError::PermissionDenied("head read denied".to_owned()),
    ] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = namespace_id("demo");
        let failing = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::exact(hint(&namespace_id)),
            OperationClass::Read,
            injected,
        );
        failing.fail_all();
        let store = RecordingStore::new(failing, KeyPredicate::any());
        for allow_existing in [false, true] {
            let error =
                bootstrap_namespace(&store, &namespace_id, &mutation_context(), allow_existing)
                    .await
                    .expect_err("head read failed");
            assert!(matches!(
                error,
                loonfs_core::BootstrapNamespaceError::Core(CoreError::ControlObjectLoad(_))
            ));
        }
        let counts = store.counts();
        assert_eq!(
            (counts.puts, counts.compare_and_swaps, counts.deletes),
            (0, 0, 0)
        );
    }
}

#[tokio::test]
async fn bootstrap_of_a_corrupt_head_writes_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    store
        .put_if_absent(&hint(&namespace_id), Bytes::from_static(b"invalid head"))
        .await
        .expect("corrupt head");
    store.reset();
    for allow_existing in [false, true] {
        let error = bootstrap_namespace(&store, &namespace_id, &mutation_context(), allow_existing)
            .await
            .expect_err("corrupt head");
        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }
    let counts = store.counts();
    assert_eq!(
        (counts.puts, counts.compare_and_swaps, counts.deletes),
        (0, 0, 0)
    );
}

#[tokio::test]
async fn a_creator_losing_after_the_head_read_leaves_only_an_orphan_descriptor() {
    let directory = tempdir().expect("tempdir");
    let store = loonfs_test_support::stores::BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("content-stores/"),
        OperationClass::PutCreateIfAbsent,
    );
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    store.block_next();
    let delayed = bootstrap_namespace(&store, &namespace_id, &context, false);
    let winner = async {
        store.wait_until_blocked().await;
        let result = bootstrap_namespace(&store, &namespace_id, &context, false).await;
        store.release();
        result.expect("concurrent creator wins")
    };
    let (delayed, winner) = tokio::join!(delayed, winner);
    assert_eq!(
        delayed.expect_err("lost conditional write").code(),
        ErrorCode::NamespaceExists
    );
    assert_eq!(winner.namespace_id, namespace_id);
    let head = head_state(&store, &namespace_id).await;
    let descriptors = store
        .list_prefix("content-stores/")
        .await
        .expect("descriptors");
    assert_eq!(descriptors.len(), 2);
    assert!(descriptors.contains(&content_store(&head.content_store_id)));
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
    let setup = mutation_context();
    let source = namespace_id("template");
    let sibling = namespace_id("sibling");
    seed_source_namespace_for_fork(&store, &source, &setup).await;
    fork_namespace(&store, &source, &sibling, &setup)
        .await
        .expect("fork sibling");
    write_file_bytes(
        &store,
        &sibling,
        "/docs/sibling.txt",
        b"sibling",
        &setup,
        Some("sibling-write"),
    )
    .await
    .expect("write sibling");
    let content_store_id = head_state(&store, &source).await.content_store_id;
    let source_prefix = content_owner_prefix(&content_store_id, &source);
    let sibling_prefix = content_owner_prefix(&content_store_id, &sibling);
    let source_keys = store
        .list_prefix(&source_prefix)
        .await
        .expect("source content");
    let sibling_keys = store
        .list_prefix(&sibling_prefix)
        .await
        .expect("sibling content");
    assert!(!source_keys.is_empty());
    assert!(!sibling_keys.is_empty());
    let config = loonfs_core::GcConfig::default();
    let mut aged = setup.clone();
    aged.now_ms = u64::MAX / 4;
    for number in 0..4 {
        let leaf = namespace_id(&format!("leaf-{number}"));
        fork_namespace(&store, &source, &leaf, &setup)
            .await
            .expect("fork leaf");
        write_file_bytes(
            &store,
            &leaf,
            "/docs/own.txt",
            b"leaf",
            &setup,
            Some("leaf-write"),
        )
        .await
        .expect("write leaf");
        let prefix = content_owner_prefix(&content_store_id, &leaf);
        assert!(!store
            .list_prefix(&prefix)
            .await
            .expect("leaf content")
            .is_empty());
        namespace_engine(&store, &leaf, &setup)
            .delete_namespace(Default::default())
            .await
            .expect("delete leaf");
        let mut reclaimed = 0;
        for call in 0..8 {
            let report = loonfs_core::gc_namespace(&store, &leaf, &config, &aged)
                .await
                .expect("collect leaf");
            reclaimed += report.deleted.retired_content_objects;
            if store
                .list_prefix(&prefix)
                .await
                .expect("leaf content")
                .is_empty()
            {
                break;
            }
            aged.now_ms = report
                .reclaim_after_ms
                .unwrap_or(aged.now_ms + config.grace_window_ms);
            assert!(call < 7, "leaf reclamation must converge");
        }
        assert!(reclaimed > 0);
        assert_eq!(
            store
                .list_prefix(&source_prefix)
                .await
                .expect("source content"),
            source_keys
        );
        assert_eq!(
            store
                .list_prefix(&sibling_prefix)
                .await
                .expect("sibling content"),
            sibling_keys
        );
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
    for _ in 0..3 {
        aged.now_ms += config.grace_window_ms;
        let report = loonfs_core::gc_namespace(&store, &source, &config, &aged)
            .await
            .expect("collect deleted source");
        assert_eq!(report.reclaim_after_ms, None);
        assert_eq!(report.deleted.retired_content_objects, 0);
        assert_eq!(
            store
                .list_prefix(&source_prefix)
                .await
                .expect("source content"),
            source_keys
        );
    }
    assert_eq!(
        read_file_bytes(&store, &sibling, "/docs/shared.txt")
            .await
            .expect("read deleted ancestor")
            .bytes,
        b"base"
    );
}
