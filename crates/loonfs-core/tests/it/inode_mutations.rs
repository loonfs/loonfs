use crate::common::commit_split_support::*;
use loonfs_api::{
    AbsolutePath, ContentRef, DeleteDirectoryBehavior, DestinationBehavior, DisplayName, InodeId,
    NamespaceId, RevisionNo, ROOT_INODE_ID,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitRequest, FilesystemOperation};
use loonfs_core::ErrorCode;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use tempfile::tempdir;

fn display_name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("valid display name")
}

async fn read_entry<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> (InodeId, String) {
    let entry = resolve_path(store, namespace_id, absolute_path)
        .await
        .expect("resolve path");
    (
        entry.inode_id,
        entry
            .binding_generation
            .expect("named entry has a binding generation"),
    )
}

async fn namespace_with_docs() -> (
    tempfile::TempDir,
    LocalFsStore,
    NamespaceId,
    loonfs_core::MutationContext,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .await
        .expect("create /docs");
    (temp_dir, store, namespace_id, context)
}

async fn rebind_report<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &loonfs_core::MutationContext,
) -> (InodeId, String, String) {
    write_file_bytes(
        store,
        namespace_id,
        "/docs/report.txt",
        b"body",
        context,
        Some("put-report"),
    )
    .await
    .expect("put file");
    let (inode_id, stale_generation) = read_entry(store, namespace_id, "/docs/report.txt").await;
    submit_operation(
        store,
        namespace_id,
        test_commit_id(Some("rename-report")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/renamed.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
            expected_destination_inode_id: None,
            expected_destination_revision_no: None,
        },
        context,
    )
    .await
    .expect("rename file");
    let (_, current_generation) = read_entry(store, namespace_id, "/docs/renamed.txt").await;
    (inode_id, stale_generation, current_generation)
}

#[tokio::test]
async fn creates_entries_under_a_parent_inode() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    let content_ref = store_bytes_as_content(&store, &namespace_id, b"january")
        .await
        .expect("stage content")
        .into_content_ref();

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("create-by-inode")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("archive"),
                },
                FilesystemOperation::PutFileByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("january.txt"),
                    content_ref,
                },
            ],
        },
        &context,
    )
    .await
    .expect("create entries by inode");

    assert_eq!(
        resolve_path(&store, &namespace_id, "/docs/archive")
            .await
            .expect("resolve created directory")
            .parent_inode_id,
        Some(docs_inode_id)
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/january.txt")
            .await
            .expect("read created file")
            .bytes,
        b"january"
    );
}

#[tokio::test]
async fn creating_by_inode_rejects_a_bound_name() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/taken.txt",
        b"first",
        &context,
        Some("put-taken"),
    )
    .await
    .expect("put existing file");
    let content_ref = store_bytes_as_content(&store, &namespace_id, b"second")
        .await
        .expect("stage content")
        .into_content_ref();

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("put-over-taken")),
        FilesystemOperation::PutFileByInode {
            parent_inode_id: docs_inode_id,
            display_name: display_name("taken.txt"),
            content_ref,
        },
        &context,
    )
    .await
    .expect_err("bound name must conflict");

    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn revision_write_requires_the_current_revision_and_survives_a_move() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/report.txt",
        b"first",
        &context,
        Some("put-first"),
    )
    .await
    .expect("put first revision");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/report.txt",
        b"second",
        &context,
        Some("put-second"),
    )
    .await
    .expect("put second revision");
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/docs/report.txt").await;

    let stale = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("write-stale")),
        put_revision_by_inode(
            &store,
            &namespace_id,
            report_inode_id,
            b"third",
            RevisionNo(1),
        )
        .await,
        &context,
    )
    .await
    .expect_err("stale revision must fail");
    assert_eq!(stale.code(), ErrorCode::StaleRevision);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-report")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            to_path: AbsolutePath::parse("/report.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
            expected_destination_inode_id: None,
            expected_destination_revision_no: None,
        },
        &context,
    )
    .await
    .expect("move file");

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("write-fresh")),
        put_revision_by_inode(
            &store,
            &namespace_id,
            report_inode_id,
            b"third",
            RevisionNo(2),
        )
        .await,
        &context,
    )
    .await
    .expect("current revision must commit");

    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/report.txt")
            .await
            .expect("read moved file")
            .bytes,
        b"third"
    );
}

#[tokio::test]
async fn move_requires_the_current_binding_generation() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (report_inode_id, stale_generation, fresh_generation) =
        rebind_report(&store, &namespace_id, &context).await;

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-stale")),
        FilesystemOperation::MoveByInode {
            inode_id: report_inode_id,
            expected_binding_generation: stale_generation,
            to_parent_inode_id: ROOT_INODE_ID,
            to_display_name: display_name("moved.txt"),
            behavior: DestinationBehavior::NoReplace,
            expected_destination_inode_id: None,
            expected_destination_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("stale binding generation must fail");
    assert_eq!(error.code(), ErrorCode::BindingGenerationMismatch);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-fresh")),
        FilesystemOperation::MoveByInode {
            inode_id: report_inode_id,
            expected_binding_generation: fresh_generation,
            to_parent_inode_id: ROOT_INODE_ID,
            to_display_name: display_name("moved.txt"),
            behavior: DestinationBehavior::NoReplace,
            expected_destination_inode_id: None,
            expected_destination_revision_no: None,
        },
        &context,
    )
    .await
    .expect("current binding generation must move file");

    let (moved_inode_id, _) = read_entry(&store, &namespace_id, "/moved.txt").await;
    assert_eq!(moved_inode_id, report_inode_id);
}

#[tokio::test]
async fn delete_requires_the_current_binding_generation() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (report_inode_id, stale_generation, fresh_generation) =
        rebind_report(&store, &namespace_id, &context).await;

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-stale")),
        FilesystemOperation::DeleteByInode {
            inode_id: report_inode_id,
            expected_binding_generation: stale_generation,
            behavior: DeleteDirectoryBehavior::NonRecursive,
        },
        &context,
    )
    .await
    .expect_err("stale binding generation must fail");
    assert_eq!(error.code(), ErrorCode::BindingGenerationMismatch);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-fresh")),
        FilesystemOperation::DeleteByInode {
            inode_id: report_inode_id,
            expected_binding_generation: fresh_generation,
            behavior: DeleteDirectoryBehavior::NonRecursive,
        },
        &context,
    )
    .await
    .expect("current binding generation must delete file");

    assert_eq!(
        resolve_path(&store, &namespace_id, "/docs/renamed.txt")
            .await
            .expect_err("file must be deleted")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn earlier_move_makes_a_later_guard_stale_and_rolls_back_the_commit() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/report.txt",
        b"body",
        &context,
        Some("put-report"),
    )
    .await
    .expect("put file");
    let (report_inode_id, binding_generation) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("move-then-delete-with-old-generation")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::MoveByInode {
                    inode_id: report_inode_id,
                    expected_binding_generation: binding_generation.clone(),
                    to_parent_inode_id: ROOT_INODE_ID,
                    to_display_name: display_name("moved.txt"),
                    behavior: DestinationBehavior::NoReplace,
                    expected_destination_inode_id: None,
                    expected_destination_revision_no: None,
                },
                FilesystemOperation::DeleteByInode {
                    inode_id: report_inode_id,
                    expected_binding_generation: binding_generation,
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                },
            ],
        },
        &context,
    )
    .await
    .expect_err("the move must make the old generation stale");

    assert_eq!(error.code(), ErrorCode::BindingGenerationMismatch);
    assert_eq!(
        read_entry(&store, &namespace_id, "/docs/report.txt")
            .await
            .0,
        report_inode_id
    );
    assert_eq!(
        resolve_path(&store, &namespace_id, "/moved.txt")
            .await
            .expect_err("the rejected commit must not publish its first operation")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn content_write_preserves_the_guard_for_a_later_move() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/report.txt",
        b"first",
        &context,
        Some("put-report"),
    )
    .await
    .expect("put file");
    let (report_inode_id, binding_generation) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("write-then-move-with-same-generation")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                put_revision_by_inode(
                    &store,
                    &namespace_id,
                    report_inode_id,
                    b"second",
                    RevisionNo(1),
                )
                .await,
                FilesystemOperation::MoveByInode {
                    inode_id: report_inode_id,
                    expected_binding_generation: binding_generation.clone(),
                    to_parent_inode_id: ROOT_INODE_ID,
                    to_display_name: display_name("moved.txt"),
                    behavior: DestinationBehavior::NoReplace,
                    expected_destination_inode_id: None,
                    expected_destination_revision_no: None,
                },
            ],
        },
        &context,
    )
    .await
    .expect("content-only writes must preserve the binding guard");

    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/moved.txt")
            .await
            .expect("read moved file")
            .bytes,
        b"second"
    );
    let (moved_inode_id, moved_generation) = read_entry(&store, &namespace_id, "/moved.txt").await;
    assert_eq!(moved_inode_id, report_inode_id);
    assert_ne!(moved_generation, binding_generation);
}

#[tokio::test]
async fn malformed_foreign_and_root_binding_guards_are_invalid() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, local_generation) = read_entry(&store, &namespace_id, "/docs").await;

    let other_namespace_id = NamespaceId::parse("other").expect("valid namespace id");
    bootstrap_namespace(&store, &other_namespace_id, &context, false)
        .await
        .expect("bootstrap other namespace");
    create_directory_path(
        &store,
        &other_namespace_id,
        "/docs",
        &context,
        Some("mkdir-other-docs"),
    )
    .await
    .expect("create directory in other namespace");
    let (_, foreign_generation) = read_entry(&store, &other_namespace_id, "/docs").await;

    for token in [foreign_generation, "not-a-token".to_owned()] {
        let error = submit_operation(
            &store,
            &namespace_id,
            test_commit_id(None),
            FilesystemOperation::DeleteByInode {
                inode_id: docs_inode_id,
                expected_binding_generation: token,
                behavior: DeleteDirectoryBehavior::NonRecursive,
            },
            &context,
        )
        .await
        .expect_err("invalid guard must fail");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }

    let root_error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-root")),
        FilesystemOperation::DeleteByInode {
            inode_id: ROOT_INODE_ID,
            expected_binding_generation: local_generation,
            behavior: DeleteDirectoryBehavior::Recursive,
        },
        &context,
    )
    .await
    .expect_err("root mutation must fail");
    assert_eq!(root_error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn inode_operation_observes_an_earlier_delete_in_the_same_commit() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/report.txt",
        b"body",
        &context,
        Some("put-report"),
    )
    .await
    .expect("put file");
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/docs/report.txt").await;

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("delete-then-write")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/docs/report.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
                put_revision_by_inode(
                    &store,
                    &namespace_id,
                    report_inode_id,
                    b"after",
                    RevisionNo(1),
                )
                .await,
            ],
        },
        &context,
    )
    .await
    .expect_err("deleted inode must not be addressable");
    assert_eq!(error.code(), ErrorCode::InodeNotFound);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/report.txt")
            .await
            .expect("failed commit must leave file unchanged")
            .bytes,
        b"body"
    );
}

async fn put_revision_by_inode<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    bytes: &[u8],
    expected_revision_no: RevisionNo,
) -> FilesystemOperation {
    let content_ref: ContentRef = store_bytes_as_content(store, namespace_id, bytes)
        .await
        .expect("stage content")
        .into_content_ref();
    FilesystemOperation::PutFileRevisionByInode {
        inode_id,
        content_ref,
        expected_revision_no,
    }
}
