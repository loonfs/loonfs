#![allow(clippy::panic)]

use crate::common::commit_split_support::*;
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitRequest, FilesystemOperation};
use loonfs_core::{Error as CoreError, ErrorCode};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_types::{
    AbsolutePath, AccessGrants, AttributeKey, AttributeValue, AttributesRevisionNo, BindingVersion,
    CommitPrecondition, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
    DestinationPrecondition, DisplayName, InodeId, NamespaceId, RevisionNo, ROOT_INODE_ID,
};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn display_name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("valid display name")
}

fn attributes(entries: &[(&str, &str)]) -> BTreeMap<AttributeKey, AttributeValue> {
    entries
        .iter()
        .map(|(key, value)| {
            (
                AttributeKey::parse(*key).expect("attribute key"),
                AttributeValue::parse(*value).expect("attribute value"),
            )
        })
        .collect()
}

fn copy_by_inode(
    inode_id: InodeId,
    destination_parent_inode_id: InodeId,
    name: &str,
    precondition: DestinationPrecondition,
) -> FilesystemOperation {
    FilesystemOperation::CopyByInode {
        inode_id,
        destination_parent_inode_id,
        destination_display_name: display_name(name),
        precondition,
    }
}

fn undelete(
    inode_id: InodeId,
    deletion_seq: loonfs_types::ChangeSeq,
    destination_path: Option<&str>,
    destination_parent_inode_id: Option<InodeId>,
    destination_display_name: Option<&str>,
) -> FilesystemOperation {
    FilesystemOperation::Undelete {
        inode_id,
        deletion_seq,
        destination_path: destination_path.map(|path| AbsolutePath::parse(path).expect("path")),
        destination_parent_inode_id,
        destination_display_name: destination_display_name.map(display_name),
    }
}

async fn guarded_mkdir(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &loonfs_core::MutationContext,
    path: &str,
    precondition: CommitPrecondition,
) -> Result<loonfs_types::Commit, CoreError> {
    submit_commit(
        store,
        namespace_id,
        CommitRequest::single(
            test_commit_id(None),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(path).expect("path"),
                parents: false,
            },
        )
        .preconditions(vec![precondition]),
        context,
    )
    .await
}

async fn read_entry<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> (InodeId, BindingVersion) {
    let entry = resolve_path(store, namespace_id, absolute_path)
        .await
        .expect("resolve path");
    (
        entry.inode_id,
        entry
            .binding_version
            .expect("named entry has a binding version"),
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
    bootstrap_namespace(&store, &namespace_id, &context)
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
) -> (InodeId, BindingVersion, BindingVersion) {
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
    let (inode_id, stale_version) = read_entry(store, namespace_id, "/docs/report.txt").await;
    submit_operation(
        store,
        namespace_id,
        test_commit_id(Some("rename-report")),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/renamed.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        context,
    )
    .await
    .expect("rename file");
    let (_, current_version) = read_entry(store, namespace_id, "/docs/renamed.txt").await;
    (inode_id, stale_version, current_version)
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
            preconditions: Vec::new(),
            commit_id: test_commit_id(Some("create-by-inode")),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("archive"),
                },
                FilesystemOperation::CreateFileByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("january.txt"),
                    content_ref: Some(content_ref),
                    inline_content: None,
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
        FilesystemOperation::CreateFileByInode {
            parent_inode_id: docs_inode_id,
            display_name: display_name("taken.txt"),
            content_ref: Some(content_ref),
            inline_content: None,
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
            source_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            destination_path: AbsolutePath::parse("/report.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
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
async fn move_requires_the_current_binding_version() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (report_inode_id, stale_version, fresh_version) =
        rebind_report(&store, &namespace_id, &context).await;

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-stale")),
        FilesystemOperation::MoveByInode {
            inode_id: report_inode_id,
            expected_binding_version: stale_version.clone(),
            destination_parent_inode_id: ROOT_INODE_ID,
            destination_display_name: display_name("moved.txt"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("stale binding version must fail");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
    let details = error.details().expect("operation details");
    assert_eq!(details.operation_index, Some(0));
    assert_eq!(details.precondition_index, None);
    assert_eq!(details.inode_id, Some(report_inode_id));
    assert_eq!(
        details.expected_binding_version,
        Some(stale_version.clone())
    );
    assert_eq!(details.actual_binding_version, Some(fresh_version.clone()));

    let binding_precondition = |path: &str, inode_id| {
        CommitRequest::single(
            test_commit_id(Some("binding-precondition")),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/unwritten").expect("path"),
                parents: false,
            },
        )
        .preconditions(vec![loonfs_types::CommitPrecondition::PathBinding {
            path: AbsolutePath::parse(path).expect("path"),
            expected_inode_id: inode_id,
            expected_binding_version: Some(stale_version.clone()),
        }])
    };
    let error = submit_commit(
        &store,
        &namespace_id,
        binding_precondition("/docs/renamed.txt", report_inode_id),
        &context,
    )
    .await
    .expect_err("binding precondition must fail");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
    let details = error.details().expect("precondition details");
    assert_eq!(details.precondition_index, Some(0));
    assert_eq!(details.operation_index, None);
    assert_eq!(details.inode_id, Some(report_inode_id));
    assert_eq!(
        details.expected_binding_version,
        Some(stale_version.clone())
    );
    assert_eq!(details.actual_binding_version, Some(fresh_version.clone()));
    let error = submit_commit(
        &store,
        &namespace_id,
        binding_precondition("/", ROOT_INODE_ID),
        &context,
    )
    .await
    .expect_err("the root has no binding version to expect");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-fresh")),
        FilesystemOperation::MoveByInode {
            inode_id: report_inode_id,
            expected_binding_version: fresh_version,
            destination_parent_inode_id: ROOT_INODE_ID,
            destination_display_name: display_name("moved.txt"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect("current binding version must move file");

    let (moved_inode_id, _) = read_entry(&store, &namespace_id, "/moved.txt").await;
    assert_eq!(moved_inode_id, report_inode_id);
}

#[tokio::test]
async fn delete_requires_the_current_binding_version() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (report_inode_id, stale_version, fresh_version) =
        rebind_report(&store, &namespace_id, &context).await;

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-stale")),
        FilesystemOperation::DeleteByInode {
            inode_id: report_inode_id,
            expected_binding_version: stale_version,
            behavior: DeleteDirectoryBehavior::NonRecursive,
        },
        &context,
    )
    .await
    .expect_err("stale binding version must fail");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-fresh")),
        FilesystemOperation::DeleteByInode {
            inode_id: report_inode_id,
            expected_binding_version: fresh_version,
            behavior: DeleteDirectoryBehavior::NonRecursive,
        },
        &context,
    )
    .await
    .expect("current binding version must delete file");

    assert_eq!(
        resolve_path(&store, &namespace_id, "/docs/renamed.txt")
            .await
            .expect_err("file must be deleted")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn earlier_move_makes_a_later_precondition_stale_and_rolls_back_the_commit() {
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
    let (report_inode_id, binding_version) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            preconditions: Vec::new(),
            commit_id: test_commit_id(Some("move-then-delete-with-old-version")),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations: vec![
                FilesystemOperation::MoveByInode {
                    inode_id: report_inode_id,
                    expected_binding_version: binding_version.clone(),
                    destination_parent_inode_id: ROOT_INODE_ID,
                    destination_display_name: display_name("moved.txt"),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                },
                FilesystemOperation::DeleteByInode {
                    inode_id: report_inode_id,
                    expected_binding_version: binding_version,
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                },
            ],
        },
        &context,
    )
    .await
    .expect_err("the move must make the old version stale");

    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
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
async fn content_write_preserves_the_precondition_for_a_later_move() {
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
    let (report_inode_id, binding_version) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            preconditions: Vec::new(),
            commit_id: test_commit_id(Some("write-then-move-with-same-version")),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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
                    expected_binding_version: binding_version.clone(),
                    destination_parent_inode_id: ROOT_INODE_ID,
                    destination_display_name: display_name("moved.txt"),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                },
            ],
        },
        &context,
    )
    .await
    .expect("content-only writes must preserve the binding precondition");

    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/moved.txt")
            .await
            .expect("read moved file")
            .bytes,
        b"second"
    );
    let (moved_inode_id, moved_version) = read_entry(&store, &namespace_id, "/moved.txt").await;
    assert_eq!(moved_inode_id, report_inode_id);
    assert_ne!(moved_version, binding_version);
}

#[tokio::test]
async fn foreign_and_root_binding_preconditions_are_invalid() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, local_version) = read_entry(&store, &namespace_id, "/docs").await;

    let other_namespace_id = NamespaceId::parse("other").expect("valid namespace id");
    bootstrap_namespace(&store, &other_namespace_id, &context)
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
    let (_, foreign_version) = read_entry(&store, &other_namespace_id, "/docs").await;

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(None),
        FilesystemOperation::DeleteByInode {
            inode_id: docs_inode_id,
            expected_binding_version: foreign_version,
            behavior: DeleteDirectoryBehavior::NonRecursive,
        },
        &context,
    )
    .await
    .expect_err("foreign precondition must fail");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let root_error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-root")),
        FilesystemOperation::DeleteByInode {
            inode_id: ROOT_INODE_ID,
            expected_binding_version: local_version,
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
            preconditions: Vec::new(),
            commit_id: test_commit_id(Some("delete-then-write")),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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
        content_ref: Some(content_ref),
        inline_content: None,
        expected_revision_no,
    }
}

#[tokio::test]
async fn copy_by_inode_follows_the_source_and_carries_its_attributes() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/report.txt",
        b"body",
        &context,
        Some("put-report"),
    )
    .await
    .expect("put file");
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/report.txt").await;
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("label-report")),
        FilesystemOperation::UpdateAttributesByInode {
            inode_id: report_inode_id,
            set: attributes(&[("owner", "ada")]),
            remove: Vec::new(),
            expected_attributes_revision_no: Some(AttributesRevisionNo(0)),
        },
        &context,
    )
    .await
    .expect("label by inode");
    let stale = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("label-report-stale")),
        FilesystemOperation::UpdateAttributesByInode {
            inode_id: report_inode_id,
            set: attributes(&[("owner", "grace")]),
            remove: Vec::new(),
            expected_attributes_revision_no: Some(AttributesRevisionNo(0)),
        },
        &context,
    )
    .await
    .expect_err("stale attribute revision must fail");
    assert_eq!(stale.code(), ErrorCode::StaleAttributes);
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-report")),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/report.txt").expect("path"),
            destination_path: AbsolutePath::parse("/moved.txt").expect("path"),
            precondition: DestinationPrecondition::default(),
        },
        &context,
    )
    .await
    .expect("move source");

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-by-inode")),
        copy_by_inode(
            report_inode_id,
            docs_inode_id,
            "copy.txt",
            DestinationPrecondition::default(),
        ),
        &context,
    )
    .await
    .expect("copy by inode");
    let copy = resolve_path(&store, &namespace_id, "/docs/copy.txt")
        .await
        .expect("resolve copy");
    assert_ne!(copy.inode_id, report_inode_id);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/copy.txt")
            .await
            .expect("read copy")
            .bytes,
        b"body"
    );
    let copied_attributes = copy.attributes.expect("projected attributes");
    assert_eq!(
        copied_attributes.attributes_revision_no,
        AttributesRevisionNo(1)
    );
    assert_eq!(
        copied_attributes.attributes.as_map(),
        &attributes(&[("owner", "ada")])
    );

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/existing.txt",
        b"old",
        &context,
        Some("put-existing"),
    )
    .await
    .expect("put destination");
    let (existing_inode_id, _) = read_entry(&store, &namespace_id, "/docs/existing.txt").await;
    let occupied = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-onto-existing")),
        copy_by_inode(
            report_inode_id,
            docs_inode_id,
            "existing.txt",
            DestinationPrecondition::default(),
        ),
        &context,
    )
    .await
    .expect_err("an occupied name must conflict without replace");
    assert_eq!(occupied.code(), ErrorCode::PathConflict);
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-replacing-existing")),
        copy_by_inode(
            report_inode_id,
            docs_inode_id,
            "existing.txt",
            DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(existing_inode_id),
                expected_revision_no: Some(RevisionNo(1)),
            },
        ),
        &context,
    )
    .await
    .expect("replace by inode");
    let replaced = resolve_path(&store, &namespace_id, "/docs/existing.txt")
        .await
        .expect("resolve replaced");
    assert_eq!(replaced.inode_id, existing_inode_id);
    assert_eq!(replaced.revision_no(), Some(RevisionNo(2)));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/existing.txt")
            .await
            .expect("read replaced")
            .bytes,
        b"body"
    );
}

#[tokio::test]
async fn restore_revision_by_inode_restores_a_moved_file() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    for (bytes, commit_id) in [(&b"first"[..], "put-first"), (&b"second"[..], "put-second")] {
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/report.txt",
            bytes,
            &context,
            Some(commit_id),
        )
        .await
        .expect("put revision");
    }
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/docs/report.txt").await;
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-report")),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            destination_path: AbsolutePath::parse("/report.txt").expect("path"),
            precondition: DestinationPrecondition::default(),
        },
        &context,
    )
    .await
    .expect("move file");

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("restore-by-inode")),
        FilesystemOperation::RestoreRevisionByInode {
            inode_id: report_inode_id,
            source_revision_no: RevisionNo(1),
        },
        &context,
    )
    .await
    .expect("restore by inode");
    let restored = read_file_bytes(&store, &namespace_id, "/report.txt")
        .await
        .expect("read restored file");
    assert_eq!(restored.bytes, b"first");
    assert_eq!(restored.entry.revision_no(), Some(RevisionNo(3)));

    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    let directory = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("restore-directory")),
        FilesystemOperation::RestoreRevisionByInode {
            inode_id: docs_inode_id,
            source_revision_no: RevisionNo(1),
        },
        &context,
    )
    .await
    .expect_err("a directory has no revisions");
    assert_eq!(directory.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn inode_writes_refuse_the_root_except_for_access() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    for operation in [
        copy_by_inode(
            ROOT_INODE_ID,
            docs_inode_id,
            "root-copy",
            DestinationPrecondition::default(),
        ),
        FilesystemOperation::RestoreRevisionByInode {
            inode_id: ROOT_INODE_ID,
            source_revision_no: RevisionNo(1),
        },
        FilesystemOperation::UpdateAttributesByInode {
            inode_id: ROOT_INODE_ID,
            set: attributes(&[("owner", "ada")]),
            remove: Vec::new(),
            expected_attributes_revision_no: None,
        },
    ] {
        let error = submit_operation(
            &store,
            &namespace_id,
            test_commit_id(None),
            operation,
            &context,
        )
        .await
        .expect_err("the root is not a mutation target");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
    let unrestricted = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(None),
        FilesystemOperation::UpdateAccessByInode {
            inode_id: ROOT_INODE_ID,
            boundary: false,
            grants: AccessGrants::default(),
            expected_access_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("an unrestricted namespace holds no access rows");
    assert_eq!(unrestricted.code(), ErrorCode::NamespaceUnrestricted);
}

#[tokio::test]
async fn undelete_binds_under_a_parent_inode() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    for (path, commit_id) in [
        ("/docs/report.txt", "put-report"),
        ("/taken.txt", "put-taken"),
    ] {
        write_file_bytes(
            &store,
            &namespace_id,
            path,
            b"body",
            &context,
            Some(commit_id),
        )
        .await
        .expect("put file");
    }
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/docs/report.txt").await;
    let deletion_seq = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-report")),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete file")
    .committed_seq;

    for (operation, field) in [
        (
            undelete(
                report_inode_id,
                deletion_seq,
                Some("/restored.txt"),
                Some(ROOT_INODE_ID),
                Some("restored.txt"),
            ),
            "destination_path",
        ),
        (
            undelete(
                report_inode_id,
                deletion_seq,
                None,
                Some(ROOT_INODE_ID),
                None,
            ),
            "destination_display_name",
        ),
        (
            undelete(
                report_inode_id,
                deletion_seq,
                None,
                None,
                Some("restored.txt"),
            ),
            "destination_parent_inode_id",
        ),
    ] {
        let error = submit_operation(
            &store,
            &namespace_id,
            test_commit_id(None),
            operation,
            &context,
        )
        .await
        .expect_err("a destination is one path or one parent and name");
        match error {
            CoreError::FailedOperation { source, .. } => match *source {
                CoreError::InvalidCommitField {
                    field: actual_field,
                    ..
                } => assert_eq!(actual_field, field),
                other => panic!("expected an invalid field, got {other:?}"),
            },
            other => panic!("expected a failed operation, got {other:?}"),
        }
    }
    let taken = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(None),
        undelete(
            report_inode_id,
            deletion_seq,
            None,
            Some(ROOT_INODE_ID),
            Some("TAKEN.txt"),
        ),
        &context,
    )
    .await
    .expect_err("an occupied name must conflict");
    assert_eq!(taken.code(), ErrorCode::PathConflict);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("undelete-under-root")),
        undelete(
            report_inode_id,
            deletion_seq,
            None,
            Some(ROOT_INODE_ID),
            Some("restored.txt"),
        ),
        &context,
    )
    .await
    .expect("undelete under a parent inode");
    let (restored_inode_id, _) = read_entry(&store, &namespace_id, "/restored.txt").await;
    assert_eq!(restored_inode_id, report_inode_id);
    assert_eq!(
        resolve_path(&store, &namespace_id, "/docs/report.txt")
            .await
            .expect_err("the recorded binding stays free")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn inode_binding_precondition_requires_the_current_binding() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (report_inode_id, stale_version, fresh_version) =
        rebind_report(&store, &namespace_id, &context).await;
    let inode_binding =
        |expected_binding_version: BindingVersion| CommitPrecondition::InodeBinding {
            inode_id: report_inode_id,
            expected_binding_version,
        };

    let error = guarded_mkdir(
        &store,
        &namespace_id,
        &context,
        "/stale",
        inode_binding(stale_version.clone()),
    )
    .await
    .expect_err("a stale binding version must fail");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
    let details = error.details().expect("precondition details");
    assert_eq!(details.precondition_index, Some(0));
    assert_eq!(details.inode_id, Some(report_inode_id));
    assert_eq!(details.expected_binding_version, Some(stale_version));
    assert_eq!(details.actual_binding_version, Some(fresh_version.clone()));

    let malformed = guarded_mkdir(
        &store,
        &namespace_id,
        &context,
        "/malformed",
        inode_binding(BindingVersion::parse("aaaa").expect("token")),
    )
    .await
    .expect_err("a malformed binding version is invalid");
    assert_eq!(malformed.code(), ErrorCode::InvalidRequest);

    guarded_mkdir(
        &store,
        &namespace_id,
        &context,
        "/fresh",
        inode_binding(fresh_version.clone()),
    )
    .await
    .expect("the current binding version passes");

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("delete-renamed")),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/docs/renamed.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete file");
    let deleted = guarded_mkdir(
        &store,
        &namespace_id,
        &context,
        "/deleted",
        inode_binding(fresh_version.clone()),
    )
    .await
    .expect_err("a deleted inode has no binding");
    assert_eq!(deleted.code(), ErrorCode::BindingVersionMismatch);
    let details = deleted.details().expect("precondition details");
    assert_eq!(details.expected_binding_version, Some(fresh_version));
    assert_eq!(details.actual_binding_version, None);
}

#[tokio::test]
async fn name_absence_precondition_checks_the_folded_name_under_the_parent() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
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
    let name_absence = |parent_inode_id, name: &str| CommitPrecondition::NameAbsence {
        parent_inode_id,
        display_name: display_name(name),
    };

    let error = guarded_mkdir(
        &store,
        &namespace_id,
        &context,
        "/bound",
        name_absence(docs_inode_id, "REPORT.txt"),
    )
    .await
    .expect_err("a bound name must fail");
    assert_eq!(error.code(), ErrorCode::PathConflict);
    let details = error.details().expect("precondition details");
    assert_eq!(details.precondition_index, Some(0));
    assert_eq!(details.expected_inode_id, None);
    assert_eq!(details.actual_inode_id, Some(report_inode_id));

    for (path, precondition) in [
        ("/free", name_absence(docs_inode_id, "free.txt")),
        ("/under-file", name_absence(report_inode_id, "child")),
        ("/under-missing", name_absence(InodeId(999), "child")),
    ] {
        guarded_mkdir(&store, &namespace_id, &context, path, precondition)
            .await
            .expect("an unbound name passes");
    }
}
