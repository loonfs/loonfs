//! Inode-addressed mutations: that they land exactly what their path twins
//! land, that their guards are required and race-free, and that they resolve
//! their targets against what earlier operations in the same commit did.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::read_context;
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{
    AbsolutePath, BindingGeneration, ChangeSeq, ContentRef, DeleteDirectoryBehavior,
    DestinationBehavior, DisplayName, InodeId, NamespaceId, RevisionNo, ROOT_INODE_ID,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitRequest, FilesystemOperation};
use loonfs_core::ErrorCode;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use tempfile::tempdir;

fn display_name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("valid display name")
}

/// The inode and current binding-generation token a read reports for a path,
/// which is exactly what a client would hold before writing by inode.
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
            .expect("a non-root entry reports its binding generation"),
    )
}

async fn events_at<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> Vec<FilesystemChange> {
    let changes = list_changes_after(store, namespace_id, ChangeSeq(committed_seq.0 - 1))
        .await
        .expect("list changes");
    changes
        .changes
        .into_iter()
        .find(|change| change.committed_seq == committed_seq)
        .expect("the commit is in the change feed")
        .events
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

#[tokio::test]
async fn creating_by_inode_lands_what_creating_by_path_lands() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, _) = read_entry(&store, &namespace_id, "/docs").await;
    let content_ref = store_bytes_as_content(&store, &namespace_id, b"january")
        .await
        .expect("stage content")
        .into_content_ref();

    let by_path = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("by-path")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/by_path").expect("path"),
                    parents: false,
                },
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse("/docs/by_path.txt").expect("path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                },
            ],
        },
        &context,
    )
    .await
    .expect("path creates commit");

    let by_inode = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("by-inode")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("by_inode"),
                },
                FilesystemOperation::PutFileByInode {
                    parent_inode_id: docs_inode_id,
                    display_name: display_name("by_inode.txt"),
                    content_ref: content_ref.clone(),
                },
            ],
        },
        &context,
    )
    .await
    .expect("inode creates commit");

    let path_events = events_at(&store, &namespace_id, by_path.committed_seq).await;
    let inode_events = events_at(&store, &namespace_id, by_inode.committed_seq).await;

    match (&path_events[0], &inode_events[0]) {
        (
            FilesystemChange::DirectoryCreated {
                parent_inode_id: path_parent,
                display_name: path_name,
                ..
            },
            FilesystemChange::DirectoryCreated {
                parent_inode_id: inode_parent,
                display_name: inode_name,
                ..
            },
        ) => {
            assert_eq!(path_parent, &docs_inode_id);
            assert_eq!(inode_parent, &docs_inode_id);
            assert_eq!(path_name.as_str(), "by_path");
            assert_eq!(inode_name.as_str(), "by_inode");
        }
        other => panic!("expected two directory-created events, got {other:?}"),
    }

    match (&path_events[1], &inode_events[1]) {
        (
            FilesystemChange::FileCreated {
                parent_inode_id: path_parent,
                binding_generation: path_generation,
                revision_no: path_revision,
                content_ref: path_content,
                ..
            },
            FilesystemChange::FileCreated {
                inode_id: inode_created,
                parent_inode_id: inode_parent,
                binding_generation: inode_generation,
                revision_no: inode_revision,
                content_ref: inode_content,
                ..
            },
        ) => {
            assert_eq!(path_parent, inode_parent);
            assert_eq!(path_revision, inode_revision);
            assert_eq!(path_revision, &RevisionNo(1));
            assert_eq!(path_content, inode_content);
            // Each event carries the generation of the binding it created,
            // which is the token a read of that entry now reports.
            let (read_inode_id, read_generation) =
                read_entry(&store, &namespace_id, "/docs/by_inode.txt").await;
            assert_eq!(&read_inode_id, inode_created);
            assert_eq!(inode_generation, &read_generation);
            assert_ne!(path_generation, inode_generation);
        }
        other => panic!("expected two file-created events, got {other:?}"),
    }
}

#[tokio::test]
async fn creating_by_inode_conflicts_with_a_bound_name() {
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
    .expect("put /docs/taken.txt");
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
    .expect_err("a bound name conflicts");

    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn a_revision_write_by_inode_requires_the_current_revision() {
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
    .expect_err("a stale revision guard is rejected");
    assert_eq!(stale.code(), ErrorCode::StaleRevision);

    // The write names the file, not a place in the tree, so a rebinding
    // between the read and the write changes nothing about it.
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-report")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            to_path: AbsolutePath::parse("/report.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("move the file");

    let fresh = submit_operation(
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
    .expect("a current revision guard commits");

    match &events_at(&store, &namespace_id, fresh.committed_seq).await[..] {
        [FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            ..
        }] => {
            assert_eq!(inode_id, &report_inode_id);
            assert_eq!(revision_no, &RevisionNo(3));
        }
        other => panic!("expected one content-changed event, got {other:?}"),
    }
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/report.txt")
            .await
            .expect("read the file")
            .bytes,
        b"third"
    );
}

#[tokio::test]
async fn a_stale_binding_generation_rejects_a_move_and_a_fresh_one_moves_the_inode() {
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
    .expect("put the file");
    let (report_inode_id, stale_generation) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;
    // Another writer rebinds the name, which mints a new generation.
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("rename-report")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/renamed.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("rename the file");

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
        },
        &context,
    )
    .await
    .expect_err("a stale binding generation is rejected");
    assert_eq!(error.code(), ErrorCode::BindingGenerationMismatch);

    let (_, fresh_generation) = read_entry(&store, &namespace_id, "/docs/renamed.txt").await;
    let moved = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-fresh")),
        FilesystemOperation::MoveByInode {
            inode_id: report_inode_id,
            expected_binding_generation: fresh_generation,
            to_parent_inode_id: ROOT_INODE_ID,
            to_display_name: display_name("moved.txt"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("a fresh binding generation moves the inode");

    match &events_at(&store, &namespace_id, moved.committed_seq).await[..] {
        [FilesystemChange::Moved {
            inode_id,
            to_parent_inode_id,
            to_display_name,
            ..
        }] => {
            assert_eq!(inode_id, &report_inode_id);
            assert_eq!(to_parent_inode_id, &ROOT_INODE_ID);
            assert_eq!(to_display_name.as_str(), "moved.txt");
        }
        other => panic!("expected one moved event, got {other:?}"),
    }
    let (moved_inode_id, _) = read_entry(&store, &namespace_id, "/moved.txt").await;
    assert_eq!(moved_inode_id, report_inode_id);
}

#[tokio::test]
async fn a_stale_binding_generation_rejects_a_delete_and_a_fresh_one_deletes_the_inode() {
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
    .expect("put the file");
    let (report_inode_id, stale_generation) =
        read_entry(&store, &namespace_id, "/docs/report.txt").await;
    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("rename-report")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/renamed.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("rename the file");

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
    .expect_err("a stale binding generation is rejected");
    assert_eq!(error.code(), ErrorCode::BindingGenerationMismatch);

    let (_, fresh_generation) = read_entry(&store, &namespace_id, "/docs/renamed.txt").await;
    let deleted = submit_operation(
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
    .expect("a fresh binding generation deletes the inode");

    match &events_at(&store, &namespace_id, deleted.committed_seq).await[..] {
        [FilesystemChange::Deleted { inode_id, .. }] => assert_eq!(inode_id, &report_inode_id),
        other => panic!("expected one deleted event, got {other:?}"),
    }
    assert_eq!(
        resolve_path(&store, &namespace_id, "/docs/renamed.txt")
            .await
            .expect_err("the entry is gone")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn a_binding_generation_minted_elsewhere_is_an_invalid_request() {
    let (_temp_dir, store, namespace_id, context) = namespace_with_docs().await;
    let (docs_inode_id, generation) = read_entry(&store, &namespace_id, "/docs").await;
    let foreign = BindingGeneration::decode(&generation, &namespace_id)
        .expect("decode this namespace's token")
        .encode(&NamespaceId::parse("other").expect("valid namespace id"))
        .expect("mint a token for another namespace");

    for token in [foreign, "not-a-token".to_owned()] {
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
        .expect_err("an unreadable guard is rejected");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
}

#[tokio::test]
async fn an_inode_operation_observes_what_an_earlier_operation_in_its_commit_did() {
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
    .expect("put the file");
    let (report_inode_id, _) = read_entry(&store, &namespace_id, "/docs/report.txt").await;
    // The head names the identity the next allocation assigns, so the
    // second operation can address what the first one creates.
    let created_inode_id = read_context(&store, &namespace_id).await.head.next_inode_id;
    let content_ref = store_bytes_as_content(&store, &namespace_id, b"nested")
        .await
        .expect("stage content")
        .into_content_ref();

    let committed = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: test_commit_id(Some("create-then-fill")),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/2026").expect("path"),
                    parents: false,
                },
                FilesystemOperation::PutFileByInode {
                    parent_inode_id: created_inode_id,
                    display_name: display_name("january.pdf"),
                    content_ref,
                },
            ],
        },
        &context,
    )
    .await
    .expect("the inode operation resolves the directory its predecessor created");

    match &events_at(&store, &namespace_id, committed.committed_seq).await[..] {
        [FilesystemChange::DirectoryCreated {
            inode_id: created, ..
        }, FilesystemChange::FileCreated {
            parent_inode_id, ..
        }] => {
            assert_eq!(created, &created_inode_id);
            assert_eq!(parent_inode_id, &created_inode_id);
        }
        other => panic!("expected a directory create and a file create, got {other:?}"),
    }

    // The same observation the other way: a target an earlier operation
    // removed is gone for the operation that follows it.
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
    .expect_err("the deleted inode is not addressable");
    assert_eq!(error.code(), ErrorCode::InodeNotFound);
}

/// Stages `bytes` and names them in a guarded revision write on `inode_id`.
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
