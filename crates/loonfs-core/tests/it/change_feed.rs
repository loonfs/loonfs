//! Change-feed behavior for operations that create or replace file content.

#![allow(clippy::panic)]
// Unexpected variants include the full event in the failure message.

use crate::common::commit_split_support::*;
use loonfs_api::v0::{FilesystemChange, ListChangesResponse};
use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, NamespaceId,
    RevisionNo,
};
use loonfs_core::publish::FilesystemOperation;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use tempfile::tempdir;

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid commit id")
}

fn path(value: &str) -> AbsolutePath {
    AbsolutePath::parse(value).expect("valid absolute path")
}

fn event_kind(event: &FilesystemChange) -> &'static str {
    match event {
        FilesystemChange::DirectoryCreated { .. } => "directory_created",
        FilesystemChange::FileCreated { .. } => "file_created",
        FilesystemChange::ContentChanged { .. } => "content_changed",
        FilesystemChange::Moved { .. } => "moved",
        FilesystemChange::Deleted { .. } => "deleted",
        FilesystemChange::Undeleted { .. } => "undeleted",
        FilesystemChange::AttributesChanged { .. } => "attributes_changed",
    }
}

fn event_kinds(changes: &ListChangesResponse, id: &str) -> Vec<&'static str> {
    changes
        .changes
        .iter()
        .find(|change| change.commit_id.as_str() == id)
        .unwrap_or_else(|| panic!("missing change for commit `{id}`"))
        .events
        .iter()
        .map(event_kind)
        .collect()
}

#[tokio::test]
async fn creation_and_republication_operations_emit_exact_event_kinds_in_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("feed-matrix").expect("namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    put_file_bytes(
        &store,
        &namespace_id,
        "/report.txt",
        b"revision one",
        DestinationBehavior::NoReplace,
        &context,
        Some("create-file"),
    )
    .await
    .expect("create file");
    create_directory_path(&store, &namespace_id, "/empty", &context, Some("mkdir"))
        .await
        .expect("create directory");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("mkdir-parents"),
        FilesystemOperation::CreateDirectory {
            path: path("/a/b/c"),
            parents: true,
        },
        &context,
    )
    .await
    .expect("create nested directories");
    put_file_bytes(
        &store,
        &namespace_id,
        "/report.txt",
        b"revision two",
        DestinationBehavior::Replace,
        &context,
        Some("replace-file"),
    )
    .await
    .expect("replace file");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-file"),
        FilesystemOperation::CopyPath {
            from_path: path("/report.txt"),
            to_path: path("/copy.txt"),
            guard: loonfs_api::DestinationGuard {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect("copy file");

    let copied_inode_id = resolve_path(&store, &namespace_id, "/copy.txt")
        .await
        .expect("resolve copied file")
        .inode_id;
    let deletion = submit_operation(
        &store,
        &namespace_id,
        commit_id("delete-copy"),
        FilesystemOperation::DeletePath {
            path: path("/copy.txt"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete copied file");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("undelete-copy"),
        FilesystemOperation::Undelete {
            inode_id: copied_inode_id,
            deletion_seq: deletion.committed_seq,
            path: None,
        },
        &context,
    )
    .await
    .expect("undelete copied file");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("restore-file"),
        FilesystemOperation::RestoreRevision {
            path: path("/report.txt"),
            source_revision_no: RevisionNo(1),
        },
        &context,
    )
    .await
    .expect("restore first revision");

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("read change feed");
    assert_eq!(event_kinds(&changes, "create-file"), vec!["file_created"]);
    assert_eq!(event_kinds(&changes, "mkdir"), vec!["directory_created"]);
    assert_eq!(
        event_kinds(&changes, "mkdir-parents"),
        vec![
            "directory_created",
            "directory_created",
            "directory_created"
        ]
    );
    assert_eq!(
        event_kinds(&changes, "replace-file"),
        vec!["content_changed"]
    );
    assert_eq!(event_kinds(&changes, "copy-file"), vec!["file_created"]);
    assert_eq!(event_kinds(&changes, "delete-copy"), vec!["deleted"]);
    assert_eq!(event_kinds(&changes, "undelete-copy"), vec!["undeleted"]);
    assert_eq!(
        event_kinds(&changes, "restore-file"),
        vec!["content_changed"]
    );
}
