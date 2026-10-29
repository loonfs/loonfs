//! Change-feed behavior for operations that create or replace file content.

use crate::common::*;
use loonfs_core::publish::{CommitIntent, FilesystemOperation};
use loonfs_core::test_support::NamespaceFixture;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::commit_id;
use loonfs_types::api::v0::{FilesystemChange, ListChangesResponse};
use loonfs_types::{
    AbsolutePath, ChangeSeq, DeleteDirectoryBehavior, DestinationBehavior, NamespaceId, RevisionNo,
};
use tempfile::tempdir;

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
        FilesystemChange::AccessChanged { .. } => "access_changed",
        FilesystemChange::Unknown => "unknown",
    }
}

fn commit_events<'a>(changes: &'a ListChangesResponse, id: &str) -> &'a [FilesystemChange] {
    &changes
        .changes
        .iter()
        .find(|change| change.commit_id.as_str() == id)
        .unwrap_or_else(|| panic!("missing change for commit `{id}`"))
        .events
}

fn event_kinds(changes: &ListChangesResponse, id: &str) -> Vec<&'static str> {
    commit_events(changes, id).iter().map(event_kind).collect()
}

fn append_offsets(changes: &ListChangesResponse, id: &str) -> Vec<Option<u64>> {
    commit_events(changes, id)
        .iter()
        .map(|event| match event {
            FilesystemChange::ContentChanged { append_offset, .. } => *append_offset,
            other => panic!("expected `content_changed` in commit `{id}`, got {other:?}"),
        })
        .collect()
}

fn append(target: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::AppendFile {
        path: path(target),
        inline_content: bytes.to_vec(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

#[tokio::test]
async fn creation_and_republication_operations_emit_exact_event_kinds_in_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("feed-matrix").expect("namespace id");
    let context = mutation_context();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap namespace");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/report.txt",
            b"revision one",
            DestinationBehavior::NoReplace,
            Some(&commit_id("create-file")),
        )
        .await
        .expect("create file");
    create_directory_path(
        &store,
        &namespace_id,
        "/empty",
        &context,
        Some(&commit_id("mkdir")),
    )
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
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/report.txt",
            b"revision two",
            DestinationBehavior::Replace,
            Some(&commit_id("replace-file")),
        )
        .await
        .expect("replace file");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-file"),
        FilesystemOperation::CopyPath {
            source_path: path("/report.txt"),
            destination_path: path("/copy.txt"),
            precondition: loonfs_types::DestinationPrecondition {
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
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
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

#[tokio::test]
async fn append_events_report_where_the_appended_bytes_begin_before_and_after_a_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("feed-appends").expect("namespace id");
    let context = mutation_context();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap namespace");
    for (target, bytes) in [("/log.txt", b"hello".as_slice()), ("/empty.txt", b"")] {
        NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(target, bytes, DestinationBehavior::NoReplace, None)
            .await
            .expect("create file");
    }
    for (id, operations) in [
        ("append", vec![append("/log.txt", b" world")]),
        (
            "append-twice",
            vec![append("/log.txt", b"!"), append("/log.txt", b"?")],
        ),
        ("append-to-empty", vec![append("/empty.txt", b"x")]),
        (
            "restore",
            vec![FilesystemOperation::RestoreRevision {
                path: path("/log.txt"),
                source_revision_no: RevisionNo(2),
            }],
        ),
        // Revision 4 extended the chain past the restored revision 2, so
        // this append starts a new content ID.
        ("append-to-restored", vec![append("/log.txt", b" again")]),
    ] {
        let request = CommitIntent {
            commit_id: commit_id(id),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations,
        };
        submit_commit(&store, &namespace_id, request, &context)
            .await
            .expect("publish commit");
    }
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/log.txt",
            b"replaced",
            DestinationBehavior::Replace,
            Some(&commit_id("replace")),
        )
        .await
        .expect("replace file");

    let expected = [
        ("append", vec![Some(5)]),
        ("append-twice", vec![Some(11), Some(12)]),
        ("append-to-empty", vec![Some(0)]),
        ("restore", vec![None]),
        ("append-to-restored", vec![Some(11)]),
        ("replace", vec![None]),
    ];
    for folded in [false, true] {
        if folded {
            namespace_engine(&store, &namespace_id, &context)
                .fold_wal()
                .await
                .expect("fold the WAL tail into commit rows");
        }
        let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
            .await
            .expect("read change feed");
        for (id, offsets) in &expected {
            assert_eq!(
                &append_offsets(&changes, id),
                offsets,
                "{id}, folded: {folded}"
            );
        }
        let content_id = |id| match commit_events(&changes, id) {
            [FilesystemChange::ContentChanged { content_ref, .. }] => &content_ref.content_id,
            other => panic!("expected one `content_changed` event, got {other:?}"),
        };
        assert_ne!(content_id("append-to-restored"), content_id("restore"));
    }
}

#[tokio::test]
async fn an_append_in_a_fork_to_an_inherited_file_reports_its_offset() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("feed-source").expect("namespace id");
    let fork = NamespaceId::parse("feed-fork").expect("namespace id");
    let context = mutation_context();
    NamespaceFixture::new(&store, &source, &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &source, &context)
        .put_file("/log.txt", b"hello", DestinationBehavior::NoReplace, None)
        .await
        .expect("create file");
    let forked = namespace_engine(&store, &source, &context)
        .fork_namespace(&fork, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    submit_operation(
        &store,
        &fork,
        commit_id("append-in-fork"),
        append("/log.txt", b" world"),
        &context,
    )
    .await
    .expect("append in the fork");

    for folded in [false, true] {
        if folded {
            namespace_engine(&store, &fork, &context)
                .fold_wal()
                .await
                .expect("fold the fork's WAL tail into commit rows");
        }
        let changes = list_changes_after(&store, &fork, forked.response.head_seq)
            .await
            .expect("read the fork's change feed");
        let [FilesystemChange::ContentChanged {
            content_ref,
            append_offset,
            ..
        }] = commit_events(&changes, "append-in-fork")
        else {
            panic!("expected one `content_changed` event, folded: {folded}");
        };
        assert_eq!(content_ref.owner_namespace_id, fork);
        assert_eq!(*append_offset, Some(5), "folded: {folded}");
    }
}
