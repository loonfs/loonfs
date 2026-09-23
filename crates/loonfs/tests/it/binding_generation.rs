#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{ChangeSeq, CreateNamespaceOptions, DestinationBehavior, MoveOptions, PutFileOptions};
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{CommitId, DeleteDirectoryBehavior, ErrorCode};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn binding_generation_changes_on_move_but_not_content_update() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "binding-generation-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft one",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put the first revision");

    let created = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat the created file")
        .binding_generation
        .expect("a named entry carries its binding generation");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/")
            .expect("stat the root")
            .binding_generation,
        None,
        "the nameless root has no binding"
    );

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft two",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        },
    )
    .expect("put the second revision");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat the rewritten file")
            .binding_generation,
        Some(created.clone()),
        "new content does not rebind the name"
    );

    let moved_at = fs
        .move_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            "/docs/final.txt",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("move the file")
        .committed_seq;
    let renamed = fs
        .stat_path_blocking(&namespace_id, "/docs/final.txt")
        .expect("stat the moved file")
        .binding_generation
        .expect("a named entry carries its binding generation");
    assert_ne!(renamed, created, "a move creates a new binding generation");
    assert_eq!(
        fs.list_path_blocking(&namespace_id, "/docs")
            .expect("list the parent directory")
            .first()
            .expect("the moved file is the directory's only child")
            .binding_generation,
        Some(renamed.clone()),
        "a listing reports the generation the stat reports"
    );

    let changes = fs
        .list_changes_blocking(&namespace_id, ChangeSeq(moved_at.0 - 1))
        .expect("read the change feed");
    let events = &changes
        .changes
        .first()
        .expect("the move is a committed change")
        .events;
    match events.as_slice() {
        [FilesystemChange::Moved {
            binding_generation, ..
        }] => assert_eq!(
            *binding_generation, renamed,
            "the event reports the generation the read reports"
        ),
        other => panic!("expected one moved event, got {other:?}"),
    }
}

#[tokio::test]
async fn binding_tokens_from_a_prior_generation_cannot_delete_a_recreated_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = writer(store.clone(), "binding-generation-recreation").await;
    let reader = writer.reader();
    let namespace = namespace_id("demo");
    let foreign_namespace = namespace_id("other");
    for namespace in [&namespace, &foreign_namespace] {
        writer
            .create_namespace(
                namespace,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                namespace,
                "/file.txt",
                b"before",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create file");
    }
    let original = reader
        .get_path_entry(&namespace, "/file.txt", Default::default())
        .await
        .expect("original entry");
    let foreign = reader
        .get_path_entry(&foreign_namespace, "/file.txt", Default::default())
        .await
        .expect("foreign entry");
    writer
        .delete_namespace(&namespace, Default::default())
        .await
        .expect("delete namespace");
    writer
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("recreate namespace");
    let created = writer
        .put_file_bytes(
            &namespace,
            "/file.txt",
            b"after",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("recreate file");
    let current = reader
        .get_path_entry(&namespace, "/file.txt", Default::default())
        .await
        .expect("current entry");
    assert_eq!(current.inode_id, original.inode_id);
    assert_eq!(created.committed_seq, ChangeSeq(1));
    assert_ne!(current.binding_generation, original.binding_generation);
    match created.events.as_slice() {
        [FilesystemChange::FileCreated {
            binding_generation, ..
        }] => {
            assert_eq!(
                Some(binding_generation),
                current.binding_generation.as_ref()
            );
        }
        other => panic!("expected one file creation, got {other:?}"),
    }
    let head = reader
        .get_namespace(&namespace)
        .await
        .expect("current head");
    let mut errors = Vec::new();
    for token in [foreign.binding_generation, original.binding_generation] {
        store.reset();
        let error = writer
            .create_commit(
                &namespace,
                CommitRequest::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::DeleteByInode {
                        inode_id: current.inode_id,
                        expected_binding_generation: token.expect("named entry token"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                    },
                ),
            )
            .await
            .expect_err("a foreign binding token must fail");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        let counts = store.counts();
        assert_eq!(counts.puts, 0);
        assert_eq!(counts.compare_and_swaps, 0);
        assert_eq!(counts.deletes, 0);
        errors.push(error.to_string());
    }
    assert_eq!(errors[0], errors[1]);
    assert_eq!(
        reader
            .get_namespace(&namespace)
            .await
            .expect("unchanged head"),
        head
    );
    assert_eq!(
        reader
            .get_path_entry(&namespace, "/file.txt", Default::default())
            .await
            .expect("unchanged entry"),
        current
    );
}
