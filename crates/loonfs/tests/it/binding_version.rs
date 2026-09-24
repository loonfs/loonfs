#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{ChangeSeq, CreateNamespaceOptions, DestinationBehavior, MoveOptions, PutFileOptions};
use loonfs_api::v0::FilesystemChange;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[test]
fn binding_version_changes_on_move_but_not_content_update() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "binding-version-test");
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
        .binding_version
        .expect("a named entry carries its binding version");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/")
            .expect("stat the root")
            .binding_version,
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
            .binding_version,
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
        .binding_version
        .expect("a named entry carries its binding version");
    assert_ne!(renamed, created, "a move creates a new binding version");
    assert_eq!(
        fs.list_path_blocking(&namespace_id, "/docs")
            .expect("list the parent directory")
            .first()
            .expect("the moved file is the directory's only child")
            .binding_version,
        Some(renamed.clone()),
        "a listing reports the version the stat reports"
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
            binding_version, ..
        }] => assert_eq!(
            *binding_version, renamed,
            "the event reports the version the read reports"
        ),
        other => panic!("expected one moved event, got {other:?}"),
    }
}
