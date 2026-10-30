use crate::common::*;
use loonfs::{ChangeSeq, DestinationBehavior, PutFileOptions};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::FilesystemChange;
use tempfile::tempdir;

/// Checks that a file's binding version, as a stat and a listing report it,
/// stays the same across a content write and changes on a move.
#[test]
fn binding_version_changes_on_move_but_not_content_update() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "binding-version-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft one",
        &loonfs_test_support::test_actor(),
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

    fs.put_file_with_options_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft two",
        &loonfs_test_support::test_actor(),
        &PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..Default::default()
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
    // The listing reads the summary row that the second revision wrote
    // under the file's binding, which keeps the bind's version.
    assert_eq!(
        fs.list_path_blocking(&namespace_id, "/docs")
            .expect("list the parent directory")
            .first()
            .expect("the file is the directory's only child")
            .binding_version,
        Some(created.clone()),
        "a listing after new content reports the same version"
    );

    let moved_at = fs
        .move_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            "/docs/final.txt",
            &loonfs_test_support::test_actor(),
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
