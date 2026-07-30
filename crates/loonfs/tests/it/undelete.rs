//! Undelete generations, delete guards, and restored visibility.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CreateNamespaceOptions, DeleteOptions,
    DestinationBehavior, ErrorCode, InodeId, ListChangesOptions, MaintenanceStepOptions,
    PutFileOptions, RuntimeError,
};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let error = fs
        .delete_path_blocking(&namespace_id, "/docs", DeleteOptions::default())
        .expect_err("non-recursive delete should reject non-empty directory");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::DirectoryNotEmpty
    ));

    fs.delete_path_blocking(
        &namespace_id,
        "/docs",
        DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::Recursive,
            commit_id: None,
            message: None,
            expected_inode_id: None,
        },
    )
    .expect("recursive delete");
    let error = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect_err("deleted file should not stat");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    ));
}

#[test]
fn undelete_recovers_a_deleted_file_and_generations_stay_scoped() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft one",
        PutFileOptions::default(),
    )
    .expect("put revision one");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft two",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("put revision two");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat before delete")
        .inode_id;

    let first_deletion = fs
        .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
        .expect("delete file")
        .committed_seq;

    // Recovery re-attaches the same inode — identity, content, and the full
    // revision history come back, even at a new path.
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/recovered.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete");
    let recovered = fs
        .stat_path_blocking(&namespace_id, "/docs/recovered.txt")
        .expect("stat recovered file");
    assert_eq!(recovered.inode_id, inode_id);
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/recovered.txt")
            .expect("read recovered content")
            .bytes,
        b"draft two"
    );
    assert_eq!(
        block_on(fs.reader.get_file_revision_bytes(
            &namespace_id,
            "/docs/recovered.txt",
            loonfs::RevisionNo(1),
        ))
        .expect("read prior revision through the recovered path")
        .bytes,
        b"draft one"
    );

    // The recovered inode is no longer deleted: replaying the handle
    // conflicts.
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/again.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("double undelete should conflict");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    // Delete again: the old generation handle must not cancel the new
    // deletion, and the failure names both generations.
    let second_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/recovered.txt",
            DeleteOptions::default(),
        )
        .expect("delete recovered file again")
        .committed_seq;
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/stale.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("stale generation handle must not clear the newer deletion");
    match &error {
        RuntimeError::Core(error) => {
            assert_eq!(error.code(), ErrorCode::NotDeleted);
            let details = error.details().expect("generation mismatch details");
            assert_eq!(details.requested_deletion_seq, Some(first_deletion));
            assert_eq!(details.active_deletion_seq, Some(second_deletion));
        }
        other => panic!("expected core error, got {other:?}"),
    }
    let still_gone = fs.stat_path_blocking(&namespace_id, "/docs/stale.txt");
    assert!(still_gone.is_err(), "stale undelete must not bind anything");

    // The current generation's handle recovers to the original path.
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        second_deletion,
        "/docs/report.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the active generation");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat restored original path")
            .inode_id,
        inode_id
    );
}

#[test]
fn undelete_recovers_a_deleted_subtree_and_rejects_covered_children() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-subtree-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/a.txt",
        b"alpha",
        PutFileOptions::default(),
    )
    .expect("put nested file");
    let directory_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes")
        .expect("stat directory")
        .inode_id;
    let child_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes/a.txt")
        .expect("stat child")
        .inode_id;

    let deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/notes",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit_id: None,
                message: None,
                expected_inode_id: None,
            },
        )
        .expect("recursive delete")
        .committed_seq;

    // A child is covered by the subtree root's tombstone, not its own:
    // recovery targets the root.
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        child_inode,
        deletion,
        "/docs/a-alone.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("child of a deleted directory is not the deletion root");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    block_on(fs.writer.undelete(
        &namespace_id,
        directory_inode,
        deletion,
        "/docs/notes",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the subtree root");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/notes/a.txt")
            .expect("nested file is visible again")
            .bytes,
        b"alpha"
    );
}

#[test]
fn undelete_of_an_ancestor_keeps_independently_deleted_children_hidden() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-nested-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/secret.txt",
        b"independently deleted",
        PutFileOptions::default(),
    )
    .expect("put nested file");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/kept.txt",
        b"kept",
        PutFileOptions::default(),
    )
    .expect("put sibling file");
    let directory_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes")
        .expect("stat directory")
        .inode_id;

    // Delete the child on its own, then the whole ancestor directory.
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/notes/secret.txt",
        DeleteOptions::default(),
    )
    .expect("delete child independently");
    let ancestor_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/notes",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit_id: None,
                message: None,
                expected_inode_id: None,
            },
        )
        .expect("recursive delete of the ancestor")
        .committed_seq;

    // Recovering the ancestor revokes exactly its own deletion: the
    // independently deleted child stays hidden behind its own tombstone.
    block_on(fs.writer.undelete(
        &namespace_id,
        directory_inode,
        ancestor_deletion,
        "/docs/notes",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the ancestor");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/notes/kept.txt")
            .expect("sibling is visible again")
            .bytes,
        b"kept"
    );
    let hidden = fs.stat_path_blocking(&namespace_id, "/docs/notes/secret.txt");
    assert!(matches!(
        hidden,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::PathNotFound
    ));
}

#[test]
fn undelete_survives_checkpoints_and_reopen_in_both_orders() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());
    let namespace_id = namespace_id("demo");

    // Order one: delete + undelete in the WAL tail, then checkpoint,
    // then reopen cold from object storage.
    let deletion = {
        let fs = open_runtime(object_store.clone(), "undelete-persist-a");
        fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
            .expect("create namespace");
        fs.put_file_bytes_blocking(
            &namespace_id,
            "/docs/report.txt",
            b"persisted",
            PutFileOptions::default(),
        )
        .expect("put file");
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let deletion = fs
            .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
            .expect("delete")
            .committed_seq;
        block_on(fs.writer.undelete(
            &namespace_id,
            inode_id,
            deletion,
            "/docs/report.txt",
            loonfs::UndeleteOptions::default(),
        ))
        .expect("undelete before checkpoint");
        // The default threshold (32 segments) would answer NotNeeded for
        // this short history; force the flush so reopen reads Set and
        // Revoke rows out of durable tables, not WAL replay.
        let step = fs
            .maintenance_step_namespace_blocking(
                &namespace_id,
                MaintenanceStepOptions {
                    max_wal_tail_segments: 1,
                    retention: false,
                    gc: None,
                    only: None,
                },
            )
            .expect("checkpoint the revoke into durable tables");
        assert!(
            matches!(step.wal_flush, loonfs::WalFlushStepOutcome::Flushed { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_flush
        );
        deletion
    };
    {
        let fs = open_runtime(object_store.clone(), "undelete-persist-b");
        assert_eq!(
            fs.get_file_bytes_blocking(&namespace_id, "/docs/report.txt")
                .expect("recovered file survives checkpoint and reopen")
                .bytes,
            b"persisted"
        );

        // Order two: delete, checkpoint, reopen, THEN undelete — the
        // revoke must resolve a deletion that lives in durable tables,
        // not the WAL tail.
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let second_deletion = fs
            .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
            .expect("delete again")
            .committed_seq;
        assert!(second_deletion > deletion);
        let step = fs
            .maintenance_step_namespace_blocking(
                &namespace_id,
                MaintenanceStepOptions {
                    max_wal_tail_segments: 1,
                    retention: false,
                    gc: None,
                    only: None,
                },
            )
            .expect("checkpoint the deletion");
        assert!(
            matches!(step.wal_flush, loonfs::WalFlushStepOutcome::Flushed { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_flush
        );
        let fs = open_runtime(object_store.clone(), "undelete-persist-c");
        block_on(fs.writer.undelete(
            &namespace_id,
            inode_id,
            second_deletion,
            "/docs/report.txt",
            loonfs::UndeleteOptions::default(),
        ))
        .expect("undelete a checkpointed deletion after reopen");
        let step = fs
            .maintenance_step_namespace_blocking(
                &namespace_id,
                MaintenanceStepOptions {
                    max_wal_tail_segments: 1,
                    retention: false,
                    gc: None,
                    only: None,
                },
            )
            .expect("checkpoint the second revoke");
        assert!(
            matches!(step.wal_flush, loonfs::WalFlushStepOutcome::Flushed { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_flush
        );
    }
    let fs = open_runtime(object_store, "undelete-persist-d");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("recovered file survives the second cycle")
            .bytes,
        b"persisted"
    );
}

#[test]
fn change_feed_reports_the_deletion_generation_an_undelete_takes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-feed-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"feed",
        PutFileOptions::default(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;
    let deletion = fs
        .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
        .expect("delete")
        .committed_seq;
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        deletion,
        "/docs/report.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete");

    let changes = block_on(fs.reader.list_changes(
        &namespace_id,
        ChangeSeq(0),
        ListChangesOptions::default(),
    ))
    .expect("list changes");
    let mut deleted_seq = None;
    let mut undeleted = None;
    for change in &changes.changes {
        for event in &change.events {
            match event {
                loonfs::FilesystemChange::Deleted {
                    inode_id: deleted_inode_id,
                    ..
                } if *deleted_inode_id == inode_id => {
                    deleted_seq = Some(change.seq);
                }
                loonfs::FilesystemChange::Undeleted {
                    inode_id: undeleted_inode_id,
                    name,
                    ..
                } if *undeleted_inode_id == inode_id => {
                    undeleted = Some(name.as_str().to_owned());
                }
                _ => {}
            }
        }
    }
    // The `Deleted` event's enclosing sequence is the deletion generation an
    // undelete passes as `deleted_at_seq`, so a feed projection can drive a
    // recovery without guessing at "newest".
    assert_eq!(deleted_seq, Some(deletion));
    assert_eq!(undeleted.as_deref(), Some("report.txt"));
}

#[test]
fn the_feed_names_deleted_entries_and_their_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "feed-identity-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/Quarterly Report.PDF",
        b"body",
        PutFileOptions::default(),
    )
    .expect("put");
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/Quarterly Report.PDF",
        DeleteOptions::default(),
    )
    .expect("delete");

    let changes = block_on(fs.reader.list_changes(
        &namespace_id,
        ChangeSeq(0),
        ListChangesOptions::default(),
    ))
    .expect("list changes");

    // A projection of the feed sees the spelling a person typed — on the
    // deletion as well as the creation — plus which session wrote each
    // commit, without a second lookup per entry.
    let deleted_name = changes
        .changes
        .iter()
        .flat_map(|change| &change.events)
        .find_map(|event| match event {
            loonfs::FilesystemChange::Deleted {
                name: Some(name), ..
            } => Some(name.as_str().to_owned()),
            _ => None,
        });
    assert_eq!(deleted_name.as_deref(), Some("Quarterly Report.PDF"));
    for change in &changes.changes {
        assert!(!change.writer_id.is_empty());
        assert!(
            change.writer_session_id.starts_with("wrs_"),
            "session id: {}",
            change.writer_session_id
        );
    }
}

#[test]
fn undelete_rejects_deletions_from_the_same_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-same-commit-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"cycled",
        PutFileOptions::default(),
    )
    .expect("put file");
    let entry = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat");

    // Assigned sequences are head + 1 and therefore guessable: without the
    // earlier-commit bound, one commit could delete, undelete, and
    // re-delete the inode, minting two deletion generations that share a
    // sequence. The undelete must refuse a target in its own commit.
    let guessed_seq = ChangeSeq(entry.head_seq.0 + 1);
    let error = fs
        .commit_operations_blocking(
            &namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("same-commit-cycle").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![
                    CommitOp::DeleteFile {
                        inode_id: entry.inode_id,
                    },
                    CommitOp::Undelete {
                        inode_id: entry.inode_id,
                        deleted_at_seq: guessed_seq,
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("resurrected.txt")
                            .expect("valid display name"),
                    },
                ],
                message: None,
            },
        )
        .expect_err("same-commit delete/undelete cycling must be rejected");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));
    // The rejected commit changed nothing.
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("file untouched")
            .bytes,
        b"cycled"
    );
}

#[test]
fn delete_with_expected_inode_refuses_a_raced_rebinding() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-expectation-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"original",
        PutFileOptions::default(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;

    // Stand-in for a rebinding that raced the caller's stat: the path now
    // holds a different inode than the one the caller resolved.
    let error = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
                commit_id: None,
                message: None,
                expected_inode_id: Some(InodeId(inode_id.0 + 1)),
            },
        )
        .expect_err("a mismatched expectation must fail the delete");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::PathConflict
    ));
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("file untouched")
            .bytes,
        b"original"
    );

    // The matching expectation deletes exactly that inode.
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/report.txt",
        DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
            commit_id: None,
            message: None,
            expected_inode_id: Some(inode_id),
        },
    )
    .expect("matching expectation deletes");
}
