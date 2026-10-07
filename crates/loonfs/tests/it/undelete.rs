//! Undelete positions, delete preconditions, and restored visibility.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, DeleteOptions, DestinationBehavior, Error,
    ErrorCode, InodeId, PutFileOptions,
};
use loonfs_test_support::ids::{first_page, namespace_id};
use tempfile::tempdir;

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");

    let error = fs
        .delete_path_blocking(&namespace_id, "/docs", &loonfs_test_support::test_actor())
        .expect_err("non-recursive delete should reject non-empty directory");
    assert!(matches!(
        error,
        Error::Core(error) if error.code() == loonfs::ErrorCode::DirectoryNotEmpty
    ));

    fs.delete_path_with_options_blocking(
        &namespace_id,
        "/docs",
        &loonfs_test_support::test_actor(),
        &DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::Recursive,
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: None,
                message: None,
            },
            expected_inode_id: None,
        },
    )
    .expect("recursive delete");
    let error = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect_err("deleted file should not stat");
    assert!(matches!(
        error,
        Error::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    ));
}

#[test]
fn undelete_recovers_a_deleted_file_and_positions_stay_scoped() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-test");
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft one",
        &loonfs_test_support::test_actor(),
    )
    .expect("put revision one");
    fs.put_file_with_options_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft two",
        &loonfs_test_support::test_actor(),
        &PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: None,
                message: None,
            },
            expected_inode_id: None,
            expected_revision_no: None,
        },
    )
    .expect("put revision two");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat before delete")
        .inode_id;

    let first_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            &loonfs_test_support::test_actor(),
        )
        .expect("delete file")
        .committed_seq;

    // Recovery re-attaches the same inode — identity, content, and the full
    // revision history come back, even at a new path.
    block_on(namespace_writer.undelete(
        inode_id,
        first_deletion,
        Some("/docs/recovered.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect("undelete");
    let recovered = fs
        .stat_path_blocking(&namespace_id, "/docs/recovered.txt")
        .expect("stat recovered file");
    assert_eq!(recovered.inode_id, inode_id);
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/recovered.txt")
            .expect("read recovered content")
            .bytes,
        b"draft two"
    );
    assert_eq!(
        block_on(namespace.read_file_revision("/docs/recovered.txt", loonfs::RevisionNo(1),))
            .expect("read prior revision through the recovered path")
            .bytes,
        b"draft one"
    );

    // The recovered inode is no longer deleted: replaying the handle
    // conflicts.
    let error = block_on(namespace_writer.undelete(
        inode_id,
        first_deletion,
        Some("/docs/again.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect_err("double undelete should conflict");
    assert!(matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    // Delete again: the old position handle must not cancel the new
    // deletion, and the failure names both positions.
    let second_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/recovered.txt",
            &loonfs_test_support::test_actor(),
        )
        .expect("delete recovered file again")
        .committed_seq;
    let error = block_on(namespace_writer.undelete(
        inode_id,
        first_deletion,
        Some("/docs/stale.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect_err("stale position handle must not clear the newer deletion");
    match &error {
        Error::Core(error) => {
            assert_eq!(error.code(), ErrorCode::NotDeleted);
            let details = error.details().expect("position mismatch details");
            assert_eq!(details.expected_deletion_seq, Some(first_deletion));
            assert_eq!(details.actual_deletion_seq, Some(second_deletion));
        }
        other => panic!("expected core error, got {other:?}"),
    }
    let still_gone = fs.stat_path_blocking(&namespace_id, "/docs/stale.txt");
    assert!(still_gone.is_err(), "stale undelete must not bind anything");

    // The current position's handle recovers to the original path.
    block_on(namespace_writer.undelete(
        inode_id,
        second_deletion,
        Some("/docs/report.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect("undelete the active position");
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
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/notes/a.txt",
        b"alpha",
        &loonfs_test_support::test_actor(),
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
        .delete_path_with_options_blocking(
            &namespace_id,
            "/docs/notes",
            &loonfs_test_support::test_actor(),
            &DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit: loonfs_types::options::CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: None,
            },
        )
        .expect("recursive delete")
        .committed_seq;

    // A child is covered by the subtree root's tombstone, not its own:
    // recovery targets the root.
    let error = block_on(namespace.undelete(
        child_inode,
        deletion,
        Some("/docs/a-alone.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect_err("child of a deleted directory is not the deletion root");
    assert!(matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    block_on(namespace.undelete(
        directory_inode,
        deletion,
        Some("/docs/notes"),
        &loonfs_test_support::test_actor(),
    ))
    .expect("undelete the subtree root");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/notes/a.txt")
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
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/notes/secret.txt",
        b"independently deleted",
        &loonfs_test_support::test_actor(),
    )
    .expect("put nested file");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/notes/kept.txt",
        b"kept",
        &loonfs_test_support::test_actor(),
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
        &loonfs_test_support::test_actor(),
    )
    .expect("delete child independently");
    let ancestor_deletion = fs
        .delete_path_with_options_blocking(
            &namespace_id,
            "/docs/notes",
            &loonfs_test_support::test_actor(),
            &DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit: loonfs_types::options::CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: None,
            },
        )
        .expect("recursive delete of the ancestor")
        .committed_seq;

    // Recovering the ancestor revokes exactly its own deletion: the
    // independently deleted child stays hidden behind its own tombstone.
    block_on(namespace.undelete(
        directory_inode,
        ancestor_deletion,
        Some("/docs/notes"),
        &loonfs_test_support::test_actor(),
    ))
    .expect("undelete the ancestor");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/notes/kept.txt")
            .expect("sibling is visible again")
            .bytes,
        b"kept"
    );
    let hidden = fs.stat_path_blocking(&namespace_id, "/docs/notes/secret.txt");
    assert!(matches!(
        hidden,
        Err(Error::Core(error)) if error.code() == ErrorCode::PathNotFound
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
        fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
            .expect("create namespace");
        let namespace = fs
            .writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        fs.put_file_blocking(
            &namespace_id,
            "/docs/report.txt",
            b"persisted",
            &loonfs_test_support::test_actor(),
        )
        .expect("put file");
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let deletion = fs
            .delete_path_blocking(
                &namespace_id,
                "/docs/report.txt",
                &loonfs_test_support::test_actor(),
            )
            .expect("delete")
            .committed_seq;
        block_on(namespace.undelete(
            inode_id,
            deletion,
            Some("/docs/report.txt"),
            &loonfs_test_support::test_actor(),
        ))
        .expect("undelete before checkpoint");
        // The default threshold (32 WAL objects) would answer NotNeeded for
        // this short history; force the fold so reopen reads Set and
        // Revoke rows out of durable segments, not WAL replay.
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("checkpoint the revoke into durable segments");
        assert!(
            matches!(step.wal_fold, loonfs::WalFoldStepOutcome::Folded { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
        deletion
    };
    {
        let fs = open_runtime(object_store.clone(), "undelete-persist-b");
        assert_eq!(
            fs.read_file_blocking(&namespace_id, "/docs/report.txt")
                .expect("recovered file survives checkpoint and reopen")
                .bytes,
            b"persisted"
        );

        // Order two: delete, checkpoint, reopen, THEN undelete — the
        // revoke must resolve a deletion that lives in durable segments,
        // not the WAL tail.
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let second_deletion = fs
            .delete_path_blocking(
                &namespace_id,
                "/docs/report.txt",
                &loonfs_test_support::test_actor(),
            )
            .expect("delete again")
            .committed_seq;
        assert!(second_deletion > deletion);
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("checkpoint the deletion");
        assert!(
            matches!(step.wal_fold, loonfs::WalFoldStepOutcome::Folded { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
        let fs = open_runtime(object_store.clone(), "undelete-persist-c");
        let namespace = fs
            .writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        block_on(namespace.undelete(
            inode_id,
            second_deletion,
            Some("/docs/report.txt"),
            &loonfs_test_support::test_actor(),
        ))
        .expect("undelete a checkpointed deletion after reopen");
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("checkpoint the second revoke");
        assert!(
            matches!(step.wal_fold, loonfs::WalFoldStepOutcome::Folded { .. }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
    }
    let fs = open_runtime(object_store, "undelete-persist-d");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/report.txt")
            .expect("recovered file survives the second cycle")
            .bytes,
        b"persisted"
    );
}

#[test]
fn change_feed_reports_the_deletion_position_an_undelete_takes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-feed-test");
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"feed",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;
    let deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            &loonfs_test_support::test_actor(),
        )
        .expect("delete")
        .committed_seq;
    block_on(namespace_writer.undelete(
        inode_id,
        deletion,
        Some("/docs/report.txt"),
        &loonfs_test_support::test_actor(),
    ))
    .expect("undelete");

    let changes =
        block_on(namespace.list_changes(ChangeSeq(0)).page(first_page())).expect("list changes");
    let mut deleted_seq = None;
    let mut undeleted = None;
    for change in &changes.changes {
        for event in &change.events {
            match event {
                loonfs::FilesystemChange::Deleted {
                    inode_id: deleted_inode_id,
                    ..
                } if *deleted_inode_id == inode_id => {
                    deleted_seq = Some(change.committed_seq);
                }
                loonfs::FilesystemChange::Undeleted {
                    inode_id: undeleted_inode_id,
                    display_name,
                    ..
                } if *undeleted_inode_id == inode_id => {
                    undeleted = Some(display_name.as_str().to_owned());
                }
                _ => {}
            }
        }
    }
    // The change sequence can be copied directly into an undelete request.
    assert_eq!(deleted_seq, Some(deletion));
    assert_eq!(undeleted.as_deref(), Some("report.txt"));
}

#[test]
fn the_feed_names_deleted_entries_and_their_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "feed-identity-test");
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/Quarterly Report.PDF",
        b"body",
        &loonfs_test_support::test_actor(),
    )
    .expect("put");
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/Quarterly Report.PDF",
        &loonfs_test_support::test_actor(),
    )
    .expect("delete");

    let changes =
        block_on(namespace.list_changes(ChangeSeq(0)).page(first_page())).expect("list changes");

    // A projection of the feed sees the spelling a person typed — on the
    // deletion as well as the creation — without a second lookup per entry.
    let deleted_name = changes
        .changes
        .iter()
        .flat_map(|change| &change.events)
        .find_map(|event| match event {
            loonfs::FilesystemChange::Deleted {
                deleted_binding, ..
            } => Some(deleted_binding.display_name.as_str().to_owned()),
            _ => None,
        });
    assert_eq!(deleted_name.as_deref(), Some("Quarterly Report.PDF"));
}

#[test]
fn undelete_rejects_deletions_from_the_same_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-same-commit-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"cycled",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");
    let entry = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat");

    // Assigned sequences are head + 1 and therefore guessable: without the
    // earlier-commit bound, one commit could delete, undelete, and
    // re-delete the inode, minting two deletion positions that share a
    // sequence. The undelete must refuse a target in its own commit.
    let guessed_seq = ChangeSeq(entry.head_seq.0 + 1);
    let error = fs
        .mutate_blocking(
            &namespace_id,
            CommitRequest {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("same-commit-cycle").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: vec![
                    FilesystemOperation::DeletePath {
                        path: parse_mutation_path("/docs/report.txt").expect("valid mutation path"),
                        behavior: DeleteDirectoryBehavior::Recursive,
                        expected_inode_id: Some(entry.inode_id),
                    },
                    FilesystemOperation::Undelete {
                        inode_id: entry.inode_id,
                        deletion_seq: guessed_seq,
                        destination_path: Some(
                            parse_mutation_path("/resurrected.txt").expect("valid mutation path"),
                        ),
                        destination_parent_inode_id: None,
                        destination_display_name: None,
                    },
                ],
            },
        )
        .expect_err("same-commit delete/undelete cycling must be rejected");
    assert!(matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::NotDeleted
    ));
    // The rejected commit changed nothing.
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/report.txt")
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
    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"original",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;

    // Stand-in for a rebinding that raced the caller's stat: the path now
    // holds a different inode than the one the caller resolved.
    let error = fs
        .delete_path_with_options_blocking(
            &namespace_id,
            "/docs/report.txt",
            &loonfs_test_support::test_actor(),
            &DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
                commit: loonfs_types::options::CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: Some(InodeId(inode_id.0 + 1)),
            },
        )
        .expect_err("a mismatched expectation must fail the delete");
    assert!(matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::PathConflict
    ));
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/report.txt")
            .expect("file untouched")
            .bytes,
        b"original"
    );

    // The matching expectation deletes exactly that inode.
    fs.delete_path_with_options_blocking(
        &namespace_id,
        "/docs/report.txt",
        &loonfs_test_support::test_actor(),
        &DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: None,
                message: None,
            },
            expected_inode_id: Some(inode_id),
        },
    )
    .expect("matching expectation deletes");
}
