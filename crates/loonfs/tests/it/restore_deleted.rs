//! Restore positions, delete preconditions, and restored visibility.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitIntent, FilesystemOperation};
use loonfs::test_support::fresh_reader;
use loonfs::{
    AdvanceRetentionFloorOptions, CommitId, CommitSeq, DeleteDirectoryBehavior, DeleteOptions,
    DestinationBehavior, Error, ErrorCode, InodeId, PutFileOptions, RetentionTarget, RevisionNo,
};
use loonfs_test_support::ids::namespace_id;
use std::assert_matches;
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
    assert_matches!(
        error,
        Error::Core(error) if error.code() == loonfs::ErrorCode::DirectoryNotEmpty
    );

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
    assert_matches!(
        error,
        Error::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    );
}

#[test]
fn a_restore_recovers_a_deleted_file_and_positions_stay_scoped() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "restore-test");
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
    block_on(namespace_writer.restore_deleted_with_options(
        inode_id,
        first_deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/recovered.txt"),
    ))
    .expect("restore_deleted");
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
    let error = block_on(namespace_writer.restore_deleted_with_options(
        inode_id,
        first_deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/again.txt"),
    ))
    .expect_err("double restore should conflict");
    assert_matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::NotDeleted
    );

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
    let error = block_on(namespace_writer.restore_deleted_with_options(
        inode_id,
        first_deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/stale.txt"),
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
    assert_eq!(
        still_gone
            .expect_err("stale restore must not bind anything")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );

    // The current position's handle recovers to the original path.
    block_on(namespace_writer.restore_deleted_with_options(
        inode_id,
        second_deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/report.txt"),
    ))
    .expect("restore the active position");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat restored original path")
            .inode_id,
        inode_id
    );
}

#[test]
fn a_restore_recovers_a_deleted_subtree_and_rejects_covered_children() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "restore-subtree-test");
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
    let error = block_on(namespace.restore_deleted_with_options(
        child_inode,
        deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/a-alone.txt"),
    ))
    .expect_err("child of a deleted directory is not the deletion root");
    assert_matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::NotDeleted
    );

    block_on(namespace.restore_deleted_with_options(
        directory_inode,
        deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/notes"),
    ))
    .expect("restore the subtree root");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/notes/a.txt")
            .expect("nested file is visible again")
            .bytes,
        b"alpha"
    );
}

#[test]
fn a_restore_of_an_ancestor_keeps_independently_deleted_children_hidden() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "restore-nested-test");
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
    block_on(namespace.restore_deleted_with_options(
        directory_inode,
        ancestor_deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/notes"),
    ))
    .expect("restore the ancestor");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/notes/kept.txt")
            .expect("sibling is visible again")
            .bytes,
        b"kept"
    );
    let hidden = fs.stat_path_blocking(&namespace_id, "/docs/notes/secret.txt");
    assert_matches!(
        hidden,
        Err(Error::Core(error)) if error.code() == ErrorCode::PathNotFound
    );
}

#[test]
fn a_restore_survives_snapshots_and_reopen_in_both_orders() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());
    let namespace_id = namespace_id("demo");

    // Order one: delete + restore in the WAL tail, then snapshot,
    // then reopen cold from object storage.
    let deletion = {
        let fs = open_runtime(object_store.clone(), "restore-persist-a");
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
        block_on(namespace.restore_deleted_with_options(
            inode_id,
            deletion,
            &loonfs_test_support::test_actor(),
            &restore_deleted_at("/docs/report.txt"),
        ))
        .expect("restore before snapshot");
        // The default threshold (32 WAL objects) would answer NotNeeded for
        // this short history; force the fold so reopen reads Set and
        // Revoke rows out of durable segments, not WAL replay.
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("snapshot the revoke into durable segments");
        assert_matches!(
            step.wal_fold,
            Some(loonfs::FoldWalResponse {
                outcome: loonfs::FoldWalOutcome::Published,
                ..
            }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
        deletion
    };
    {
        let fs = open_runtime(object_store.clone(), "restore-persist-b");
        assert_eq!(
            fs.read_file_blocking(&namespace_id, "/docs/report.txt")
                .expect("recovered file survives snapshot and reopen")
                .bytes,
            b"persisted"
        );

        // Order two: delete, snapshot, reopen, THEN restore — the
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
            .expect("snapshot the deletion");
        assert_matches!(
            step.wal_fold,
            Some(loonfs::FoldWalResponse {
                outcome: loonfs::FoldWalOutcome::Published,
                ..
            }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
        let fs = open_runtime(object_store.clone(), "restore-persist-c");
        let namespace = fs
            .writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        block_on(namespace.restore_deleted_with_options(
            inode_id,
            second_deletion,
            &loonfs_test_support::test_actor(),
            &restore_deleted_at("/docs/report.txt"),
        ))
        .expect("restore a deletion the snapshot captured after reopen");
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("snapshot the second revoke");
        assert_matches!(
            step.wal_fold,
            Some(loonfs::FoldWalResponse {
                outcome: loonfs::FoldWalOutcome::Published,
                ..
            }),
            "step must materialize the tail, got {:?}",
            step.wal_fold
        );
    }
    let fs = open_runtime(object_store, "restore-persist-d");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/report.txt")
            .expect("recovered file survives the second cycle")
            .bytes,
        b"persisted"
    );
}

#[test]
fn change_feed_reports_the_deletion_position_a_restore_takes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "restore-feed-test");
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
    block_on(namespace_writer.restore_deleted_with_options(
        inode_id,
        deletion,
        &loonfs_test_support::test_actor(),
        &restore_deleted_at("/docs/report.txt"),
    ))
    .expect("restore_deleted");

    let changes = block_on(namespace.list_changes(CommitSeq(0)).page()).expect("list changes");
    let mut deleted_seq = None;
    let mut restored = None;
    for change in &changes.changes {
        for event in &change.events {
            match event {
                loonfs::FilesystemEvent::Deleted {
                    inode_id: deleted_inode_id,
                    ..
                } if *deleted_inode_id == inode_id => {
                    deleted_seq = Some(change.committed_seq);
                }
                loonfs::FilesystemEvent::Restored {
                    inode_id: restored_inode_id,
                    display_name,
                    ..
                } if *restored_inode_id == inode_id => {
                    restored = Some(display_name.as_str().to_owned());
                }
                _ => {}
            }
        }
    }
    // The change sequence can be copied directly into a restore request.
    assert_eq!(deleted_seq, Some(deletion));
    assert_eq!(restored.as_deref(), Some("report.txt"));
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

    let changes = block_on(namespace.list_changes(CommitSeq(0)).page()).expect("list changes");

    // A projection of the feed sees the spelling a person typed — on the
    // deletion as well as the creation — without a second lookup per entry.
    let deleted_name = changes
        .changes
        .iter()
        .flat_map(|change| &change.events)
        .find_map(|event| match event {
            loonfs::FilesystemEvent::Deleted {
                deleted_binding, ..
            } => Some(deleted_binding.display_name.as_str().to_owned()),
            _ => None,
        });
    assert_eq!(deleted_name.as_deref(), Some("Quarterly Report.PDF"));
}

#[test]
fn a_restore_that_misses_the_trash_entry_reports_both_sequences() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "restore-details-test");
    let namespace_id = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    fs.create_namespace_blocking(&namespace_id, &actor)
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(&namespace_id, "/docs/report.txt", b"draft", &actor)
        .expect("put file");
    let report = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat report")
        .inode_id;
    let deletion = fs
        .delete_path_blocking(&namespace_id, "/docs/report.txt", &actor)
        .expect("delete report")
        .committed_seq;

    let later = CommitSeq(100);
    let error = block_on(namespace_writer.restore_deleted_with_options(
        report,
        later,
        &actor,
        &restore_deleted_at("/docs/again.txt"),
    ))
    .expect_err("no deletion of the report is at a later sequence");
    let Error::Core(error) = &error else {
        panic!("expected a core error, got {error:?}")
    };
    assert_eq!(error.code(), ErrorCode::NotDeleted);
    let details = error.details().expect("mismatch details");
    assert_eq!(
        (
            details.inode_id,
            details.expected_deletion_seq,
            details.actual_deletion_seq
        ),
        (Some(report), Some(later), Some(deletion))
    );

    fs.put_file_blocking(&namespace_id, "/docs/fresh.txt", b"fresh", &actor)
        .expect("put fresh file");
    let fresh = fs
        .stat_path_blocking(&namespace_id, "/docs/fresh.txt")
        .expect("stat fresh file");
    let error = fs
        .mutate_blocking(
            &namespace_id,
            CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("same-commit-details").expect("valid commit id"),
                actor_id: actor.clone(),
                subject: None,
                message: None,
                operations: vec![
                    FilesystemOperation::DeletePath {
                        path: parse_mutation_path("/docs/fresh.txt").expect("valid mutation path"),
                        behavior: DeleteDirectoryBehavior::Recursive,
                        expected_inode_id: Some(fresh.inode_id),
                    },
                    FilesystemOperation::RestoreDeleted {
                        inode_id: fresh.inode_id,
                        deletion_seq: CommitSeq(fresh.head_seq.0 + 1),
                        destination: loonfs::RestoreDeletedDestination::Path(
                            parse_mutation_path("/docs/back.txt").expect("valid mutation path"),
                        ),
                    },
                ],
            },
        )
        .expect_err("a restore can't target a deletion from its own commit");
    let Error::Core(error) = &error else {
        panic!("expected a core error, got {error:?}")
    };
    assert_eq!(error.code(), ErrorCode::NotDeleted);
    let details = error.details().expect("same-commit details");
    assert_eq!(details.inode_id, Some(fresh.inode_id));
    assert_eq!(details.expected_deletion_seq, None);
    assert_eq!(details.actual_deletion_seq, None);
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/fresh.txt")
            .expect("file untouched")
            .bytes,
        b"fresh"
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
    assert_matches!(
        &error,
        Error::Core(error) if error.code() == ErrorCode::PathConflict
    );
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

#[tokio::test]
async fn a_deleted_file_keeps_its_newest_revision_at_the_floor_and_a_restore_brings_it_back() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_compacting_runtime_with_async(
        store(temp_dir.path()),
        "restore-floor-test",
        |builder| builder,
    )
    .await;
    let namespace_id = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    fs.create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..Default::default()
    };
    // Each revision is written to its own run, so the compaction below is a base
    // compaction that applies the floor; a lone base run is never rewritten.
    let mut newest_seq = None;
    for body in ["draft one", "draft two"] {
        newest_seq = Some(
            fs.put_file_with_options(
                &namespace_id,
                "/docs/report.txt",
                body.as_bytes(),
                &actor,
                &replace,
            )
            .await
            .expect("put revision")
            .committed_seq,
        );
        fs.create_snapshot(&namespace_id)
            .await
            .expect("fold the revision");
    }
    let newest_seq = newest_seq.expect("a revision");
    let inode_id = fs
        .stat(&namespace_id, "/docs/report.txt")
        .await
        .expect("stat before delete")
        .inode_id;
    let namespace_writer = fs.namespace_writer(&namespace_id).expect("open namespace");
    let deletion = namespace_writer
        .delete_path("/docs/report.txt", &actor)
        .await
        .expect("delete file");
    fs.create_snapshot(&namespace_id)
        .await
        .expect("fold the deletion");
    // The floor stops just below the deletion, at the file's newest revision,
    // so the deletion stays in the trash. The advance takes the writer epoch,
    // so the restore opens a new writer session.
    drop(namespace_writer);
    let advanced = fs
        .advance_retention_floor_with_options(
            &namespace_id,
            &AdvanceRetentionFloorOptions {
                target: RetentionTarget::Seq(newest_seq),
            },
        )
        .await
        .expect("advance the floor to the newest revision");
    assert_eq!(advanced.retention_floor_seq, newest_seq);
    fs.namespace_writer(&namespace_id)
        .expect("open a new session")
        .restore_deleted(inode_id, deletion.committed_seq, &actor)
        .await
        .expect("restore the file");
    fs.create_snapshot(&namespace_id)
        .await
        .expect("fold the restore");
    // The base compaction applies the floor: the file keeps its newest
    // revision at the floor and loses the older one.
    compact_until_done(&fs.maintenance, &namespace_id).await;
    let reader = fresh_reader(store(temp_dir.path())).await;
    let namespace = reader.namespace(&namespace_id);
    assert_eq!(
        namespace
            .read_file("/docs/report.txt")
            .await
            .expect("read the restored file")
            .bytes,
        b"draft two"
    );
    let revisions = namespace
        .list_file_revisions("/docs/report.txt")
        .page()
        .await
        .expect("list revisions");
    assert_eq!(
        revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        [RevisionNo(2)]
    );
    expect_code(
        namespace
            .read_file_revision("/docs/report.txt", RevisionNo(1))
            .await,
        ErrorCode::RevisionNotFound,
    );
}
