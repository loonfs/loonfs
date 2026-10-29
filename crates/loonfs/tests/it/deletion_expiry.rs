//! Deletions that a retention advance reaches, and a fork's inherited
//! deletions, expire without a commit.

use crate::common::*;
use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{ChangeSeq, CommitId, DeleteDirectoryBehavior, ErrorCode, InodeId, LoonFs};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::api::v0::{Commit, FilesystemChange};
use loonfs_types::AbsolutePath;
use std::sync::Arc;
use tempfile::tempdir;

fn intent(commit_id: &str, operations: Vec<FilesystemOperation>) -> CommitIntent {
    CommitIntent {
        commit_id: CommitId::parse(commit_id).expect("commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations,
    }
}

fn directory_path(index: usize) -> AbsolutePath {
    AbsolutePath::parse(format!("/directory-{index}")).expect("path")
}

fn create_directories(indexes: std::ops::Range<usize>) -> Vec<FilesystemOperation> {
    indexes
        .map(|index| FilesystemOperation::CreateDirectory {
            path: directory_path(index),
            parents: false,
        })
        .collect()
}

fn delete_directories(indexes: std::ops::Range<usize>) -> Vec<FilesystemOperation> {
    indexes
        .map(|index| FilesystemOperation::DeletePath {
            path: directory_path(index),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        })
        .collect()
}

/// The deletion roots that a delete commit reports, in its order.
fn deleted_roots(commit: &Commit) -> Vec<InodeId> {
    commit
        .events
        .iter()
        .filter_map(|event| match event {
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .collect()
}

/// The sequences of the commits after `after_seq`.
async fn commits_after(
    namespace: &loonfs::Namespace<loonfs::ReadOnly>,
    after_seq: ChangeSeq,
) -> Vec<ChangeSeq> {
    namespace
        .list_changes(after_seq)
        .collect_up_to(64)
        .await
        .expect("list changes")
        .into_iter()
        .map(|commit| commit.committed_seq)
        .collect()
}

/// Checks that a retention advance expires every deletion at or below the
/// new floor without a commit: the trash leaves them out, an undelete of
/// one returns `not_deleted`, and the deletion above the floor stays
/// recoverable.
#[tokio::test]
async fn a_retention_advance_expires_every_deletion_at_or_below_the_floor_without_a_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = LoonFs::builder_with_store(store(temp_dir.path()))
        .writer_id(writer_id("expiry-writer"))
        .min_publish_interval_ms(0)
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build writer");
    let maintenance = writer.maintenance(writer_id("expiry-writer"));
    let namespace_id = namespace_id("expiry");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    let below_floor = 3;
    namespace
        .commit(intent("create", create_directories(0..below_floor + 1)))
        .await
        .expect("create directories");
    let deleted = namespace
        .commit(intent("delete", delete_directories(0..below_floor)))
        .await
        .expect("delete directories");
    maintenance.fold_wal(&namespace_id).await.expect("fold");
    let above_floor = namespace
        .commit(intent(
            "delete-above-floor",
            delete_directories(below_floor..below_floor + 1),
        ))
        .await
        .expect("delete above the floor");
    // Every folded commit is now one grace window old.
    clock.advance_ms(loonfs::GC_DEFAULT_GRACE_WINDOW_MS);
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, deleted.committed_seq);
    writer.drain().await.expect("drain");

    let reader = writer.read_only().namespace(&namespace_id);
    assert_eq!(
        commits_after(&reader, above_floor.committed_seq).await,
        Vec::new(),
        "the advance commits nothing for the deletions it expires"
    );
    let trash = namespace
        .list_trash()
        .collect_up_to(below_floor + 1)
        .await
        .expect("list trash");
    assert_eq!(
        trash
            .iter()
            .map(|entry| (entry.inode_id, entry.deletion_seq))
            .collect::<Vec<_>>(),
        deleted_roots(&above_floor)
            .into_iter()
            .map(|inode_id| (inode_id, above_floor.committed_seq))
            .collect::<Vec<_>>(),
        "only the deletion above the floor stays in the trash"
    );
    let error = namespace
        .undelete(deleted_roots(&deleted)[0], deleted.committed_seq, &actor)
        .await
        .expect_err("an undelete at the floor");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    namespace
        .undelete(
            deleted_roots(&above_floor)[0],
            above_floor.committed_seq,
            &actor,
        )
        .await
        .expect("an undelete above the floor");
}

/// Checks that every deletion a fork inherits has expired in the fork from
/// its creation: the fork's trash is empty, its first request, an undelete
/// of an inherited deletion, returns `not_deleted`, and its first write
/// commits right after the fork point with nothing before it. The source
/// keeps its own trash.
#[tokio::test]
async fn a_forks_inherited_deletions_have_expired_and_its_first_write_commits_nothing_else() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = writer(store(temp_dir.path()), "fork-expiry-writer").await;
    let actor = loonfs_test_support::test_actor();
    let source_id = namespace_id("source");
    let fork_id = namespace_id("fork");
    writer
        .create_namespace(&source_id, &actor)
        .await
        .expect("create the source");
    let source = writer.open_namespace(&source_id).expect("open the source");
    source
        .commit(intent("create", create_directories(0..2)))
        .await
        .expect("create directories");
    let deleted = source
        .commit(intent("delete", delete_directories(0..2)))
        .await
        .expect("delete directories");
    let fork = writer
        .fork_namespace(&source_id, &fork_id, &actor)
        .await
        .expect("fork");
    assert_eq!(fork.retention_floor_seq, deleted.committed_seq);

    let fork_reader = writer.read_only().namespace(&fork_id);
    let fork_trash = fork_reader
        .list_trash()
        .collect_up_to(4)
        .await
        .expect("list the fork's trash");
    assert!(fork_trash.is_empty(), "{fork_trash:?}");
    let fork_writer = writer.open_namespace(&fork_id).expect("open the fork");
    let error = fork_writer
        .undelete(deleted_roots(&deleted)[0], deleted.committed_seq, &actor)
        .await
        .expect_err("the fork's first request");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    let created = fork_writer
        .commit(intent("fork-create", create_directories(2..3)))
        .await
        .expect("the fork's first write");
    assert_eq!(created.committed_seq.0, fork.retention_floor_seq.0 + 1);
    assert_eq!(
        commits_after(&fork_reader, fork.retention_floor_seq).await,
        vec![created.committed_seq],
        "the fork commits nothing before its first write"
    );
    let source_trash = writer
        .read_only()
        .namespace(&source_id)
        .list_trash()
        .collect_up_to(4)
        .await
        .expect("list the source's trash");
    assert_eq!(source_trash.len(), 2, "the source keeps its own trash");
}
