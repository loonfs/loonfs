//! The writer session's expiry of the deletions that a retention advance
//! reaches, and of a fork's inherited deletions.

use crate::common::*;
use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{ChangeSeq, CommitId, DeleteDirectoryBehavior, ErrorCode, InodeId};
use loonfs_core::limits::MAX_EXPIRED_DELETIONS_PER_COMMIT;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::api::v0::{Commit, FilesystemChange};
use loonfs_types::{AbsolutePath, ActorId};
use std::collections::BTreeSet;
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

/// The deletions that each expiry commit after `after_seq` expired, one list
/// for each commit, checking that the writer made each of them as `loonfs`.
async fn expiry_commits(
    namespace: &loonfs::Namespace<loonfs::ReadOnly>,
    after_seq: ChangeSeq,
) -> Vec<Vec<(InodeId, ChangeSeq)>> {
    namespace
        .list_changes(after_seq)
        .collect_up_to(64)
        .await
        .expect("list changes")
        .into_iter()
        .filter_map(|commit| {
            let expired = commit
                .events
                .iter()
                .filter_map(|event| match event {
                    FilesystemChange::DeletionExpired {
                        inode_id,
                        deletion_seq,
                    } => Some((*inode_id, *deletion_seq)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if expired.is_empty() {
                return None;
            }
            assert_eq!(commit.committed_by, ActorId::loonfs());
            assert_eq!(expired.len(), commit.events.len(), "{commit:?}");
            Some(expired)
        })
        .collect()
}

#[tokio::test]
async fn a_retention_advance_expires_every_deletion_at_or_below_the_floor_in_bounded_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = writer(store(temp_dir.path()), "expiry-writer").await;
    let maintenance = writer.maintenance(writer_id("expiry-writer"));
    let namespace_id = namespace_id("expiry");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    // One commit deletes one more directory than an expiry commit can
    // expire, so the expiry takes two commits.
    let below_floor = MAX_EXPIRED_DELETIONS_PER_COMMIT + 1;
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
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, deleted.committed_seq);
    writer.drain().await.expect("drain the expiry");

    let reader = writer.read_only().namespace(&namespace_id);
    let expired = expiry_commits(&reader, above_floor.committed_seq).await;
    assert_eq!(
        expired.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![MAX_EXPIRED_DELETIONS_PER_COMMIT, 1],
        "the advance's position move expires the floor's deletions one bounded commit at a time"
    );
    assert_eq!(
        expired.into_iter().flatten().collect::<BTreeSet<_>>(),
        deleted_roots(&deleted)
            .into_iter()
            .map(|inode_id| (inode_id, deleted.committed_seq))
            .collect::<BTreeSet<_>>(),
        "every deletion at or below the floor expires once"
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
        "the deletion above the floor stays in the trash"
    );
}

#[tokio::test]
async fn a_forks_first_writer_session_expires_every_inherited_deletion_before_its_first_request() {
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

    let fork_writer = writer.open_namespace(&fork_id).expect("open the fork");
    let restored = deleted_roots(&deleted)[0];
    let error = fork_writer
        .undelete(restored, deleted.committed_seq, &actor)
        .await
        .expect_err("the fork's first request");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");

    let fork_reader = writer.read_only().namespace(&fork_id);
    assert_eq!(
        expiry_commits(&fork_reader, fork.retention_floor_seq).await,
        vec![deleted_roots(&deleted)
            .into_iter()
            .map(|inode_id| (inode_id, deleted.committed_seq))
            .collect::<Vec<_>>()],
        "one commit expires both inherited deletions before the undelete"
    );
    let fork_trash = fork_reader
        .list_trash()
        .collect_up_to(4)
        .await
        .expect("list the fork's trash");
    assert!(fork_trash.is_empty(), "{fork_trash:?}");
    let source_trash = writer
        .read_only()
        .namespace(&source_id)
        .list_trash()
        .collect_up_to(4)
        .await
        .expect("list the source's trash");
    assert_eq!(source_trash.len(), 2, "the source keeps its own trash");
}
