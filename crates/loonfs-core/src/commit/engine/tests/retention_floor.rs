//! Validation of restores of deleted items against the retention floor of a writer session's
//! position, around a floor object that the session creates itself.

use super::*;
use loonfs_types::FilesystemEvent;
use loonfs_types::{AbsolutePath, DeleteDirectoryBehavior, InodeId};

fn operation(commit_id: &str, operation: FilesystemOperation) -> CommitCandidate {
    CommitCandidate::new(CommitIntent::single(
        CommitId::parse(commit_id).expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        operation,
    ))
}

fn path(absolute_path: &str) -> AbsolutePath {
    AbsolutePath::parse(absolute_path).expect("path")
}

fn restore_deleted(commit_id: &str, inode_id: InodeId, deletion_seq: CommitSeq) -> CommitCandidate {
    operation(
        commit_id,
        FilesystemOperation::RestoreDeleted {
            inode_id,
            deletion_seq,
            destination: loonfs_types::options::RestoreDeletedDestination::Recorded,
        },
    )
}

async fn publish<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine<S>,
    candidate: CommitCandidate,
) -> Result<Commit> {
    engine
        .publish_batch([candidate], &context("writer").clock())
        .await
        .results
        .pop()
        .expect("one result")
}

/// Creates and deletes `/name`, and returns the deleted root and the
/// deletion's sequence.
async fn delete<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine<S>,
    name: &str,
) -> (InodeId, CommitSeq) {
    publish(
        engine,
        operation(
            &format!("create-{name}"),
            FilesystemOperation::CreateDirectory {
                path: path(&format!("/{name}")),
                parents: false,
            },
        ),
    )
    .await
    .expect("create directory");
    let deleted = publish(
        engine,
        operation(
            &format!("delete-{name}"),
            FilesystemOperation::DeletePath {
                path: path(&format!("/{name}")),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
        ),
    )
    .await
    .expect("delete directory");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemEvent::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("a delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// Checks that a session's own floor object orders the session's restores
/// around the new floor. The floor object follows the session's tip, the
/// position's floor rises at once, so a restore of a deletion at the new
/// floor returns `not_deleted` before any manifest records the floor, and
/// one above it succeeds. The fold that covers the floor object records the
/// floor, and it contains the restore made before the advance. The session
/// claims no epoch for the advance.
#[tokio::test]
async fn a_sessions_floor_object_orders_its_restores_around_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("restore-floor").expect("valid namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context("writer"))
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context("writer").writer_id,
    );
    let (restored, restored_seq) = delete(&mut engine, "restored").await;
    let (expired, expired_seq) = delete(&mut engine, "expired").await;
    let restored_commit = publish(
        &mut engine,
        restore_deleted("restore-before", restored, restored_seq),
    )
    .await
    .expect("a restore before the advance");
    let before = engine.writer_position().expect("a position");
    let planned = engine
        .namespace
        .load_current_manifest()
        .await
        .expect("the planned manifest");

    let report = engine
        .publish_floor_object(&planned, expired_seq, &context("writer").clock())
        .await
        .expect("create the floor object");
    let after = engine.writer_position().expect("the position moves");
    assert_eq!(report.retention_floor_seq, expired_seq);
    assert_eq!(report.manifest_retention_floor_seq, CommitSeq(0));
    assert_eq!(
        after.read_basis.head.wal_no.0,
        before.read_basis.head.wal_no.0 + 1
    );
    assert_eq!(after.read_basis.head.seq, before.read_basis.head.seq);
    assert_eq!(after.retention_floor_seq(), expired_seq);
    assert_eq!(
        after.read_basis.head.writer_epoch, before.read_basis.head.writer_epoch,
        "the advance claims no epoch"
    );

    let error = publish(
        &mut engine,
        restore_deleted("restore-expired", expired, expired_seq),
    )
    .await
    .expect_err("a restore at the new floor");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    let folded = engine
        .namespace
        .fold_wal_tail(engine.writer_position(), &context("writer").clock())
        .await
        .expect("fold")
        .manifest;
    assert_eq!(folded.retention_floor_seq, expired_seq);
    assert!(folded.manifest.head_seq >= restored_commit.committed_seq);
    engine.record_manifest_published(folded);
    let (above, above_seq) = delete(&mut engine, "above").await;
    publish(
        &mut engine,
        restore_deleted("restore-above", above, above_seq),
    )
    .await
    .expect("a restore above the new floor");
}
