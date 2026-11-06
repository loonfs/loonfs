//! Undelete validation against the retention floor of a writer session's
//! basis, around a retention advance that the session makes itself.

use super::*;
use loonfs_types::FilesystemChange;
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

fn undelete(commit_id: &str, inode_id: InodeId, deletion_seq: ChangeSeq) -> CommitCandidate {
    operation(
        commit_id,
        FilesystemOperation::Undelete {
            inode_id,
            deletion_seq,
            destination: loonfs_types::options::UndeleteDestination::Recorded,
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
) -> (InodeId, ChangeSeq) {
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
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("a delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// Checks that a session's own retention advance orders the session's
/// undeletes around it. An undelete before the advance is folded into the
/// manifest before the floor moves, even when the advance expires the
/// deletion it restored. After the advance, an undelete of a deletion at or
/// below the new floor returns `not_deleted`, and one above it succeeds. The
/// session keeps its writer epoch throughout.
#[tokio::test]
async fn a_sessions_advance_orders_its_undeletes_around_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("undelete-floor").expect("valid namespace id");
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
        undelete("undelete-before", restored, restored_seq),
    )
    .await
    .expect("an undelete before the advance");
    let epoch = engine
        .acquire_position(&context("writer").clock())
        .await
        .expect("the session's epoch")
        .writer_epoch;

    let (folded, advanced) = advance_in_session(&mut engine, expired_seq).await;
    assert!(
        folded.folded_wal_no
            >= crate::test_support::namespace_store(&store, &namespace_id)
                .load_read_state()
                .await
                .expect("read the head")
                .wal_no,
        "the fold covers the session's tip"
    );
    assert!(advanced.manifest.head_seq >= restored_commit.committed_seq);
    assert_eq!(advanced.retention_floor_seq, expired_seq);
    assert_eq!(advanced.writer_epoch, epoch, "the session keeps its epoch");

    let error = publish(
        &mut engine,
        undelete("undelete-expired", expired, expired_seq),
    )
    .await
    .expect_err("an undelete at the new floor");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    let (above, above_seq) = delete(&mut engine, "above").await;
    publish(&mut engine, undelete("undelete-above", above, above_seq))
        .await
        .expect("an undelete above the new floor");
}
