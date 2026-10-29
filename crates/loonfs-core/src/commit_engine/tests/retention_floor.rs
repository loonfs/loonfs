//! Undelete validation against the retention floor of a writer session's
//! basis, around a retention advance that the session hasn't moved onto yet.

use super::*;
use crate::manifest::RetentionTarget;
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
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
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
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

/// The basis floor of the WAL object that commits `committed_seq`.
async fn basis_floor_of(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> ChangeSeq {
    for key in store
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list the WAL")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("read a WAL object")
            .expect("the WAL object exists");
        let payload = decode_wal_object_envelope_zstd(&bytes)
            .expect("decode a WAL object")
            .into_payload();
        if payload
            .records
            .iter()
            .any(|record| record.committed_seq == committed_seq)
        {
            return payload.basis_floor_seq;
        }
    }
    panic!("no WAL object commits sequence {committed_seq}")
}

/// Checks that a session validates an undelete against the floor of its own
/// basis: before it moves onto a retention advance's manifest, it restores a
/// deletion that the advance passed, and its WAL object records the old
/// floor. After the move, an undelete of a deletion at or below the new
/// floor returns `not_deleted`, and one above it succeeds.
#[tokio::test]
async fn an_undelete_is_validated_against_the_floor_of_the_sessions_basis() {
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
    let folded = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(
            engine.writer_position(),
            &OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .expect("fold");
    engine.record_manifest_published(folded.manifest);
    let advanced = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(1_000),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor");
    assert_eq!(advanced.response.retention_floor_seq, expired_seq);

    // The session still plans against the fold's manifest, whose floor is 0.
    let restored_commit = publish(
        &mut engine,
        undelete("undelete-before", restored, restored_seq),
    )
    .await
    .expect("an undelete against the old floor");
    assert_eq!(
        basis_floor_of(&store, &namespace_id, restored_commit.committed_seq).await,
        ChangeSeq(0)
    );

    engine.record_manifest_published(advanced.published.expect("a published advance"));
    let error = publish(
        &mut engine,
        undelete("undelete-expired", expired, expired_seq),
    )
    .await
    .expect_err("an undelete at the new floor");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
    let (above, above_seq) = delete(&mut engine, "above").await;
    let above_commit = publish(&mut engine, undelete("undelete-above", above, above_seq))
        .await
        .expect("an undelete above the new floor");
    assert_eq!(
        basis_floor_of(&store, &namespace_id, above_commit.committed_seq).await,
        expired_seq
    );
}
