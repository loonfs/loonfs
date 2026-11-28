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
        .publish_batch_alone([candidate], &context("writer").clock())
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
        .publish_floor_object_alone(&planned, expired_seq, &context("writer").clock())
        .await
        .expect("create the floor object");
    let after = engine.writer_position().expect("the position moves");
    assert_eq!(report.retention_floor_seq, expired_seq);
    assert_eq!(report.manifest_retention_floor_seq, CommitSeq(0));
    assert_eq!(
        after.read_basis().head.wal_no.0,
        before.read_basis().head.wal_no.0 + 1
    );
    assert_eq!(after.read_basis().head.seq, before.read_basis().head.seq);
    assert_eq!(after.retention_floor_seq(), expired_seq);
    assert_eq!(
        after.read_basis().head.writer_epoch,
        before.read_basis().head.writer_epoch,
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
        .fold_wal_tail_in_new_lane(
            &engine.writer_position().expect("a writer position"),
            &context("writer").clock(),
        )
        .await
        .expect("fold")
        .current;
    assert_eq!(folded.manifest().payload().retention_floor_seq, expired_seq);
    assert!(folded.manifest().manifest().head_seq >= restored_commit.committed_seq);
    engine.move_position_onto(folded);
    let (above, above_seq) = delete(&mut engine, "above").await;
    publish(
        &mut engine,
        restore_deleted("restore-above", above, above_seq),
    )
    .await
    .expect("a restore above the new floor");
}

/// Checks that a session's own floor object has no check after its put: the
/// put is the call's last request, and the call sends no manifest HEAD.
#[tokio::test]
async fn a_sessions_floor_object_has_no_check_after_its_put() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("session-floor").expect("valid namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context("writer"))
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context("writer").writer_id,
    );
    let (_, deletion_seq) = delete(&mut engine, "expired").await;
    let planned = engine
        .namespace
        .load_current_manifest()
        .await
        .expect("the planned manifest");
    store.reset();
    engine
        .publish_floor_object_alone(&planned, deletion_seq, &context("writer").clock())
        .await
        .expect("create the floor object");
    let operations = store.take();
    let manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id);
    assert!(
        matches!(operations.last(), Some(put) if is_wal_put(put, &namespace_id))
            && !operations.iter().any(|operation| {
                matches!(operation, RecordedOperation::Head { key } if key.starts_with(&manifests))
            }),
        "{operations:?}"
    );
}

/// Checks that an advance puts its floor object at the session's tip, that
/// a session without its writer epoch claims it first, and that a session
/// that another claim displaced writes no floor object: its put collides
/// with the claim's fence, the writer's load that follows finds the claim,
/// and the advance returns `writer_fenced`.
#[tokio::test]
async fn an_advance_puts_its_floor_object_at_the_sessions_tip_or_returns_writer_fenced() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("fenced-floor").expect("valid namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context("writer"))
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context("writer").writer_id,
    );
    let (_, first_seq) = delete(&mut engine, "first").await;
    let (_, second_seq) = delete(&mut engine, "second").await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let planned = namespace.load_current_manifest().await.expect("planned");

    // A new session of the same writer has no epoch, so it claims first.
    let mut claiming = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context("writer").writer_id,
    );
    let epoch_before = namespace
        .load_read_state()
        .await
        .expect("head")
        .writer_epoch;
    let report = claiming
        .publish_floor_object_alone(&planned, first_seq, &context("writer").clock())
        .await
        .expect("the claiming session's floor object");
    assert_eq!(report.retention_floor_seq, first_seq);
    let head = namespace.load_read_state().await.expect("head");
    assert_eq!(
        head.writer_epoch.0,
        epoch_before.0 + 1,
        "the session claims"
    );
    assert_eq!(
        head.wal_no,
        claiming
            .writer_position()
            .expect("a position")
            .read_basis()
            .head
            .wal_no,
        "the floor object is at the session's tip"
    );
    assert_eq!(
        head.retention_floor_seq, first_seq,
        "the head's floor is the target"
    );
    #[expect(
        clippy::disallowed_methods,
        reason = "the test reads the WAL object at the tip to check its body"
    )]
    let tip = {
        store
            .get(
                &loonfs_objectstore::keys::wal_object(&namespace_id, &head.wal_no),
                None,
            )
            .await
            .expect("get the object at the tip")
            .expect("the object at the tip exists")
    };
    assert_eq!(
        loonfs_types::format::wal::decode_wal_object_envelope_zstd(&tip)
            .expect("decode the object at the tip")
            .payload()
            .body,
        loonfs_types::format::wal::WalObjectBody::Floor {
            retention_floor_seq: first_seq
        },
        "the object at the tip is the floor object"
    );

    // The first session was displaced by that claim. Its put at its tip
    // collides with the claim's fence, and the writer's load after the
    // collision finds the higher epoch.
    let fenced = engine
        .publish_floor_object_alone(&planned, second_seq, &context("writer").clock())
        .await
        .expect_err("a displaced session's advance");
    assert_matches!(fenced, CoreError::WriterFenced(_));
    assert_eq!(
        namespace.load_read_state().await.expect("head").wal_no,
        head.wal_no,
        "the displaced session writes no floor object"
    );
}
