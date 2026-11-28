//! Validation of restores of deleted items against the retention floor of a
//! writer session's position, around a retention advance that the session
//! makes itself.

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

/// Checks that a session's own advance orders the session's restores around
/// the new floor. The advance folds the tail and then publishes a manifest
/// with the new floor, which the position follows, so a restore of a
/// deletion at the new floor returns `not_deleted`, and one above it
/// succeeds. The manifest with the floor contains the restore made before
/// the advance, the advance writes no WAL object, and the session claims no
/// epoch for it.
#[tokio::test]
async fn a_sessions_advance_orders_its_restores_around_the_floor() {
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
        .advance_retention_floor_alone(&planned, expired_seq)
        .await
        .expect("advance the floor");
    let after = engine.writer_position().expect("the position moves");
    assert_eq!(report.response, expired_seq);
    let floor = report.published.expect("the manifest with the floor");
    assert_eq!(floor.manifest().payload().retention_floor_seq, expired_seq);
    assert!(floor.manifest().manifest().head_seq >= restored_commit.committed_seq);
    assert_eq!(
        after.read_basis().head.wal_no,
        before.read_basis().head.wal_no,
        "the advance writes no WAL object"
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
    let (above, above_seq) = delete(&mut engine, "above").await;
    publish(
        &mut engine,
        restore_deleted("restore-above", above, above_seq),
    )
    .await
    .expect("a restore above the new floor");
}

/// Checks that an advance publishes its floor in its own manifest, after the
/// fold of what arrived, and puts no WAL object: its ordinary fold publishes the tail with
/// the old floor, and the manifest after it, with nothing more to fold, has
/// the fold's head and the new floor.
#[tokio::test]
async fn an_advance_publishes_its_floor_in_a_manifest_and_puts_no_wal_object() {
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
    let report = engine
        .advance_retention_floor_alone(&planned, deletion_seq)
        .await
        .expect("advance the floor");
    let operations = store.take();
    assert!(
        !operations
            .iter()
            .any(|operation| is_wal_put(operation, &namespace_id)),
        "{operations:?}"
    );
    let floor = report.published.expect("the manifest with the floor");
    let floor = floor.manifest().payload();
    let fold = engine
        .namespace
        .load_manifest_if_present(loonfs_types::ManifestNo(floor.manifest_no.0 - 1))
        .await
        .expect("load the ordinary fold's manifest")
        .expect("the ordinary fold's manifest");
    let fold = fold.payload();
    assert_eq!(fold.manifest_no.0, planned.payload().manifest_no.0 + 1);
    assert_eq!(fold.retention_floor_seq, CommitSeq(0));
    assert_eq!(fold.head_seq, deletion_seq);
    assert_eq!(floor.retention_floor_seq, deletion_seq);
    assert_eq!(floor.head_seq, fold.head_seq);
    assert_eq!(floor.folded_wal_no, fold.folded_wal_no);
    assert_eq!(floor.runs, fold.runs, "nothing arrived to fold");
}

/// Checks that an advance of a session without its writer epoch claims it
/// first and publishes the floor in a manifest of the new epoch, and that a
/// session that the claim displaced publishes nothing: its ordinary fold's
/// put collides with the claim's manifests, and the advance returns
/// `writer_fenced`.
#[tokio::test]
async fn an_advance_claims_first_or_returns_writer_fenced() {
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
        .advance_retention_floor_alone(&planned, first_seq)
        .await
        .expect("the claiming session's advance");
    assert_eq!(report.response, first_seq);
    let current = namespace.load_current_manifest().await.expect("current");
    assert_eq!(
        current.payload().writer_epoch.0,
        epoch_before.0 + 1,
        "the session claims"
    );
    assert_eq!(
        current.payload().retention_floor_seq,
        first_seq,
        "the newest manifest records the floor"
    );
    assert_eq!(
        namespace
            .load_read_state()
            .await
            .expect("head")
            .retention_floor_seq,
        first_seq,
        "the head's floor is the target"
    );

    // The first session was displaced by that claim. Its ordinary fold's put
    // collides with a manifest of the claim's epoch.
    let fenced = engine
        .advance_retention_floor_alone(&planned, second_seq)
        .await
        .expect_err("a displaced session's advance");
    assert_matches!(fenced, CoreError::WriterFenced(_));
    assert_eq!(
        namespace
            .load_current_manifest()
            .await
            .expect("current")
            .payload()
            .manifest_no,
        current.payload().manifest_no,
        "the displaced session publishes nothing"
    );
}
