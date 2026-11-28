//! A retention advance's two folds: an ordinary fold while commits continue,
//! and a pause of the commit queue for the fold of what arrived and the
//! manifest that raises the floor.

use super::deletion::{
    commit_now, fold_the_tail, is_commit_put, let_the_worker_run, newest_manifest,
    seeded_publisher, two_fold_store, wait_until_the_lane_is_kept,
};
use super::*;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{DeleteDirectoryBehavior, FilesystemEvent, InodeId, ManifestNo};

fn spawn_advance(
    publisher: &NamespacePublisher,
    target: CommitSeq,
) -> JoinHandle<crate::Result<CommitSeq>> {
    let publisher = publisher.clone();
    tokio::spawn(async move { publisher.advance_retention_floor(target).await })
}

/// Bounded: a stranded advance is a hang, and a hang must fail the test.
async fn settle_advance(
    handle: JoinHandle<crate::Result<CommitSeq>>,
    label: &str,
) -> crate::Result<CommitSeq> {
    bounded(Duration::from_secs(10), handle)
        .await
        .unwrap_or_else(|_| panic!("{label} must settle, not hang"))
        .unwrap_or_else(|err| panic!("{label} task failed: {err}"))
}

/// Whether `operation` is the put of a manifest whose floor is `floor`.
fn is_manifest_put_with_floor(operation: &OperationContext<'_>, floor: CommitSeq) -> bool {
    matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
        if loonfs_types::format::manifest::decode_namespace_manifest_json(bytes)
            .is_ok_and(|manifest| manifest.payload().retention_floor_seq == floor))
}

/// Returns the payload of manifest `manifest_no` of the namespace.
async fn manifest_at<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> NamespaceManifestPayload {
    let bytes = store
        .get(
            &loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &manifest_no),
            None,
        )
        .await
        .expect("read the manifest")
        .expect("the manifest exists");
    loonfs_types::format::manifest::decode_namespace_manifest_json(&bytes)
        .expect("decode the manifest")
        .payload()
        .clone()
}

/// Creates `/{name}` and deletes it, and returns the deleted directory with
/// the deletion's sequence.
async fn create_and_delete(publisher: &NamespacePublisher, name: &str) -> (InodeId, CommitSeq) {
    commit_now(publisher, name).await;
    let namespace_id = publisher.namespace_id.clone();
    let deleted = recv_commit(
        admit_commit(
            publisher,
            &namespace_id,
            CommitIntent::single(
                CommitId::parse(format!("delete-{name}")).expect("commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse(format!("/{name}")).expect("path"),
                    behavior: DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                },
            ),
        ),
        "the deletion",
    )
    .await;
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemEvent::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("the delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// Admits a restore of the deletion of `inode_id` at `deletion_seq`.
fn admit_restore(
    publisher: &NamespacePublisher,
    commit_id: &str,
    inode_id: InodeId,
    deletion_seq: CommitSeq,
) -> oneshot::Receiver<crate::Result<Commit>> {
    let namespace_id = publisher.namespace_id.clone();
    admit_commit(
        publisher,
        &namespace_id,
        CommitIntent::single(
            CommitId::parse(commit_id).expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::RestoreDeleted {
                inode_id,
                deletion_seq,
                destination: loonfs_types::options::RestoreDeletedDestination::Recorded,
            },
        ),
    )
}

/// Checks that an advance publishes its floor in its own manifest, with the
/// fold of what arrived, and pauses commits only for that fold: a commit is acknowledged
/// while the ordinary fold waits at its put, whose manifest keeps the old
/// floor, and a commit sent while the manifest with the floor waits at its
/// put is acknowledged only after it, above the manifest that folds the
/// first commit and records the floor.
#[tokio::test]
async fn an_advance_publishes_the_floor_with_the_fold_of_what_arrived() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = two_fold_store(directory.path());
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.block_next();
    store.inner().block_next();
    let advance = spawn_advance(&publisher, CommitSeq(1));
    store.wait_until_blocked().await;
    assert_eq!(
        commit_now(&publisher, "during-the-fold")
            .await
            .committed_seq,
        CommitSeq(2),
        "a commit is acknowledged during the ordinary fold"
    );
    store.release();

    store.inner().wait_until_blocked().await;
    let mut paused = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("during-the-pause", "during-the-pause"),
    );
    let_the_worker_run().await;
    assert_matches!(
        paused.try_recv(),
        Err(oneshot::error::TryRecvError::Empty),
        "no commit is acknowledged while the advance's fold of what arrived runs"
    );
    store.inner().release();

    assert_eq!(
        settle_advance(advance, "the advance")
            .await
            .expect("the advance"),
        CommitSeq(1)
    );
    assert_eq!(
        paused
            .await
            .expect("the paused commit's answer")
            .expect("a commit queued at the pause")
            .committed_seq,
        CommitSeq(3)
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, CommitSeq(1));
    assert_eq!(floor.head_seq, CommitSeq(2));
    let fold = manifest_at(
        store.as_ref(),
        &namespace_id,
        ManifestNo(floor.manifest_no.0 - 1),
    )
    .await;
    assert_eq!(fold.head_seq, CommitSeq(1));
    assert_eq!(fold.retention_floor_seq, CommitSeq(0));
}

/// Checks that the advance's pause waits for the restore whose WAL put is in
/// flight, which was validated against the old floor, so the manifest that
/// records the floor folds the restore.
#[tokio::test]
async fn a_restore_in_flight_at_the_pause_is_folded_below_the_floor_manifest() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        is_commit_put,
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    let (gone, deletion_seq) = create_and_delete(&publisher, "gone").await;
    fold_the_tail(&publisher).await;

    store.block_next();
    let in_flight = admit_restore(&publisher, "restore-in-flight", gone, deletion_seq);
    store.wait_until_blocked().await;
    let advance = spawn_advance(&publisher, deletion_seq);
    wait_until_the_lane_is_kept(&publisher).await;
    store.release();

    let restored = recv_commit(in_flight, "the restore in flight").await;
    assert_eq!(
        settle_advance(advance, "the advance")
            .await
            .expect("the advance"),
        deletion_seq
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, deletion_seq);
    assert_eq!(
        floor.head_seq, restored.committed_seq,
        "the advance's manifest folds the restore"
    );
}

/// Checks that a restore sent while the manifest that raises the floor waits
/// at its put is validated at its turn against the new floor, so a restore
/// of a deletion that the floor expired returns `not_deleted`.
#[tokio::test]
async fn a_restore_queued_behind_the_pause_is_validated_against_the_new_floor() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        |operation| is_manifest_put_with_floor(operation, CommitSeq(3)),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    let (gone, deletion_seq) = create_and_delete(&publisher, "gone").await;
    assert_eq!(deletion_seq, CommitSeq(3));
    fold_the_tail(&publisher).await;

    store.block_next();
    let advance = spawn_advance(&publisher, deletion_seq);
    store.wait_until_blocked().await;
    let queued = admit_restore(&publisher, "restore-queued", gone, deletion_seq);
    let_the_worker_run().await;
    store.release();

    assert_eq!(
        settle_advance(advance, "the advance")
            .await
            .expect("the advance"),
        deletion_seq
    );
    let refused = queued
        .await
        .expect("the queued restore's answer")
        .expect_err("a restore of a deletion that the floor expired");
    assert_eq!(refused.code(), ErrorCode::NotDeleted, "{refused:?}");
}

/// Checks that the floor that an advance publishes is the higher of the old
/// floor and the target: an advance toward a target below the floor that
/// folds a commit that arrived during its ordinary fold keeps the old floor.
#[tokio::test]
async fn the_floor_is_the_higher_of_the_old_floor_and_the_target() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        |operation| is_fold_at(operation, CommitSeq(3)),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    commit_now(&publisher, "second").await;
    assert_eq!(
        settle_advance(spawn_advance(&publisher, CommitSeq(2)), "the first advance")
            .await
            .expect("the first advance"),
        CommitSeq(2)
    );
    commit_now(&publisher, "third").await;

    store.block_next();
    let advance = spawn_advance(&publisher, CommitSeq(1));
    store.wait_until_blocked().await;
    assert_eq!(
        commit_now(&publisher, "during-the-fold")
            .await
            .committed_seq,
        CommitSeq(4)
    );
    store.release();

    assert_eq!(
        settle_advance(advance, "the second advance")
            .await
            .expect("the second advance"),
        CommitSeq(2)
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.head_seq, CommitSeq(4), "the fold of what arrived");
    assert_eq!(floor.retention_floor_seq, CommitSeq(2));
}

/// Checks that an advance whose ordinary fold fails returns the fold's error
/// and leaves the floor where it was.
#[tokio::test]
async fn a_failed_first_fold_leaves_the_floor_where_it_was() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        |operation| {
            operation.key().contains("/segments/")
                && matches!(operation.kind(), OperationKind::Put { .. })
        },
        InjectedError::PermissionDenied("segment puts refused".to_owned()),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.fail_all();
    settle_advance(spawn_advance(&publisher, CommitSeq(1)), "the advance")
        .await
        .expect_err("the ordinary fold can't write its segments");
    store.clear();
    let newest = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(newest.retention_floor_seq, CommitSeq(0));
    assert_eq!(newest.head_seq, CommitSeq(0), "nothing was folded");
    assert_eq!(
        commit_now(&publisher, "after").await.committed_seq,
        CommitSeq(2)
    );
}

/// Checks that an advance whose session loses its writer epoch before the
/// pause continues from the position that the next publication's claim
/// loads. The advance waits for the fold lock while a commit's put is
/// written but its answer is lost, the commit's retry claims and finds its
/// receipt, and the advance then folds from the claim's position and
/// publishes its floor under that claim's epoch, without a claim of its own.
#[tokio::test]
async fn an_advance_whose_session_loses_its_epoch_before_the_pause_continues_from_the_claim() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(
        FailStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            is_commit_put,
            InjectedError::Transport("lost WAL put acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    let epoch_before = newest_manifest(store.as_ref(), &namespace_id)
        .await
        .writer_epoch;

    let fold_lock = publisher.turns.fold.lock().await;
    let advance = spawn_advance(&publisher, CommitSeq(1));
    let_the_worker_run().await;
    store.fail_next(1);
    assert_eq!(
        commit_now(&publisher, "lost-answer").await.committed_seq,
        CommitSeq(2),
        "the commit's retry claims and finds its receipt"
    );
    let claimed = newest_manifest(store.as_ref(), &namespace_id)
        .await
        .writer_epoch;
    assert!(claimed > epoch_before, "the commit's retry claimed");
    drop(fold_lock);

    assert_eq!(
        settle_advance(advance, "the advance")
            .await
            .expect("the advance"),
        CommitSeq(1)
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, CommitSeq(1));
    assert_eq!(floor.head_seq, CommitSeq(2));
    assert_eq!(
        floor.writer_epoch, claimed,
        "the advance publishes under the claim's epoch and claims nothing"
    );
}

/// Checks that an advance whose session loses its writer epoch at the pause
/// claims the epoch itself, with the lane and the commit queue kept, folds
/// from the claim's position, and publishes the floor that it started with.
/// The commit whose put was in flight at the pause was written but its
/// answer was lost, so the claim's discovery finds it, and the manifest
/// that records the floor folds it.
#[tokio::test]
async fn an_advance_whose_session_loses_its_epoch_at_the_pause_claims_and_publishes_its_floor() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        FailStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            is_commit_put,
            InjectedError::Transport("lost WAL put acknowledgement".to_owned()),
        )
        .apply_then_fail(),
        is_commit_put,
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;
    let epoch_before = newest_manifest(store.as_ref(), &namespace_id)
        .await
        .writer_epoch;

    store.block_next();
    store.inner().fail_next(1);
    let lost = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("lost-answer", "lost-answer"),
    );
    store.wait_until_blocked().await;
    let advance = spawn_advance(&publisher, CommitSeq(1));
    wait_until_the_lane_is_kept(&publisher).await;
    store.release();

    assert_eq!(
        settle_advance(advance, "the advance")
            .await
            .expect("the advance"),
        CommitSeq(1)
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert!(
        floor.writer_epoch > epoch_before,
        "the advance claimed a new writer epoch"
    );
    assert_eq!(floor.retention_floor_seq, CommitSeq(1));
    assert_eq!(
        floor.head_seq,
        CommitSeq(2),
        "the claim's discovery found the written commit"
    );
    assert_eq!(
        recv_commit(lost, "lost-answer").await.committed_seq,
        CommitSeq(2),
        "the commit's retry finds its receipt"
    );
}

/// Checks that an advance with nothing to fold still publishes its floor:
/// one manifest put, no WAL put, and a manifest with the head and the
/// folded WAL number of the one before it.
#[tokio::test]
async fn an_advance_with_nothing_to_fold_still_publishes_its_floor() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;
    let before = newest_manifest(store.as_ref(), &namespace_id).await;

    store.reset();
    assert_eq!(
        settle_advance(spawn_advance(&publisher, CommitSeq(1)), "the advance")
            .await
            .expect("the advance"),
        CommitSeq(1)
    );
    let puts = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            loonfs_test_support::stores::RecordedOperation::Put { key, .. } => Some(key),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        puts,
        [loonfs_objectstore::keys::metadata_manifest_object(
            &namespace_id,
            &ManifestNo(before.manifest_no.0 + 1)
        )],
        "the advance puts one manifest and no WAL object"
    );
    let floor = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(floor.retention_floor_seq, CommitSeq(1));
    assert_eq!(floor.head_seq, before.head_seq);
    assert_eq!(floor.folded_wal_no, before.folded_wal_no);
    assert_eq!(floor.runs, before.runs);
}

/// Checks that two advances at once take turns at the fold lock: the second
/// waits while the first folds, then folds from the manifest that the first
/// published, and publishes nothing.
#[tokio::test]
async fn two_advances_at_once_take_turns_and_the_second_publishes_nothing() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        |operation| is_fold_at(operation, CommitSeq(1)),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    let before = newest_manifest(store.as_ref(), &namespace_id).await;

    store.block_next();
    let first = spawn_advance(&publisher, CommitSeq(1));
    store.wait_until_blocked().await;
    let second = spawn_advance(&publisher, CommitSeq(1));
    let_the_worker_run().await;
    store.release();

    for (advance, label) in [(first, "the first advance"), (second, "the second advance")] {
        assert_eq!(
            settle_advance(advance, label).await.expect("an advance"),
            CommitSeq(1)
        );
    }
    let newest = newest_manifest(store.as_ref(), &namespace_id).await;
    assert_eq!(newest.retention_floor_seq, CommitSeq(1));
    assert_eq!(
        newest.manifest_no.0,
        before.manifest_no.0 + 2,
        "the first advance's fold and floor, and nothing from the second"
    );
}
