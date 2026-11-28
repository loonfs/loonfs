//! A namespace deletion's two folds: an ordinary fold while commits
//! continue, and a pause of the commit queue for the fold of what arrived
//! and the tombstone.

use super::*;
use loonfs_test_support::stores::RecordedOperation;

/// Whether `operation` is the WAL put of a commit batch.
pub(super) fn is_commit_put(operation: &OperationContext<'_>) -> bool {
    matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
    if decode_wal_object_envelope_zstd(bytes).is_ok_and(|wal| matches!(
        wal.payload().body,
        loonfs_types::format::wal::WalObjectBody::Commits(_)
    )))
}

/// Whether `operation` is the put of a tombstone.
fn is_tombstone_put(operation: &OperationContext<'_>) -> bool {
    matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
        if loonfs_types::format::manifest::decode_namespace_manifest_json(bytes)
            .is_ok_and(|manifest| manifest.payload().status.is_deleted()))
}

/// A store that parks an operation's ordinary fold at the put of its
/// manifest, whose head is 1, and its fold of what arrived at the put of its
/// manifest, whose head is 2, each once the test arms it.
pub(super) type TwoFoldStore = BlockingStore<BlockingStore<LocalFsStore>>;

pub(super) fn two_fold_store(root: &Path) -> Arc<TwoFoldStore> {
    Arc::new(BlockingStore::matching(
        BlockingStore::matching(LocalFsStore::new(root).expect("store"), |operation| {
            is_fold_at(operation, CommitSeq(2))
        }),
        |operation| is_fold_at(operation, CommitSeq(1)),
    ))
}

/// Returns an unpaced standalone publisher of a new namespace in `store`
/// whose session committed one directory at sequence 1, which is unfolded.
pub(super) async fn seeded_publisher(
    store: SharedStore,
    namespace_id: &NamespaceId,
) -> (TestRuntime, NamespacePublisher) {
    let runtime = test_runtime(store);
    create_namespace(&runtime, namespace_id).await;
    let publisher = standalone_publisher_with(namespace_id, &runtime, TestCore::unpaced());
    let seed = recv_commit(
        admit_commit(
            &publisher,
            namespace_id,
            create_directory_request("seed", "seed"),
        ),
        "seed",
    )
    .await;
    assert_eq!(seed.committed_seq, CommitSeq(1));
    (runtime, publisher)
}

/// Folds the session's tail, so an operation's ordinary fold finds nothing
/// to fold and takes the lane at once for its pause.
pub(super) async fn fold_the_tail(publisher: &NamespacePublisher) {
    publisher
        .fold(FoldWhen::Always, &MaintenanceCancellation::default())
        .await
        .expect("fold");
}

/// Commits a directory and returns the commit, within a bound.
pub(super) async fn commit_now(publisher: &NamespacePublisher, name: &str) -> Commit {
    let namespace_id = publisher.namespace_id.clone();
    bounded(
        Duration::from_secs(10),
        recv_commit(
            admit_commit(
                publisher,
                &namespace_id,
                create_directory_request(name, name),
            ),
            name,
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("the commit {name} is acknowledged"))
}

/// Yields until a task keeps the session's publication lane, which in these
/// tests is a deletion's or an advance's pause.
pub(super) async fn wait_until_the_lane_is_kept(publisher: &NamespacePublisher) {
    bounded(Duration::from_secs(10), async {
        loop {
            let kept = {
                let mut lock = std::pin::pin!(publisher.lane.lock());
                futures::poll!(lock.as_mut()).is_pending()
            };
            if kept {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the deletion keeps the lane");
}

/// Yields many times, so the session's worker would acknowledge a commit
/// that it could publish.
pub(super) async fn let_the_worker_run() {
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
}

/// Returns the payload of the namespace's newest manifest.
pub(super) async fn newest_manifest<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> loonfs_types::format::manifest::NamespaceManifestPayload {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("the newest manifest")
        .payload()
        .clone()
}

/// Checks that a deletion folds the tail while commits continue, and pauses
/// commits only for the fold of what arrived: a commit is acknowledged while
/// the ordinary fold waits at its put, the fold of what arrived folds that
/// commit, and a commit sent while that fold waits at its put is never
/// acknowledged and returns `namespace_deleted`.
#[tokio::test]
async fn a_deletion_folds_while_commits_continue_and_pauses_only_for_what_arrived() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = two_fold_store(directory.path());
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.block_next();
    store.inner().block_next();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    store.wait_until_blocked().await;
    assert_eq!(
        commit_now(&publisher, "during-the-fold")
            .await
            .committed_seq,
        CommitSeq(2)
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
        "no commit is acknowledged while the deletion's fold of what arrived runs"
    );
    store.inner().release();

    let deleted = settle_delete(delete, "the deletion")
        .await
        .expect("the deletion");
    assert_eq!(deleted.head_seq, CommitSeq(2));
    let paused = paused
        .await
        .expect("the paused commit's answer")
        .expect_err("a commit queued at the pause");
    assert_eq!(paused.code(), ErrorCode::NamespaceDeleted);
    let tombstone = newest_manifest(store.as_ref(), &namespace_id).await;
    assert!(tombstone.status.is_deleted());
    assert_eq!(tombstone.head_seq, CommitSeq(2));
}

/// Checks that a deletion checks its expected head once commits are paused,
/// not before its ordinary fold: a commit acknowledged during the ordinary
/// fold moves the head, so the deletion returns `stale_head` and the
/// namespace stays active.
#[tokio::test]
async fn a_deletion_checks_its_expected_head_once_commits_are_paused() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = two_fold_store(directory.path());
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.block_next();
    let delete = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(CommitSeq(1)),
        },
    );
    store.wait_until_blocked().await;
    assert_eq!(
        commit_now(&publisher, "during-the-fold")
            .await
            .committed_seq,
        CommitSeq(2)
    );
    store.release();

    let refused = settle_delete(delete, "the deletion")
        .await
        .expect_err("the head moved during the ordinary fold");
    assert_eq!(refused.code(), ErrorCode::StaleHead);
    assert!(newest_manifest(store.as_ref(), &namespace_id)
        .await
        .status
        .is_active());
    assert_eq!(
        commit_now(&publisher, "after").await.committed_seq,
        CommitSeq(3)
    );
}

/// Checks that a deletion's pause waits for the commit whose WAL put is in
/// flight, so the expected head is checked against a head that includes
/// that commit.
#[tokio::test]
async fn a_commit_in_flight_at_the_pause_is_settled_before_the_head_check() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        is_commit_put,
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;

    store.block_next();
    let in_flight = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("in-flight", "in-flight"),
    );
    store.wait_until_blocked().await;
    let delete = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(CommitSeq(2)),
        },
    );
    wait_until_the_lane_is_kept(&publisher).await;
    store.release();

    assert_eq!(
        recv_commit(in_flight, "in-flight").await.committed_seq,
        CommitSeq(2)
    );
    let deleted = settle_delete(delete, "the deletion")
        .await
        .expect("the head includes the commit that was in flight");
    assert_eq!(deleted.head_seq, CommitSeq(2));
}

/// Checks that the tombstone is published with commits still paused: a
/// commit acknowledged during the ordinary fold is covered by the
/// tombstone, and a commit sent while the tombstone's put is in flight is
/// never acknowledged and returns `namespace_deleted`.
#[tokio::test]
async fn the_tombstone_is_published_with_commits_still_paused() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        BlockingStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            is_tombstone_put,
        ),
        |operation| is_fold_at(operation, CommitSeq(1)),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.block_next();
    store.inner().block_next();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    store.wait_until_blocked().await;
    assert_eq!(
        commit_now(&publisher, "during-the-fold")
            .await
            .committed_seq,
        CommitSeq(2)
    );
    store.release();

    store.inner().wait_until_blocked().await;
    let mut paused = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("during-the-tombstone", "during-the-tombstone"),
    );
    let_the_worker_run().await;
    assert_matches!(
        paused.try_recv(),
        Err(oneshot::error::TryRecvError::Empty),
        "no commit is acknowledged while the tombstone's put is in flight"
    );
    store.inner().release();

    let deleted = settle_delete(delete, "the deletion")
        .await
        .expect("the deletion");
    assert_eq!(deleted.head_seq, CommitSeq(2));
    let paused = paused
        .await
        .expect("the paused commit's answer")
        .expect_err("a commit sent during the tombstone's put");
    assert_eq!(paused.code(), ErrorCode::NamespaceDeleted);
}

/// Checks that a deletion whose session loses its writer epoch at the pause
/// claims the epoch itself, with the lane and the commit queue kept, and
/// checks its expected head after the claim. The commit whose put was in
/// flight at the pause was written but its response was lost, so the
/// claim's discovery finds it, and the deletion returns `stale_head`.
#[tokio::test]
async fn a_deletion_whose_session_loses_its_epoch_at_the_pause_claims_and_returns_stale_head() {
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
    let delete = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(CommitSeq(1)),
        },
    );
    wait_until_the_lane_is_kept(&publisher).await;
    store.release();

    let refused = settle_delete(delete, "the deletion")
        .await
        .expect_err("the claim's discovery found the written commit");
    assert_matches!(
        refused,
        Error::Core(CoreError::StaleHeadPrecondition {
            expected: CommitSeq(1),
            actual: CommitSeq(2),
            ..
        })
    );
    let newest = newest_manifest(store.as_ref(), &namespace_id).await;
    assert!(newest.status.is_active());
    assert!(
        newest.writer_epoch > epoch_before,
        "the deletion claimed a new writer epoch"
    );
    assert_eq!(
        recv_commit(lost, "lost-answer").await.committed_seq,
        CommitSeq(2),
        "the commit's retry finds its receipt"
    );
}

/// Checks that a deletion whose ordinary fold fails returns the fold's
/// error and leaves the namespace active.
#[tokio::test]
async fn a_failed_first_fold_leaves_the_namespace_active() {
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
    let failed = settle_delete(
        spawn_delete(&publisher, DeleteNamespaceOptions::default()),
        "the deletion",
    )
    .await
    .expect_err("the ordinary fold can't write its segments");
    assert_ne!(failed.code(), ErrorCode::NamespaceDeleted);
    store.clear();
    let newest = newest_manifest(store.as_ref(), &namespace_id).await;
    assert!(newest.status.is_active());
    assert_eq!(newest.head_seq, CommitSeq(0), "nothing was folded");
    assert_eq!(
        commit_now(&publisher, "after").await.committed_seq,
        CommitSeq(2)
    );
}

/// Checks the rules for commits around a deletion without an expected
/// head: a commit acknowledged before the pause is deleted with the
/// namespace, and a commit that is still queued at the pause returns
/// `namespace_deleted`, even when it was sent before the deletion.
#[tokio::test]
async fn a_deletion_deletes_acknowledged_commits_and_refuses_those_still_queued() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        is_commit_put,
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;

    store.block_next();
    let in_flight = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("in-flight", "in-flight"),
    );
    store.wait_until_blocked().await;
    let queued = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("queued", "queued"),
    );
    wait_for_queued_candidates(&publisher, 1).await;
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    wait_until_the_lane_is_kept(&publisher).await;
    store.release();

    assert_eq!(
        recv_commit(in_flight, "in-flight").await.committed_seq,
        CommitSeq(2)
    );
    let deleted = settle_delete(delete, "the deletion")
        .await
        .expect("the deletion");
    assert_eq!(deleted.head_seq, CommitSeq(2));
    let queued = queued
        .await
        .expect("the queued commit's answer")
        .expect_err("a commit queued at the pause");
    assert_eq!(queued.code(), ErrorCode::NamespaceDeleted);
    let fast = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("too-late", "too-late"),
    );
    assert_matches!(fast, Err(CoreError::NamespaceDeleted { .. }));
}

/// Returns whether the session refuses new work because its namespace is
/// gone, with its stop set, so its compaction is cancelled.
fn has_ended_as_deleted(publisher: &NamespacePublisher) -> bool {
    matches!(
        publisher.live_session().map(drop),
        Err(CoreError::NamespaceDeleted { .. })
    ) && publisher.stop.is_cancelled()
}

/// Checks that a tombstone put whose outcome is unknown ends the session
/// once a later unit finds the tombstone: the deletion returns
/// `outcome_unknown`, the next commit claims, finds the tombstone and
/// returns `namespace_deleted`, and from then on the session refuses work
/// at admission without a request to the store, with its compaction
/// cancelled.
#[tokio::test]
async fn a_unit_that_finds_the_tombstone_after_an_unknown_tombstone_put_ends_the_session() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    // The boundary read after the tombstone's put fails, so the deletion
    // doesn't learn the outcome of the put, which is written.
    let tombstone_put = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let boundary = loonfs_objectstore::keys::boundary(&namespace_id);
    let store = Arc::new(RecordingStore::new(
        FailStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            {
                let tombstone_put = Arc::clone(&tombstone_put);
                move |operation| {
                    if is_tombstone_put(operation) {
                        tombstone_put.store(true, AtomicOrdering::SeqCst);
                    }
                    tombstone_put.load(AtomicOrdering::SeqCst) && operation.key() == boundary
                }
            },
            InjectedError::Transport("lost boundary read".to_owned()),
        ),
        KeyPredicate::any(),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;

    store.inner().fail_next(1);
    let unknown = settle_delete(
        spawn_delete(&publisher, DeleteNamespaceOptions::default()),
        "the deletion",
    )
    .await
    .expect_err("the boundary read after the tombstone's put failed");
    assert_eq!(unknown.code(), ErrorCode::OutcomeUnknown);
    assert!(!has_ended_as_deleted(&publisher));

    let found = bounded(
        Duration::from_secs(10),
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("after", "after"),
        ),
    )
    .await
    .expect("the commit settles")
    .expect("the commit's response")
    .expect_err("the commit's claim finds the tombstone");
    assert_eq!(found.code(), ErrorCode::NamespaceDeleted);
    assert!(has_ended_as_deleted(&publisher));

    store.reset();
    assert_matches!(
        try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("refused", "refused"),
        ),
        Err(CoreError::NamespaceDeleted { .. })
    );
    assert_eq!(
        store.counts(),
        loonfs_test_support::stores::StoreCounts::default()
    );
}

/// Checks that a deletion whose future is dropped while its ordinary fold's
/// put is in flight leaves nothing locked: a commit, a fold and a second
/// deletion run, and the second deletion ends the session.
#[tokio::test]
async fn a_deletion_dropped_during_its_ordinary_fold_leaves_nothing_locked() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(HeldFoldAnswer::new(directory.path(), &namespace_id));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;

    store.hold_next();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    store.wait_until_held().await;
    delete.abort();
    let _ = delete.await;

    assert_eq!(
        commit_now(&publisher, "after-the-drop").await.committed_seq,
        CommitSeq(2)
    );
    bounded(Duration::from_secs(10), fold_the_tail(&publisher))
        .await
        .expect("the fold runs");
    let deleted = settle_delete(
        spawn_delete(&publisher, DeleteNamespaceOptions::default()),
        "the second deletion",
    )
    .await
    .expect("the second deletion");
    assert_eq!(deleted.head_seq, CommitSeq(2));
    assert!(has_ended_as_deleted(&publisher));
}

/// Checks that a deletion whose future is dropped after its tombstone is
/// written, before the deletion learns the put's outcome, leaves nothing
/// locked, and that the first unit that finds the tombstone ends the
/// session: a commit returns `namespace_deleted`, and so do a fold and a
/// second deletion.
#[tokio::test]
async fn a_deletion_dropped_during_its_tombstone_put_ends_the_session_once_a_unit_finds_it() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(HeldFoldAnswer::new(directory.path(), &namespace_id));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;

    store.hold_next();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    store.wait_until_held().await;
    delete.abort();
    let _ = delete.await;
    assert!(newest_manifest(store.as_ref(), &namespace_id)
        .await
        .status
        .is_deleted());

    let commit = bounded(
        Duration::from_secs(10),
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("after", "after"),
        ),
    )
    .await
    .expect("the commit settles")
    .expect("the commit's response")
    .expect_err("the commit's claim finds the tombstone");
    assert_eq!(commit.code(), ErrorCode::NamespaceDeleted);
    assert!(has_ended_as_deleted(&publisher));
    let fold = publisher
        .fold(FoldWhen::Always, &MaintenanceCancellation::default())
        .await
        .err()
        .expect("a fold of a deleted namespace");
    assert_eq!(fold.code(), ErrorCode::NamespaceDeleted);
    let second = settle_delete(
        spawn_delete(&publisher, DeleteNamespaceOptions::default()),
        "the second deletion",
    )
    .await
    .expect_err("a second deletion of a deleted namespace");
    assert_eq!(second.code(), ErrorCode::NamespaceDeleted);
}

/// Checks that a deletion of an initializing namespace through the runtime
/// passes over the ordinary fold, which finds no namespace, publishes the
/// tombstone, and ends the session.
#[tokio::test]
async fn a_deletion_of_an_initializing_namespace_publishes_its_tombstone() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("half").expect("namespace");
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(|key| key.starts_with("pools/") && key.contains("/members/")),
        OperationSelector::Put,
        InjectedError::Transport("member entry".into()),
    ));
    let writer = test_writer(store.clone() as SharedStore).await;
    store.fail_all();
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect_err("the member entry write fails");
    store.clear();
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    namespace.delete().await.expect("the deletion");
    assert!(newest_manifest(store.as_ref(), &namespace_id)
        .await
        .status
        .is_deleted());
    assert!(has_ended_as_deleted(&namespace.session().publisher));
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a deletion with a stale expected head, on a session that
/// has no writer epoch yet, runs its ordinary fold and its pause once: the
/// refusal reports the epoch of the session's claim, which the session's
/// check confirms.
#[tokio::test]
async fn a_stale_deletion_on_a_session_without_an_epoch_folds_once() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store")) as SharedStore;
    let (_runtime, _seeded) = seeded_publisher(store.clone(), &namespace_id).await;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(store, MetadataCache::default(), recorder.clone()).await;
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    assert!(matches!(
        *namespace.session().publisher.session.lock(),
        WriterSessionStatus::Unacquired
    ));

    let refused = namespace
        .delete_with_options(&DeleteNamespaceOptions {
            expected_head_seq: Some(CommitSeq(5)),
        })
        .await
        .expect_err("the head is 1");
    assert_eq!(refused.code(), ErrorCode::StaleHead);
    assert_eq!(
        crate::test_support::histogram_count(
            &recorder.snapshot(),
            "loonfs.publisher.wal_fold_seconds"
        ),
        1,
        "one ordinary fold"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that work still queued after a deletion returns
/// `namespace_deleted` at its start without a request to the store: a
/// commit sent while the tombstone's put is in flight waits for the pause,
/// and the store sees no request after the tombstone's put and the boundary
/// read that settles it.
#[tokio::test]
async fn work_queued_after_a_deletion_returns_namespace_deleted_without_a_request() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        BlockingStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            is_tombstone_put,
        ),
        KeyPredicate::any(),
    ));
    let (_runtime, publisher) = seeded_publisher(store.clone() as SharedStore, &namespace_id).await;
    fold_the_tail(&publisher).await;

    store.inner().block_next();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    store.inner().wait_until_blocked().await;
    let queued = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("queued", "queued"),
    );
    let_the_worker_run().await;
    store.inner().release();

    settle_delete(delete, "the deletion")
        .await
        .expect("the deletion");
    let queued = bounded(Duration::from_secs(10), queued)
        .await
        .expect("the queued commit settles")
        .expect("the queued commit's response")
        .expect_err("a commit queued at the pause");
    assert_eq!(queued.code(), ErrorCode::NamespaceDeleted);
    let operations = store.take();
    let manifests = metadata_manifest_prefix(&namespace_id);
    let tombstone = operations
        .iter()
        .rposition(|operation| {
            matches!(operation, RecordedOperation::Put { key, .. } if key.starts_with(&manifests))
        })
        .expect("the tombstone's put");
    let boundary = loonfs_objectstore::keys::boundary(&namespace_id);
    let after = &operations[tombstone + 1..];
    assert!(
        matches!(
            after,
            [RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. }]
                if *key == boundary
        ),
        "only the tombstone's boundary read follows its put: {after:?}"
    );
}
