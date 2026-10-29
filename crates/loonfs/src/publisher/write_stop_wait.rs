//! A new commit at the write stop waits in its session's queue for the
//! session's fold, for at most the write-stop wait.

use super::*;
use loonfs_test_support::clock::ManualClock;

/// The inline bytes of each waiting commit, which fill a WAL object's inline
/// budget, so that each commit publishes in a WAL object of its own.
const BODY_BYTES: usize = 1000;

/// A writer that records metrics, over a store that parks the next fold of
/// its namespace at the fold's manifest put once the test arms it.
struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<BlockingStore<LocalFsStore>>,
    recorder: Arc<DefaultMetricsRecorder>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
}

/// Creates a fixture with a writer built by `configure`, with its namespace
/// created and opened.
async fn fixture(
    configure: impl FnOnce(
        crate::LoonFsBuilder<crate::Writable>,
    ) -> crate::LoonFsBuilder<crate::Writable>,
) -> Fixture {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("stopped").expect("valid namespace id");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(directory.path()).expect("store"),
        metadata_manifest_prefix(&namespace_id),
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let builder = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .min_publish_interval_ms(0)
        .metrics_recorder(recorder.clone());
    let writer = configure(builder).build().await.expect("build writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    Fixture {
        _directory: directory,
        store,
        recorder,
        writer,
        namespace_id,
        namespace,
    }
}

/// Creates a publication policy whose write-stop wait ends in half a second.
fn short_write_stop_wait() -> crate::PublicationPolicy {
    crate::PublicationPolicy {
        write_stop_wait_budget_ms: 500,
        ..crate::PublicationPolicy::default()
    }
}

/// The write-stop wait of a writer whose clock the test moves.
///
/// The worker sleeps this long before the wait expires, so the test moves
/// the clock past the wait well before then.
const MANUAL_WRITE_STOP_WAIT_BUDGET_MS: u64 = 2_000;

/// Creates a fixture whose writer reads `clock`, with a write-stop wait of
/// [`MANUAL_WRITE_STOP_WAIT_BUDGET_MS`].
async fn fixture_with_clock(clock: &Arc<ManualClock>) -> Fixture {
    let clock = Arc::clone(clock);
    fixture(|builder| {
        builder
            .monotonic_timer(clock)
            .publication_policy(crate::PublicationPolicy {
                write_stop_wait_budget_ms: MANUAL_WRITE_STOP_WAIT_BUDGET_MS,
                ..crate::PublicationPolicy::default()
            })
    })
    .await
}

fn spawn_commit(
    namespace: &crate::Namespace<crate::Writable>,
    candidate: CommitCandidate,
) -> JoinHandle<crate::Result<Commit>> {
    let namespace = namespace.clone();
    tokio::spawn(async move { namespace.commit_candidate(candidate).await })
}

/// Yields until `ready` returns true, and fails the test after ten seconds.
async fn wait_until(mut ready: impl FnMut() -> bool, what: &str) {
    bounded(Duration::from_secs(10), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

async fn unfolded_wal_objects<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> u64 {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_diagnostics()
        .await
        .expect("load namespace diagnostics")
        .wal_tail_objects
}

/// Appends WAL objects from another writer until the namespace's tail has
/// `objects` WAL objects.
///
/// Runs before the session's first publish, which acquires a later writer
/// epoch.
async fn seed_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    objects: u64,
) {
    // The other writer's epoch fence is one of the appended objects.
    let count = objects - unfolded_wal_objects(store, namespace_id).await - 1;
    loonfs_core::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &WriterAt {
            writer_id: loonfs_types::WriterId::parse("tail-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .append_wal_objects(count)
    .await
    .expect("seed the WAL tail");
    assert_eq!(unfolded_wal_objects(store, namespace_id).await, objects);
}

/// Seeds the tail to the write stop, parks the next fold at its manifest
/// put, and starts a commit, which the engine refuses at the write stop.
///
/// Returns once the commit waits at the front of the queue, or once it has
/// settled after a short write-stop wait.
async fn start_commit_at_the_write_stop(fixture: &Fixture) -> JoinHandle<crate::Result<Commit>> {
    seed_wal_tail(
        fixture.store.inner(),
        &fixture.namespace_id,
        MAX_UNFOLDED_WAL_OBJECTS,
    )
    .await;
    fixture.store.block_next();
    let commit = spawn_commit(
        &fixture.namespace,
        CommitCandidate::new(create_directory_request("at-the-stop", "at-the-stop")),
    );
    // The publish that the engine refuses starts the session's fold, after
    // the worker took the commit from the queue, so a queued commit beside a
    // fold task is a commit that waits.
    let publisher = fixture.namespace.session().publisher.clone();
    wait_until(
        || {
            let state = publisher_state(&publisher);
            (state.fold.is_some() && queued_candidates(&state) == 1) || commit.is_finished()
        },
        "the commit waits at the write stop",
    )
    .await;
    commit
}

async fn settle_commit(commit: JoinHandle<crate::Result<Commit>>) -> crate::Result<Commit> {
    bounded(Duration::from_secs(10), commit)
        .await
        .expect("the commit settles")
        .expect("join the commit")
}

fn histogram_count(recorder: &DefaultMetricsRecorder, name: &str) -> u64 {
    recorder
        .snapshot()
        .by_name(name)
        .map(|entry| match entry.value {
            MetricValue::Histogram { count, .. } => count,
            ref other => panic!("expected a histogram, found {other:?}"),
        })
        .sum()
}

async fn engine_wal_tail_objects(publisher: &NamespacePublisher) -> u64 {
    publisher
        .engine
        .lock()
        .await
        .engine
        .as_ref()
        .and_then(NamespaceCommitEngine::wal_tail_objects)
        .unwrap_or(0)
}

/// Checks that a commit at the write stop waits in the queue while its fold
/// runs, and is committed once the fold publishes, without an error or a
/// retry, and that `write_stop_waits` records it.
#[tokio::test]
async fn a_commit_at_the_write_stop_waits_for_the_fold_and_commits() {
    let timer = Arc::new(ManualClock::new(0));
    let fixture = fixture(|builder| builder.monotonic_timer(timer.clone())).await;
    let commit = start_commit_at_the_write_stop(&fixture).await;
    assert!(!commit.is_finished(), "the commit waits for the fold");

    // The fold's hint raise starts an interval after the claim's.
    timer.advance_ms(loonfs_core::limits::HINT_RAISE_INTERVAL_MS);
    fixture.store.release();
    settle_commit(commit)
        .await
        .expect("the commit is committed after the fold");
    // The runtime raises the hint to the manifest of the fold at the write stop.
    fixture.writer.drain().await.expect("the fold settles");
    assert_hint_at_current_manifest(
        &fixture.writer,
        fixture.store.inner(),
        &fixture.namespace_id,
    )
    .await;
    let recorder = &fixture.recorder;
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_waits"), 1);
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_refusals"), 0);
    assert_eq!(
        histogram_count(recorder, "loonfs.publisher.write_stop_wait_seconds"),
        1
    );
    fixture.writer.shutdown().await.expect("shut down writer");
}

/// Checks that a commit that waits at the write stop past the write-stop
/// wait returns `maintenance_required`, isn't committed, and that
/// `write_stop_refusals` records it.
#[tokio::test]
async fn a_commit_past_the_write_stop_wait_returns_maintenance_required() {
    let fixture = fixture(|builder| builder.publication_policy(short_write_stop_wait())).await;
    let commit = start_commit_at_the_write_stop(&fixture).await;
    let error = settle_commit(commit)
        .await
        .expect_err("the parked fold outlasts the write-stop wait");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

    fixture.store.release();
    fixture
        .namespace
        .wait_for_fold()
        .await
        .expect("the released fold settles");
    let missing = fixture
        .namespace
        .stat("/at-the-stop")
        .await
        .expect_err("the refused commit isn't committed");
    assert_eq!(missing.code(), ErrorCode::PathNotFound);
    let recorder = &fixture.recorder;
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_waits"), 1);
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_refusals"), 1);
    fixture.writer.shutdown().await.expect("shut down writer");
}

/// Checks that commits waiting at the write stop are committed in arrival
/// order, including when the first fold frees room for only some of them
/// and the rest wait for the next fold.
#[tokio::test]
async fn commits_waiting_at_the_write_stop_are_committed_in_arrival_order() {
    const WAITING_COMMITS: usize = 40;
    let fixture = fixture(|builder| {
        builder
            .execution_budget(one_fold_at_a_time())
            .inline_content(crate::InlineContentPolicy {
                inline_content_wal_object_budget_bytes: BODY_BYTES,
                ..crate::InlineContentPolicy::default()
            })
    })
    .await;
    let (store, namespace) = (&fixture.store, &fixture.namespace);
    let publisher = namespace.session().publisher.clone();
    // The session's epoch fence and first commit then reach the fold
    // trigger, and the fold parks at its manifest put, so it frees only the
    // tail that it began with.
    seed_wal_tail(
        store.inner(),
        &fixture.namespace_id,
        FOLD_AT_WAL_OBJECTS - 2,
    )
    .await;
    store.block_next();
    for fill in 0.. {
        if engine_wal_tail_objects(&publisher).await >= MAX_UNFOLDED_WAL_OBJECTS {
            break;
        }
        let name = format!("fill-{fill}");
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(&name, &name)))
            .await
            .expect("fill the tail to the write stop");
        if fill == 0 {
            store.wait_until_blocked().await;
        }
    }
    // Queued for the only fold permit behind the parked fold, so the next
    // fold waits until the test drops the permit.
    let (drop_permit, permit_dropped) = oneshot::channel::<()>();
    let budget = fixture.writer.execution_budget().clone();
    let held = tokio::spawn(async move {
        let _permit = budget.fold_permit().await;
        let _ = permit_dropped.await;
    });
    wait_for_fold_waiters(&fixture.writer, 1).await;

    let mut commits = Vec::new();
    for index in 0..WAITING_COMMITS {
        let namespace = namespace.clone();
        commits.push(tokio::spawn(async move {
            namespace
                .put_file(
                    &format!("/waiting-{index:02}"),
                    &[b'w'; BODY_BYTES],
                    &loonfs_test_support::test_actor(),
                )
                .await
        }));
        wait_until(
            || publisher_state(&publisher).in_flight.len() > index,
            "the commit is admitted",
        )
        .await;
    }
    store.release();
    bounded(Duration::from_secs(10), async {
        while counter(&fixture.recorder, "loonfs.publisher.wal_folds") < 1
            || engine_wal_tail_objects(&publisher).await < MAX_UNFOLDED_WAL_OBJECTS
            || fixture.writer.execution_budget().stats().folds_waiting != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the commits after the first fold refill the tail to the write stop");
    let still_waiting = queued_candidates(&publisher_state(&publisher));
    assert!(
        (1..WAITING_COMMITS).contains(&still_waiting),
        "the first fold frees room for some of the commits: {still_waiting} still wait"
    );

    drop(drop_permit);
    held.await.expect("join the permit holder");
    let mut committed_seqs = Vec::new();
    for commit in commits {
        committed_seqs.push(settle_commit(commit).await.expect("commit").committed_seq);
    }
    assert!(
        committed_seqs.windows(2).all(|pair| pair[0] < pair[1]),
        "commits are committed in arrival order: {committed_seqs:?}"
    );
    let recorder = &fixture.recorder;
    assert_eq!(
        counter(recorder, "loonfs.publisher.write_stop_waits"),
        usize_to_u64(WAITING_COMMITS)
    );
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_refusals"), 0);
    assert_eq!(counter(recorder, "loonfs.publisher.wal_folds"), 2);
    fixture.writer.shutdown().await.expect("shut down writer");
}

/// Checks that while one namespace's commit waits at the write stop, its
/// worker releases the publication slot, so another namespace publishes.
#[tokio::test]
async fn another_namespace_publishes_while_a_commit_waits_at_the_write_stop() {
    let fixture = fixture(|builder| {
        builder.execution_budget(
            ExecutionBudget::builder()
                .max_concurrent_publications(NonZeroUsize::MIN)
                .build(),
        )
    })
    .await;
    let other_id = NamespaceId::parse("other").expect("valid namespace id");
    fixture
        .writer
        .create_namespace(&other_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the other namespace");
    let other = fixture
        .writer
        .open_namespace(&other_id)
        .expect("open the other namespace");
    let commit = start_commit_at_the_write_stop(&fixture).await;

    bounded(
        Duration::from_secs(10),
        other.commit_candidate(CommitCandidate::new(create_directory_request(
            "beside-the-stop",
            "docs",
        ))),
    )
    .await
    .expect("the other namespace doesn't wait for the publication slot")
    .expect("commit to the other namespace");
    assert!(!commit.is_finished(), "the commit still waits for its fold");

    fixture.store.release();
    settle_commit(commit)
        .await
        .expect("the commit is committed after the fold");
    fixture.writer.shutdown().await.expect("shut down writer");
}

/// Checks that a session close waits for a commit at the write stop until
/// the commit is committed, and that a shutdown drain waits for one until it
/// reaches the write-stop wait.
#[tokio::test]
async fn closing_and_draining_wait_for_commits_at_the_write_stop() {
    let closing = fixture(|builder| builder).await;
    let commit = start_commit_at_the_write_stop(&closing).await;
    let close = tokio::spawn(closing.namespace.clone().close());
    wait_until(
        || closing.namespace.session().state() == NamespaceSessionStatus::Closed,
        "the close stops admitting work",
    )
    .await;
    assert!(!close.is_finished(), "the close waits for the commit");
    closing.store.release();
    settle_commit(commit)
        .await
        .expect("the commit is committed after the fold");
    let report = bounded(Duration::from_secs(10), close)
        .await
        .expect("the close finishes")
        .expect("join the close")
        .expect("close the session");
    assert_eq!(report.drained_commits, Some(1));

    let draining = fixture(|builder| builder.publication_policy(short_write_stop_wait())).await;
    let commit = start_commit_at_the_write_stop(&draining).await;
    let shutdown = tokio::spawn({
        let writer = draining.writer.clone();
        async move { writer.shutdown().await }
    });
    let error = settle_commit(commit)
        .await
        .expect_err("the commit reaches the write-stop wait during the drain");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);
    draining.store.release();
    bounded(Duration::from_secs(10), shutdown)
        .await
        .expect("the drain finishes")
        .expect("join the drain")
        .expect("shut down writer");
}

/// Checks that a commit whose first attempt had an unknown outcome, and that
/// the engine then refuses at the write stop, keeps `commit_outcome_unknown`
/// under the settling rule instead of waiting.
///
/// The first attempt's epoch fence is written, but its acknowledgement is
/// lost, so the retry acquires another epoch, and the second fence fills the
/// tail to the write stop.
#[tokio::test]
async fn an_unknown_outcome_before_the_write_stop_keeps_commit_outcome_unknown() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("unknown").expect("valid namespace id");
    let prefix = wal_prefix(&namespace_id);
    let failing = Arc::new(
        FailStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            move |operation| {
                operation.key().starts_with(&prefix)
                    && matches!(
                        operation.kind(),
                        OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
                            if decode_wal_object_envelope_zstd(bytes)
                                .is_ok_and(|wal| wal.payload().records.is_empty())
                    )
            },
            InjectedError::Transport("lost fence acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = crate::LoonFs::builder_with_store(failing.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    seed_wal_tail(failing.inner(), &namespace_id, MAX_UNFOLDED_WAL_OBJECTS - 2).await;

    failing.fail_next(1);
    let request = || CommitCandidate::new(create_directory_request("unknown", "unknown"));
    let error = bounded(
        Duration::from_secs(10),
        namespace.commit_candidate(request()),
    )
    .await
    .expect("the commit doesn't wait")
    .expect_err("the first attempt's outcome is unknown");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert_eq!(counter(&recorder, "loonfs.publisher.write_stop_waits"), 0);

    namespace
        .commit_candidate(request())
        .await
        .expect("a retry with the same commit ID is committed after the fold");
    writer.shutdown().await.expect("shut down writer");
}

/// Checks that a retry of a committed request, queued behind a commit that
/// waits at the write stop, returns its original commit when that wait
/// expires, instead of `maintenance_required`.
#[tokio::test]
async fn a_retry_queued_behind_an_expired_wait_returns_its_original_commit() {
    let clock = Arc::new(ManualClock::new(0));
    let fixture = fixture_with_clock(&clock).await;
    // The session's epoch fence and the committed request fill the tail to
    // the write stop, and the fold that the request starts parks at its
    // manifest put.
    seed_wal_tail(
        fixture.store.inner(),
        &fixture.namespace_id,
        MAX_UNFOLDED_WAL_OBJECTS - 2,
    )
    .await;
    fixture.store.block_next();
    let committed = || CommitCandidate::new(create_directory_request("committed", "committed"));
    let original = fixture
        .namespace
        .commit_candidate(committed())
        .await
        .expect("commit below the write stop");
    let commit = spawn_commit(
        &fixture.namespace,
        CommitCandidate::new(create_directory_request("at-the-stop", "at-the-stop")),
    );
    let publisher = fixture.namespace.session().publisher.clone();
    // The worker took the new commit's batch, the second batch here, and put
    // the refused commit back in the queue.
    wait_until(
        || {
            counter(&fixture.recorder, "loonfs.publisher.batches") == 2
                && queued_candidates(&publisher_state(&publisher)) == 1
        },
        "the new commit waits at the write stop",
    )
    .await;
    let retry = spawn_commit(&fixture.namespace, committed());
    wait_until(
        || queued_candidates(&publisher_state(&publisher)) == 2,
        "the retry is queued behind the waiting commit",
    )
    .await;

    // Both commits entered the queue at the clock's start, so both have
    // waited for the whole write-stop wait when it expires.
    clock.advance_ms(MANUAL_WRITE_STOP_WAIT_BUDGET_MS);
    let error = settle_commit(commit)
        .await
        .expect_err("the waiting commit reaches the write-stop wait");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);
    let replay = settle_commit(retry)
        .await
        .expect("the engine returns the retry's result from its receipt");
    assert_eq!(replay, original);
    fixture.store.release();
    fixture.writer.shutdown().await.expect("shut down writer");
}

/// Checks that a new commit queued behind a commit that waits at the write
/// stop, and past the write-stop wait when that wait expires, returns
/// `maintenance_required` only after the engine examines it at the next
/// publish.
#[tokio::test]
async fn a_commit_queued_behind_an_expired_wait_is_refused_after_the_engine_examines_it() {
    let clock = Arc::new(ManualClock::new(0));
    let fixture = fixture_with_clock(&clock).await;
    let commit = start_commit_at_the_write_stop(&fixture).await;
    let behind = spawn_commit(
        &fixture.namespace,
        CommitCandidate::new(create_directory_request("behind", "behind")),
    );
    let publisher = fixture.namespace.session().publisher.clone();
    wait_until(
        || queued_candidates(&publisher_state(&publisher)) == 2,
        "the new commit is queued behind the waiting commit",
    )
    .await;

    clock.advance_ms(MANUAL_WRITE_STOP_WAIT_BUDGET_MS);
    for commit in [commit, behind] {
        let error = settle_commit(commit)
            .await
            .expect_err("the commit waits for the whole write-stop wait");
        assert_eq!(error.code(), ErrorCode::MaintenanceRequired);
    }
    let recorder = &fixture.recorder;
    assert_eq!(
        counter(recorder, "loonfs.publisher.batches"),
        2,
        "the commit behind is published once before it is refused"
    );
    assert_eq!(counter(recorder, "loonfs.publisher.write_stop_refusals"), 2);
    assert_eq!(
        histogram_count(recorder, "loonfs.publisher.write_stop_wait_seconds"),
        2
    );
    fixture.store.release();
    fixture.writer.shutdown().await.expect("shut down writer");
}
