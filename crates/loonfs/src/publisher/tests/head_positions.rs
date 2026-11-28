//! The read position that each writer session keeps for reads in its runtime.

use super::*;

/// Creates a writer and a namespace, and commits one directory through the
/// writer's session.
async fn published_namespace(
    store: SharedStore,
) -> (
    crate::LoonFs<crate::Writable>,
    crate::Namespace<crate::Writable>,
) {
    let writer = test_writer_with_interval(store, 0).await;
    let namespace = publish_once_into_each(
        &writer,
        &[NamespaceId::parse("demo").expect("valid namespace id")],
    )
    .await
    .pop()
    .expect("one namespace");
    (writer, namespace)
}

/// Creates a writer that reports to `recorder`.
async fn test_writer_with_recorder(
    store: SharedStore,
    recorder: Arc<DefaultMetricsRecorder>,
) -> crate::LoonFs<crate::Writable> {
    crate::LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            trace_mode: TraceMode::Remote,
            ..crate::RuntimeConfig::default()
        })
        .metrics_recorder(recorder)
        .store_kind(ConfiguredObjectStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

/// Returns how many reads started from `source`: `position`, `refreshed`,
/// `loaded`, or `eventual`.
fn reads_from(recorder: &DefaultMetricsRecorder, source: &str) -> u64 {
    crate::test_support::counter(
        &recorder.snapshot(),
        "loonfs.read_head.source",
        &[("source", source)],
    )
}

/// Checks that the position moves to each commit before the commit's caller
/// hears the result, and that a read-only view of the writable runtime finds
/// the same position.
#[tokio::test]
async fn the_read_position_follows_each_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let namespace_id = namespace.id().clone();
    for name in ["second", "third"] {
        let commit = namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(name, name)))
            .await
            .expect("commit");
        let position = writer
            .core
            .writer_position(&namespace_id)
            .expect("a position after a commit");
        assert_eq!(
            position.read_basis().head().seq,
            commit.committed_seq,
            "{name}"
        );
        assert_eq!(
            position.read_basis().basis().manifest_no(),
            current_manifest_no(&store, &namespace_id).await,
            "{name}"
        );
    }
    let reader = writer.read_only();
    assert_eq!(
        reader
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis().head().seq),
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis().head().seq),
        "a read-only view shares the writer's core"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold through maintenance moves the position to the fold's
/// manifest at the same head.
#[tokio::test]
async fn a_fold_moves_the_read_position_to_its_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    let folded = writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the fold");
    assert_eq!(after.read_basis().basis().manifest_no(), folded.manifest_no);
    assert!(after.read_basis().basis().manifest_no() > before.read_basis().basis().manifest_no());
    assert_eq!(
        after.read_basis().head().seq,
        before.read_basis().head().seq
    );
    assert_eq!(
        after.read_basis().head().folded_wal_no,
        before.read_basis().head().wal_no
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a retention advance through maintenance runs in the
/// namespace's session: its floor object moves the position's tip and floor,
/// the fold after it moves the position onto the manifest that records the
/// floor, and the session's next commit publishes under the same epoch,
/// validated against the advanced floor.
#[tokio::test]
async fn an_advance_moves_the_sessions_position_onto_its_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build writer");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let commit = |name: &str| {
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(name, name)))
    };
    let before = commit("before").await.expect("commit");
    let maintenance = writer.clone();
    maintenance
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold");
    let epoch = writer_epoch(&store, &namespace_id).await;
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    let advanced = maintenance
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .advance_retention_floor()
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, before.committed_seq);
    let diagnostics = maintenance
        .maintenance()
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.retention_floor_seq, before.committed_seq);
    assert_eq!(
        writer
            .core
            .writer_position(&namespace_id)
            .expect("a position after the advance")
            .read_basis()
            .basis()
            .manifest_no(),
        current_manifest_no(&store, &namespace_id).await,
        "the advance's fold moves the position onto its manifest"
    );

    commit("after").await.expect("commit");
    assert_eq!(
        writer_epoch(&store, &namespace_id).await,
        epoch,
        "the advance claims no epoch"
    );
    assert_eq!(
        writer
            .core
            .writer_position(&namespace_id)
            .expect("a position after the commit")
            .retention_floor_seq(),
        before.committed_seq
    );
    writer.shutdown().await.expect("shutdown");
}

/// The writer epoch of the namespace's current manifest.
async fn writer_epoch(
    store: &SharedStore,
    namespace_id: &NamespaceId,
) -> loonfs_types::WriterEpoch {
    loonfs_core::test_support::namespace_store(store.as_ref(), namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .writer_epoch
}

/// Checks that a session without a position, such as one that the test hook
/// dropped, stores one again at its next commit.
#[tokio::test]
async fn a_session_without_a_position_stores_one_at_its_next_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    writer.mode.publisher.drop_session_view(&namespace_id).await;
    assert!(writer.core.writer_position(&namespace_id).is_none());
    let commit = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "after", "after",
        )))
        .await
        .expect("commit");
    assert_eq!(
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis().head().seq),
        Some(commit.committed_seq)
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold that this runtime publishes while a commit's WAL put is
/// in flight moves the position that reads start from onto its manifest at
/// once, without waiting for the publish to release the engine, and that the
/// commit is then in the tail above the fold's manifest. The session keeps
/// its writer epoch while the commit's put is in flight, so the fold
/// publishes without a claim (`writer_lane`
/// `MC_TwoQueuesFoldDuringCommitWitness`).
#[tokio::test]
async fn a_fold_moves_the_read_position_while_a_commits_put_is_in_flight() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let (writer, namespace) = published_namespace(store.clone() as SharedStore).await;
    let options = crate::CreateSnapshotOptions {
        name: Some("during".to_owned()),
        ..crate::CreateSnapshotOptions::default()
    };
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    store.block_next();
    let (during, snapshot) = futures::join!(
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(
            "during", "during"
        ))),
        async {
            store.wait_until_blocked().await;
            let snapshot = bounded(
                Duration::from_secs(10),
                namespace.create_snapshot_with_options(&options),
            )
            .await
            .expect("the snapshot's fold doesn't wait for the commit's put")
            .expect("snapshot");
            assert_matches!(
                *namespace.session().publisher.session.lock(),
                WriterSessionStatus::Acquired { .. },
                "the session keeps its epoch while the commit's put is in flight"
            );
            let moved = writer
                .core
                .writer_position(&namespace_id)
                .expect("reads keep a position while the commit's put is in flight");
            assert_eq!(
                moved.read_basis().basis().manifest_no(),
                snapshot.snapshot_id.manifest_no(),
                "the fold moves the position while the commit's put is in flight"
            );
            assert_eq!(
                moved.read_basis().head().seq,
                before.read_basis().head().seq
            );
            store.release();
            snapshot
        }
    );
    let during = during.expect("the publish succeeds");
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the publish");
    assert_eq!(
        after.read_basis().basis().manifest_no(),
        snapshot.snapshot_id.manifest_no()
    );
    assert_eq!(after.read_basis().head().seq, during.committed_seq);
    assert_eq!(
        after.wal_tail_objects(),
        1,
        "the commit is in the tail above the fold's manifest"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold that this runtime publishes moves the position that
/// reads start from before anything releases the session's engine: the test
/// keeps the engine locked while the fold runs and reads.
#[tokio::test]
async fn a_fold_moves_the_read_position_before_the_engine_is_released() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    let engine = namespace.session().publisher.lock_engine().await;
    let folded = bounded(Duration::from_secs(10), namespace.fold_wal())
        .await
        .expect("the fold doesn't wait for the engine")
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let moved = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position while the engine is locked");
    assert_eq!(moved.read_basis().basis().manifest_no(), folded.manifest_no);
    assert_eq!(
        moved.read_basis().head().seq,
        before.read_basis().head().seq
    );
    drop(engine);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a position that reads start from, and that can't follow the
/// session's status, is removed by the first read while the session's
/// engine is idle, so later reads find no position to move.
///
/// Another writer claims the namespace, and a fold through this runtime's
/// session then loses its put to the claim, which fences the session.
#[tokio::test]
async fn a_read_position_that_cannot_follow_a_publication_is_removed_by_the_next_read() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let rival = crate::LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer-b"))
        .build()
        .await
        .expect("rival");
    rival
        .open_namespace(namespace.id())
        .expect("open namespace")
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "rival", "rival",
        )))
        .await
        .expect("the rival takes over the namespace");
    let fenced = namespace
        .fold_wal()
        .await
        .expect_err("a fold that loses to the rival's claim");
    assert_eq!(fenced.code(), crate::ErrorCode::WriterFenced, "{fenced:?}");
    let slot = &namespace.session().publisher.read_position;
    assert!(
        slot.current().is_none(),
        "the position can't follow the fenced session"
    );
    assert!(
        slot.stored().is_none(),
        "the read removed the position, so later reads find none"
    );
    rival.shutdown().await.expect("rival shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Returns the WAL objects and bytes of the tails that the session's engine
/// and the position that reads start from store, without following the
/// session's newest published manifest. The engine must be idle.
fn stored_tails(namespace: &crate::Namespace<crate::Writable>) -> [Option<(u64, usize)>; 2] {
    let publisher = &namespace.session().publisher;
    let tail = |position: &loonfs_core::publish::WriterPosition| {
        (position.wal_tail_objects(), position.wal_tail_bytes())
    };
    let engine = publisher
        .engine
        .try_lock()
        .expect("no task has the engine")
        .stored_position();
    [
        engine.as_ref().map(tail),
        publisher.read_position.stored().as_deref().map(tail),
    ]
}

/// Checks that a fold of an idle session's tail leaves neither the engine
/// nor the position that reads start from storing the folded tail.
#[tokio::test]
async fn a_fold_of_an_idle_session_leaves_no_copy_of_the_folded_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    assert!(
        stored_tails(&namespace)
            .iter()
            .all(|tail| tail.is_some_and(|(objects, _)| objects > 0)),
        "the commit is in both stored tails"
    );
    let folded = namespace.fold_wal().await.expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    assert_eq!(
        stored_tails(&namespace),
        [Some((0, 0)), Some((0, 0))],
        "neither the engine nor the read copy stores the folded tail"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a commit whose WAL put was in flight during a fold puts back
/// a position without the tail that the fold covered: once the commit has
/// finished, the engine and the position that reads start from store only
/// that commit's WAL object.
#[tokio::test]
async fn a_commit_that_puts_back_its_position_after_a_fold_stores_no_folded_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let (writer, namespace) = published_namespace(store.clone() as SharedStore).await;
    store.block_next();
    let (during, ()) = futures::join!(
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(
            "during", "during"
        ))),
        async {
            store.wait_until_blocked().await;
            let folded = namespace.fold_wal().await.expect("fold");
            assert_eq!(folded.outcome, FoldWalOutcome::Published);
            store.release();
        }
    );
    during.expect("the publish succeeds");
    let [engine, read_copy] = stored_tails(&namespace);
    assert_eq!(
        engine.map(|(objects, _)| objects),
        Some(1),
        "the engine stores only the commit's WAL object"
    );
    assert_eq!(read_copy, engine, "the read copy stores the same tail");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold that finds the session's engine locked, and that a
/// read's check follows, leaves the folded tail in the engine until the
/// engine's release, and that the release then brings both stored positions
/// up to the fold's manifest.
#[tokio::test]
async fn the_release_after_a_fold_brings_both_stored_positions_up_to_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    let engine = namespace.session().publisher.lock_engine().await;
    let folded = bounded(Duration::from_secs(10), namespace.fold_wal())
        .await
        .expect("the fold doesn't wait for the engine")
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let crate::runtime_core::CheckedPosition::Confirmed(checked) = writer
        .core
        .checked_writer_position(&namespace_id, None)
        .await
        .expect("a read's check between the record and the release")
    else {
        panic!("the check confirms a position");
    };
    assert_eq!(
        checked.read_basis().basis().manifest_no(),
        folded.manifest_no
    );
    assert!(
        engine
            .stored_position()
            .is_some_and(|position| position.wal_tail_objects() > 0),
        "the fold found the engine locked"
    );
    drop(engine);
    assert_eq!(
        stored_tails(&namespace),
        [Some((0, 0)), Some((0, 0))],
        "the release brings both stored positions up to the fold's manifest"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a publication of the session's lane moves the publication's
/// check onto the position that reads start from, so a read with a freshness
/// limit that the position's own check is past sends no request.
#[tokio::test]
async fn a_publication_of_the_lane_saves_the_next_reads_check() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(ManualClock::new(1_000_000));
    let writer = crate::LoonFs::builder_with_store(
        recording.clone() as SharedStore,
        crate::Context::Service,
    )
    .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
    .config(crate::RuntimeConfig {
        min_publish_interval_ms: 0,
        max_staleness_ms: 1_000,
        ..crate::RuntimeConfig::default()
    })
    .monotonic_timer(clock.clone())
    .build()
    .await
    .expect("build writer");
    let namespace = publish_once_into_each(
        &writer,
        &[NamespaceId::parse("demo").expect("valid namespace id")],
    )
    .await
    .pop()
    .expect("one namespace");
    let namespace_id = namespace.id().clone();
    clock.advance_ms(1_000);
    let published = writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .set_retention_window(Some(
            crate::RetentionWindow::new(2 * crate::GC_GRACE_WINDOW_MS)
                .expect("a window longer than the grace window"),
        ))
        .await
        .expect("change the window");
    assert_eq!(
        published.retention_window_ms,
        Some(2 * crate::GC_GRACE_WINDOW_MS)
    );
    let current = current_manifest_no(recording.as_ref(), &namespace_id).await;
    assert_eq!(
        Some(current),
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis().basis().manifest_no()),
        "the position moves onto the publication's manifest"
    );

    recording.reset();
    namespace.stat("/").await.expect("stat the root");
    assert_eq!(
        recording.take(),
        [],
        "the read takes the publication's check"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fenced session keeps no position, so reads in its runtime
/// stop starting from the head it last published.
#[tokio::test]
async fn a_fenced_session_keeps_no_read_position() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let rival = crate::LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer-b"))
        .build()
        .await
        .expect("rival");
    rival
        .open_namespace(namespace.id())
        .expect("open namespace")
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "rival", "rival",
        )))
        .await
        .expect("the rival takes over the namespace");
    assert!(
        namespace
            .session()
            .publisher
            .read_position
            .current()
            .is_some(),
        "the session hasn't found the fence yet"
    );
    let fenced = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "fenced", "fenced",
        )))
        .await
        .expect_err("the old session is fenced");
    assert_eq!(fenced.code(), ErrorCode::WriterFenced);
    assert!(namespace
        .session()
        .publisher
        .read_position
        .current()
        .is_none());
    assert!(writer.core.writer_position(namespace.id()).is_none());
    rival.shutdown().await.expect("rival shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a read during an explicit fold keeps the position, and that
/// the fold then moves the position to its manifest.
///
/// The fold's manifest is durable before the answer to its put arrives, and
/// the publisher learns of the manifest only once the fold returns. A strong
/// read between the two waits for the session's check, which finds a
/// successor of the position's manifest that this runtime hasn't recorded as
/// its own yet.
#[tokio::test]
async fn a_read_during_an_explicit_fold_keeps_the_read_position() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(HeldFoldAnswer::new(temp_dir.path(), &namespace_id));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        test_writer_with_recorder(store.clone() as SharedStore, Arc::clone(&recorder)).await;
    let namespace = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id))
        .await
        .pop()
        .expect("one namespace");
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    let maintenance = writer.clone();
    store.hold_next();
    let handle = maintenance
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let (folded, ()) = futures::join!(handle.fold_wal(), async {
        store.wait_until_held().await;
        let from_position = reads_from(&recorder, "position");
        namespace.stat("/docs").await.expect("stat during the fold");
        assert_eq!(
            reads_from(&recorder, "position"),
            from_position + 1,
            "the read starts from the position"
        );
        assert!(
            writer.core.writer_position(&namespace_id).is_some(),
            "the read keeps the position"
        );
        store.release();
    });
    let folded = folded.expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the fold");
    assert_eq!(after.read_basis().basis().manifest_no(), folded.manifest_no);
    assert_eq!(
        after.read_basis().head().seq,
        before.read_basis().head().seq
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks the same for the session's own fold, which the publish that
/// crosses the fold threshold starts.
#[tokio::test]
async fn a_read_during_a_session_fold_keeps_the_read_position() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(HeldFoldAnswer::new(temp_dir.path(), &namespace_id));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        test_writer_with_recorder(store.clone() as SharedStore, Arc::clone(&recorder)).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    loonfs_core::test_support::NamespaceFixture::new(
        store.as_ref(),
        &namespace_id,
        &WriterAt {
            writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .append_wal_objects(FOLD_AT_WAL_OBJECTS.get() - 1)
    .await
    .expect("seed the WAL tail below the fold threshold");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    // The fold waits for a permit until the gate is armed, so the gate holds
    // the answer to the fold's manifest put and nothing else.
    let permits = hold_every_fold_permit(&writer).await;
    let committed = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "cross-threshold",
            "docs",
        )))
        .await
        .expect("publish across the fold threshold");
    wait_for_fold_waiters(&writer, 1).await;
    store.hold_next();
    drop(permits);
    store.wait_until_held().await;
    let from_position = reads_from(&recorder, "position");
    namespace.stat("/docs").await.expect("stat during the fold");
    assert_eq!(
        reads_from(&recorder, "position"),
        from_position + 1,
        "the read starts from the position"
    );
    assert!(
        writer.core.writer_position(&namespace_id).is_some(),
        "the read keeps the position"
    );
    store.release();
    namespace.wait_for_fold().await.expect("fold");
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the fold");
    assert_eq!(after.read_basis().head().seq, committed.committed_seq);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that an open session that hasn't committed keeps the cached head
/// across a fold in its runtime that finds the manifest current, so the next
/// read sends no request.
#[tokio::test]
async fn a_fold_that_finds_the_manifest_current_keeps_an_unused_sessions_cached_head() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = test_writer_with_interval(recording.clone() as SharedStore, 0).await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let folded = writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::AlreadyCurrent);

    recording.reset();
    namespace.stat("/").await.expect("stat the root");
    assert_eq!(recording.take(), [], "the read starts from the cached head");
    writer.shutdown().await.expect("shutdown");
}

/// Returns a writer over `store` whose reads take the cached head for a
/// minute, on a timer that doesn't move, so an eventual read never checks
/// the head that an earlier read cached.
async fn writer_with_a_long_staleness(store: SharedStore) -> crate::LoonFs<crate::Writable> {
    crate::LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            max_staleness_ms: 60_000,
            ..crate::RuntimeConfig::default()
        })
        .monotonic_timer(Arc::new(ManualClock::new(1_000_000)))
        .build()
        .await
        .expect("build writer")
}

/// Waits until the session of `namespace` has no writer epoch.
async fn wait_for_the_epoch_to_end(namespace: &crate::Namespace<crate::Writable>) {
    bounded(Duration::from_secs(10), async {
        while matches!(
            *namespace.session().publisher.session.lock(),
            WriterSessionStatus::Acquired { .. }
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the session loses its epoch");
}

/// Checks that a read after a commit's acknowledgement sees the commit when
/// the session lost its epoch while the commit's WAL put was in flight: a
/// fold's manifest put has an unknown outcome, and a read meanwhile caches a
/// head below the commit. The commit's put ends while the session has no
/// epoch, so it drops the cached head before the commit is acknowledged.
#[tokio::test]
async fn a_read_after_a_commit_acknowledged_without_the_epoch_sees_the_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(FailStore::new(
        blocking_publication_store(temp_dir.path(), &namespace_id),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost manifest acknowledgment".to_owned()),
    ));
    let writer = writer_with_a_long_staleness(store.clone() as SharedStore).await;
    let namespace = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id))
        .await
        .pop()
        .expect("one namespace");

    store.inner().block_next();
    let committing = namespace.clone();
    let commit = tokio::spawn(async move {
        committing
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "during", "during",
            )))
            .await
    });
    bounded(Duration::from_secs(10), store.inner().wait_until_blocked())
        .await
        .expect("the commit reaches its WAL put");
    store.fail_next(1);
    assert_eq!(
        namespace
            .fold_wal()
            .await
            .expect_err("the fold's outcome is unknown")
            .code(),
        ErrorCode::OutcomeUnknown
    );
    wait_for_the_epoch_to_end(&namespace).await;
    namespace
        .stat("/during")
        .await
        .expect_err("the commit isn't written yet");
    store.inner().release();
    bounded(Duration::from_secs(10), commit)
        .await
        .expect("the commit settles")
        .expect("join the commit")
        .expect("the commit is acknowledged");
    namespace
        .stat("/during")
        .await
        .expect("the read after the acknowledgement sees the commit");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a window change whose manifest put succeeds after the session
/// lost its epoch to a commit's WAL put with an unknown outcome leaves no
/// cached head below its manifest when its caller hears the outcome, so a
/// read meanwhile that cached a head below the manifest doesn't hide the
/// change. The test keeps the engine, so the commit's retry can't claim
/// before the window change returns.
#[tokio::test]
async fn a_read_after_a_publication_acknowledged_without_the_epoch_sees_the_publication() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(FailStore::new(
        BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::PutCreateIfAbsent,
        ),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost WAL acknowledgment".to_owned()),
    ));
    let writer = writer_with_a_long_staleness(store.clone() as SharedStore).await;
    let namespace = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id))
        .await
        .pop()
        .expect("one namespace");
    let before = current_manifest_no(store.as_ref(), &namespace_id).await;

    store.inner().block_next();
    let changing = namespace.clone();
    let change = tokio::spawn(async move {
        changing
            .set_retention_window(Some(
                crate::RetentionWindow::new(2 * crate::GC_GRACE_WINDOW_MS)
                    .expect("a window longer than the grace window"),
            ))
            .await
    });
    bounded(Duration::from_secs(10), store.inner().wait_until_blocked())
        .await
        .expect("the window change reaches its manifest put");
    store.fail_next(1);
    let committing = namespace.clone();
    let commit = tokio::spawn(async move {
        committing
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "lost", "lost",
            )))
            .await
    });
    wait_for_the_epoch_to_end(&namespace).await;
    let engine = namespace.session().publisher.lock_engine().await;
    namespace.stat("/").await.expect("stat the root");
    store.inner().release();
    bounded(Duration::from_secs(10), change)
        .await
        .expect("the window change settles")
        .expect("join the window change")
        .expect("the window change is acknowledged");
    let cached = writer
        .core
        .head_and_wal_tail_cache()
        .get_head(&namespace_id);
    assert!(
        cached
            .as_ref()
            .is_none_or(|head| head.basis().manifest_no() > before),
        "the cached head is on the window change's manifest or later: {:?}",
        cached.map(|head| head.basis().manifest_no())
    );
    drop(engine);
    let _ = bounded(Duration::from_secs(10), commit)
        .await
        .expect("the commit settles");
    writer.shutdown().await.expect("shutdown");
}
