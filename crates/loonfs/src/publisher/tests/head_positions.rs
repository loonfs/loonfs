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

/// Checks that a check of the position that finds two manifests of the
/// position's writer epoch after it (another runtime's fold and a retention
/// window change) loads each of them, reads the manifest boundary, and moves
/// the position once, onto the newer one, with the tail that the fold left.
#[tokio::test]
async fn a_check_moves_the_position_onto_the_newest_manifest_it_walks_to() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store = recording.clone() as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let namespace_id = namespace.id().clone();
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    let peer = test_writer_with_interval(store, 0).await;
    let maintenance = peer.maintenance(loonfs_test_support::ids::writer_id("peer"));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("the peer's fold");
    maintenance
        .set_retention_window(
            &namespace_id,
            loonfs_types::RetentionWindow::new(30 * 86_400_000).ok(),
        )
        .await
        .expect("the peer's window change");
    let base = before.read_basis().basis().manifest_no();
    let numbers: Vec<_> = std::iter::successors(Some(base), |number| number.successor().ok())
        .take(4)
        .collect();

    recording.reset();
    writer
        .core
        .checked_writer_position(&namespace_id, None)
        .await
        .expect("the check");
    let manifest =
        |number| loonfs_objectstore::keys::metadata_manifest_object(&namespace_id, number);
    let requests: Vec<_> = recording
        .take()
        .into_iter()
        .map(|operation| {
            (
                matches!(operation, RecordedOperation::Head { .. }),
                operation.key().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        requests,
        [
            (true, manifest(&numbers[1])),
            (false, manifest(&numbers[1])),
            (false, manifest(&numbers[2])),
            (false, manifest(&numbers[3])),
            (false, loonfs_objectstore::keys::boundary(&namespace_id)),
        ],
        "the successor check, the walk's loads, and the boundary read"
    );
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("the position moved");
    assert_eq!(after.read_basis().basis().manifest_no(), numbers[2]);
    assert_eq!(
        after.read_basis().head().seq,
        before.read_basis().head().seq
    );
    assert_eq!(
        after.read_basis().head().folded_wal_no,
        before.read_basis().head().wal_no
    );
    assert_eq!(after.wal_tail_objects(), 0);
    peer.shutdown().await.expect("peer shutdown");
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
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .fold_wal(&namespace_id)
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
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    maintenance.fold_wal(&namespace_id).await.expect("fold");
    let epoch = writer_epoch(&store, &namespace_id).await;
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, before.committed_seq);
    let diagnostics = maintenance
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

/// Checks that a change of unknown effect drops the position, and that the
/// next commit stores one again.
#[tokio::test]
async fn an_unknown_change_drops_the_read_position_until_the_next_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    writer
        .mode
        .publisher
        .invalidate_namespace(&namespace_id, NamespaceChange::Unknown)
        .await;
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
/// commit is then in the tail above the fold's manifest.
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
    let folded = bounded(
        Duration::from_secs(10),
        writer
            .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
            .fold_wal(&namespace_id),
    )
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

/// Checks that a position that reads start from, and that can't move onto
/// a manifest this runtime published, is removed by the first read while
/// the session's engine is idle, so later reads find no position to move.
///
/// Another writer claims the namespace, and a fold through this runtime's
/// maintenance then folds over the claim, so the fold's manifest has the
/// other writer's epoch.
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
    let folded = writer
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .fold_wal(namespace.id())
        .await
        .expect("a fold over the rival's claim");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let slot = &namespace.session().publisher.read_position;
    assert!(
        slot.current().is_none(),
        "the position can't follow the fold"
    );
    assert!(
        !slot.stores_a_position(),
        "the read removed the position, so later reads find none"
    );
    rival.shutdown().await.expect("rival shutdown");
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
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    store.hold_next();
    let (folded, ()) = futures::join!(maintenance.fold_wal(&namespace_id), async {
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
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::AlreadyCurrent);

    recording.reset();
    namespace.stat("/").await.expect("stat the root");
    assert_eq!(recording.take(), [], "the read starts from the cached head");
    writer.shutdown().await.expect("shutdown");
}
