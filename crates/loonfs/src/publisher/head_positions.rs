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
    crate::LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .min_publish_interval_ms(0)
        .metrics_recorder(recorder)
        .trace_mode(TraceMode::Remote)
        .store_kind(ConfiguredObjectStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

/// Returns how many reads started from `source`: `position`, `anchor`, or
/// `eventual`.
fn reads_from(recorder: &DefaultMetricsRecorder, source: &str) -> u64 {
    let labels: &[(&str, &str)] = &[("source", source)];
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.read_head.source")
        .find(|entry| entry.labels == labels)
        .unwrap_or_else(|| panic!("no read-head counter for `{source}`"));
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
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
        assert_eq!(position.read_basis.head.seq, commit.committed_seq, "{name}");
        assert_eq!(
            position.read_basis.basis.manifest_no(),
            current_manifest_no(&store, &namespace_id).await,
            "{name}"
        );
    }
    let reader = writer.read_only();
    assert_eq!(
        reader
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis.head.seq),
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.read_basis.head.seq),
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
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the fold");
    assert_eq!(after.read_basis.basis.manifest_no(), folded.manifest_no);
    assert!(after.read_basis.basis.manifest_no() > before.read_basis.basis.manifest_no());
    assert_eq!(after.read_basis.head.seq, before.read_basis.head.seq);
    assert_eq!(
        after.read_basis.head.folded_wal_no,
        before.read_basis.head.wal_no
    );
    writer.shutdown().await.expect("shutdown");
}

/// Returns the basis floor of the WAL object that commits `committed_seq`.
async fn basis_floor_of_commit(
    store: &dyn ObjectStore,
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

/// Checks that a retention advance through maintenance moves the position
/// onto the advance's manifest, so that the session's next WAL object
/// records the advanced floor, and that the diagnostics report the
/// acknowledged floor before and after a fold of that WAL object.
#[tokio::test]
async fn the_next_wal_object_after_an_advance_records_the_advanced_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .min_publish_interval_ms(0)
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
    clock.advance_ms(crate::GC_DEFAULT_GRACE_WINDOW_MS);
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, before.committed_seq);
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(
        (
            diagnostics.retention_floor_seq,
            diagnostics.acknowledged_floor_seq
        ),
        (before.committed_seq, ChangeSeq(0))
    );
    assert_eq!(
        writer
            .core
            .writer_position(&namespace_id)
            .expect("a position after the advance")
            .read_basis
            .basis
            .manifest_no(),
        current_manifest_no(&store, &namespace_id).await,
        "the advance moves the position onto its manifest"
    );

    let after = commit("after").await.expect("commit");
    assert_eq!(
        basis_floor_of_commit(&store, &namespace_id, before.committed_seq).await,
        ChangeSeq(0)
    );
    assert_eq!(
        basis_floor_of_commit(&store, &namespace_id, after.committed_seq).await,
        before.committed_seq
    );
    maintenance.fold_wal(&namespace_id).await.expect("fold");
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.acknowledged_floor_seq, before.committed_seq);
    writer.shutdown().await.expect("shutdown");
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
            .map(|position| position.read_basis.head.seq),
        Some(commit.committed_seq)
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a manifest that this runtime publishes while a publish has
/// the engine locked leaves the position for reads in place until the
/// publish releases the engine, which then moves the position to that
/// manifest.
#[tokio::test]
async fn a_manifest_published_during_a_publish_moves_the_read_position_when_it_ends() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let (writer, namespace) = published_namespace(store.clone() as SharedStore).await;
    let options = crate::CreateSnapshotOptions::default();
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    store.block_next();
    let (during, checkpoint) = futures::join!(
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(
            "during", "during"
        ))),
        async {
            store.wait_until_blocked().await;
            let (checkpoint, ()) = futures::join!(
                namespace.create_snapshot_with_options("during", &options),
                async {
                    // The checkpoint's manifest is durable, and its move of
                    // the position waits for the publish's engine.
                    bounded(Duration::from_secs(10), async {
                        let shared = store.clone() as SharedStore;
                        while current_manifest_no(&shared, &namespace_id).await
                            <= before.read_basis.basis.manifest_no()
                        {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("the checkpoint publishes its manifest");
                    let kept = writer
                        .core
                        .writer_position(&namespace_id)
                        .expect("reads keep the position while the publish runs");
                    assert_eq!(
                        kept.read_basis.basis.manifest_no(),
                        before.read_basis.basis.manifest_no()
                    );
                    store.release();
                }
            );
            checkpoint.expect("checkpoint")
        }
    );
    let during = during.expect("the publish succeeds");
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the publish");
    assert_eq!(
        after.read_basis.basis.manifest_no(),
        checkpoint.snapshot_id.manifest_no()
    );
    assert_eq!(after.read_basis.head.seq, during.committed_seq);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fenced session keeps no position, so reads in its runtime
/// stop starting from the head it last published.
#[tokio::test]
async fn a_fenced_session_keeps_no_read_position() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let rival = crate::LoonFs::builder_with_store(store)
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

/// Checks that a read-only runtime keeps no writer sessions for its reads.
#[tokio::test]
async fn a_read_only_runtime_has_no_read_positions() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let reader = crate::LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("reader");
    assert!(reader.core.inner.head_positions.get().is_none());
    assert!(reader.core.writer_position(namespace.id()).is_none());
    assert!(writer.core.writer_position(namespace.id()).is_some());
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
    assert_eq!(after.read_basis.basis.manifest_no(), folded.manifest_no);
    assert_eq!(after.read_basis.head.seq, before.read_basis.head.seq);
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
    .append_wal_objects(FOLD_AT_WAL_OBJECTS - 1)
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
    assert_eq!(after.read_basis.head.seq, committed.committed_seq);
    writer.shutdown().await.expect("shutdown");
}
