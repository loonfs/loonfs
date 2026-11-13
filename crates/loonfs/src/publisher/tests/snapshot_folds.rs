//! Snapshot and fork creation fold the WAL tail under the fold permits of
//! the runtime's execution budget.

use super::*;

async fn seed_unfolded_tail<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) {
    loonfs_core::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &WriterAt {
            writer_id: loonfs_types::WriterId::parse("tail-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .append_wal_objects(2)
    .await
    .expect("seed an unfolded WAL tail");
}

async fn wal_tail_objects<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> u64 {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_diagnostics()
        .await
        .expect("load namespace diagnostics")
        .wal_tail_objects
}

async fn writer_over(
    store: SharedStore,
    execution_budget: ExecutionBudget,
    namespaces: &[&NamespaceId],
) -> crate::LoonFs<crate::Writable> {
    let writer = crate::LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .execution_budget(execution_budget)
        .build()
        .await
        .expect("build writer");
    for namespace_id in namespaces {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
    }
    writer
}

/// Starts a snapshot and a fork of the current head of `namespace_id`, each
/// on its own task, in that order.
fn start_creations(
    writer: &crate::LoonFs<crate::Writable>,
    namespace_id: &NamespaceId,
) -> Vec<JoinHandle<crate::Result<()>>> {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    let fork_writer = writer.clone();
    let source = namespace_id.clone();
    let target = NamespaceId::parse(format!("{namespace_id}-fork")).expect("fork namespace id");
    vec![
        tokio::spawn(async move {
            namespace
                .create_snapshot_with_options(&crate::CreateSnapshotOptions {
                    name: Some("snapshot".to_owned()),
                    ..crate::CreateSnapshotOptions::default()
                })
                .await
                .map(|_| ())
        }),
        tokio::spawn(async move {
            fork_writer
                .fork_namespace(&source, &target, &loonfs_test_support::test_actor())
                .await
                .map(|_| ())
        }),
    ]
}

async fn finish(creations: Vec<JoinHandle<crate::Result<()>>>) {
    for creation in creations {
        bounded(Duration::from_secs(10), creation)
            .await
            .expect("the creation finishes")
            .expect("join the creation")
            .expect("create");
    }
}

#[tokio::test]
async fn creations_wait_for_a_fold_permit_whether_or_not_the_tail_is_folded() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let unfolded = NamespaceId::parse("unfolded").expect("namespace id");
    let folded = NamespaceId::parse("folded").expect("namespace id");
    let writer = writer_over(
        store.clone(),
        ExecutionBudget::default(),
        &[&unfolded, &folded],
    )
    .await;
    seed_unfolded_tail(store.as_ref(), &unfolded).await;
    let snapshot = writer
        .open_namespace(&folded)
        .expect("open namespace")
        .create_snapshot_with_options(&crate::CreateSnapshotOptions {
            name: Some("fork-basis".to_owned()),
            ttl_ms: std::num::NonZeroU64::new(60_000),
        })
        .await
        .expect("create the snapshot a fork starts from");
    let unfolded_tail = wal_tail_objects(store.as_ref(), &unfolded).await;
    assert!(unfolded_tail > 0);
    assert_eq!(wal_tail_objects(store.as_ref(), &folded).await, 0);

    let permits = hold_every_fold_permit(&writer).await;
    let mut creations = start_creations(&writer, &unfolded);
    creations.extend(start_creations(&writer, &folded));
    // The creations of one namespace fold under its fold lock, one at a
    // time, so one creation of each namespace waits for a permit, and the
    // other waits for the lock.
    wait_for_fold_waiters(&writer, 2).await;
    bounded(
        Duration::from_secs(10),
        writer.fork_namespace_with_options(
            &folded,
            &NamespaceId::parse("snapshot-fork").expect("namespace id"),
            &loonfs_test_support::test_actor(),
            &crate::ForkNamespaceOptions {
                snapshot_id: Some(snapshot.snapshot_id),
            },
        ),
    )
    .await
    .expect("a fork of a snapshot never folds, so it takes no fold permit")
    .expect("fork the snapshot");
    assert_eq!(
        wal_tail_objects(store.as_ref(), &unfolded).await,
        unfolded_tail,
        "nothing folds while every fold permit is held"
    );

    drop(permits);
    finish(creations).await;
    assert_eq!(wal_tail_objects(store.as_ref(), &unfolded).await, 0);
    assert_eq!(writer.execution_budget().stats().folds_waiting, 0);
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn creations_started_together_fold_one_at_a_time_at_a_limit_of_one() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(segment_write_watch(temp_dir.path()));
    let namespaces = (0..3)
        .map(|index| NamespaceId::parse(format!("snapshot-fold-{index}")).expect("namespace id"))
        .collect::<Vec<_>>();
    let writer = writer_over(
        store.clone(),
        one_fold_at_a_time(),
        &namespaces.iter().collect::<Vec<_>>(),
    )
    .await;
    for namespace_id in &namespaces {
        seed_unfolded_tail(store.inner(), namespace_id).await;
    }

    store.reset_peak();
    let creations = namespaces
        .iter()
        .flat_map(|namespace_id| start_creations(&writer, namespace_id))
        .collect::<Vec<_>>();
    finish(creations).await;

    assert_eq!(
        store.peak_namespaces(),
        1,
        "one fold at a time, across every creation"
    );
    for namespace_id in &namespaces {
        assert_eq!(wal_tail_objects(store.inner(), namespace_id).await, 0);
    }
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn dropped_creations_return_their_fold_permits() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("dropped").expect("namespace id");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        metadata_manifest_prefix(&namespace_id),
    ));
    let writer = writer_over(store.clone(), one_fold_at_a_time(), &[&namespace_id]).await;
    seed_unfolded_tail(store.as_ref(), &namespace_id).await;

    // The snapshot takes the namespace's fold lock and the permit, and parks
    // at its manifest put, and the fork waits for the lock.
    store.block_next();
    let creations = start_creations(&writer, &namespace_id);
    store.wait_until_blocked().await;
    assert_eq!(writer.execution_budget().stats().folds_running, 1);

    for creation in &creations {
        creation.abort();
    }
    for creation in creations {
        assert!(creation
            .await
            .expect_err("the creation was aborted")
            .is_cancelled());
    }
    assert_eq!(
        writer.execution_budget().stats(),
        crate::ExecutionBudgetStats::default()
    );
    store.release();
    writer.shutdown().await.expect("shut down writer");
}

/// Returns how many folds the runtime that reports to `recorder` has run.
fn folds_run(recorder: &DefaultMetricsRecorder) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.publisher.wal_fold_seconds")
        .next()
        .expect("the fold duration is registered");
    match entry.value {
        MetricValue::Histogram { count, .. } => count,
        ref other => panic!("expected a histogram, found {other:?}"),
    }
}

/// Checks that the snapshots that wait for a fold in progress share one
/// fold after it. The first snapshot's fold is parked at its manifest put,
/// three more snapshots arrive, and one more commit is published. The three
/// snapshots then keep the manifest of one fold, which folds the commit, so
/// two folds run and two manifests are published after the park.
#[tokio::test]
async fn snapshots_that_wait_for_a_fold_share_the_next_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("shared-fold").expect("namespace id");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        metadata_manifest_prefix(&namespace_id),
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file("/before.txt", b"before", &actor)
        .await
        .expect("a commit before the folds");
    let created = writer
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics")
        .current_manifest_no
        .expect("a manifest");
    let folds_before = folds_run(&recorder);
    let snapshot = |name: &'static str| {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .create_snapshot_with_options(&crate::CreateSnapshotOptions {
                    name: Some(name.to_owned()),
                    ..crate::CreateSnapshotOptions::default()
                })
                .await
        })
    };

    store.block_next();
    let first = snapshot("first");
    store.wait_until_blocked().await;
    let waiting = ["second", "third", "fourth"].map(snapshot);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    namespace
        .put_file("/after.txt", b"after", &actor)
        .await
        .expect("a commit beside the parked fold");
    store.release();

    let first = bounded(Duration::from_secs(10), first)
        .await
        .expect("the first snapshot finishes")
        .expect("join the first snapshot")
        .expect("the first snapshot");
    assert_eq!(first.snapshot_id.manifest_no().0, created.0 + 1);
    for snapshot in waiting {
        let snapshot = bounded(Duration::from_secs(10), snapshot)
            .await
            .expect("the snapshot finishes")
            .expect("join the snapshot")
            .expect("the snapshot");
        assert_eq!(snapshot.snapshot_id.manifest_no().0, created.0 + 2);
    }
    assert_eq!(folds_run(&recorder) - folds_before, 2);
    writer.shutdown().await.expect("shut down writer");
}
