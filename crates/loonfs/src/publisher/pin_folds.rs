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
                .create_snapshot("snapshot", &crate::CreateSnapshotOptions::default())
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
        .create_snapshot(
            "fork-basis",
            &crate::CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("create the snapshot a fork starts from");
    let unfolded_tail = wal_tail_objects(store.as_ref(), &unfolded).await;
    assert!(unfolded_tail > 0);
    assert_eq!(wal_tail_objects(store.as_ref(), &folded).await, 0);

    let permits = hold_every_fold_permit(&writer).await;
    let mut creations = start_creations(&writer, &unfolded);
    creations.extend(start_creations(&writer, &folded));
    wait_for_fold_waiters(&writer, creations.len()).await;
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
        .map(|index| NamespaceId::parse(format!("pin-fold-{index}")).expect("namespace id"))
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

    // The snapshot takes the permit and parks at its manifest put, and the
    // fork waits for that permit.
    store.block_next();
    let creations = start_creations(&writer, &namespace_id);
    store.wait_until_blocked().await;
    wait_for_fold_waiters(&writer, 1).await;
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
