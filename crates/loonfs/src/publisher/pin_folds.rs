//! Checkpoint, snapshot, and fork creation fold the WAL tail under the
//! runtime's fold permits.

use super::*;

async fn seed_unfolded_tail<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) {
    append_wal_objects(
        store,
        namespace_id,
        2,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("tail-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed an unfolded WAL tail");
}

async fn wal_tail_objects<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> u64 {
    loonfs_core::cache::load_namespace_diagnostics(store, namespace_id)
        .await
        .expect("load namespace diagnostics")
        .wal_tail_objects
}

async fn writer_over(
    store: SharedStore,
    max_concurrent_folds: NonZeroUsize,
    namespaces: &[&NamespaceId],
) -> crate::LoonFs<crate::Writable> {
    let writer = crate::LoonFs::builder_with_store(store)
        .writer_id("writer-a")
        .max_concurrent_folds(max_concurrent_folds)
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

/// Starts a checkpoint, a snapshot, and a fork of the current head of
/// `namespace_id`, each on its own task, in that order.
fn start_creations(
    writer: &crate::LoonFs<crate::Writable>,
    namespace_id: &NamespaceId,
) -> Vec<JoinHandle<crate::Result<()>>> {
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("pin-maintenance"));
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    let expires_at_ms = writer.now_ms().expect("read the clock") + 60_000;
    let fork_writer = writer.clone();
    let source = namespace_id.clone();
    let target = NamespaceId::parse(format!("{namespace_id}-fork")).expect("fork namespace id");
    let checkpoint_namespace = namespace_id.clone();
    vec![
        tokio::spawn(async move {
            maintenance
                .create_checkpoint(&checkpoint_namespace, "checkpoint")
                .await
                .map(|_| ())
        }),
        tokio::spawn(async move {
            namespace
                .create_snapshot("snapshot", expires_at_ms, &crate::SnapshotPolicy::default())
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
        timeout(Duration::from_secs(10), creation)
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
        NonZeroUsize::new(crate::DEFAULT_MAX_CONCURRENT_FOLDS).expect("nonzero fold limit"),
        &[&unfolded, &folded],
    )
    .await;
    seed_unfolded_tail(store.as_ref(), &unfolded).await;
    let snapshot = writer
        .open_namespace(&folded)
        .expect("open namespace")
        .create_snapshot(
            "fork-basis",
            writer.now_ms().expect("read the clock") + 60_000,
            &crate::SnapshotPolicy::default(),
        )
        .await
        .expect("create the snapshot a fork starts from");
    let unfolded_tail = wal_tail_objects(store.as_ref(), &unfolded).await;
    assert!(unfolded_tail > 0);
    assert_eq!(wal_tail_objects(store.as_ref(), &folded).await, 0);

    let permits = writer
        .mode
        .bits
        .wal_fold_permits
        .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
        .await
        .expect("hold every fold permit");
    let mut creations = start_creations(&writer, &unfolded);
    creations.extend(start_creations(&writer, &folded));
    wait_for_fold_waiters(&writer, creations.len()).await;
    timeout(
        Duration::from_secs(10),
        writer.fork_namespace_with_options(
            &folded,
            &NamespaceId::parse("snapshot-fork").expect("namespace id"),
            &loonfs_test_support::test_actor(),
            &crate::ForkNamespaceOptions {
                snapshot_id: Some(snapshot.checkpoint_id),
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
    assert_eq!(writer.mode.bits.wal_folds_waiting.load(Ordering::SeqCst), 0);
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn creations_started_together_fold_one_at_a_time_at_a_limit_of_one() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(SegmentWriteWatch::new(temp_dir.path()));
    let namespaces = (0..3)
        .map(|index| NamespaceId::parse(format!("pin-fold-{index}")).expect("namespace id"))
        .collect::<Vec<_>>();
    let writer = writer_over(
        store.clone(),
        NonZeroUsize::MIN,
        &namespaces.iter().collect::<Vec<_>>(),
    )
    .await;
    for namespace_id in &namespaces {
        seed_unfolded_tail(&store.inner, namespace_id).await;
    }

    store.peak_namespaces.store(0, AtomicOrdering::SeqCst);
    let creations = namespaces
        .iter()
        .flat_map(|namespace_id| start_creations(&writer, namespace_id))
        .collect::<Vec<_>>();
    finish(creations).await;

    assert_eq!(
        store.peak_namespaces.load(AtomicOrdering::SeqCst),
        1,
        "one fold at a time, across every creation"
    );
    for namespace_id in &namespaces {
        assert_eq!(wal_tail_objects(&store.inner, namespace_id).await, 0);
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
    let writer = writer_over(store.clone(), NonZeroUsize::MIN, &[&namespace_id]).await;
    seed_unfolded_tail(store.as_ref(), &namespace_id).await;

    // The checkpoint takes the permit and parks at its manifest put; the
    // snapshot and the fork wait for that permit.
    store.block_next();
    let creations = start_creations(&writer, &namespace_id);
    store.wait_until_blocked().await;
    wait_for_fold_waiters(&writer, 2).await;
    assert_eq!(writer.mode.bits.wal_fold_permits.available_permits(), 0);

    for creation in &creations {
        creation.abort();
    }
    for creation in creations {
        assert!(creation
            .await
            .expect_err("the creation was aborted")
            .is_cancelled());
    }
    assert_eq!(writer.mode.bits.wal_folds_waiting.load(Ordering::SeqCst), 0);
    assert_eq!(writer.mode.bits.wal_fold_permits.available_permits(), 1);
    store.release();
    writer.shutdown().await.expect("shut down writer");
}
