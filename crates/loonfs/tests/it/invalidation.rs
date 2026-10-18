//! Cache behavior around publishes: reads in the writer's runtime start from
//! the writer session's position, and no cache lifecycle event (invalidation,
//! LRU eviction, or running with zero cache limits) erases writer fencing.

use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    Error, LoonFs, LoonFsBuilder, MetadataCache, NamespaceId, ReadOnly, SharedObjectStore,
    SnapshotPolicy, Writable, WriterFence,
};
use loonfs_core::control::NamespaceReadState;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::WalNo;
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

async fn writer(store: &SharedObjectStore, writer_id: &str) -> LoonFs<Writable> {
    writer_with_cache(store, writer_id, |builder| builder).await
}

/// `cache` sets the writer's metadata cache and read policy.
async fn writer_with_cache(
    store: &SharedObjectStore,
    writer_id: &str,
    cache: impl FnOnce(LoonFsBuilder<Writable>) -> LoonFsBuilder<Writable>,
) -> LoonFs<Writable> {
    cache(
        LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id(writer_id))
            .min_publish_interval_ms(0),
    )
    .build()
    .await
    .expect("build writer")
}

async fn writer_with_cache_and_metrics(
    store: &SharedObjectStore,
    writer_id: &str,
    metadata_cache: MetadataCache,
    recorder: Arc<DefaultMetricsRecorder>,
) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(writer_id))
        .min_publish_interval_ms(0)
        .metadata_cache(metadata_cache)
        .metrics_recorder(recorder)
        .build()
        .await
        .expect("build writer")
}

fn tail_replays(recorder: &DefaultMetricsRecorder) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.publisher.tail_replays")
        .next()
        .expect("the publisher registers its replay counter");
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("replays are reported as a counter, found {other:?}"),
    }
}

/// Asserts a terminal fencing refusal and hands back the fence it carries.
fn expect_writer_fenced<T: std::fmt::Debug>(result: loonfs::Result<T>, when: &str) -> WriterFence {
    let error = result.expect_err(when);
    assert_matches!(
        &error,
        Error::Core(core) if core.code() == loonfs::ErrorCode::WriterFenced,
        "{when}: unexpected error: {error:?}"
    );
    match error {
        Error::Core(loonfs::CoreError::WriterFenced(fence)) => fence,
        other => panic!("{when}: {other:?}"),
    }
}

async fn head_state(store: &SharedObjectStore, namespace_id: &NamespaceId) -> NamespaceReadState {
    loonfs_core::control::load_namespace_read_state(
        store,
        namespace_id,
        loonfs_core::test_support::std_timer(),
    )
    .await
    .expect("load head")
}

#[tokio::test]
async fn fenced_writer_stays_fenced_instead_of_reacquiring() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("fence").expect("valid namespace id");

    let writer_a = writer(&store, "writer-a").await;
    writer_a
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file("/a1.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let takeover = namespace_writer_b
        .put_file("/b1.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("writer b takes over the epoch");

    let fenced = namespace_writer_a
        .put_file("/a2.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect_err("superseded writer surfaces fencing");
    assert_matches!(
        &fenced,
        Error::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced,
        "unexpected error: {fenced:?}"
    );

    // The fenced session stays fenced on the next attempt too, and the live
    // writer keeps publishing undisturbed.
    let still_fenced = namespace_writer_a
        .put_file("/a3.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect_err("fenced session never reacquires on its own");
    assert_matches!(
        &still_fenced,
        Error::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced,
        "unexpected error: {still_fenced:?}"
    );
    let reader = writer_a.read_only();
    let namespace = reader.namespace(&namespace_id);
    let entry = namespace
        .stat("/b1.txt")
        .await
        .expect("fencing refreshes the former writer's reader");
    assert_eq!(entry.head_seq, takeover.committed_seq);
    let bytes = namespace
        .read_file("/b1.txt")
        .await
        .expect("read the new writer's inline content");
    assert_eq!(bytes.entry.head_seq, entry.head_seq);
    assert_eq!(bytes.bytes, b"b");
    namespace_writer_b
        .put_file("/b2.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("live writer is not fenced back");
}

#[tokio::test]
async fn fenced_session_cannot_delete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("fence").expect("valid namespace id");

    let writer_a = writer(&store, "writer-a").await;
    writer_a
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file("/a1.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_b
        .put_file("/b1.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("writer b takes over the epoch");
    expect_writer_fenced(
        namespace_writer_a
            .put_file("/a2.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "superseded writer surfaces fencing",
    );
    let head_after_fencing = head_state(&store, &namespace_id).await;

    let fence = expect_writer_fenced(
        namespace_writer_a.delete().await,
        "a fenced session must not delete the namespace",
    );
    assert_eq!(
        fence
            .active_writer_id
            .as_ref()
            .map(|writer| writer.as_str()),
        Some("writer-b")
    );
    assert_eq!(fence.active_epoch, head_after_fencing.writer_epoch);

    // The namespace is untouched: same epoch, still active, still writer B's.
    let head = head_state(&store, &namespace_id).await;
    assert_eq!(head.status, NamespaceStatus::Active {});
    assert_eq!(head.writer_epoch, head_after_fencing.writer_epoch);
    assert_eq!(
        head.writer.expect("writer block").writer_id.as_str(),
        "writer-b"
    );
    namespace_writer_b
        .put_file("/b2.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("live writer keeps publishing after the refused delete");
}

#[tokio::test]
async fn fenced_writer_stays_fenced_with_zero_cache_limits() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("nocache").expect("valid namespace id");
    let counting = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::hint(&namespace_id),
    ));
    let store: SharedObjectStore = counting.clone();

    let writer_a = writer_with_cache(&store, "writer-a", |builder| {
        builder.metadata_cache(
            MetadataCache::builder()
                .max_segment_bytes(0)
                .max_head_bytes(0)
                .max_head_state_bytes(0)
                .build(),
        )
    })
    .await;
    writer_a
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file("/a1.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_b
        .put_file("/b1.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("writer b takes over the epoch");

    expect_writer_fenced(
        namespace_writer_a
            .put_file("/a2.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "superseded writer surfaces fencing",
    );

    let hint_raises_after_fencing = counting.count(OperationSelector::CompareAndSwap);
    expect_writer_fenced(
        namespace_writer_a
            .put_file("/a3.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "fenced session stays fenced with caches disabled",
    );
    assert_eq!(
        counting.count(OperationSelector::CompareAndSwap),
        hint_raises_after_fencing,
        "a fenced session must not raise the namespace's hint"
    );
    namespace_writer_b
        .put_file("/b2.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("live writer is not fenced back");
}

#[tokio::test]
async fn a_cached_view_older_than_the_revalidation_bound_rediscovers() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("revalidation-bound").expect("namespace");
    let timer = Arc::new(ManualClock::new(0));
    let interval_ms = 1_000;
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .monotonic_timer(timer.clone())
        .manifest_revalidation_interval_ms(interval_ms)
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "revalidation-writer").await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/file.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("commit file");
    writer.drain().await.expect("finish hints");
    namespace
        .stat("/file.txt")
        .await
        .expect("warm the reader cache");

    recording.reset();
    timer.advance_ms(READ_REVALIDATION_BOUND_MS);
    namespace
        .stat("/file.txt")
        .await
        .expect("rediscover at the revalidation bound");
    let operations = recording.take();
    assert_matches!(
        operations.first(),
        Some(RecordedOperation::GetWithMetadata { key, .. })
            if key == &loonfs_objectstore::keys::hint(&namespace_id)
    );

    timer.advance_ms(interval_ms);
    namespace
        .stat("/file.txt")
        .await
        .expect("revalidate a fresh cached view");
    let operations = recording.take();
    assert_matches!(
        operations.as_slice(),
        [
            RecordedOperation::Head { key: manifest_key },
            RecordedOperation::Get {
                key: wal_key,
                range: None,
                result_bytes: 0,
            },
        ] if manifest_key.starts_with(
            &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
        ) && wal_key.starts_with(&loonfs_objectstore::keys::wal_prefix(&namespace_id))
    );
}

async fn read_across_a_held_successor_probe(
    reader: &LoonFs<ReadOnly>,
    store: &RecordingStore<BlockingStore<LocalFsStore>>,
    timer: &ManualClock,
    namespace_id: &NamespaceId,
    pause_ms: u64,
) {
    let namespace = reader.namespace(namespace_id);
    store.inner().block_next();
    let (entry, ()) = futures::join!(namespace.stat("/file.txt"), async {
        store.inner().wait_until_blocked().await;
        timer.advance_ms(pause_ms);
        store.inner().release();
    });
    entry.expect("read across the held successor probe");
}

#[tokio::test]
async fn warm_answers_are_measured_against_the_previous_check() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("probe-pause").expect("namespace");
    let blocking = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace_id,
        )),
        OperationSelector::Head,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let timer = Arc::new(ManualClock::new(0));
    let interval_ms = 1_000;
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .monotonic_timer(timer.clone())
        .manifest_revalidation_interval_ms(interval_ms)
        .max_staleness_ms(0)
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "probe-pause-writer").await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/file.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("commit file");
    writer.drain().await.expect("finish hints");
    namespace.stat("/file.txt").await.expect("cache a view");
    let manifest_no = loonfs_core::control::load_namespace_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest")
        .state
        .manifest()
        .manifest_no;
    let next_wal_no = head_state(&store, &namespace_id)
        .await
        .wal_no
        .successor()
        .expect("next WAL number");
    let warm_check = [
        RecordedOperation::Head {
            key: loonfs_objectstore::keys::metadata_manifest_object(
                &namespace_id,
                &manifest_no.successor().expect("next manifest number"),
            ),
        },
        crate::common::wal_probe(&namespace_id, next_wal_no),
    ];

    // The check falls due just inside the bound, and both answers arrive inside it.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - 2);
    recording.reset();
    read_across_a_held_successor_probe(&reader, &recording, &timer, &namespace_id, 1).await;
    assert_eq!(recording.take(), warm_check);

    // That check replaced the first one, which is now past the bound. A read
    // inside the interval still probes only the WAL.
    timer.advance_ms(interval_ms - 2);
    namespace
        .stat("/file.txt")
        .await
        .expect("read inside the interval");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, next_wal_no);

    // The next HEAD is sent just inside the bound of that check and answered
    // at the bound. Both answers find nothing, and the view is not served.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - interval_ms);
    read_across_a_held_successor_probe(&reader, &recording, &timer, &namespace_id, 1).await;
    let operations = recording.take();
    assert_eq!(operations[..2], warm_check, "{operations:?}");
    assert_matches!(
        operations.get(2),
        Some(RecordedOperation::GetWithMetadata { key, .. })
            if key == &loonfs_objectstore::keys::hint(&namespace_id),
        "the late answer was trusted: {operations:?}"
    );
}

/// Checks that a warm read rediscovers from the current manifest when its WAL
/// probe finds a missing number below a present one, as it can after a fold and
/// a collection pass.
#[tokio::test]
async fn a_warm_read_rediscovers_when_its_wal_probe_finds_a_collected_gap() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("collected-gap").expect("namespace");
    // The reader's clock doesn't move, so its manifest check doesn't fall due,
    // and with a maximum staleness of 0 each warm read starts with the WAL
    // probe.
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .max_staleness_ms(0)
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "collected-gap-writer").await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    let put = |path: &'static str| namespace_writer.put_file(path, b"file", &actor);
    put("/cached.txt").await.expect("first put");
    writer.drain().await.expect("finish hints");
    namespace.stat("/cached.txt").await.expect("cache a view");
    let tip = head_state(&store, &namespace_id).await.wal_no.0;

    // A fold moves WAL objects tip + 1 to tip + 3 into a newer manifest, and
    // tip + 4 is written after it.
    for path in ["/one.txt", "/two.txt", "/three.txt"] {
        put(path).await.expect(path);
    }
    writer
        .maintenance(loonfs_test_support::ids::writer_id("collected-gap-writer"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold the WAL");
    put("/four.txt").await.expect("put after the fold");
    writer.drain().await.expect("finish hints");
    let state = head_state(&store, &namespace_id).await;
    assert_eq!(
        (state.folded_wal_no, state.wal_no),
        (WalNo(tip + 3), WalNo(tip + 4))
    );

    // Garbage collection deletes the WAL objects below the folded boundary. A
    // collection pass that has deleted tip + 2 but not yet tip + 3 leaves the
    // gap that the probe's second window reads.
    store
        .delete(&crate::common::wal_key(&namespace_id, WalNo(tip + 2)))
        .await
        .expect("collect a folded WAL object");
    for path in [
        "/cached.txt",
        "/one.txt",
        "/two.txt",
        "/three.txt",
        "/four.txt",
    ] {
        namespace.stat(path).await.expect(path);
    }
}

/// Checks that a warm read reloads its view when the view passes the
/// revalidation bound while the WAL probe runs, even if the probe fails.
#[tokio::test]
async fn a_view_that_passes_the_bound_during_a_failed_wal_probe_is_reloaded() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("late-probe-error").expect("namespace");
    let wal_keys = || KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(&namespace_id));
    let blocking = Arc::new(BlockingStore::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            wal_keys(),
            OperationSelector::Get,
            InjectedError::Transport("injected WAL GET failure".to_owned()),
        ),
        wal_keys(),
        OperationSelector::Get,
    ));
    let store: SharedObjectStore = blocking.clone();
    let timer = Arc::new(ManualClock::new(0));
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .monotonic_timer(timer.clone())
        .max_staleness_ms(0)
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "late-probe-error-writer").await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    namespace_writer
        .put_file("/cached.txt", b"file", &actor)
        .await
        .expect("first put");
    writer.drain().await.expect("finish hints");
    namespace.stat("/cached.txt").await.expect("cache a view");
    namespace_writer
        .put_file("/next.txt", b"file", &actor)
        .await
        .expect("put after the cached view");
    writer.drain().await.expect("finish hints");

    // Park the probe's first GET until the view is past the bound, and then
    // fail it.
    blocking.block_next();
    blocking.inner().fail_next(1);
    let (entry, ()) = futures::join!(namespace.stat("/next.txt"), async {
        blocking.wait_until_blocked().await;
        timer.advance_ms(READ_REVALIDATION_BOUND_MS);
        blocking.release();
    });
    assert_eq!(blocking.inner().remaining(), 0, "the probe's GET failed");
    entry.expect("reload the view instead of returning the probe's error");
}

/// Creates `other_id` from a writer of its own and gives it one 16 KiB
/// inline file, so its head state outweighs a namespace of a few small files.
async fn fill_other(store: &SharedObjectStore, other_id: &NamespaceId) {
    let setup = writer(store, "other-setup").await;
    setup
        .create_namespace(other_id, &loonfs_test_support::test_actor())
        .await
        .expect("create other namespace");
    setup
        .open_namespace(other_id)
        .expect("open other namespace")
        .put_file(
            "/large.bin",
            &[b'x'; 16 * 1024],
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("put large file");
    setup.drain().await.expect("finish hints");
}

/// What a cold read of the root of `other_id` caches as head state once
/// [`fill_other`] has written it, measured on a store of its own.
async fn other_head_state_bytes(other_id: &NamespaceId) -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    fill_other(&store, other_id).await;
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("build reader");
    reader
        .namespace(other_id)
        .stat("/")
        .await
        .expect("cold read");
    reader.metadata_cache().stats().head_state_bytes
}

/// Checks that a read after a write in the writer's runtime sends no
/// request and reads the WAL tail that the writer's position keeps, even
/// after a snapshot read that loads another view.
#[tokio::test]
async fn a_read_after_a_write_sends_no_request_and_replays_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("read-after-write").expect("valid namespace id");

    let writer = writer_with_cache(&store, "read-after-write-writer", |builder| {
        builder.max_staleness_ms(READ_REVALIDATION_BOUND_MS)
    })
    .await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    for index in 0..3 {
        namespace_writer
            .put_file(
                &format!("/docs/warm-{index}.txt"),
                b"warm",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("warmup put");
    }
    let snapshot = namespace_writer
        .create_snapshot("pinned", u64::MAX, &SnapshotPolicy::default())
        .await
        .expect("create snapshot");
    namespace
        .stat("/docs/warm-0.txt")
        .await
        .expect("warmup stat");

    recording.take_get_keys();
    namespace_writer
        .put_file(
            "/docs/fresh.txt",
            b"fresh",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("steady-state put");
    writer.drain().await.expect("finish background hints");
    let _view = namespace
        .read_view_at_snapshot(&snapshot.snapshot_id)
        .await
        .expect("a snapshot read view keeps the writer's tail");
    recording.reset();
    let before_read = reader.metadata_cache().stats();

    namespace
        .stat("/docs/fresh.txt")
        .await
        .expect("read after write");
    assert_eq!(recording.take(), []);
    let after_read = reader.metadata_cache().stats();
    assert_eq!(after_read.wal_tail_misses, before_read.wal_tail_misses);
    assert_eq!(
        after_read.wal_tail_hits, before_read.wal_tail_hits,
        "the read takes the position's tail, not one from the read cache"
    );
}

/// Checks that reads in the writer's runtime after a fold see the folded
/// manifest, and that the next read sends no request and takes the tail
/// that the writer's position keeps after the fold.
#[tokio::test]
async fn reads_after_maintenance_are_current_and_reuse_their_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("maintained").expect("valid namespace id");
    let writer = writer_with_cache(&store, "maintained-writer", |builder| {
        builder.max_staleness_ms(READ_REVALIDATION_BOUND_MS)
    })
    .await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/file.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("put file");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    namespace
        .stat("/file.txt")
        .await
        .expect("read at the writer's position");

    writer
        .maintenance(loonfs_test_support::ids::writer_id("maintained-writer"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold the tail into a new manifest");
    let file = namespace
        .read_file("/file.txt")
        .await
        .expect("read after maintenance");
    assert_eq!(file.bytes, b"file");

    writer.drain().await.expect("finish hints");
    recording.reset();
    let before_read = reader.metadata_cache().stats();
    namespace.stat("/file.txt").await.expect("next read");
    assert_eq!(recording.take(), []);
    let after_read = reader.metadata_cache().stats();
    assert_eq!(after_read.wal_tail_misses, before_read.wal_tail_misses);
    assert_eq!(
        after_read.wal_tail_hits, before_read.wal_tail_hits,
        "the read takes the position's tail, not one from the read cache"
    );
}

/// Checks that reads that fill the shared head-state cache leave an idle
/// writer's tail alone, so the writer's next publish reads no tail from the
/// store.
#[tokio::test]
async fn read_pressure_leaves_an_idle_writer_tail_and_its_next_publish_replays_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let namespace_id = NamespaceId::parse("idle-writer").expect("namespace");
    let other_id = NamespaceId::parse("other").expect("namespace");
    // The budget fits exactly the other namespace's head state, so one read
    // of it fills the cache.
    let cache = MetadataCache::builder()
        .max_head_state_bytes(other_head_state_bytes(&other_id).await)
        .build();
    fill_other(&store, &other_id).await;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        writer_with_cache_and_metrics(&store, "idle-writer", cache.clone(), recorder.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for path in ["/first.txt", "/second.txt"] {
        namespace
            .put_file(path, b"warm", &loonfs_test_support::test_actor())
            .await
            .expect("warm put");
    }
    let replays = tail_replays(&recorder);

    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("reader on the shared cache");
    reader
        .namespace(&other_id)
        .stat("/")
        .await
        .expect("read the other namespace");
    assert!(cache.stats().head_state_bytes > 0);

    let landed = namespace
        .put_file(
            "/after-pressure.txt",
            b"after",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("the publish succeeds after the read pressure");
    assert_eq!(
        tail_replays(&recorder),
        replays,
        "the writer's position keeps its tail whatever reads fill the cache"
    );
    let entry = reader
        .namespace(&namespace_id)
        .stat("/after-pressure.txt")
        .await
        .expect("read the landed file");
    assert_eq!(entry.head_seq, landed.committed_seq);
}
