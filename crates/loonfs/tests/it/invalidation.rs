//! Post-publish cache behavior: a landed publish seeds the read caches
//! instead of dropping them, and no cache lifecycle event — invalidation,
//! LRU eviction, or running with zero cache limits — erases writer fencing.

#![allow(clippy::panic)]

use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    Error, LoonFs, LoonFsBuilder, MetadataCache, NamespaceId, ReadOnly, SharedObjectStore,
    SnapshotPolicy, Writable, WriterFence,
};
use loonfs_core::control::NamespaceReadState;
use loonfs_core::limits::WAL_PUBLISH_BUDGET_MS;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_types::format::control::NamespaceStatus;
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
            .writer_id(writer_id)
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
        .writer_id(writer_id)
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

/// Head state the writer holds once `fence` and `other` each hold the first
/// file the fencing test publishes, with nothing evicted.
async fn first_head_state_bytes(ns_fence: &NamespaceId, ns_other: &NamespaceId) -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let writer = writer(&store, "writer-a").await;
    let mut namespaces = Vec::new();
    for (namespace_id, path) in [(ns_fence, "/a1.txt"), (ns_other, "/spill.txt")] {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer.open_namespace(namespace_id).expect("open namespace");
        namespace
            .put_file(path, b"a", &loonfs_test_support::test_actor())
            .await
            .expect("first put");
        namespaces.push(namespace);
    }
    writer.metadata_cache().stats().head_state_bytes
}

/// Asserts a terminal fencing refusal and hands back the fence it carries.
fn expect_writer_fenced<T: std::fmt::Debug>(result: loonfs::Result<T>, when: &str) -> WriterFence {
    let error = result.expect_err(when);
    assert!(
        matches!(
            &error,
            Error::Core(core) if core.code() == loonfs::ErrorCode::WriterFenced
        ),
        "{when}: unexpected error: {error:?}"
    );
    match error {
        Error::Core(loonfs::CoreError::WriterFenced(fence)) => fence,
        other => panic!("{when}: {other:?}"),
    }
}

async fn head_state(store: &SharedObjectStore, namespace_id: &NamespaceId) -> NamespaceReadState {
    loonfs_core::control::load_namespace_read_state(store, namespace_id)
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
    assert!(
        matches!(
            &fenced,
            Error::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
        "unexpected error: {fenced:?}"
    );

    // The fenced session stays fenced on the next attempt too, and the live
    // writer keeps publishing undisturbed.
    let still_fenced = namespace_writer_a
        .put_file("/a3.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect_err("fenced session never reacquires on its own");
    assert!(
        matches!(
            &still_fenced,
            Error::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
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
async fn fenced_writer_stays_fenced_after_its_tail_projection_is_evicted() {
    let temp_dir = tempdir().expect("tempdir");
    let ns_fence = NamespaceId::parse("fence").expect("valid namespace id");
    let ns_other = NamespaceId::parse("other").expect("valid namespace id");
    let counting = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::hint(&ns_fence),
    ));
    let store: SharedObjectStore = counting.clone();

    // One byte short of both namespaces' head state: publishing to the
    // other namespace evicts the oldest entry, the fence namespace's tail.
    let both_namespaces = first_head_state_bytes(&ns_fence, &ns_other).await;
    let writer_a = writer_with_cache(&store, "writer-a", |builder| {
        builder.metadata_cache(
            MetadataCache::builder()
                .max_head_state_bytes(both_namespaces - 1)
                .build(),
        )
    })
    .await;
    writer_a
        .create_namespace(&ns_fence, &loonfs_test_support::test_actor())
        .await
        .expect("create fence namespace");
    let fence_writer_a = writer_a.open_namespace(&ns_fence).expect("open namespace");
    writer_a
        .create_namespace(&ns_other, &loonfs_test_support::test_actor())
        .await
        .expect("create other namespace");
    let other_writer_a = writer_a.open_namespace(&ns_other).expect("open namespace");
    fence_writer_a
        .put_file("/a1.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("writer a first put");

    // Publishing to the other namespace pushes the first namespace's tail
    // out of the cache. Its publisher, engine position, and writer session
    // stay behind.
    other_writer_a
        .put_file("/spill.txt", b"s", &loonfs_test_support::test_actor())
        .await
        .expect("writer a publishes to the other namespace");
    assert!(writer_a.metadata_cache().stats().head_state_evictions > 0);
    let misses_before_fencing = writer_a.metadata_cache().stats().wal_tail_misses;

    let writer_b = writer(&store, "writer-b").await;
    let fence_writer_b = writer_b.open_namespace(&ns_fence).expect("open namespace");
    fence_writer_b
        .put_file("/b1.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("writer b takes over the epoch");

    // The rebuilt projection publishes under the epoch the session still
    // holds, so the takeover surfaces as fencing instead of a silent
    // re-acquisition.
    let fence = expect_writer_fenced(
        fence_writer_a
            .put_file("/a2.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "superseded writer surfaces fencing",
    );
    assert_eq!(
        writer_a.metadata_cache().stats().wal_tail_misses,
        misses_before_fencing + 1,
        "the evicted tail sends the fenced publish to the store"
    );
    let head_after_fencing = head_state(&store, &ns_fence).await;
    assert_eq!(fence.active_epoch, head_after_fencing.writer_epoch);
    assert_eq!(
        fence
            .active_writer_id
            .as_ref()
            .map(|writer| writer.as_str()),
        Some("writer-b")
    );

    // More budget pressure, now against a publisher whose session is fenced:
    // fencing is session state, so no eviction can reach it.
    other_writer_a
        .put_file("/spill2.txt", b"s", &loonfs_test_support::test_actor())
        .await
        .expect("writer a keeps publishing to the other namespace");

    let hint_raises_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        fence_writer_a
            .put_file("/a3.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "fenced session stays fenced after eviction",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        hint_raises_after_fencing,
        "a fenced session must not raise the fenced namespace's hint"
    );
    let head = head_state(&store, &ns_fence).await;
    assert_eq!(head.status, NamespaceStatus::Active {});
    assert_eq!(head.writer_epoch, head_after_fencing.writer_epoch);
    fence_writer_b
        .put_file("/b2.txt", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("live writer is not fenced back");
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

    let hint_raises_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        namespace_writer_a
            .put_file("/a3.txt", b"a", &loonfs_test_support::test_actor())
            .await,
        "fenced session stays fenced with caches disabled",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
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
        .expect("seed reader cache");

    recording.reset();
    timer.advance_ms(READ_REVALIDATION_BOUND_MS);
    namespace
        .stat("/file.txt")
        .await
        .expect("rediscover at the revalidation bound");
    let operations = recording.take();
    assert!(matches!(
        operations.first(),
        Some(RecordedOperation::GetWithMetadata { key, .. })
            if key == &loonfs_objectstore::keys::hint(&namespace_id)
    ));

    timer.advance_ms(interval_ms);
    namespace
        .stat("/file.txt")
        .await
        .expect("revalidate a fresh cached view");
    let operations = recording.take();
    assert!(matches!(
        operations.as_slice(),
        [RecordedOperation::Head { key: manifest_key }, RecordedOperation::Get { key: wal_key, range: None, result_bytes: 0 }]
            if manifest_key.starts_with(&loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id))
                && wal_key.starts_with(&loonfs_objectstore::keys::wal_prefix(&namespace_id))
    ));
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
        OperationClass::Head,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
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
    assert!(
        matches!(
            operations.get(2),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if key == &loonfs_objectstore::keys::hint(&namespace_id)
        ),
        "the late answer was trusted: {operations:?}"
    );
}

async fn writer_with_timer(
    store: &SharedObjectStore,
    writer_id: &str,
    timer: &Arc<ManualClock>,
    cache: impl FnOnce(LoonFsBuilder<Writable>) -> LoonFsBuilder<Writable>,
) -> LoonFs<Writable> {
    writer_with_cache(store, writer_id, |builder| {
        cache(builder.monotonic_timer(timer.clone()))
    })
    .await
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

#[tokio::test]
async fn a_seeded_view_carries_the_writers_basis_confirmation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("seeded-check").expect("namespace");
    let blocking = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace_id,
        )),
        OperationClass::Head,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let timer = Arc::new(ManualClock::new(0));
    let interval_ms = 1_000;
    let writer = writer_with_timer(&store, "seeded-check-writer", &timer, |builder| {
        builder.manifest_revalidation_interval_ms(interval_ms)
    })
    .await;
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let actor = loonfs_test_support::test_actor();
    let put = |path: &'static str| namespace_writer.put_file(path, b"file", &actor);
    let read = || namespace.stat("/file.txt");
    let next_wal_no = || async {
        head_state(&store, &namespace_id)
            .await
            .wal_no
            .successor()
            .expect("next WAL number")
    };

    // The first put discovers the namespace, which confirms its basis, and
    // the read right after it probes only the WAL.
    put("/file.txt").await.expect("first put");
    writer.drain().await.expect("finish hints");
    let wal_no = next_wal_no().await;
    recording.reset();
    read().await.expect("read after the first put");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, wal_no);

    // A seed on the same basis keeps a later check by the reader, so a read
    // right after a put in a busy writer still probes only the WAL.
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS - 2 * interval_ms);
    read().await.expect("a due check");
    timer.advance_ms(interval_ms - 1);
    put("/two.txt").await.expect("second put");
    writer.drain().await.expect("finish hints");
    let wal_no = next_wal_no().await;
    recording.reset();
    read().await.expect("read after the second put");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, wal_no);

    // A collection pass drops the cached view but not the writer's tail.
    // The third put still plans against the basis confirmed at zero, and
    // seeds a new view.
    writer
        .maintenance(loonfs_test_support::ids::writer_id("seeded-check-gc"))
        .gc(&namespace_id)
        .await
        .expect("drop the cached view");
    timer.advance_ms(interval_ms);
    put("/three.txt").await.expect("third put");
    writer.drain().await.expect("finish hints");
    let manifest_no = loonfs_core::control::load_namespace_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest")
        .state
        .manifest()
        .manifest_no;
    let wal_no = next_wal_no().await;

    // The seeded view is younger than the bound, but its basis was confirmed
    // a bound before the answers arrive, so the view is not served.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - WAL_PUBLISH_BUDGET_MS);
    recording.reset();
    read_across_a_held_successor_probe(&reader, &recording, &timer, &namespace_id, 1).await;
    let operations = recording.take();
    assert_eq!(
        operations[..2],
        [
            RecordedOperation::Head {
                key: loonfs_objectstore::keys::metadata_manifest_object(
                    &namespace_id,
                    &manifest_no.successor().expect("next manifest number"),
                ),
            },
            crate::common::wal_probe(&namespace_id, wal_no),
        ],
        "{operations:?}"
    );
    assert!(
        matches!(
            operations.get(2),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if key == &loonfs_objectstore::keys::hint(&namespace_id)
        ),
        "the seed claimed a later check: {operations:?}"
    );
}

#[tokio::test]
async fn a_seed_on_another_basis_does_not_keep_the_cached_check() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("reseeded").expect("namespace");
    let hint_key = loonfs_objectstore::keys::hint(&namespace_id);
    let blocking = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationClass::Head,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let timer = Arc::new(ManualClock::new(0));
    let writer = writer_with_timer(&store, "reseeded-writer", &timer, |builder| {
        builder.manifest_revalidation_interval_ms(1_000)
    })
    .await;
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id("reseeded-maintenance")
        .build()
        .await
        .expect("build maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("reseeded-maintenance"));
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/a.txt", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("first put");
    writer.drain().await.expect("finish hints");

    // The quiet writer confirms its tip. Hold the successor HEAD while the
    // reader checks the old basis and a fold publishes a new one. The held
    // HEAD then finds that fold and the writer reloads.
    let check_after_attempt_ms = 10_000;
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    recording.inner().block_next();
    let actor = loonfs_test_support::test_actor();
    let (put, ()) = futures::join!(namespace_writer.put_file("/b.txt", b"b", &actor), async {
        recording.inner().wait_until_blocked().await;
        timer.advance_ms(check_after_attempt_ms);
        namespace.stat("/a.txt").await.expect("check the old basis");
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("publish a new basis");
        recording.inner().release();
    });
    put.expect("put on the new basis");
    writer.drain().await.expect("finish hints");

    // The seeded view stands on the new basis, confirmed when the put's
    // attempt began. A bound after that, it is rediscovered, although the
    // reader checked the old basis later.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - check_after_attempt_ms);
    recording.reset();
    namespace
        .stat("/b.txt")
        .await
        .expect("read on the new basis");
    let operations = recording.take();
    assert!(
        matches!(
            operations.first(),
            Some(RecordedOperation::GetWithMetadata { key, .. }) if key == &hint_key
        ),
        "the seed kept the old basis's check: {operations:?}"
    );
}

#[tokio::test]
async fn read_after_write_only_probes_the_next_wal_number_without_replay() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("seeded").expect("valid namespace id");

    let writer = writer_with_cache(&store, "seed-writer", |builder| {
        builder.manifest_revalidation_interval_ms(u64::MAX)
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
        .read_view_at_snapshot(&snapshot.checkpoint_id)
        .await
        .expect("a snapshot read view keeps the live read cache");
    let next_wal_no = head_state(&store, &namespace_id)
        .await
        .wal_no
        .successor()
        .expect("next WAL number");
    recording.reset();
    let before_read = reader.metadata_cache().stats();

    namespace
        .stat("/docs/fresh.txt")
        .await
        .expect("read after write");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, next_wal_no);
    let after_read = reader.metadata_cache().stats();
    assert_eq!(after_read.wal_tail_misses, before_read.wal_tail_misses);
    assert_eq!(after_read.wal_tail_hits, before_read.wal_tail_hits + 1);
}

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
        builder.manifest_revalidation_interval_ms(u64::MAX)
    })
    .await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file("/file.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("put file");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    namespace
        .stat("/file.txt")
        .await
        .expect("read the seeded view");

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
    let next_wal_no = head_state(&store, &namespace_id)
        .await
        .wal_no
        .successor()
        .expect("next WAL number");
    recording.reset();
    let before_read = reader.metadata_cache().stats();
    namespace.stat("/file.txt").await.expect("next read");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, next_wal_no);
    let after_read = reader.metadata_cache().stats();
    assert_eq!(after_read.wal_tail_misses, before_read.wal_tail_misses);
    assert_eq!(after_read.wal_tail_hits, before_read.wal_tail_hits + 1);
}

#[tokio::test]
async fn read_pressure_evicts_an_idle_writer_tail_and_its_next_publish_replays_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let namespace_id = NamespaceId::parse("idle-writer").expect("namespace");
    let other_id = NamespaceId::parse("other").expect("namespace");
    // The budget holds exactly the other namespace's head state, which
    // outweighs the writer's, so one read of it evicts the writer's tail.
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
    assert!(cache.stats().head_state_evictions > 0);

    let landed = namespace
        .put_file(
            "/after-pressure.txt",
            b"after",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("the publish lands after its tail was evicted");
    assert_eq!(
        tail_replays(&recorder),
        replays + 1,
        "the evicted tail is read back from the store exactly once"
    );
    let entry = reader
        .namespace(&namespace_id)
        .stat("/after-pressure.txt")
        .await
        .expect("read the landed file");
    assert_eq!(entry.head_seq, landed.committed_seq);
}

#[tokio::test]
async fn a_tail_evicted_while_its_publish_runs_returns_when_the_publish_lands() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("busy-writer").expect("namespace");
    let other_id = NamespaceId::parse("other").expect("namespace");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(&namespace_id)),
        OperationClass::PutCreateIfAbsent,
    ));
    let store: SharedObjectStore = blocking.clone();
    let cache = MetadataCache::builder()
        .max_head_state_bytes(other_head_state_bytes(&other_id).await)
        .build();
    fill_other(&store, &other_id).await;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        writer_with_cache_and_metrics(&store, "busy-writer", cache.clone(), recorder.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    let put = |path: &'static str| namespace.put_file(path, b"busy", &actor);
    put("/first.txt").await.expect("first put");
    let replays = tail_replays(&recorder);
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("reader on the shared cache");

    // The put holds its tail while its WAL put is parked. Reading the other
    // namespace evicts that tail from the cache meanwhile.
    blocking.block_next();
    let (landed, ()) = futures::join!(put("/second.txt"), async {
        blocking.wait_until_blocked().await;
        reader
            .namespace(&other_id)
            .stat("/")
            .await
            .expect("read the other namespace");
        assert!(cache.stats().head_state_evictions > 0);
        blocking.release();
    });
    landed.expect("the put lands with the tail it held");
    put("/third.txt")
        .await
        .expect("the next put starts from the landed tail");
    assert_eq!(
        tail_replays(&recorder),
        replays,
        "the landed put put its tail back, so nothing was read again"
    );
}
