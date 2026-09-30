//! Post-publish cache behavior: a landed publish seeds the read caches
//! instead of dropping them, and no cache lifecycle event — invalidation,
//! LRU eviction, or running with caches disabled — erases writer fencing.

#![allow(clippy::panic)]

use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    CreateNamespaceOptions, CreateSnapshotOptions, DeleteNamespaceOptions, FsMaintenance, FsReader,
    FsWriter, NamespaceId, PutFileOptions, RuntimeCacheConfig, RuntimeError, SharedObjectStore,
    SnapshotPolicy, WriterFence, READ_REVALIDATION_BOUND_MS,
};
use loonfs_api::wire::control::NamespaceStatus;
use loonfs_core::control::NamespaceReadState;
use loonfs_core::limits::WAL_PUBLISH_BUDGET_MS;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use std::sync::Arc;
use tempfile::tempdir;

async fn writer(store: &SharedObjectStore, writer_id: &str) -> FsWriter {
    writer_with_cache(store, writer_id, RuntimeCacheConfig::default()).await
}

async fn writer_with_cache(
    store: &SharedObjectStore,
    writer_id: &str,
    runtime_cache: RuntimeCacheConfig,
) -> FsWriter {
    FsWriter::builder_with_store(store.clone())
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .runtime_cache(runtime_cache)
        .build()
        .await
        .expect("build writer")
}

async fn writer_with_cache_and_metrics(
    store: &SharedObjectStore,
    writer_id: &str,
    runtime_cache: RuntimeCacheConfig,
    recorder: Arc<DefaultMetricsRecorder>,
) -> FsWriter {
    FsWriter::builder_with_store(store.clone())
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .runtime_cache(runtime_cache)
        .metrics_recorder(recorder)
        .build()
        .await
        .expect("build writer")
}

/// What the writer's namespace publishers retain, read off one of the
/// gauges that report it.
fn retention_gauge(recorder: &DefaultMetricsRecorder, name: &str) -> i64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name(name)
        .next()
        .expect("the publisher registers its retention gauges");
    match entry.value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("retention is reported as a gauge, found {other:?}"),
    }
}

/// Decoded bytes the publishers retain once `fence` and `other` each hold
/// the first file the fencing test publishes, with nothing evicted.
async fn first_projections_decoded_bytes(ns_fence: &NamespaceId, ns_other: &NamespaceId) -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = writer_with_cache_and_metrics(
        &store,
        "writer-a",
        RuntimeCacheConfig::default(),
        recorder.clone(),
    )
    .await;
    let mut namespace_writers = Vec::new();
    for (namespace_id, path) in [(ns_fence, "/a1.txt"), (ns_other, "/spill.txt")] {
        writer
            .create_namespace(
                namespace_id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
        let namespace_writer = writer.open_namespace(namespace_id).expect("open namespace");
        namespace_writer
            .put_file_bytes(
                path,
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("first put");
        namespace_writers.push(namespace_writer);
    }
    let bytes = retention_gauge(&recorder, "loonfs.publisher.retained_projection_bytes");
    usize::try_from(bytes).expect("retained bytes are positive")
}

/// Asserts a terminal fencing refusal and hands back the fence it carries.
fn expect_writer_fenced<T: std::fmt::Debug>(result: loonfs::Result<T>, when: &str) -> WriterFence {
    let error = result.expect_err(when);
    assert!(
        matches!(
            &error,
            RuntimeError::Core(core) if core.code() == loonfs::ErrorCode::WriterFenced
        ),
        "{when}: unexpected error: {error:?}"
    );
    match error {
        RuntimeError::Core(loonfs::CoreError::WriterFenced(fence)) => fence,
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
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file_bytes(
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let takeover = namespace_writer_b
        .put_file_bytes(
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");

    let fenced = namespace_writer_a
        .put_file_bytes(
            "/a2.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("superseded writer surfaces fencing");
    assert!(
        matches!(
            &fenced,
            RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
        "unexpected error: {fenced:?}"
    );

    // The fenced session stays fenced on the next attempt too, and the live
    // writer keeps publishing undisturbed.
    let still_fenced = namespace_writer_a
        .put_file_bytes(
            "/a3.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("fenced session never reacquires on its own");
    assert!(
        matches!(
            &still_fenced,
            RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
        "unexpected error: {still_fenced:?}"
    );
    let reader = writer_a.reader();
    let namespace = reader.namespace(&namespace_id);
    let entry = namespace
        .get_path_entry("/b1.txt", Default::default())
        .await
        .expect("fencing refreshes the former writer's reader");
    assert_eq!(entry.head_seq, takeover.committed_seq);
    let bytes = namespace
        .get_file_bytes("/b1.txt")
        .await
        .expect("read the new writer's inline content");
    assert_eq!(bytes.entry.head_seq, entry.head_seq);
    assert_eq!(bytes.bytes, b"b");
    namespace_writer_b
        .put_file_bytes(
            "/b2.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file_bytes(
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_b
        .put_file_bytes(
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");
    expect_writer_fenced(
        namespace_writer_a
            .put_file_bytes(
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
    );
    let head_after_fencing = head_state(&store, &namespace_id).await;

    let fence = expect_writer_fenced(
        namespace_writer_a
            .delete_namespace(DeleteNamespaceOptions::default())
            .await,
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
        .put_file_bytes(
            "/b2.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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

    // Room for either projection but not both: publishing to either
    // namespace evicts the other's.
    let both_projections = first_projections_decoded_bytes(&ns_fence, &ns_other).await;
    let single_projection_cache = RuntimeCacheConfig {
        max_cached_wal_tail_projection_decoded_bytes: both_projections - 1,
        ..RuntimeCacheConfig::default()
    };
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer_a = writer_with_cache_and_metrics(
        &store,
        "writer-a",
        single_projection_cache,
        recorder.clone(),
    )
    .await;
    writer_a
        .create_namespace(
            &ns_fence,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create fence namespace");
    let fence_writer_a = writer_a.open_namespace(&ns_fence).expect("open namespace");
    writer_a
        .create_namespace(
            &ns_other,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create other namespace");
    let other_writer_a = writer_a.open_namespace(&ns_other).expect("open namespace");
    fence_writer_a
        .put_file_bytes(
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    // Publishing to the other namespace pushes the first namespace's
    // projection out of the budget. Its publisher, engine identity, and
    // writer session stay behind.
    other_writer_a
        .put_file_bytes(
            "/spill.txt",
            b"s",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a publishes to the other namespace");
    assert_eq!(
        retention_gauge(&recorder, "loonfs.publisher.retained_projections"),
        1,
        "the budget admits one namespace's projection at a time"
    );

    let writer_b = writer(&store, "writer-b").await;
    let fence_writer_b = writer_b.open_namespace(&ns_fence).expect("open namespace");
    fence_writer_b
        .put_file_bytes(
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");

    // The rebuilt projection publishes under the epoch the session still
    // holds, so the takeover surfaces as fencing instead of a silent
    // re-acquisition.
    let fence = expect_writer_fenced(
        fence_writer_a
            .put_file_bytes(
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
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
        .put_file_bytes(
            "/spill2.txt",
            b"s",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a keeps publishing to the other namespace");

    let hint_raises_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        fence_writer_a
            .put_file_bytes(
                "/a3.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
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
        .put_file_bytes(
            "/b2.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("live writer is not fenced back");
}

#[tokio::test]
async fn fenced_writer_stays_fenced_with_runtime_caches_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("nocache").expect("valid namespace id");
    let counting = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::hint(&namespace_id),
    ));
    let store: SharedObjectStore = counting.clone();

    let writer_a = writer_with_cache(&store, "writer-a", RuntimeCacheConfig::disabled()).await;
    writer_a
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_a
        .put_file_bytes(
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    let namespace_writer_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer_b
        .put_file_bytes(
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");

    expect_writer_fenced(
        namespace_writer_a
            .put_file_bytes(
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
    );

    let hint_raises_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        namespace_writer_a
            .put_file_bytes(
                "/a3.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "fenced session stays fenced with caches disabled",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        hint_raises_after_fencing,
        "a fenced session must not raise the namespace's hint"
    );
    namespace_writer_b
        .put_file_bytes(
            "/b2.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
    let reader = FsReader::builder_with_store(store.clone())
        .monotonic_timer(timer.clone())
        .runtime_cache(RuntimeCacheConfig {
            manifest_revalidation_interval_ms: interval_ms,
            ..Default::default()
        })
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "revalidation-writer").await;
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file_bytes(
            "/file.txt",
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("commit file");
    writer.publisher().drain().await.expect("finish hints");
    namespace
        .get_path_entry("/file.txt", Default::default())
        .await
        .expect("seed reader cache");

    recording.reset();
    timer.advance_ms(READ_REVALIDATION_BOUND_MS);
    namespace
        .get_path_entry("/file.txt", Default::default())
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
        .get_path_entry("/file.txt", Default::default())
        .await
        .expect("revalidate a fresh cached view");
    let operations = recording.take();
    assert!(matches!(
        operations.as_slice(),
        [RecordedOperation::Head { key: manifest_key }, RecordedOperation::Get { key: wal_key, range: None, result_bytes: 0 }]
            if manifest_key.starts_with(&loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id))
                && wal_key.starts_with(&loonfs_objectstore::keys::wal_segment_prefix(&namespace_id))
    ));
}

async fn read_across_a_held_successor_probe(
    reader: &FsReader,
    store: &RecordingStore<BlockingStore<LocalFsStore>>,
    timer: &ManualClock,
    namespace_id: &NamespaceId,
    pause_ms: u64,
) {
    let namespace = reader.namespace(namespace_id);
    store.inner().block_next();
    let (entry, ()) = futures::join!(
        namespace.get_path_entry("/file.txt", Default::default()),
        async {
            store.inner().wait_until_blocked().await;
            timer.advance_ms(pause_ms);
            store.inner().release();
        }
    );
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
    let reader = FsReader::builder_with_store(store.clone())
        .monotonic_timer(timer.clone())
        .runtime_cache(RuntimeCacheConfig {
            manifest_revalidation_interval_ms: interval_ms,
            ..Default::default()
        })
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let writer = writer(&store, "probe-pause-writer").await;
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file_bytes(
            "/file.txt",
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("commit file");
    writer.publisher().drain().await.expect("finish hints");
    namespace
        .get_path_entry("/file.txt", Default::default())
        .await
        .expect("cache a view");
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
        .get_path_entry("/file.txt", Default::default())
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
    runtime_cache: RuntimeCacheConfig,
) -> FsWriter {
    FsWriter::builder_with_store(store.clone())
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .monotonic_timer(timer.clone())
        .runtime_cache(runtime_cache)
        .build()
        .await
        .expect("build writer")
}

#[tokio::test]
async fn a_seeded_view_carries_the_writers_basis_confirmation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("seeded-check").expect("namespace");
    let other_id = NamespaceId::parse("other").expect("namespace");
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
    let writer = writer_with_timer(
        &store,
        "seeded-check-writer",
        &timer,
        RuntimeCacheConfig {
            manifest_revalidation_interval_ms: interval_ms,
            max_cached_namespaces: 1,
            ..Default::default()
        },
    )
    .await;
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let reader = writer.reader();
    let other_namespace = reader.namespace(&other_id);
    let namespace = reader.namespace(&namespace_id);
    for id in [&namespace_id, &other_id] {
        writer
            .create_namespace(
                id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
    }
    let put = |path: &'static str| {
        namespace_writer.put_file_bytes(
            path,
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
    };
    let read = || namespace.get_path_entry("/file.txt", Default::default());
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
    writer.publisher().drain().await.expect("finish hints");
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
    writer.publisher().drain().await.expect("finish hints");
    let wal_no = next_wal_no().await;
    recording.reset();
    read().await.expect("read after the second put");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, wal_no);

    // Reading another namespace evicts the cached view. The third put still
    // plans against the basis confirmed at zero, and seeds a new view.
    other_namespace
        .get_path_entry("/", Default::default())
        .await
        .expect("evict the cached view");
    timer.advance_ms(interval_ms);
    put("/three.txt").await.expect("third put");
    writer.publisher().drain().await.expect("finish hints");
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
        KeyPredicate::exact(hint_key.clone()),
        OperationClass::GetWithMetadata,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let timer = Arc::new(ManualClock::new(0));
    let writer = writer_with_timer(
        &store,
        "reseeded-writer",
        &timer,
        RuntimeCacheConfig {
            manifest_revalidation_interval_ms: 1_000,
            ..Default::default()
        },
    )
    .await;
    let reader = writer.reader();
    let namespace = reader.namespace(&namespace_id);
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("reseeded-maintenance")
        .build()
        .await
        .expect("build maintenance");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file_bytes(
            "/a.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("first put");
    writer.publisher().drain().await.expect("finish hints");

    // The next put starts a budget later, so it discovers the namespace
    // again, and that discovery is held at the hint. Meanwhile the reader
    // checks the old basis, and a fold publishes a new one.
    let check_after_attempt_ms = 10_000;
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS);
    recording.inner().block_next();
    let (put, ()) = futures::join!(
        namespace_writer.put_file_bytes(
            "/b.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        ),
        async {
            recording.inner().wait_until_blocked().await;
            timer.advance_ms(check_after_attempt_ms);
            namespace
                .get_path_entry("/a.txt", Default::default())
                .await
                .expect("check the old basis");
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("publish a new basis");
            recording.inner().release();
        }
    );
    put.expect("put on the new basis");
    writer.publisher().drain().await.expect("finish hints");

    // The seeded view stands on the new basis, confirmed when the put's
    // attempt began. A bound after that, it is rediscovered, although the
    // reader checked the old basis later.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - check_after_attempt_ms);
    recording.reset();
    namespace
        .get_path_entry("/b.txt", Default::default())
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

    let writer = writer_with_cache(
        &store,
        "seed-writer",
        RuntimeCacheConfig {
            manifest_revalidation_interval_ms: u64::MAX,
            ..Default::default()
        },
    )
    .await;
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let reader = writer.reader();
    let namespace = reader.namespace(&namespace_id);
    for index in 0..3 {
        namespace_writer
            .put_file_bytes(
                &format!("/docs/warm-{index}.txt"),
                b"warm",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("warmup put");
    }
    let snapshot = namespace_writer
        .create_snapshot(
            CreateSnapshotOptions {
                name: "pinned".to_owned(),
                expires_at_ms: u64::MAX,
            },
            SnapshotPolicy::default().max_live_per_namespace,
        )
        .await
        .expect("create snapshot");
    namespace
        .get_path_entry("/docs/warm-0.txt", Default::default())
        .await
        .expect("warmup stat");

    recording.take_get_keys();
    namespace_writer
        .put_file_bytes(
            "/docs/fresh.txt",
            b"fresh",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("steady-state put");
    writer
        .publisher()
        .drain()
        .await
        .expect("finish background hints");
    let _pinned = namespace
        .pin_namespace_at_snapshot(&snapshot.checkpoint_id)
        .await
        .expect("a pinned read keeps the live read cache");
    let next_wal_no = head_state(&store, &namespace_id)
        .await
        .wal_no
        .successor()
        .expect("next WAL number");
    recording.reset();
    let before_read = reader.runtime_cache_stats();

    namespace
        .get_path_entry("/docs/fresh.txt", Default::default())
        .await
        .expect("read after write");
    crate::common::assert_wal_probe(recording.take(), &namespace_id, next_wal_no);
    let after_read = reader.runtime_cache_stats();
    assert_eq!(
        after_read.wal_tail_projection_cache_misses,
        before_read.wal_tail_projection_cache_misses
    );
    assert_eq!(
        after_read.wal_tail_projection_cache_hits,
        before_read.wal_tail_projection_cache_hits + 1
    );
}
