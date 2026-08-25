//! Post-publish cache behavior: a landed publish seeds the read caches
//! instead of dropping them, and no cache lifecycle event — invalidation,
//! LRU eviction, or running with caches disabled — erases writer fencing.

#![allow(clippy::panic)]

use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, FsWriter, NamespaceId, PutFileOptions,
    RuntimeCacheConfig, RuntimeError, SharedObjectStore, WriterFence,
};
use loonfs_api::wire::control::{HeadState, NamespaceStatus};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass, RecordingStore};
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

/// The WAL-tail projections the writer's namespace publishers retain, read
/// off the gauge that reports them.
fn retained_projections(recorder: &DefaultMetricsRecorder) -> i64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.publisher.retained_projections")
        .next()
        .expect("the publisher registers its retention gauges");
    match entry.value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("retention is reported as a gauge, found {other:?}"),
    }
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

async fn head_state(store: &SharedObjectStore, namespace_id: &NamespaceId) -> HeadState {
    loonfs_core::control::load_namespace_head_control(store, namespace_id)
        .await
        .expect("load head")
        .state
}

#[tokio::test]
async fn fenced_writer_stays_fenced_instead_of_reacquiring() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("fence").expect("valid namespace id");

    let writer_a = writer(&store, "writer-a").await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .put_file_bytes(
            &namespace_id,
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(
            &namespace_id,
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");

    let fenced = writer_a
        .put_file_bytes(
            &namespace_id,
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
    let still_fenced = writer_a
        .put_file_bytes(
            &namespace_id,
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
    writer_b
        .put_file_bytes(
            &namespace_id,
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
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .put_file_bytes(
            &namespace_id,
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(
            &namespace_id,
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");
    expect_writer_fenced(
        writer_a
            .put_file_bytes(
                &namespace_id,
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
    );
    let head_after_fencing = head_state(&store, &namespace_id).await;

    let fence = expect_writer_fenced(
        writer_a
            .delete_namespace(&namespace_id, DeleteNamespaceOptions::default())
            .await,
        "a fenced session must not delete the namespace",
    );
    assert_eq!(fence.active_writer.as_deref(), Some("writer-b"));
    assert_eq!(fence.active_epoch, head_after_fencing.writer_epoch);

    // The namespace is untouched: same epoch, still active, still writer B's.
    let head = head_state(&store, &namespace_id).await;
    assert_eq!(head.status, NamespaceStatus::Active {});
    assert_eq!(head.writer_epoch, head_after_fencing.writer_epoch);
    assert_eq!(head.writer.expect("writer block").writer_id, "writer-b");
    writer_b
        .put_file_bytes(
            &namespace_id,
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
    let counting = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::wal_head(&ns_fence),
    ));
    let store: SharedObjectStore = counting.clone();

    // Room for one projection: publishing to either namespace evicts the
    // other's.
    let single_projection_cache = RuntimeCacheConfig {
        max_cached_namespaces: 1,
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
        .create_namespace(&ns_fence, CreateNamespaceOptions::default())
        .await
        .expect("create fence namespace");
    writer_a
        .create_namespace(&ns_other, CreateNamespaceOptions::default())
        .await
        .expect("create other namespace");
    writer_a
        .put_file_bytes(
            &ns_fence,
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    // Publishing to the other namespace pushes the first namespace's
    // projection out of the budget. Its publisher, engine identity, and
    // writer session stay behind.
    writer_a
        .put_file_bytes(
            &ns_other,
            "/spill.txt",
            b"s",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a publishes to the other namespace");
    assert_eq!(
        retained_projections(&recorder),
        1,
        "the budget admits one namespace's projection at a time"
    );

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(
            &ns_fence,
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
        writer_a
            .put_file_bytes(
                &ns_fence,
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
    );
    let head_after_fencing = head_state(&store, &ns_fence).await;
    assert_eq!(fence.active_epoch, head_after_fencing.writer_epoch);
    assert_eq!(fence.active_writer.as_deref(), Some("writer-b"));

    // More budget pressure, now against a publisher whose session is fenced:
    // fencing is session state, so no eviction can reach it.
    writer_a
        .put_file_bytes(
            &ns_other,
            "/spill2.txt",
            b"s",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a keeps publishing to the other namespace");

    let head_cas_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        writer_a
            .put_file_bytes(
                &ns_fence,
                "/a3.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "fenced session stays fenced after eviction",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        head_cas_after_fencing,
        "a fenced session must not touch the fenced namespace's head"
    );
    let head = head_state(&store, &ns_fence).await;
    assert_eq!(head.status, NamespaceStatus::Active {});
    assert_eq!(head.writer_epoch, head_after_fencing.writer_epoch);
    writer_b
        .put_file_bytes(
            &ns_fence,
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
    let counting = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::wal_head(&namespace_id),
    ));
    let store: SharedObjectStore = counting.clone();

    let writer_a = writer_with_cache(&store, "writer-a", RuntimeCacheConfig::disabled()).await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .put_file_bytes(
            &namespace_id,
            "/a1.txt",
            b"a",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(
            &namespace_id,
            "/b1.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("writer b takes over the epoch");

    expect_writer_fenced(
        writer_a
            .put_file_bytes(
                &namespace_id,
                "/a2.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "superseded writer surfaces fencing",
    );

    let head_cas_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        writer_a
            .put_file_bytes(
                &namespace_id,
                "/a3.txt",
                b"a",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        "fenced session stays fenced with caches disabled",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        head_cas_after_fencing,
        "a fenced session must not touch the namespace's head"
    );
    writer_b
        .put_file_bytes(
            &namespace_id,
            "/b2.txt",
            b"b",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("live writer is not fenced back");
}

#[tokio::test]
async fn read_after_write_is_served_from_seeded_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("seeded").expect("valid namespace id");

    let writer = writer(&store, "seed-writer").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let reader = writer.reader();
    for index in 0..3 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/warm-{index}.txt"),
                b"warm",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("warmup put");
    }
    reader
        .get_path_entry(&namespace_id, "/docs/warm-0.txt", Default::default())
        .await
        .expect("warmup stat");

    recording.take_get_keys();
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/fresh.txt",
            b"fresh",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("steady-state put");
    // The publish must read the live head and root for freshness. Because no
    // root has been published yet, it also reads the floor to distinguish a
    // young namespace from a lost recovery root. The upload session that owns
    // the staged content has to be read to be completed on its own etag.
    let write_gets = recording.take_get_keys();
    let uploads_prefix = loonfs_objectstore::keys::upload_session_prefix(&namespace_id);
    let classify = |suffix: &str| {
        write_gets
            .iter()
            .filter(|key| key.ends_with(suffix))
            .count()
    };
    assert_eq!(classify("/wal/head.json"), 1, "got {write_gets:?}");
    assert_eq!(classify("/metadata/root.json"), 1, "got {write_gets:?}");
    assert_eq!(classify("/wal/floor.json"), 1, "got {write_gets:?}");
    assert_eq!(
        write_gets
            .iter()
            .filter(|key| key.starts_with(&uploads_prefix))
            .count(),
        1,
        "one staging session is opened and completed per put, got {write_gets:?}"
    );
    assert_eq!(
        write_gets.len(),
        4,
        "a steady-state write reads nothing beyond those controls, got {write_gets:?}"
    );

    reader
        .get_path_entry(&namespace_id, "/docs/fresh.txt", Default::default())
        .await
        .expect("read after write");
    let read_gets = recording.take_get_keys();
    assert_eq!(
        read_gets,
        Vec::<String>::new(),
        "read-after-write must be served from the seeded caches"
    );
}
