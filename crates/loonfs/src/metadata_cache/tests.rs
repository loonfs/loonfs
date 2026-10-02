//! One metadata cache shared by runtimes over different stores: scoping,
//! one capacity, binding lifetimes, per-binding policy, and metrics.

#![allow(clippy::panic)]

use super::MetadataCache;
use crate::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder, MetricsSnapshot};
use crate::{LoonFs, NamespaceId, ReadOnly, SharedObjectStore, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

const FOLDED: &str = "/folded/file.txt";
const TAIL: &str = "/tail.txt";

#[derive(Clone, Copy, Debug)]
enum PinCreation {
    Checkpoint,
    Snapshot,
    Fork,
}

async fn pin_fold_reads(
    creation: PinCreation,
    max_block_memo_bytes: usize,
    max_segment_bytes: usize,
) -> usize {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(
        LocalFsStore::new(root.path()).expect("store"),
    ));
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id("pin-writer")
        .max_block_memo_bytes(max_block_memo_bytes)
        .metadata_cache(
            MetadataCache::builder()
                .max_segment_bytes(max_segment_bytes)
                .build(),
        )
        .build()
        .await
        .expect("build writer");
    runtime
        .create_namespace(&demo(), &test_actor())
        .await
        .expect("create namespace");
    let namespace = runtime.open_namespace(&demo()).expect("open namespace");
    for path in ["/first", "/second"] {
        namespace
            .create_directory(path, &test_actor())
            .await
            .expect("create directory");
    }
    let maintenance = runtime.maintenance(writer_id("pin-maintenance"));
    maintenance
        .fold_wal(&demo())
        .await
        .expect("fold directories");
    for path in ["/first", "/second"] {
        namespace
            .delete_path(path, &test_actor())
            .await
            .expect("delete directory");
    }
    assert!(
        loonfs_core::cache::load_namespace_diagnostics(store.as_ref(), &demo())
            .await
            .expect("diagnostics before creation")
            .wal_tail_objects
            > 0
    );
    store.reset();
    match creation {
        PinCreation::Checkpoint => {
            maintenance
                .create_checkpoint(&demo(), "checkpoint")
                .await
                .expect("create checkpoint");
        }
        PinCreation::Snapshot => {
            namespace
                .create_snapshot(
                    "snapshot",
                    runtime.now_ms().expect("read clock") + 60_000,
                    &crate::SnapshotPolicy::default(),
                )
                .await
                .expect("create snapshot");
        }
        PinCreation::Fork => {
            runtime
                .fork_namespace(&demo(), &namespace_id("fork"), &test_actor())
                .await
                .expect("fork head");
        }
    }
    let reads = store.count(OperationClass::Read);
    assert_eq!(
        loonfs_core::cache::load_namespace_diagnostics(store.as_ref(), &demo())
            .await
            .expect("diagnostics after creation")
            .wal_tail_objects,
        0
    );
    runtime.shutdown().await.expect("shut down writer");
    reads
}

#[tokio::test]
async fn pin_folds_use_the_runtime_block_memo_and_segment_cache() {
    for creation in [
        PinCreation::Checkpoint,
        PinCreation::Snapshot,
        PinCreation::Fork,
    ] {
        let small_memo_reads = pin_fold_reads(creation, 1, 0).await;
        let default_memo_reads = pin_fold_reads(creation, 64 * 1024 * 1024, 0).await;
        let warm_cache_reads = pin_fold_reads(creation, 1, 1024 * 1024).await;
        assert!(default_memo_reads > 0, "{creation:?} reads deletion roots");
        assert!(
            small_memo_reads > default_memo_reads,
            "{creation:?}: a one-byte memo must reread blocks: {small_memo_reads} against {default_memo_reads}"
        );
        assert_eq!(warm_cache_reads, 0, "{creation:?} reuses cached blocks");
    }
}

fn demo() -> NamespaceId {
    namespace_id("demo")
}

fn local_store(root: &Path) -> SharedObjectStore {
    Arc::new(LocalFsStore::new(root).expect("create local-fs store"))
}

/// Writes the same operations into the store under `root`, so every store
/// this builds has the same manifest numbers, head sequences, and WAL-tail
/// keys. Only the file bytes differ. One file sits in metadata segments and
/// one in the WAL tail.
async fn same_shape_store(
    root: &Path,
    cache: &MetadataCache,
    bytes: &'static [u8],
) -> LoonFs<Writable> {
    let writer = LoonFs::builder_with_store(local_store(root))
        .writer_id("same-shape-writer")
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&demo(), &test_actor())
        .await
        .expect("create namespace");
    let namespace = writer.open_namespace(&demo()).expect("open namespace");
    namespace
        .put_file(FOLDED, bytes, &test_actor())
        .await
        .expect("put the folded file");
    writer
        .maintenance(writer_id("same-shape-maintenance"))
        .fold_wal(&demo())
        .await
        .expect("fold the first file into segments");
    namespace
        .put_file(TAIL, bytes, &test_actor())
        .await
        .expect("put the tail file");
    writer
}

async fn reader(root: &Path, cache: &MetadataCache) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(local_store(root))
        .read_only()
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("build reader")
}

async fn assert_reads(runtime: &LoonFs<ReadOnly>, bytes: &[u8]) {
    let namespace = runtime.namespace(&demo());
    for path in [FOLDED, TAIL] {
        let file = namespace.read_file(path).await.expect("read file");
        assert_eq!(file.bytes, bytes, "{path} returned another store's bytes");
    }
}

#[tokio::test]
async fn two_stores_with_the_same_ids_read_their_own_bytes_through_one_cache() {
    let cache = MetadataCache::default();
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    let first = same_shape_store(first_root.path(), &cache, b"first").await;
    let second = same_shape_store(second_root.path(), &cache, b"other").await;
    let first_reader = reader(first_root.path(), &cache).await;
    let second_reader = reader(second_root.path(), &cache).await;
    let runtimes = [
        (first.read_only(), b"first"),
        (second.read_only(), b"other"),
        (first_reader.clone(), b"first"),
        (second_reader.clone(), b"other"),
    ];
    for _ in 0..2 {
        for (runtime, bytes) in &runtimes {
            assert_reads(runtime, *bytes).await;
        }
    }
    assert!(
        cache.stats().wal_tail_hits > 0,
        "the second pass reads warm"
    );

    first.core.invalidate_namespace_read_cache(&demo());
    second_reader.core.invalidate_namespace_read_cache(&demo());
    for (runtime, bytes) in &runtimes {
        assert_reads(runtime, *bytes).await;
    }

    drop(runtimes);
    drop(first_reader);
    let rebuilt = reader(first_root.path(), &cache).await;
    assert_reads(&rebuilt, b"first").await;
    assert_reads(&second_reader, b"other").await;
}

#[tokio::test]
async fn bindings_share_one_capacity() {
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        same_shape_store(root.path(), &MetadataCache::default(), bytes).await;
    }
    // Limits that hold what one binding reads, and so not what two read.
    let measured = MetadataCache::default();
    assert_reads(&reader(first_root.path(), &measured).await, b"first").await;
    let one_binding = measured.stats();
    let cache = MetadataCache::builder()
        .max_segment_bytes(one_binding.segment_bytes)
        .max_head_state_bytes(one_binding.head_state_bytes)
        .build();

    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        assert_reads(&reader(root.path(), &cache).await, bytes).await;
    }

    let stats = cache.stats();
    assert!(
        stats.segment_bytes <= one_binding.segment_bytes,
        "{stats:?}"
    );
    assert!(
        stats.head_state_bytes <= one_binding.head_state_bytes,
        "{stats:?}"
    );
    assert!(stats.segment_evictions > 0, "{stats:?}");
    assert!(stats.head_state_evictions > 0, "{stats:?}");
}

#[tokio::test]
async fn closing_one_runtime_leaves_the_cache_and_the_other_usable() {
    let cache = MetadataCache::default();
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    let first = same_shape_store(first_root.path(), &cache, b"first").await;
    let second = same_shape_store(second_root.path(), &cache, b"other").await;
    assert_reads(&first.read_only(), b"first").await;
    assert_reads(&second.read_only(), b"other").await;

    first.shutdown().await.expect("shut down the first runtime");
    drop(first);
    let before = cache.stats();
    assert_reads(&second.read_only(), b"other").await;
    assert!(
        cache.stats().wal_tail_hits > before.wal_tail_hits,
        "the other runtime still reads its warm entries"
    );

    let reopened = LoonFs::builder_with_store(local_store(first_root.path()))
        .writer_id("reopened-writer")
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("build a new runtime over the first store");
    assert_reads(&reopened.read_only(), b"first").await;
    reopened
        .open_namespace(&demo())
        .expect("open namespace")
        .put_file("/after.txt", b"after", &test_actor())
        .await
        .expect("write through the new runtime");
    let file = reopened
        .namespace(&demo())
        .read_file("/after.txt")
        .await
        .expect("read the new file");
    assert_eq!(file.bytes, b"after");
}

#[tokio::test]
async fn read_policy_stays_with_each_binding() {
    let root = tempdir().expect("tempdir");
    same_shape_store(root.path(), &MetadataCache::default(), b"first").await;
    // The cache keeps no segment blocks, so a lookup that a block memo does
    // not answer goes to the store.
    let cache = MetadataCache::builder().max_segment_bytes(0).build();
    let binding =
        |configure: fn(crate::LoonFsBuilder<ReadOnly>) -> crate::LoonFsBuilder<ReadOnly>| {
            let recording = Arc::new(RecordingStore::new(
                LocalFsStore::new(root.path()).expect("create local-fs store"),
                KeyPredicate::any(),
            ));
            let runtime = configure(LoonFs::builder_with_store(recording.clone()).read_only())
                .metadata_cache(cache.clone())
                .build();
            async move { (runtime.await.expect("build reader"), recording) }
        };

    let (every_read, every_read_store) =
        binding(|builder| builder.manifest_revalidation_interval_ms(0)).await;
    let (never, never_store) =
        binding(|builder| builder.manifest_revalidation_interval_ms(u64::MAX)).await;
    for (runtime, store) in [(&every_read, &every_read_store), (&never, &never_store)] {
        assert_reads(runtime, b"first").await;
        store.reset();
        assert_reads(runtime, b"first").await;
    }
    assert_eq!(
        every_read_store.count(OperationClass::Head),
        2,
        "a zero interval probes for a successor manifest on every read"
    );
    assert_eq!(
        never_store.count(OperationClass::Head),
        0,
        "the other binding never probes"
    );

    let (no_memo, no_memo_store) = binding(|builder| builder.max_block_memo_bytes(0)).await;
    let (memo, memo_store) = binding(|builder| builder).await;
    for (runtime, store) in [(&no_memo, &no_memo_store), (&memo, &memo_store)] {
        assert_reads(runtime, b"first").await;
        store.reset();
        runtime
            .namespace(&demo())
            .stat(FOLDED)
            .await
            .expect("stat the folded file");
    }
    assert!(
        no_memo_store.count(OperationClass::Get) > memo_store.count(OperationClass::Get),
        "a zero block memo rereads blocks the default memo keeps: {} against {}",
        no_memo_store.count(OperationClass::Get),
        memo_store.count(OperationClass::Get)
    );
}

const CACHE_METRICS: [&str; 4] = [
    "loonfs.metadata_segment_cache.retained_decoded_bytes",
    "loonfs.head_state_cache.retained_decoded_bytes",
    "loonfs.wal_tail_projection_cache.gets",
    "loonfs.namespace_head_cache.gets",
];

fn registered(snapshot: &MetricsSnapshot, name: &str) -> bool {
    snapshot.by_name(name).next().is_some()
}

fn gauge(snapshot: &MetricsSnapshot, name: &str) -> i64 {
    match snapshot.by_name(name).next().map(|entry| &entry.value) {
        Some(MetricValue::Gauge(value)) => *value,
        other => panic!("expected a `{name}` gauge, found {other:?}"),
    }
}

#[tokio::test]
async fn the_cache_reports_its_own_metrics_once() {
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        same_shape_store(root.path(), &MetadataCache::default(), bytes).await;
    }
    let cache_recorder = Arc::new(DefaultMetricsRecorder::new());
    let cache = MetadataCache::builder()
        .metrics_recorder(cache_recorder.clone() as Arc<dyn MetricsRecorder>)
        .build();
    let runtime_recorder = Arc::new(DefaultMetricsRecorder::new());
    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        let runtime = LoonFs::builder_with_store(local_store(root.path()))
            .read_only()
            .metadata_cache(cache.clone())
            .metrics_recorder(runtime_recorder.clone())
            .build()
            .await
            .expect("build reader");
        assert_reads(&runtime, bytes).await;
    }

    let runtime_snapshot = runtime_recorder.snapshot();
    for name in CACHE_METRICS {
        assert!(
            !registered(&runtime_snapshot, name),
            "a runtime given a cache does not report `{name}`"
        );
    }
    assert!(registered(
        &runtime_snapshot,
        "loonfs.runtime_cache.latest_metadata_view_reads"
    ));
    let cache_snapshot = cache_recorder.snapshot();
    for name in CACHE_METRICS {
        assert!(
            registered(&cache_snapshot, name),
            "the cache reports `{name}`"
        );
    }
    assert_eq!(
        gauge(
            &cache_snapshot,
            "loonfs.head_state_cache.retained_decoded_bytes"
        ),
        i64::try_from(cache.stats().head_state_bytes).expect("small byte count"),
        "one gauge reports both bindings' head state"
    );

    let private_recorder = Arc::new(DefaultMetricsRecorder::new());
    let private = LoonFs::builder_with_store(local_store(first_root.path()))
        .read_only()
        .metrics_recorder(private_recorder.clone())
        .build()
        .await
        .expect("build reader");
    assert_reads(&private, b"first").await;
    let private_snapshot = private_recorder.snapshot();
    for name in CACHE_METRICS {
        assert!(
            registered(&private_snapshot, name),
            "a private cache reports `{name}` to the runtime's recorder"
        );
    }
    assert_eq!(
        gauge(
            &private_snapshot,
            "loonfs.head_state_cache.retained_decoded_bytes"
        ),
        i64::try_from(private.metadata_cache().stats().head_state_bytes).expect("small byte count"),
    );
}
