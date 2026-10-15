//! One metadata cache shared by runtimes over one store and over different
//! stores: scoping, one capacity, binding lifetimes, per-binding policy,
//! metrics, and the object reads the cache saves.

use super::MetadataCache;
use crate::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder, MetricsSnapshot};
use crate::{LoonFs, NamespaceId, ReadOnly, SharedObjectStore, Writable};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{first_page, namespace_id, test_actor, writer_id};
use loonfs_test_support::stores::{
    KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::ChecksumAlgorithm;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

const FOLDED: &str = "/folded/file.txt";
const TAIL: &str = "/tail.txt";

#[derive(Clone, Copy, Debug)]
enum PinCreation {
    Fold,
    Checkpoint,
    Snapshot,
    Fork,
}

async fn pin_fold_reads(
    creation: PinCreation,
    max_read_working_bytes: usize,
    max_segment_bytes: usize,
) -> usize {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(
        loonfs_test_support::stores::BlockingStore::new(
            LocalFsStore::new(root.path()).expect("store"),
            KeyPredicate::metadata_segment(),
            OperationSelector::Put,
        ),
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let budget = crate::ExecutionBudget::builder()
        .max_read_working_bytes(max_read_working_bytes)
        .metrics_recorder(recorder.clone())
        .build();
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("pin-writer"))
        .execution_budget(budget.clone())
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
        loonfs_core::cache::load_namespace_diagnostics(
            store.as_ref(),
            &demo(),
            loonfs_core::test_support::std_timer()
        )
        .await
        .expect("diagnostics before creation")
        .wal_tail_objects
            > 0
    );
    store.reset();
    store.inner().block_next();
    let mut operation = Box::pin(async {
        match creation {
            PinCreation::Fold => {
                maintenance.fold_wal(&demo()).await.expect("fold tail");
            }
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
                        runtime.now_ms() + 60_000,
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
    });
    tokio::select! {
        () = &mut operation => panic!("fold should block before writing a segment"),
        () = store.inner().wait_until_blocked() => {}
    }
    let retained = budget.stats().read_working_bytes;
    if max_read_working_bytes > 1 {
        assert!(retained > 0, "{creation:?} charges the shared pool");
    } else {
        assert_eq!(retained, 0);
    }
    assert_eq!(
        gauge(
            &recorder.snapshot(),
            "loonfs.execution_budget.read_working_bytes"
        ),
        retained as i64
    );
    store.inner().release();
    operation.await;
    assert_eq!(budget.stats().read_working_bytes, 0);
    assert_eq!(
        gauge(
            &recorder.snapshot(),
            "loonfs.execution_budget.read_working_bytes"
        ),
        0
    );
    let reads = store.count(OperationSelector::Read);
    assert_eq!(
        loonfs_core::cache::load_namespace_diagnostics(
            store.as_ref(),
            &demo(),
            loonfs_core::test_support::std_timer()
        )
        .await
        .expect("diagnostics after creation")
        .wal_tail_objects,
        0
    );
    runtime.shutdown().await.expect("shut down writer");
    reads
}

#[tokio::test]
async fn folds_and_pin_folds_use_the_shared_pool_and_segment_cache() {
    for creation in [
        PinCreation::Fold,
        PinCreation::Checkpoint,
        PinCreation::Snapshot,
        PinCreation::Fork,
    ] {
        let small_memo_reads = pin_fold_reads(creation, 1, 0).await;
        let default_memo_reads =
            pin_fold_reads(creation, crate::DEFAULT_MAX_READ_WORKING_BYTES, 0).await;
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

/// Creates the same namespace in `store` on every call, and returns its
/// writer.
///
/// Only the file bytes differ, so every store written this way has the same
/// manifest numbers, head sequences, and WAL-tail keys. One file is in
/// metadata segments, and one is in the WAL tail.
async fn same_shape_store(
    store: &SharedObjectStore,
    cache: &MetadataCache,
    bytes: &'static [u8],
) -> LoonFs<Writable> {
    let writer = LoonFs::builder_with_store(Arc::clone(store))
        .writer_id(loonfs_test_support::ids::writer_id("same-shape-writer"))
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

async fn reader(store: &SharedObjectStore, cache: &MetadataCache) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(Arc::clone(store))
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

/// Checks that runtimes over two stores with the same object keys read their
/// own bytes through one cache.
#[tokio::test]
async fn two_stores_with_the_same_ids_read_their_own_bytes_through_one_cache() {
    let cache = MetadataCache::default();
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    let first_store = local_store(first_root.path());
    let second_store = local_store(second_root.path());
    let first = same_shape_store(&first_store, &cache, b"first").await;
    let second = same_shape_store(&second_store, &cache, b"other").await;
    let first_reader = reader(&first_store, &cache).await;
    let second_reader = reader(&second_store, &cache).await;
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
    let rebuilt = reader(&local_store(first_root.path()), &cache).await;
    assert_reads(&rebuilt, b"first").await;
    assert_reads(&second_reader, b"other").await;
}

/// Checks that a second runtime over the same object store reads a directory
/// from the segment blocks and manifest that the first runtime decoded.
#[tokio::test]
async fn runtimes_over_one_store_share_decoded_segment_blocks() {
    let store: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    // The writer reads through a cache of its own, so the first reader
    // starts cold.
    same_shape_store(&store, &MetadataCache::default(), b"file").await;
    let cache = MetadataCache::default();
    let (first, second) = (reader(&store, &cache).await, reader(&store, &cache).await);
    let mut stats_after_each_read = Vec::new();
    for runtime in [&first, &second] {
        let page = runtime
            .namespace(&demo())
            .list("/folded")
            .page(first_page())
            .await
            .expect("list the folded directory");
        assert_eq!(page.entries.len(), 1);
        stats_after_each_read.push(cache.stats());
    }
    let (cold, warm) = (stats_after_each_read[0], stats_after_each_read[1]);
    assert!(
        cold.segment_misses > 0,
        "the first runtime reads cold: {cold:?}"
    );
    assert!(warm.segment_hits > cold.segment_hits, "{warm:?}");
    assert_eq!(
        warm.segment_misses, cold.segment_misses,
        "the second runtime reads the blocks the first runtime decoded: {warm:?}"
    );
}

/// Checks that clones of one object store share one scope, and that no other
/// object store is assigned that scope, even one allocated after the first
/// object store is dropped.
#[test]
fn each_object_store_allocation_has_its_own_scope() {
    let memory_store =
        || -> SharedObjectStore { Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme)) };
    let mut scopes = super::ObjectStoreScopes::default();
    let (first, second) = (memory_store(), memory_store());
    let first_scope = scopes.scope_of(&first);
    assert_eq!(scopes.scope_of(&Arc::clone(&first)), first_scope);
    let second_scope = scopes.scope_of(&second);
    assert_ne!(second_scope, first_scope);

    drop(first);
    // This call removes the entry of the dropped object store, so the
    // allocator can reuse its address for the next object store.
    assert_eq!(scopes.scope_of(&second), second_scope);
    assert_eq!(scopes.stores.len(), 1);
    let third_scope = scopes.scope_of(&memory_store());
    assert!(![first_scope, second_scope].contains(&third_scope));
}

#[tokio::test]
async fn bindings_share_one_capacity() {
    let (first_root, second_root) = (tempdir().expect("tempdir"), tempdir().expect("tempdir"));
    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        same_shape_store(&local_store(root.path()), &MetadataCache::default(), bytes).await;
    }
    // Limits that hold what one binding reads, and so not what two read.
    let measured = MetadataCache::default();
    assert_reads(
        &reader(&local_store(first_root.path()), &measured).await,
        b"first",
    )
    .await;
    let one_binding = measured.stats();
    let cache = MetadataCache::builder()
        .max_segment_bytes(one_binding.segment_bytes)
        .max_head_state_bytes(one_binding.head_state_bytes)
        .build();

    for (root, bytes) in [(&first_root, b"first"), (&second_root, b"other")] {
        assert_reads(&reader(&local_store(root.path()), &cache).await, bytes).await;
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
    let first = same_shape_store(&local_store(first_root.path()), &cache, b"first").await;
    let second = same_shape_store(&local_store(second_root.path()), &cache, b"other").await;
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
        .writer_id(loonfs_test_support::ids::writer_id("reopened-writer"))
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
    same_shape_store(
        &local_store(root.path()),
        &MetadataCache::default(),
        b"first",
    )
    .await;
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
        every_read_store.count(OperationSelector::Head),
        2,
        "a zero interval probes for a successor manifest on every read"
    );
    assert_eq!(
        never_store.count(OperationSelector::Head),
        0,
        "the other binding never probes"
    );

    let (no_memo, no_memo_store) = binding(|builder| {
        builder.execution_budget(
            crate::ExecutionBudget::builder()
                .max_read_working_bytes(0)
                .build(),
        )
    })
    .await;
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
        no_memo_store.count(OperationSelector::Get) > memo_store.count(OperationSelector::Get),
        "a zero block memo rereads blocks the default memo keeps: {} against {}",
        no_memo_store.count(OperationSelector::Get),
        memo_store.count(OperationSelector::Get)
    );
}

const CACHE_METRICS: [&str; 6] = [
    "loonfs.metadata_segment_cache.retained_decoded_bytes",
    "loonfs.head_state_cache.retained_decoded_bytes",
    "loonfs.wal_tail_projection_cache.gets",
    "loonfs.namespace_head_cache.gets",
    "loonfs.object_cache.retained_decoded_bytes",
    "loonfs.object_cache.gets",
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
        same_shape_store(&local_store(root.path()), &MetadataCache::default(), bytes).await;
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

#[tokio::test]
async fn reads_on_two_runtimes_share_read_working_memory_and_release_it() {
    use crate::ExecutionBudget;
    use loonfs_test_support::stores::BlockingStore;

    let root = tempdir().expect("tempdir");
    let writer = same_shape_store(
        &local_store(root.path()),
        &MetadataCache::default(),
        b"file",
    )
    .await;
    writer.shutdown().await.expect("shutdown");
    let cache = MetadataCache::builder().max_segment_bytes(0).build();
    let reader = |budget: ExecutionBudget| {
        let store = Arc::new(BlockingStore::new(
            RecordingStore::metadata_segments(LocalFsStore::new(root.path()).expect("store")),
            KeyPredicate::content_blob(),
            OperationSelector::Read,
        ));
        let runtime = LoonFs::builder_with_store(store.clone())
            .read_only()
            .metadata_cache(cache.clone())
            .execution_budget(budget)
            .build();
        async move { (runtime.await.expect("reader"), store) }
    };
    let (probe, probe_store) = reader(ExecutionBudget::default()).await;
    probe_store.arm();
    let mut read = Box::pin(async { probe.namespace(&demo()).read_file(FOLDED).await });
    tokio::select! {
        result = &mut read => panic!("read should block, got {result:?}"),
        () = probe_store.wait_until_blocked() => {}
    }
    let working_bytes = probe.execution_budget().stats().read_working_bytes;
    assert!(working_bytes > 0);
    drop(read);
    assert_eq!(probe.execution_budget().stats().read_working_bytes, 0);

    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let budget = ExecutionBudget::builder()
        .max_read_working_bytes(working_bytes)
        .metrics_recorder(recorder.clone())
        .build();
    let (first, first_store) = reader(budget.clone()).await;
    let (second, second_store) = reader(budget.clone()).await;
    first_store.arm();
    let mut first_read = Box::pin(async { first.namespace(&demo()).read_file(FOLDED).await });
    tokio::select! {
        result = &mut first_read => panic!("read should block, got {result:?}"),
        () = first_store.wait_until_blocked() => {}
    }
    assert_eq!(budget.stats().read_working_bytes, working_bytes);
    assert_eq!(
        gauge(
            &recorder.snapshot(),
            "loonfs.execution_budget.read_working_bytes"
        ),
        working_bytes as i64
    );
    second_store.arm();
    let mut second_read = Box::pin(async { second.namespace(&demo()).read_file(FOLDED).await });
    tokio::select! {
        result = &mut second_read => panic!("read should block, got {result:?}"),
        () = second_store.wait_until_blocked() => {}
    }
    assert_eq!(budget.stats().read_working_bytes, working_bytes);
    assert!(
        second_store.inner().count(OperationSelector::Read)
            > first_store.inner().count(OperationSelector::Read)
    );
    assert!(
        gauge(
            &recorder.snapshot(),
            "loonfs.execution_budget.read_working_reservation_failures"
        ) > 0
    );
    first_store.release();
    second_store.release();
    assert_eq!(first_read.await.expect("first read").bytes, b"file");
    assert_eq!(second_read.await.expect("second read").bytes, b"file");
    assert_eq!(budget.stats().read_working_bytes, 0);
    assert_eq!(
        gauge(
            &recorder.snapshot(),
            "loonfs.execution_budget.read_working_bytes"
        ),
        0
    );
}

/// Creates a writer whose namespace exists, over a store that records every
/// request.
async fn recorded_writer(
    cache: MetadataCache,
) -> (
    LoonFs<Writable>,
    Arc<RecordingStore<LocalFsStore>>,
    tempfile::TempDir,
) {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(root.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("object-cache-writer"))
        .metadata_cache(cache)
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&demo(), &test_actor())
        .await
        .expect("create namespace");
    (writer, store, root)
}

/// Counts the recorded GETs that returned an object of `family`.
fn object_gets(operations: &[RecordedOperation], family: DurableObjectFamily) -> usize {
    operations
        .iter()
        .filter(|operation| match operation {
            RecordedOperation::Get {
                key, result_bytes, ..
            } => {
                *result_bytes > 0
                    && parse_object_key(key).is_some_and(|parsed| parsed.family() == family)
            }
            _ => false,
        })
        .count()
}

/// Checks that the first read after a compaction, in a runtime without the
/// namespace's writer session, reloads the namespace's anchor without
/// reading the unfolded WAL objects from the store again.
///
/// The reads run on a read-only runtime that shares the writer's cache,
/// because reads in the writer's runtime start from the writer's position
/// and keep no anchor.
#[tokio::test]
async fn an_anchor_reload_reads_unfolded_wal_objects_from_the_object_cache() {
    for max_object_bytes in [crate::DEFAULT_MAX_OBJECT_BYTES, 0] {
        let cache = MetadataCache::builder()
            .max_object_bytes(max_object_bytes)
            .build();
        let (writer, store, _root) = recorded_writer(cache.clone()).await;
        let reader = LoonFs::builder_with_store(store.clone())
            .read_only()
            .metadata_cache(cache.clone())
            .manifest_revalidation_interval_ms(0)
            .build()
            .await
            .expect("build reader");
        let reader_namespace = reader.namespace(&demo());
        let namespace = writer.open_namespace(&demo()).expect("open namespace");
        let maintenance = writer.maintenance(writer_id("object-cache-maintenance"));
        for index in 0..3 {
            namespace
                .put_file(&format!("/folded-{index}"), b"folded", &test_actor())
                .await
                .expect("put a folded file");
            maintenance.fold_wal(&demo()).await.expect("fold");
        }
        for index in 0..3 {
            namespace
                .put_file(&format!("/tail-{index}"), b"tail", &test_actor())
                .await
                .expect("put a file whose bytes stay in the WAL");
        }
        // The reader's first read loads the unfolded WAL objects from the
        // store.
        assert_eq!(
            reader_namespace
                .read_file("/tail-0")
                .await
                .expect("read")
                .bytes,
            b"tail"
        );
        let compaction = maintenance
            .compact_metadata(&demo(), &crate::MaintenanceCancellation::new())
            .await
            .expect("compact");
        assert_eq!(
            compaction.compaction,
            crate::MetadataCompactionOutcome::BoundedMergePublished
        );

        store.reset();
        assert_eq!(
            reader_namespace
                .read_file("/tail-2")
                .await
                .expect("read")
                .bytes,
            b"tail"
        );
        let operations = store.snapshot();
        assert!(
            operations
                .iter()
                .any(|operation| operation.key().contains("/manifests/")),
            "the read reloads the anchor: {operations:?}"
        );
        let wal_gets = object_gets(&operations, DurableObjectFamily::WalObject);
        if max_object_bytes == 0 {
            assert_eq!(wal_gets, 3, "without the cache, the reload reads the tail");
        } else {
            assert_eq!(wal_gets, 0, "{:?}", cache.stats());
            assert_eq!(cache.stats().wal_object_hits, 3);
        }
    }
}

/// Checks that a second read of a small content object finds its bytes in
/// the object cache, and that every read of an object over the size cap goes
/// to the store.
#[tokio::test]
async fn small_content_objects_are_read_from_the_store_once() {
    for (size, content_gets) in [(1024, 1), (2 * 1024 * 1024, 2)] {
        for read in ["buffered", "stream"] {
            let (writer, store, _root) = recorded_writer(MetadataCache::default()).await;
            let namespace = writer.open_namespace(&demo()).expect("open namespace");
            let bytes = vec![7; size];
            namespace
                .put_file(FOLDED, &bytes, &test_actor())
                .await
                .expect("put file");
            // A fold writes inline bytes to a content object.
            writer
                .maintenance(writer_id("object-cache-maintenance"))
                .fold_wal(&demo())
                .await
                .expect("fold");
            store.reset();
            for _ in 0..2 {
                let read_bytes = match read {
                    "buffered" => namespace.read_file(FOLDED).await.expect("read").bytes,
                    _ => {
                        let mut stream = namespace.read_file_stream(FOLDED).await.expect("open");
                        let mut read_bytes = Vec::new();
                        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
                            read_bytes.extend_from_slice(&chunk);
                        }
                        read_bytes
                    }
                };
                assert_eq!(read_bytes, bytes, "{read} read of {size} bytes");
            }
            assert_eq!(
                object_gets(&store.take(), DurableObjectFamily::ContentBlob),
                content_gets,
                "{read} reads of {size} bytes"
            );
        }
    }
}

/// Checks that a second runtime over the same object store reads the WAL
/// objects and the content object that the first runtime read, without a
/// GET of its own.
#[tokio::test]
async fn runtimes_over_one_store_share_decoded_wal_and_content_objects() {
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    // The writer reads through a cache of its own, so the first reader
    // starts cold.
    same_shape_store(&store, &MetadataCache::default(), b"file").await;
    let cache = MetadataCache::default();
    let (first, second) = (reader(&store, &cache).await, reader(&store, &cache).await);
    let mut object_gets_by_runtime = Vec::new();
    for runtime in [&first, &second] {
        recording.reset();
        assert_reads(runtime, b"file").await;
        let operations = recording.take();
        object_gets_by_runtime.push((
            object_gets(&operations, DurableObjectFamily::WalObject),
            object_gets(&operations, DurableObjectFamily::ContentBlob),
        ));
    }
    let (wal_gets, content_gets) = object_gets_by_runtime[0];
    assert!(
        wal_gets > 0 && content_gets > 0,
        "the first runtime reads cold"
    );
    assert_eq!(
        object_gets_by_runtime[1],
        (0, 0),
        "the second runtime reads the objects the first runtime read: {:?}",
        cache.stats()
    );
}

/// Checks that reads of folded empty files keep no content objects in the
/// object cache, under a budget of one byte and under the default budget.
#[tokio::test]
async fn reads_of_empty_files_keep_no_content_objects() {
    for max_object_bytes in [1, crate::DEFAULT_MAX_OBJECT_BYTES] {
        let cache = MetadataCache::builder()
            .max_object_bytes(max_object_bytes)
            .build();
        let (writer, _store, _root) = recorded_writer(cache.clone()).await;
        let namespace = writer.open_namespace(&demo()).expect("open namespace");
        let paths: Vec<String> = (0..32).map(|index| format!("/empty-{index}")).collect();
        for path in &paths {
            namespace
                .put_file(path, b"", &test_actor())
                .await
                .expect("put an empty file");
        }
        // A read of an empty file in the WAL tail reads no content object,
        // so the files are folded first.
        writer
            .maintenance(writer_id("object-cache-maintenance"))
            .fold_wal(&demo())
            .await
            .expect("fold");
        for path in &paths {
            assert!(namespace
                .read_file(path)
                .await
                .expect("read")
                .bytes
                .is_empty());
        }
        let stats = cache.stats();
        assert_eq!(stats.content_object_inserts, 0, "{stats:?}");
        assert!(stats.object_bytes <= max_object_bytes, "{stats:?}");
    }
}
