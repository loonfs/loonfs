#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use loon_api::wire::checkpoint::decode_checkpoint_manifest_json;
use loon_core::cache::load_verified_namespace_basis;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{
    checkpoint_manifest, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::metrics::{ObjectStoreOperation, VecObjectStoreMetricsRecorder};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CompleteUploadRequest, CopyOptions,
    CreateDirOptions, CreateNamespaceOptions, DeleteOptions, ErrorCode, Fs, FsConfig, InodeId,
    MaintenanceTickOptions, MaintenanceTickOutcome, MoveOptions, NamespaceId,
    NamespaceMutationCandidate, PathMutationIntent, PutFileBehavior, PutFileOptions,
    RuntimeCacheConfig, RuntimeError, SharedObjectStore, TraceMode, TraceStoreKind,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn store(root: &Path) -> SharedObjectStore {
    Arc::new(LocalFsStore::new(root).expect("create local-fs store"))
}

fn runtime(root: &Path, writer_id: &str) -> Fs {
    Fs::builder(store(root))
        .writer_id(writer_id)
        .build()
        .expect("build runtime")
}

fn namespace() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn assert_config_error(result: loonfs::Result<Fs>, expected: &str) {
    match result {
        Err(RuntimeError::Config(message)) => assert!(
            message.contains(expected),
            "expected {message:?} to contain {expected:?}"
        ),
        Err(error) => panic!("expected config error, got {error:?}"),
        Ok(_) => panic!("expected config error"),
    }
}

fn assert_core_error_kind<T>(result: loonfs::Result<T>, expected: ErrorCode) {
    match result {
        Err(RuntimeError::Core(error)) => assert_eq!(error.code(), expected),
        Err(error) => panic!("expected core error {expected:?}, got {error:?}"),
        Ok(_) => panic!("expected core error {expected:?}"),
    }
}

fn create_dir_candidate(commit_id: &str, absolute_path: &str) -> NamespaceMutationCandidate {
    NamespaceMutationCandidate::Path(PathMutationIntent::CreateDir {
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        absolute_path: absolute_path.to_owned(),
    })
}

fn delete_candidate(commit_id: &str, absolute_path: &str) -> NamespaceMutationCandidate {
    NamespaceMutationCandidate::Path(PathMutationIntent::DeletePath {
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        absolute_path: absolute_path.to_owned(),
        recursive: true,
    })
}

fn assert_single_publish_ok(results: Vec<loonfs::Result<loonfs::CommitResponse>>) {
    let result = results
        .into_iter()
        .next()
        .expect("one publish result")
        .expect("publish succeeds");
    assert!(result.committed_seq.0 > 0);
}

#[test]
fn open_validates_runtime_config() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());

    assert_config_error(Fs::builder(object_store.clone()).build(), "writer_id");
    assert_config_error(
        Fs::open(
            object_store.clone(),
            FsConfig {
                writer_id: "   ".to_owned(),
                writer_version: "runtime-test/0.1.0".to_owned(),
                lease_duration_ms: 5_000,
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        ),
        "writer_id",
    );
    assert_config_error(
        Fs::open(
            object_store.clone(),
            FsConfig {
                writer_id: "runtime-test".to_owned(),
                writer_version: "   ".to_owned(),
                lease_duration_ms: 5_000,
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        ),
        "writer_version",
    );
    assert_config_error(
        Fs::open(
            object_store,
            FsConfig {
                writer_id: "runtime-test".to_owned(),
                writer_version: "runtime-test/0.1.0".to_owned(),
                lease_duration_ms: 0,
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        ),
        "lease_duration_ms",
    );
}

#[test]
fn builder_metrics_recorder_instruments_object_store() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("metrics-test")
        .trace_store_kind(TraceStoreKind::LocalFs)
        .with_metrics_recorder(recorder.clone())
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace(), CreateNamespaceOptions::default())
        .expect("create namespace");

    let samples = recorder.samples();
    assert!(!samples.is_empty());
    assert!(samples
        .iter()
        .any(|sample| sample.operation == ObjectStoreOperation::Put));
    assert!(samples
        .iter()
        .all(|sample| sample.store_kind.as_deref() == Some("local_fs")));
}

#[test]
fn filesystem_operations_match_core_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "filesystem-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let stat = fs
        .stat_path(&namespace_id, "/docs/hello.txt")
        .expect("stat file");
    assert_eq!(stat.absolute_path, "/docs/hello.txt");
    assert_eq!(stat.size_bytes, Some(5));

    let entries = fs.list_path(&namespace_id, "/docs").expect("list docs");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].absolute_path, "/docs/hello.txt");

    let read = fs
        .read_file_bytes(&namespace_id, "/docs/hello.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"hello");

    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .expect("replace file");
    let read = fs
        .read_file_bytes(&namespace_id, "/docs/hello.txt")
        .expect("read replaced file");
    assert_eq!(read.bytes, b"updated");

    fs.copy_path(
        &namespace_id,
        "/docs/hello.txt",
        "/docs/copy.txt",
        CopyOptions::default(),
    )
    .expect("copy file");
    fs.move_path(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        MoveOptions::default(),
    )
    .expect("move file");
    assert_eq!(
        fs.read_file_bytes(&namespace_id, "/docs/moved.txt")
            .expect("read moved copy")
            .bytes,
        b"updated"
    );
}

#[test]
fn runtime_cache_reuses_verified_basis_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first stat should load basis");
    assert_eq!(raw_store.wal_get_count(), 1);

    fs.stat_path(&namespace_id, "/docs")
        .expect("second stat should reuse cached basis");
    assert_eq!(raw_store.wal_get_count(), 1);

    fs.create_dir(&namespace_id, "/other", CreateDirOptions::default())
        .expect("create other");
    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("stat after mutation should reload basis");
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn runtime_cache_observes_head_advanced_by_another_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = Fs::builder(object_store.clone())
        .writer_id("basis-cache-reader")
        .build()
        .expect("build reader runtime");
    let writer = Fs::builder(object_store)
        .writer_id("basis-cache-writer")
        .build()
        .expect("build writer runtime");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime reader cache");

    writer
        .create_dir(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head from another runtime");

    raw_store.reset_wal_get_count();
    let stat = reader
        .stat_path(&namespace_id, "/docs/new")
        .expect("reader should observe external head advance");
    assert_eq!(stat.absolute_path, "/docs/new");
    assert_eq!(stat.authoritative_head_seq, ChangeSeq(2));
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn runtime_cache_can_be_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-disabled-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first stat should load basis");
    fs.stat_path(&namespace_id, "/docs")
        .expect("second stat should load basis again");
    assert_eq!(raw_store.wal_get_count(), 2);
}

#[test]
fn runtime_basis_cache_evicts_by_namespace_count() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("basis-count-budget")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");
    let first = NamespaceId::parse("first").expect("valid namespace id");
    let second = NamespaceId::parse("second").expect("valid namespace id");
    fs.create_namespace(&first, CreateNamespaceOptions::default())
        .expect("create first namespace");
    fs.create_namespace(&second, CreateNamespaceOptions::default())
        .expect("create second namespace");

    fs.stat_path(&first, "/").expect("cache first basis");
    fs.stat_path(&second, "/")
        .expect("cache second basis and evict first");
    let after_second = fs.runtime_cache_stats();
    assert_eq!(after_second.warm_basis_evictions, 1);
    assert_eq!(after_second.warm_basis_cached_rows, 1);

    fs.stat_path(&first, "/")
        .expect("first basis rehydrates after eviction");
    let after_reload = fs.runtime_cache_stats();
    assert_eq!(after_reload.warm_basis_cache_misses, 3);
    assert_eq!(after_reload.warm_basis_evictions, 2);
}

#[test]
fn runtime_basis_cache_evicts_by_row_budget() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("basis-row-budget")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_basis_rows: 1,
            max_cached_basis_decoded_bytes: None,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");
    let first = NamespaceId::parse("row-one").expect("valid namespace id");
    let second = NamespaceId::parse("row-two").expect("valid namespace id");
    fs.create_namespace(&first, CreateNamespaceOptions::default())
        .expect("create first namespace");
    fs.create_namespace(&second, CreateNamespaceOptions::default())
        .expect("create second namespace");

    fs.stat_path(&first, "/").expect("cache first basis");
    fs.stat_path(&second, "/")
        .expect("cache second basis and evict first by rows");
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.warm_basis_evictions, 1);
    assert_eq!(stats.warm_basis_evicted_rows, 1);
    assert_eq!(stats.warm_basis_cached_rows, 1);
}

#[test]
fn runtime_basis_budget_includes_commit_engine_bases() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("combined-budget").expect("valid namespace id");
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("combined-basis-budget")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_basis_rows: 2,
            max_cached_basis_decoded_bytes: None,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.stat_path(&namespace_id, "/").expect("cache read basis");
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("combined-budget-create-docs", "/docs")],
    ));

    let stats = fs.runtime_cache_stats();
    assert!(stats.warm_basis_evictions > 0);
    assert!(stats.warm_basis_cached_rows <= 2);

    let misses_before = stats.warm_basis_cache_misses;
    fs.stat_path(&namespace_id, "/docs")
        .expect("read still works after aggregate budget pruning");
    let stats = fs.runtime_cache_stats();
    assert!(stats.warm_basis_cache_misses > misses_before);
    assert!(stats.warm_basis_cached_rows <= 2);
}

#[test]
fn runtime_basis_cache_evicts_by_decoded_byte_budget() {
    let temp_dir = tempdir().expect("tempdir");
    let setup = runtime(temp_dir.path(), "basis-byte-budget-setup");
    let first = NamespaceId::parse("byte-one").expect("valid namespace id");
    let second = NamespaceId::parse("byte-two").expect("valid namespace id");
    setup
        .create_namespace(&first, CreateNamespaceOptions::default())
        .expect("create first namespace");
    setup
        .create_namespace(&second, CreateNamespaceOptions::default())
        .expect("create second namespace");
    let raw_store = store(temp_dir.path());
    let basis_weight = load_verified_namespace_basis(raw_store.as_ref(), &first)
        .expect("load basis")
        .weight();
    assert!(basis_weight.decoded_bytes > 1);

    let fs = Fs::builder(raw_store)
        .writer_id("basis-byte-budget")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_basis_decoded_bytes: Some(basis_weight.decoded_bytes),
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.stat_path(&first, "/").expect("cache first basis");
    fs.stat_path(&second, "/")
        .expect("cache second basis and evict first by bytes");
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.warm_basis_evictions, 1);
    assert!(stats.warm_basis_evicted_decoded_bytes >= basis_weight.decoded_bytes);
    assert_eq!(stats.warm_basis_cached_rows, basis_weight.rows);
}

#[test]
fn runtime_basis_cache_skips_oversized_basis() {
    let temp_dir = tempdir().expect("tempdir");
    let setup = runtime(temp_dir.path(), "basis-oversized-setup");
    let namespace_id = namespace();
    setup
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let raw_store = store(temp_dir.path());
    let basis_weight = load_verified_namespace_basis(raw_store.as_ref(), &namespace_id)
        .expect("load basis")
        .weight();
    assert!(basis_weight.decoded_bytes > 1);

    let fs = Fs::builder(raw_store)
        .writer_id("basis-oversized")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_basis_decoded_bytes: Some(basis_weight.decoded_bytes - 1),
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.stat_path(&namespace_id, "/")
        .expect("first stat reconstructs oversized basis");
    fs.stat_path(&namespace_id, "/")
        .expect("second stat reconstructs oversized basis again");
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.warm_basis_cache_misses, 2);
    assert_eq!(stats.warm_basis_cache_hits, 0);
    assert_eq!(stats.warm_basis_uncacheable_count, 2);
    assert_eq!(
        stats.warm_basis_uncacheable_rows,
        basis_weight.rows.saturating_mul(2)
    );
    assert_eq!(
        stats.warm_basis_uncacheable_decoded_bytes,
        basis_weight.decoded_bytes.saturating_mul(2)
    );
    assert_eq!(stats.warm_basis_cached_rows, 0);
    assert_eq!(stats.warm_basis_cached_decoded_bytes, 0);
}

#[test]
fn runtime_publish_reuses_warm_basis_for_adjacent_batches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("publish-warm-basis-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("warm-create-docs", "/docs")],
    ));

    raw_store.reset_wal_get_count();
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("warm-create-child", "/docs/child")],
    ));
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "second adjacent publish should plan from the warm basis without replaying WAL"
    );

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.publish_warm_basis_misses, 1);
    assert_eq!(stats.publish_warm_basis_hits, 1);
    assert_eq!(stats.publish_warm_basis_advances, 2);
    assert_eq!(stats.publish_warm_basis_invalidations, 0);

    let stat = fs
        .stat_path(&namespace_id, "/docs/child")
        .expect("warm-published child is visible");
    assert_eq!(stat.authoritative_head_seq, ChangeSeq(2));
}

#[test]
fn runtime_publish_cold_loads_after_external_head_advance() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let first = Fs::builder(object_store.clone())
        .writer_id("shared-publish-writer")
        .build()
        .expect("build first runtime");
    let second = Fs::builder(object_store)
        .writer_id("shared-publish-writer")
        .build()
        .expect("build second runtime");

    first
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_single_publish_ok(first.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("external-first", "/docs")],
    ));
    second
        .create_dir(&namespace_id, "/other", CreateDirOptions::default())
        .expect("external runtime advances head");

    raw_store.reset_wal_get_count();
    assert_single_publish_ok(first.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("external-after", "/docs/child")],
    ));
    assert!(
        raw_store.wal_get_count() > 0,
        "stale warm basis should be discarded and replaced by a cold replay"
    );

    let stats = first.runtime_cache_stats();
    assert_eq!(stats.publish_warm_basis_hits, 0);
    assert_eq!(stats.publish_warm_basis_misses, 2);
    assert_eq!(stats.publish_warm_basis_invalidations, 1);
    assert_eq!(stats.publish_warm_basis_advances, 2);
}

#[test]
fn runtime_publish_retries_warm_rejection_after_external_head_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let first = Fs::builder(object_store.clone())
        .writer_id("shared-race-writer")
        .build()
        .expect("build first runtime");
    let second = Fs::builder(object_store)
        .writer_id("shared-race-writer")
        .build()
        .expect("build second runtime");

    first
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_single_publish_ok(first.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("race-prime", "/docs")],
    ));
    second
        .create_dir(&namespace_id, "/a", CreateDirOptions::default())
        .expect("external runtime creates /a");

    raw_store.reset_wal_get_count();
    assert_single_publish_ok(first.publish_namespace_mutations_batch(
        &namespace_id,
        vec![delete_candidate("race-delete-a", "/a")],
    ));
    assert!(
        raw_store.wal_get_count() > 0,
        "warm rejection should be retried from a cold basis"
    );

    assert_core_error_kind(
        first.stat_path(&namespace_id, "/a"),
        ErrorCode::PathNotFound,
    );
    let stats = first.runtime_cache_stats();
    assert_eq!(stats.publish_warm_basis_hits, 0);
    assert_eq!(stats.publish_warm_basis_misses, 2);
    assert_eq!(stats.publish_warm_basis_invalidations, 1);
    assert_eq!(stats.publish_warm_basis_advances, 2);
}

#[test]
fn runtime_publish_cache_disabled_replays_for_adjacent_batches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("publish-warm-disabled-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("disabled-first", "/docs")],
    ));

    raw_store.reset_wal_get_count();
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("disabled-second", "/docs/child")],
    ));
    assert!(
        raw_store.wal_get_count() > 0,
        "cache-disabled publish path should still cold-load from WAL"
    );
    assert_eq!(fs.runtime_cache_stats(), Default::default());
}

#[test]
fn runtime_publish_stale_head_invalidates_warm_basis() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("publish-warm-stale-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("stale-prime", "/docs")],
    ));

    raw_store.fail_head_cas();
    let stale = fs
        .publish_namespace_mutations_batch(
            &namespace_id,
            vec![create_dir_candidate("stale-loses-cas", "/stale")],
        )
        .into_iter()
        .next()
        .expect("one result")
        .expect_err("head CAS failure should be stale");
    assert_core_error_kind::<()>(Err(stale), ErrorCode::StaleHead);

    raw_store.allow_head_cas();
    raw_store.reset_wal_get_count();
    assert_single_publish_ok(fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![create_dir_candidate("stale-after", "/after")],
    ));
    assert!(
        raw_store.wal_get_count() > 0,
        "publish after stale-head invalidation should cold-load visible WAL"
    );

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.publish_warm_basis_hits, 1);
    assert_eq!(stats.publish_warm_basis_misses, 2);
    assert_eq!(stats.publish_warm_basis_invalidations, 1);
    assert_eq!(stats.publish_warm_basis_advances, 2);
}

#[test]
fn stale_head_write_error_invalidates_runtime_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-stale-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");

    raw_store.fail_head_cas();
    assert_core_error_kind(
        fs.create_dir(&namespace_id, "/stale", CreateDirOptions::default()),
        ErrorCode::StaleHead,
    );

    raw_store.allow_head_cas();
    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("read after stale head should reload basis");
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let error = fs
        .delete_path(&namespace_id, "/docs", DeleteOptions::default())
        .expect_err("non-recursive delete should reject non-empty directory");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::DirectoryNotEmpty
    ));

    fs.delete_path(
        &namespace_id,
        "/docs",
        DeleteOptions {
            recursive: true,
            commit_id: None,
        },
    )
    .expect("recursive delete");
    let error = fs
        .stat_path(&namespace_id, "/docs/hello.txt")
        .expect_err("deleted file should not stat");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    ));
}

#[test]
fn forked_namespace_shares_content_then_diverges() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "fork-test");
    let source = namespace();
    let clone = NamespaceId::parse("clone").expect("valid namespace id");

    fs.create_namespace(&source, CreateNamespaceOptions::default())
        .expect("create source namespace");
    fs.put_file_bytes(
        &source,
        "/docs/shared.txt",
        b"source",
        PutFileOptions::default(),
    )
    .expect("put source file");
    fs.fork_namespace(&source, &clone).expect("fork namespace");

    let source_entry = fs
        .stat_path(&source, "/docs/shared.txt")
        .expect("stat source");
    let clone_entry = fs
        .stat_path(&clone, "/docs/shared.txt")
        .expect("stat clone");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);

    fs.put_file_bytes(
        &clone,
        "/docs/shared.txt",
        b"clone",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .expect("replace clone file");

    assert_eq!(
        fs.read_file_bytes(&source, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source"
    );
    assert_eq!(
        fs.read_file_bytes(&clone, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone"
    );
}

#[test]
fn upload_flow_is_available_from_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "upload-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs.begin_upload(&namespace_id).expect("begin upload");
    let staged = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("upload content");
    let staged_again = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("repeat upload content");
    assert_eq!(staged.content_ref, staged_again.content_ref);

    let request = CompleteUploadRequest {
        content_ref: staged.content_ref,
    };
    let completed = fs
        .complete_upload(&namespace_id, &begin.upload_id, &request)
        .expect("complete upload");
    let completed_again = fs
        .complete_upload(&namespace_id, &begin.upload_id, &request)
        .expect("repeat complete upload");
    assert_eq!(completed.content_ref, completed_again.content_ref);
}

#[test]
fn upload_then_path_put_uses_memory_proof_without_blob_validation_call() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("validated-content-checksum-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs.begin_upload(&namespace_id).expect("begin upload");
    let staged = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("upload content");
    fs.complete_upload(
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: staged.content_ref.clone(),
        },
    )
    .expect("complete upload");

    raw_store.reset_content_blob_counters();
    let responses = fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![NamespaceMutationCandidate::Path(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-checksum-upload").expect("valid commit id"),
                absolute_path: "/docs/checksum.txt".to_owned(),
                content_ref: staged.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
        )],
    );

    assert!(responses[0].is_ok());
    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 0);
}

#[test]
fn disabled_runtime_cache_still_uses_memory_proof_without_blob_validation_call() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("validated-content-checksum-disabled-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs.begin_upload(&namespace_id).expect("begin upload");
    let staged = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("upload content");
    fs.complete_upload(
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: staged.content_ref.clone(),
        },
    )
    .expect("complete upload");

    raw_store.reset_content_blob_counters();
    let responses = fs.publish_namespace_mutations_batch(
        &namespace_id,
        vec![NamespaceMutationCandidate::Path(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-uncached-upload").expect("valid commit id"),
                absolute_path: "/docs/uncached.txt".to_owned(),
                content_ref: staged.content_ref,
                behavior: PutFileBehavior::CreateOnly,
            },
        )],
    );

    assert!(responses[0].is_ok());
    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 0);
}

#[test]
fn stat_and_list_record_full_basis_fallback_without_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = runtime(temp_dir.path(), "read-fallback-test");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    fs.stat_path(&namespace_id, "/docs").expect("stat docs");
    fs.list_path(&namespace_id, "/").expect("list root");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.read_full_basis_fallbacks, 2);
    assert_eq!(stats.read_materialized_table_hits, 0);
}

#[test]
fn stat_and_list_use_materialized_tables_after_checkpoint_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("read-materialized-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint(&namespace_id).expect("checkpoint");

    raw_store.reset_content_blob_counters();
    fs.stat_path(&namespace_id, "/docs/file.txt")
        .expect("stat materialized file");
    fs.list_path(&namespace_id, "/docs")
        .expect("list materialized docs");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.read_materialized_table_hits, 2);
    assert_eq!(stats.read_full_basis_fallbacks, 0);
    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 0);
}

#[test]
fn repeated_materialized_stat_uses_metadata_table_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = runtime(temp_dir.path(), "metadata-table-cache-test");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint(&namespace_id).expect("checkpoint");

    fs.stat_path(&namespace_id, "/docs/file.txt")
        .expect("first materialized stat");
    let after_first = fs.runtime_cache_stats();
    fs.stat_path(&namespace_id, "/docs/file.txt")
        .expect("second materialized stat");
    let after_second = fs.runtime_cache_stats();

    assert!(after_first.metadata_table_cache_inserts > 0);
    assert!(after_second.metadata_table_cache_hits > after_first.metadata_table_cache_hits);
    assert_eq!(after_second.read_materialized_table_hits, 2);
    assert_eq!(after_second.read_full_basis_fallbacks, 0);
}

#[test]
fn put_file_bytes_uses_memory_proof_without_blob_validation_call() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("put-file-memory-proof-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    raw_store.reset_content_blob_counters();
    fs.put_file_bytes(
        &namespace_id,
        "/docs/direct.txt",
        b"direct bytes",
        PutFileOptions::default(),
    )
    .expect("put file bytes");

    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 0);
}

#[test]
fn begin_upload_validates_controls_without_replay_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-cache-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint(&namespace_id).expect("checkpoint");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .expect("replace file");

    raw_store.reset_control_get_counts();
    fs.begin_upload(&namespace_id).expect("first begin upload");
    fs.begin_upload(&namespace_id).expect("second begin upload");

    assert_eq!(raw_store.wal_get_count(), 0);
    assert_eq!(raw_store.checkpoint_get_count(), 0);
}

#[test]
fn runtime_control_cache_reuses_head_for_basis_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("control-cache-head-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    fs.stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");

    raw_store.reset_control_get_counts();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first cached basis validation reuses cached head state");
    fs.stat_path(&namespace_id, "/docs")
        .expect("second cached basis validation reuses cached head state");

    assert_eq!(raw_store.head_get_count(), 0);
}

#[test]
fn control_cache_eviction_reloads_head_for_basis_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let other_namespace = NamespaceId::parse("other").expect("valid namespace id");
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("control-cache-eviction-test")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.create_namespace(&other_namespace, CreateNamespaceOptions::default())
        .expect("create other namespace");
    fs.create_dir(&other_namespace, "/docs", CreateDirOptions::default())
        .expect("create other docs");

    fs.stat_path(&namespace_id, "/docs")
        .expect("prime first namespace basis");
    fs.stat_path(&namespace_id, "/docs")
        .expect("prime first namespace head cache");

    raw_store.reset_control_get_counts();
    fs.stat_path(&other_namespace, "/docs")
        .expect("load other namespace basis and evict first head cache");
    fs.stat_path(&namespace_id, "/docs")
        .expect("reload first namespace basis and head cache");

    assert_eq!(raw_store.head_get_count(), 1);
}

#[test]
fn runtime_control_cache_reloads_head_after_external_change() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = Fs::builder(object_store.clone())
        .writer_id("control-cache-reader")
        .build()
        .expect("build reader runtime");
    let writer = Fs::builder(object_store)
        .writer_id("control-cache-writer")
        .build()
        .expect("build writer runtime");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");
    raw_store.reset_control_get_counts();
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime control cache");
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("reuse unchanged control cache");
    assert_eq!(raw_store.head_get_count(), 0);

    writer
        .create_dir(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head");
    raw_store.reset_control_get_counts();
    reader
        .stat_path(&namespace_id, "/docs/new")
        .expect("reload changed head");
    assert!(raw_store.head_get_count() > 0);
}

#[test]
fn begin_upload_rejects_missing_and_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-missing-partial-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    assert_core_error_kind(fs.begin_upload(&namespace_id), ErrorCode::NamespaceNotFound);

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .delete(&namespace_descriptor(namespace_id.as_str()))
        .expect("delete namespace descriptor");

    assert_core_error_kind(fs.begin_upload(&namespace_id), ErrorCode::NamespacePartial);
}

#[test]
fn begin_upload_rejects_malformed_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-malformed-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .put_overwrite(
            &namespace_descriptor(namespace_id.as_str()),
            br#"{"not":"a namespace descriptor"}"#,
        )
        .expect("corrupt namespace descriptor");
    assert_core_error_kind(fs.begin_upload(&namespace_id), ErrorCode::NamespaceCorrupt);

    let content_bad = NamespaceId::parse("content-bad").expect("valid namespace id");
    fs.create_namespace(&content_bad, CreateNamespaceOptions::default())
        .expect("create content-bad namespace");
    for key in raw_store
        .list_prefix("content-stores/")
        .expect("list content stores")
        .into_iter()
        .filter(|key| key.ends_with("/descriptor.json"))
    {
        raw_store
            .put_overwrite(&key, br#"{"not":"a content store descriptor"}"#)
            .expect("corrupt content store descriptor");
    }
    assert_core_error_kind(fs.begin_upload(&content_bad), ErrorCode::NamespaceCorrupt);
}

#[test]
fn begin_upload_rejects_malformed_head_and_lease_when_cache_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-malformed-control-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    let head_bad = NamespaceId::parse("head-bad").expect("valid namespace id");
    fs.create_namespace(&head_bad, CreateNamespaceOptions::default())
        .expect("create head-bad namespace");
    raw_store
        .put_overwrite(&namespace_head(head_bad.as_str()), br#"{"not":"a head"}"#)
        .expect("corrupt head");
    assert_core_error_kind(fs.begin_upload(&head_bad), ErrorCode::NamespaceCorrupt);

    let lease_bad = NamespaceId::parse("lease-bad").expect("valid namespace id");
    fs.create_namespace(&lease_bad, CreateNamespaceOptions::default())
        .expect("create lease-bad namespace");
    raw_store
        .put_overwrite(
            &namespace_lease(lease_bad.as_str()),
            br#"{"not":"a lease"}"#,
        )
        .expect("corrupt lease");
    assert_core_error_kind(fs.begin_upload(&lease_bad), ErrorCode::NamespaceCorrupt);
}

#[test]
fn explicit_commit_appears_in_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "commit-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let commit_id = CommitId::parse("explicit-create-dir").expect("valid commit id");
    let response = fs
        .commit_operations(
            &namespace_id,
            CommitRequest {
                commit_id: commit_id.clone(),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "docs".to_owned(),
                }],
                message: Some("create docs".to_owned()),
                annotations: None,
            },
        )
        .expect("commit operation");

    let changes = fs
        .list_changes_after(&namespace_id, ChangeSeq(0))
        .expect("list changes");
    assert_eq!(changes.through_seq, response.committed_seq);
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].commit_id, commit_id);
}

#[test]
fn namespace_status_reports_wal_tail_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "status-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let status = fs
        .namespace_status(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.wal_tail_segments, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.wal_tail_segments, 1);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
}

#[test]
fn namespace_status_and_tick_reject_missing_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "missing-status-test");
    let namespace_id = namespace();

    assert_core_error_kind(
        fs.namespace_status(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default()),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn namespace_status_and_tick_reject_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("partial-status-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .delete(&namespace_descriptor(namespace_id.as_str()))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.namespace_status(&namespace_id),
        ErrorCode::NamespacePartial,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default()),
        ErrorCode::NamespacePartial,
    );
}

#[test]
fn maintenance_tick_below_threshold_is_not_needed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-not-needed-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.namespace_id, namespace_id);
    assert_eq!(tick.status_before.wal_tail_segments, 1);
    assert_eq!(tick.outcome, MaintenanceTickOutcome::NotNeeded);
}

#[test]
fn maintenance_tick_at_segment_threshold_publishes_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-publish-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.status_before.head_seq, ChangeSeq(1));
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after checkpoint");
    assert_eq!(status.checkpoint_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(status.wal_tail_segments, 0);
}

#[test]
fn maintenance_tick_after_existing_checkpoint_publishes_l0_run_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-l0-run-publish-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put first file");
    fs.maintenance_tick_namespace(
        &namespace_id,
        MaintenanceTickOptions {
            max_wal_tail_segments: 1,
        },
    )
    .expect("first maintenance tick");

    fs.put_file_bytes(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::default(),
    )
    .expect("put second file");
    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
            },
        )
        .expect("second maintenance tick");
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after l0 run checkpoint");
    assert_eq!(status.checkpoint_hint_seq, Some(ChangeSeq(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let manifest_key = checkpoint_manifest(namespace_id.as_str(), 2);
    let manifest_bytes = raw_store
        .get(&manifest_key, None)
        .expect("read checkpoint manifest")
        .expect("checkpoint manifest exists");
    let manifest = decode_checkpoint_manifest_json(&manifest_bytes).expect("decode manifest");
    assert_eq!(manifest.payload.base_seq, ChangeSeq(1));
    let l0_runs = manifest
        .payload
        .runs
        .iter()
        .filter(|run| run.level == 0)
        .collect::<Vec<_>>();
    assert_eq!(l0_runs.len(), 1);
    assert_eq!(l0_runs[0].run_seq, ChangeSeq(2));
}

#[test]
fn maintenance_tick_counts_segments_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-segment-count-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let first_batch = fs.commit_operations_batch(
        &namespace_id,
        vec![
            CommitRequest {
                commit_id: CommitId::parse("create-a").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "a".to_owned(),
                }],
                message: None,
                annotations: None,
            },
            CommitRequest {
                commit_id: CommitId::parse("create-b").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "b".to_owned(),
                }],
                message: None,
                annotations: None,
            },
        ],
    );
    assert!(first_batch.iter().all(Result::is_ok));

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after first batch");
    assert_eq!(status.head_seq, ChangeSeq(2));
    assert_eq!(status.wal_tail_segments, 1);

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.outcome, MaintenanceTickOutcome::NotNeeded);

    fs.commit_operations(
        &namespace_id,
        CommitRequest {
            commit_id: CommitId::parse("create-c").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "c".to_owned(),
            }],
            message: None,
            annotations: None,
        },
    )
    .expect("second segment commit");

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
            },
        )
        .expect("maintenance tick at segment threshold");
    assert_eq!(tick.status_before.head_seq, ChangeSeq(3));
    assert_eq!(tick.status_before.wal_tail_segments, 2);
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(3)
        }
    );
}

#[test]
fn maintenance_tick_rejects_zero_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-config-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 0,
            },
        )
        .expect_err("zero threshold should fail");
    match error {
        RuntimeError::Config(message) => assert!(message.contains("max_wal_tail_segments")),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn maintenance_tick_treats_checkpoint_hint_cas_loss_as_benign_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("tick-race-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.fail_head_cas();
    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
            },
        )
        .expect("maintenance tick should not fail on checkpoint hint race");

    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublishRaceLost {
            observed_head_seq: ChangeSeq(1)
        }
    );
    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after lost race");
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.wal_tail_segments, 1);
}

#[test]
fn checkpoint_and_retention_hooks_are_available() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "maintenance-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .expect("create checkpoint");
    let retention = fs
        .advance_retention_floor(&namespace_id)
        .expect("advance retention");
    assert_eq!(retention.retention_floor_seq, checkpoint.checkpoint_seq);
}

#[test]
fn separate_runtime_instances_share_object_store_state() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = runtime(temp_dir.path(), "writer");
    let reader = runtime(temp_dir.path(), "reader");
    let namespace_id = namespace();

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/shared.txt",
            b"shared",
            PutFileOptions::default(),
        )
        .expect("put file");

    let namespaces = reader.list_namespaces().expect("list namespaces");
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0].namespace_id, namespace_id);
    let file = reader
        .read_file_bytes(&namespace_id, "/docs/shared.txt")
        .expect("read shared file");
    assert_eq!(file.bytes, b"shared");
}

#[derive(Debug)]
struct HeadCasFailureStore {
    inner: LocalFsStore,
    head_key: String,
    wal_prefix: String,
    checkpoint_prefix: String,
    fail_head_cas: AtomicBool,
    wal_get_count: AtomicUsize,
    checkpoint_get_count: AtomicUsize,
    head_get_count: AtomicUsize,
}

impl HeadCasFailureStore {
    fn new(root: &Path, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            head_key: namespace_head(namespace),
            wal_prefix: format!("namespaces/{namespace}/wal/"),
            checkpoint_prefix: format!("namespaces/{namespace}/checkpoints/"),
            fail_head_cas: AtomicBool::new(false),
            wal_get_count: AtomicUsize::new(0),
            checkpoint_get_count: AtomicUsize::new(0),
            head_get_count: AtomicUsize::new(0),
        }
    }

    fn fail_head_cas(&self) {
        self.fail_head_cas.store(true, Ordering::SeqCst);
    }

    fn allow_head_cas(&self) {
        self.fail_head_cas.store(false, Ordering::SeqCst);
    }

    fn reset_wal_get_count(&self) {
        self.wal_get_count.store(0, Ordering::SeqCst);
    }

    fn reset_control_get_counts(&self) {
        self.checkpoint_get_count.store(0, Ordering::SeqCst);
        self.head_get_count.store(0, Ordering::SeqCst);
        self.reset_wal_get_count();
    }

    fn wal_get_count(&self) -> usize {
        self.wal_get_count.load(Ordering::SeqCst)
    }

    fn checkpoint_get_count(&self) -> usize {
        self.checkpoint_get_count.load(Ordering::SeqCst)
    }

    fn head_get_count(&self) -> usize {
        self.head_get_count.load(Ordering::SeqCst)
    }
}

impl ObjectStore for HeadCasFailureStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        if key.starts_with(&self.wal_prefix) {
            self.wal_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with(&self.checkpoint_prefix) {
            self.checkpoint_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key == self.head_key {
            self.head_get_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && self.fail_head_cas.load(Ordering::SeqCst)
        {
            return Err(ObjectStoreError::PreconditionFailed);
        }
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

#[derive(Debug)]
struct ContentBlobGetCountingStore {
    inner: LocalFsStore,
    content_blob_gets: AtomicUsize,
    content_blob_checksum_heads: AtomicUsize,
}

impl ContentBlobGetCountingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            content_blob_gets: AtomicUsize::new(0),
            content_blob_checksum_heads: AtomicUsize::new(0),
        }
    }

    fn reset_content_blob_counters(&self) {
        self.content_blob_gets.store(0, Ordering::SeqCst);
        self.content_blob_checksum_heads.store(0, Ordering::SeqCst);
    }

    fn content_blob_get_count(&self) -> usize {
        self.content_blob_gets.load(Ordering::SeqCst)
    }

    fn content_blob_checksum_head_count(&self) -> usize {
        self.content_blob_checksum_heads.load(Ordering::SeqCst)
    }
}

impl ObjectStore for ContentBlobGetCountingStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_checksum_heads
                .fetch_add(1, Ordering::SeqCst);
        }
        self.inner.head_with_checksum(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}
