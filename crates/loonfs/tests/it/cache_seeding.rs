//! Runtime cache seeding, reuse, eviction, and cross-handle sharing.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    ChangeSeq, CreateDirectoryOptions, CreateNamespaceOptions, ErrorCode, InodeId, InodeKind,
    NamespaceId, PutFileOptions, ReorganizeStepOutcome, RuntimeCacheConfig, SharedObjectStore,
    StoredMetadataBlockKind,
};
use loonfs_core::test_support::{
    RecordedStoredMetadataBlockCall, RecordingStoredMetadataBlockCache,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn runtime_cache_reuses_wal_tail_projection_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "tail-projection-cache-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read is served from the projection the put seeded");
    assert_eq!(raw_store.wal_get_count(), 0);
    let after_first = fs.runtime_cache_stats();
    assert_eq!(after_first.wal_tail_projection_cache_misses, 0);
    assert!(after_first.wal_tail_projection_cache_inserts >= 1);
    assert!(after_first.wal_tail_projection_cache_hits >= 1);

    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should reuse cached WAL-tail projection");
    assert_eq!(raw_store.wal_get_count(), 0);
    let after_second = fs.runtime_cache_stats();
    assert!(
        after_second.wal_tail_projection_cache_hits > after_first.wal_tail_projection_cache_hits
    );

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/other.txt",
        b"other",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put other");
    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read after local mutation reuses the newly seeded projection");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "the put seeds the projection for its own resulting head"
    );
}

#[test]
fn runtime_publish_reuses_wal_tail_projection_for_sequential_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let setup = open_runtime(object_store.clone(), "publish-tail");
    let measured = open_runtime(object_store, "publish-tail");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-a",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed first WAL segment");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-b",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed second WAL segment");

    raw_store.reset_wal_get_count();
    measured
        .create_directory_blocking(
            &namespace_id,
            "/measured-a",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("first measured write loads existing tail");
    assert!(
        raw_store.wal_get_count() > 0,
        "first measured write should read the existing WAL tail"
    );

    raw_store.reset_wal_get_count();
    measured
        .create_directory_blocking(
            &namespace_id,
            "/measured-b",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("second measured write advances cached publish tail");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "second measured write should not reread WAL tail"
    );
}

#[test]
fn runtime_publish_allows_multi_segment_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let setup = open_runtime(object_store.clone(), "publish-tail");
    let measured = open_runtime(object_store, "publish-tail");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-a",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed first WAL segment");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-b",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed second WAL segment");

    measured
        .create_directory_blocking(
            &namespace_id,
            "/should-succeed",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("publish projects the visible WAL tail without a segment limit");
}

#[test]
fn runtime_cache_observes_head_advanced_by_another_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let reader = open_runtime(object_store.clone(), "tail-cache-reader");
    let writer = open_runtime(object_store, "tail-cache-writer");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_directory_blocking(
            &namespace_id,
            "/docs",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("create docs");

    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime reader cache");

    writer
        .create_directory_blocking(
            &namespace_id,
            "/docs/new",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("advance head from another runtime");

    raw_store.reset_wal_get_count();
    let stat = reader
        .stat_path_blocking(&namespace_id, "/docs/new")
        .expect("reader should observe external head advance");
    assert_eq!(stat.path, "/docs/new");
    assert_eq!(stat.head_seq, ChangeSeq(2));
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn runtime_cache_can_be_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime_with(object_store, "tail-cache-disabled-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig::disabled())
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read should project WAL tail");
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should project WAL tail again");
    assert_eq!(raw_store.wal_get_count(), 2);
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.wal_tail_projection_cache_hits, 0);
    assert_eq!(stats.wal_tail_projection_cache_misses, 0);
}

#[test]
fn runtime_wal_tail_projection_cache_evicts_by_namespace_count() {
    let temp_dir = tempdir().expect("tempdir");
    let first = NamespaceId::parse("first").expect("valid namespace id");
    let second = NamespaceId::parse("second").expect("valid namespace id");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &first));
    let shared_store = raw_store.store();
    let setup = open_runtime(shared_store.clone(), "tail-count-setup");

    setup
        .create_namespace_blocking(&first, CreateNamespaceOptions::default())
        .expect("create first namespace");
    setup
        .put_file_bytes_blocking(
            &first,
            "/file.txt",
            b"first",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put first file");
    setup
        .create_namespace_blocking(&second, CreateNamespaceOptions::default())
        .expect("create second namespace");
    setup
        .put_file_bytes_blocking(
            &second,
            "/file.txt",
            b"second",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put second file");

    let fs = open_runtime_with(shared_store, "tail-count-budget", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
    });

    fs.get_file_bytes_blocking(&first, "/file.txt")
        .expect("cache first tail projection");
    fs.get_file_bytes_blocking(&second, "/file.txt")
        .expect("cache second tail projection and evict first");
    let after_second = fs.runtime_cache_stats();
    assert_eq!(after_second.wal_tail_projection_cache_evictions, 1);
    assert!(after_second.wal_tail_projection_cache_cached_rows > 0);

    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&first, "/file.txt")
        .expect("first tail projection reloads after eviction");
    let after_reload = fs.runtime_cache_stats();
    assert_eq!(raw_store.wal_get_count(), 1);
    assert_eq!(after_reload.wal_tail_projection_cache_evictions, 2);
}

#[test]
fn runtime_wal_tail_projection_cache_skips_oversized_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime_with(object_store, "tail-oversized-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_wal_tail_projection_rows: 0,
            ..RuntimeCacheConfig::default()
        })
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/file.txt")
        .expect("first read projects oversized tail");
    fs.get_file_bytes_blocking(&namespace_id, "/file.txt")
        .expect("second read projects oversized tail again");
    assert_eq!(raw_store.wal_get_count(), 2);
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.wal_tail_projection_cache_misses, 2);
    assert_eq!(stats.wal_tail_projection_cache_hits, 0);
    assert_eq!(stats.wal_tail_projection_cache_uncacheable_count, 2);
    assert_eq!(stats.wal_tail_projection_cache_cached_rows, 0);
}

#[test]
fn runtime_read_allows_multi_segment_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let fs = open_runtime(store(temp_dir.path()), "tail-read-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");
    fs.create_directory_blocking(
        &namespace_id,
        "/more",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create another WAL segment");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("read projects the visible WAL tail without a segment limit");
}

#[test]
fn stale_head_write_error_recovers_and_reseeds_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "tail-cache-stale-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.fail_head_cas();
    assert_core_error_kind(
        fs.create_directory_blocking(
            &namespace_id,
            "/stale",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        ),
        ErrorCode::StaleHead,
    );

    raw_store.allow_head_cas();
    fs.create_directory_blocking(
        &namespace_id,
        "/after-stale",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("write after stale head succeeds (the engine revalidates its projection by etag)");

    raw_store.reset_wal_get_count();
    fs.stat_path_blocking(&namespace_id, "/after-stale")
        .expect("read after the recovered write");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "the recovered write seeds the read caches like any landed publish"
    );
}

#[test]
fn stat_and_list_use_initial_manifest_without_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let fs = runtime(temp_dir.path(), "read-fallback-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("stat docs");
    fs.list_path_blocking(&namespace_id, "/")
        .expect("list root");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
}

#[test]
fn stat_and_list_use_materialized_segments_after_checkpoint_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "read-materialized-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");

    raw_store.reset();
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("stat materialized file");
    fs.list_path_blocking(&namespace_id, "/docs")
        .expect("list materialized docs");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
    assert_eq!(raw_store.count(OperationClass::Read), 0);
}

/// Concurrent stat and list race through the shared async metadata-view cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_materialized_stat_and_list_share_async_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let fs = open_runtime_async(store(temp_dir.path()), "concurrent-materialized-read-test").await;

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("put file");
    fs.create_checkpoint(&namespace_id)
        .await
        .expect("checkpoint");

    let (stat, list) = tokio::join!(
        fs.get_path_entry(&namespace_id, "/docs/file.txt"),
        fs.list_path(&namespace_id, "/docs"),
    );
    let stat = stat.expect("stat file");
    let list = list.expect("list docs");

    assert_eq!(stat.path, "/docs/file.txt");
    assert_eq!(stat.size_bytes(), Some(4));
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].path, "/docs/file.txt");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
}

#[test]
fn repeated_materialized_stat_uses_metadata_segment_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let fs = runtime(temp_dir.path(), "metadata-segment-cache-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");
    // The checkpoint published the namespace's first manifest. One more
    // write moves the head, so the next read resolves its anchor against
    // that manifest instead of the genesis basis it had pinned.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");

    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("first materialized stat");
    let after_first = fs.runtime_cache_stats();
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("second materialized stat");
    let after_second = fs.runtime_cache_stats();

    assert!(after_first.metadata_segment_cache_inserts > 0);
    assert!(after_second.metadata_segment_cache_hits > after_first.metadata_segment_cache_hits);
    assert_eq!(after_second.latest_metadata_view_reads, 2);
}

#[test]
fn runtime_control_cache_reuses_head_for_materialization_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "control-cache-head-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.reset_control_get_counts();
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("first cached materialization validation reuses cached head state");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("second cached materialization validation reuses cached head state");

    assert_eq!(raw_store.head_get_count(), 0);
}

#[test]
fn control_cache_eviction_reloads_head_for_materialization_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let other_namespace = NamespaceId::parse("other").expect("valid namespace id");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime_with(object_store, "control-cache-eviction-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");
    fs.create_namespace_blocking(&other_namespace, CreateNamespaceOptions::default())
        .expect("create other namespace");
    fs.create_directory_blocking(
        &other_namespace,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create other docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime first namespace materialization");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime first namespace head cache");

    raw_store.reset_control_get_counts();
    fs.stat_path_blocking(&other_namespace, "/docs")
        .expect("load other namespace materialization and evict first head cache");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("reload first namespace materialization and head cache");

    assert_eq!(raw_store.head_get_count(), 1);
}

#[test]
fn runtime_control_cache_reloads_head_after_external_change() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let reader = open_runtime(object_store.clone(), "control-cache-reader");
    let writer = open_runtime(object_store, "control-cache-writer");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_directory_blocking(
            &namespace_id,
            "/docs",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("create docs");
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");
    raw_store.reset_control_get_counts();
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime control cache");
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("reuse unchanged control cache");
    assert_eq!(raw_store.head_get_count(), 0);

    writer
        .create_directory_blocking(
            &namespace_id,
            "/docs/new",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("advance head");
    raw_store.reset_control_get_counts();
    reader
        .stat_path_blocking(&namespace_id, "/docs/new")
        .expect("reload changed head");
    assert!(raw_store.head_get_count() > 0);
}

#[test]
fn root_stat_and_list_work_immediately_after_namespace_create() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "initial-manifest-read-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let root = fs
        .stat_path_blocking(&namespace_id, "/")
        .expect("stat root after create");
    assert_eq!(root.path, "/");
    assert_eq!(root.inode_id, InodeId(1));
    assert_eq!(root.inode_kind(), InodeKind::Directory);
    assert_eq!(root.head_seq, ChangeSeq(0));

    let entries = fs
        .list_path_blocking(&namespace_id, "/")
        .expect("list root after create");
    assert!(entries.is_empty());
}

#[test]
fn separate_runtime_instances_share_object_store_state() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = runtime(temp_dir.path(), "writer");
    let reader = runtime(temp_dir.path(), "reader");
    let namespace_id = namespace_id("demo");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .put_file_bytes_blocking(
            &namespace_id,
            "/docs/shared.txt",
            b"shared",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");

    let file = reader
        .get_file_bytes_blocking(&namespace_id, "/docs/shared.txt")
        .expect("read shared file");
    assert_eq!(file.bytes, b"shared");
}

/// A runtime fills the stored-block cache while reading. A second runtime with
/// an empty decoded cache then reads a segment section from the stored cache.
/// The decoded-cache assertion confirms that both reads use this cache path.
#[test]
fn an_installed_stored_block_cache_is_filled_and_then_serves_a_later_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let shared_store = store(temp_dir.path());
    let fs = open_runtime_with(shared_store.clone(), "stored-block-writer", |builder| {
        builder.stored_metadata_block_cache(stored_blocks.clone())
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");
    // A write after the checkpoint moves the head, so the reads below
    // resolve against the published manifest and touch its segments.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");

    let file = fs
        .get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read file");
    assert_eq!(file.bytes, b"file");
    let entries = fs
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs");
    assert_eq!(entries.len(), 2);

    assert!(
        fs.runtime_cache_stats().metadata_segment_cache_inserts > 0,
        "the cycle must reach the decoded block cache for this to prove anything"
    );
    let offered: Vec<StoredMetadataBlockKind> = stored_blocks
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            RecordedStoredMetadataBlockCall::Insert { key, .. } => Some(key.kind),
            RecordedStoredMetadataBlockCall::Get { .. }
            | RecordedStoredMetadataBlockCall::Invalidate { .. } => None,
        })
        .collect();
    for kind in [
        StoredMetadataBlockKind::Index,
        StoredMetadataBlockKind::Filter,
        StoredMetadataBlockKind::Data,
    ] {
        assert!(
            offered.contains(&kind),
            "the fetched segment's {kind:?} section was not offered to the cache"
        );
    }

    let calls_before = stored_blocks.call_count();
    let reader = open_runtime_with(shared_store, "stored-block-reader", |builder| {
        builder.stored_metadata_block_cache(stored_blocks.clone())
    });
    let entries = reader
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs from a second runtime");
    assert_eq!(entries.len(), 2);
    assert!(
        stored_blocks.calls()[calls_before..]
            .iter()
            .any(|call| matches!(call, RecordedStoredMetadataBlockCall::Get { hit: true, .. })),
        "the second runtime should have been served a section by the cache"
    );
    assert!(
        !stored_blocks.is_closed(),
        "the host owns the cache and closes it"
    );
}

/// Maintenance reads through no metadata segment cache, so the local tier
/// beneath it never sees a maintenance read. Reorganization is the widest
/// read maintenance has — it decodes every row of the runs it folds — and
/// this pins the structural argument end to end: not one block offered, and
/// not one probe either.
#[test]
fn metadata_maintenance_offers_nothing_to_the_local_block_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let fs = open_runtime_with(store(temp_dir.path()), "maintenance-cold", |builder| {
        builder.stored_metadata_block_cache(stored_blocks.clone())
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    // Each step folds the tail into one more delta run, and the default policy
    // admits a reorganization unit once enough of them have piled up. Reads
    // the writes make on the way are outside every measured window.
    let mut reorganized = false;
    for index in 0..16 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/file-{index:02}.txt"),
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
        let calls_before = stored_blocks.call_count();
        let step = fs
            .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
            .expect("maintenance step");
        assert_eq!(
            stored_blocks.call_count(),
            calls_before,
            "a maintenance step carries no segment cache, so it reaches neither cache tier"
        );
        if upkeep(&step).reorganize == ReorganizeStepOutcome::UnitPublished {
            reorganized = true;
            break;
        }
    }

    assert!(
        reorganized,
        "the steps above should have folded one reorganization unit"
    );
    assert!(
        stored_blocks.call_count() > 0,
        "the reads around the steps must reach the cache for this to prove anything"
    );
}
