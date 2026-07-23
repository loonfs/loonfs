//! Post-publish cache behavior: a landed publish seeds the read caches
//! instead of dropping them, and no cache lifecycle event — invalidation,
//! LRU eviction, or running with caches disabled — erases writer fencing.

use loonfs::{
    CreateNamespaceOptions, FsWriter, NamespaceId, PutFileOptions, RuntimeCacheConfig,
    RuntimeError, SharedObjectStore,
};
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

fn expect_writer_fenced<T: std::fmt::Debug>(result: loonfs::Result<T>, when: &str) {
    let error = result.expect_err(when);
    assert!(
        matches!(
            &error,
            RuntimeError::Core(core) if core.code() == loonfs::ErrorCode::WriterFenced
        ),
        "{when}: unexpected error: {error:?}"
    );
}

/// A fenced writer session must stay fenced: before this fix, the runtime
/// dropped the commit engine after every successful publish and maintenance
/// pass, so a superseded writer forgot its fencing and silently re-acquired
/// the epoch — two live writers would fence each other back and forth
/// instead of one surfacing `writer_fenced`.
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
        .put_file_bytes(&namespace_id, "/a1.txt", b"a", PutFileOptions::default())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(&namespace_id, "/b1.txt", b"b", PutFileOptions::default())
        .await
        .expect("writer b takes over the epoch");

    let fenced = writer_a
        .put_file_bytes(&namespace_id, "/a2.txt", b"a", PutFileOptions::default())
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
        .put_file_bytes(&namespace_id, "/a3.txt", b"a", PutFileOptions::default())
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
        .put_file_bytes(&namespace_id, "/b2.txt", b"b", PutFileOptions::default())
        .await
        .expect("live writer is not fenced back");
}

/// Fencing must also survive LRU eviction of the namespace's cache entry:
/// the writer-session registry, not the evictable commit engine, owns it.
/// Before this fix, capacity pressure from touching other namespaces
/// evicted the fenced engine, and the fresh engine's first publish silently
/// re-acquired the epoch — the fenced writer resumed publishing and fenced
/// the legitimate writer back.
#[tokio::test]
async fn fenced_writer_stays_fenced_after_namespace_lru_eviction() {
    let temp_dir = tempdir().expect("tempdir");
    let ns_fence = NamespaceId::parse("fence").expect("valid namespace id");
    let ns_other = NamespaceId::parse("other").expect("valid namespace id");
    let counting = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::wal_head(ns_fence.as_str()),
    ));
    let store: SharedObjectStore = counting.clone();

    let single_entry_cache = RuntimeCacheConfig {
        max_cached_namespaces: 1,
        ..RuntimeCacheConfig::default()
    };
    let writer_a = writer_with_cache(&store, "writer-a", single_entry_cache).await;
    writer_a
        .create_namespace(&ns_fence, CreateNamespaceOptions::default())
        .await
        .expect("create fence namespace");
    writer_a
        .create_namespace(&ns_other, CreateNamespaceOptions::default())
        .await
        .expect("create other namespace");
    writer_a
        .put_file_bytes(&ns_fence, "/a1.txt", b"a", PutFileOptions::default())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(&ns_fence, "/b1.txt", b"b", PutFileOptions::default())
        .await
        .expect("writer b takes over the epoch");

    expect_writer_fenced(
        writer_a
            .put_file_bytes(&ns_fence, "/a2.txt", b"a", PutFileOptions::default())
            .await,
        "superseded writer surfaces fencing",
    );

    // Publishing to the other namespace evicts the fenced namespace's cache
    // entry (capacity one), commit engine included.
    writer_a
        .put_file_bytes(&ns_other, "/spill.txt", b"s", PutFileOptions::default())
        .await
        .expect("writer a publishes to the other namespace");

    let head_cas_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        writer_a
            .put_file_bytes(&ns_fence, "/a3.txt", b"a", PutFileOptions::default())
            .await,
        "fenced session stays fenced after eviction",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        head_cas_after_fencing,
        "a fenced session must not touch the fenced namespace's head"
    );
    writer_b
        .put_file_bytes(&ns_fence, "/b2.txt", b"b", PutFileOptions::default())
        .await
        .expect("live writer is not fenced back");
}

/// With runtime caches disabled every publish gets a throwaway engine, but
/// the session's epoch and fencing come from the registry, which no cache
/// configuration disables. Before this fix, cache-disabled runs never kept
/// fencing at all: a superseded writer re-acquired the epoch on every
/// publish and the two writers fenced each other back and forth.
#[tokio::test]
async fn fenced_writer_stays_fenced_with_runtime_caches_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("nocache").expect("valid namespace id");
    let counting = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::wal_head(namespace_id.as_str()),
    ));
    let store: SharedObjectStore = counting.clone();

    let writer_a = writer_with_cache(&store, "writer-a", RuntimeCacheConfig::disabled()).await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .put_file_bytes(&namespace_id, "/a1.txt", b"a", PutFileOptions::default())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(&namespace_id, "/b1.txt", b"b", PutFileOptions::default())
        .await
        .expect("writer b takes over the epoch");

    expect_writer_fenced(
        writer_a
            .put_file_bytes(&namespace_id, "/a2.txt", b"a", PutFileOptions::default())
            .await,
        "superseded writer surfaces fencing",
    );

    let head_cas_after_fencing = counting.count(OperationClass::CompareAndSwap);
    expect_writer_fenced(
        writer_a
            .put_file_bytes(&namespace_id, "/a3.txt", b"a", PutFileOptions::default())
            .await,
        "fenced session stays fenced across throwaway engines",
    );
    assert_eq!(
        counting.count(OperationClass::CompareAndSwap),
        head_cas_after_fencing,
        "a fenced session must not touch the namespace's head"
    );
    writer_b
        .put_file_bytes(&namespace_id, "/b2.txt", b"b", PutFileOptions::default())
        .await
        .expect("live writer is not fenced back");
}

/// A landed publish seeds the read caches with the state it just produced,
/// so read-after-write on the same core issues no store GETs at all: the
/// anchor, catalog, manifest, tail projection, and table blocks are all in
/// memory.
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
                PutFileOptions::default(),
            )
            .await
            .expect("warmup put");
    }
    reader
        .stat_path(&namespace_id, "/docs/warm-0.txt")
        .await
        .expect("warmup stat");

    recording.take_get_keys();
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/fresh.txt",
            b"fresh",
            PutFileOptions::default(),
        )
        .await
        .expect("steady-state put");
    // The publish itself must read the live head and root for freshness;
    // nothing else.
    let write_gets = recording.take_get_keys();
    assert!(
        write_gets
            .iter()
            .all(|key| key.ends_with("/wal/head.json") || key.ends_with("/metadata/root.json")),
        "a steady-state write reads only the live head and root, got {write_gets:?}"
    );

    reader
        .stat_path(&namespace_id, "/docs/fresh.txt")
        .await
        .expect("read after write");
    let read_gets = recording.take_get_keys();
    assert_eq!(
        read_gets,
        Vec::<String>::new(),
        "read-after-write must be served from the seeded caches"
    );
}
