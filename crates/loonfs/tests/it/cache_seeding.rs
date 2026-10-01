//! Runtime cache seeding, reuse, eviction, and cross-handle sharing.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    ChangeSeq, CompactionStepOutcome, CreateDirectoryOptions, CreateNamespaceOptions, ErrorCode,
    InodeId, InodeKind, MetadataCache, NamespaceId, PutFileOptions, SharedObjectStore,
    StoredMetadataBlockKind,
};
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_core::test_support::{
    RecordedStoredMetadataBlockCall, RecordingStoredMetadataBlockCache,
};
use loonfs_objectstore::layout::DurableObjectFamily;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass, RecordingStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn runtime_cache_reuses_wal_tail_projection_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let object_store: SharedObjectStore = recording.clone();
    let fs = open_runtime_with(object_store, "tail-projection-cache-test", |builder| {
        builder.manifest_revalidation_interval_ms(u64::MAX)
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    block_on(fs.writer.drain()).expect("finish hints");
    recording.reset();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read is served from the projection the put seeded");
    assert_wal_probe(recording.take(), &namespace_id, loonfs_api::WalNo(3));
    let after_first = fs.metadata_cache_stats();
    assert_eq!(after_first.wal_tail_misses, 0);
    assert!(after_first.wal_tail_inserts >= 1);
    assert!(after_first.wal_tail_hits >= 1);

    block_on(fs.writer.drain()).expect("finish hints");
    recording.reset();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should reuse cached WAL-tail projection");
    assert_wal_probe(recording.take(), &namespace_id, loonfs_api::WalNo(3));
    let after_second = fs.metadata_cache_stats();
    assert!(after_second.wal_tail_hits > after_first.wal_tail_hits);

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/other.txt",
        b"other",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put other");
    block_on(fs.writer.drain()).expect("finish hints");
    recording.reset();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read after local mutation reuses the newly seeded projection");
    assert_wal_probe(recording.take(), &namespace_id, loonfs_api::WalNo(4));
    let after_mutation = fs.metadata_cache_stats();
    assert_eq!(after_mutation.wal_tail_misses, 0);
    assert!(after_mutation.wal_tail_hits > after_second.wal_tail_hits);
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
        .create_namespace_blocking(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("create namespace");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-a",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed first WAL object");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-b",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed second WAL object");

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

#[tokio::test]
async fn runtime_publish_reuses_wal_tail_projection_while_a_fold_runs() {
    let directory = tempdir().expect("directory");
    let namespace_id = namespace_id("fold-projection");
    let blocking = BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        folded_manifest_put,
    );
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let tail_objects = Arc::new(AtomicU64::new(0));
    let writer = loonfs::LoonFs::builder_with_store(recording.clone())
        .writer_id("fold-projection")
        .maintenance_hint_observer({
            let tail_objects = Arc::clone(&tail_objects);
            move |hint| {
                if let loonfs::MaintenanceHint::Published(publication) = hint {
                    tail_objects.store(publication.wal_tail_objects, Ordering::SeqCst);
                }
            }
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    recording.inner().block_next();
    for number in 0..FOLD_AT_WAL_OBJECTS + 3 {
        recording.reset();
        namespace
            .create_directory(
                &format!("/directory-{number}"),
                CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("publish directory");
        if tail_objects.load(Ordering::SeqCst) == FOLD_AT_WAL_OBJECTS {
            recording.inner().wait_until_blocked().await;
        }
        if number > 0 {
            let wal = recording
                .snapshot()
                .into_iter()
                .filter(|operation| {
                    operation
                        .key()
                        .starts_with(&loonfs_objectstore::keys::wal_prefix(&namespace_id))
                })
                .collect::<Vec<_>>();
            assert_eq!(wal.len(), 1, "publish {number}: {wal:?}");
            assert!(matches!(
                wal[0],
                loonfs_test_support::stores::RecordedOperation::Put { .. }
            ));
        }
        assert_eq!(tail_objects.load(Ordering::SeqCst), number + 2);
    }
    recording.inner().release();
    namespace.wait_for_fold().await.expect("fold");
    writer.drain().await.expect("finish hints");
    let manifest =
        loonfs_core::control::load_namespace_current_manifest(recording.as_ref(), &namespace_id)
            .await
            .expect("folded manifest");
    let manifest_key = loonfs_objectstore::keys::metadata_manifest_object(
        &namespace_id,
        &manifest.state.manifest().manifest_no,
    );
    recording.reset();
    namespace
        .create_directory(
            "/after-fold",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish after fold");
    assert!(tail_objects.load(Ordering::SeqCst) < FOLD_AT_WAL_OBJECTS);
    let operations = recording.take();
    let mut manifest_gets = 0;
    let mut wal_puts = 0;
    for operation in &operations {
        match operation {
            loonfs_test_support::stores::RecordedOperation::Get { key, .. }
                if key == &manifest_key =>
            {
                manifest_gets += 1;
            }
            loonfs_test_support::stores::RecordedOperation::Get { key, .. }
                if key.starts_with(&format!("namespaces/{namespace_id}/segments/")) => {}
            loonfs_test_support::stores::RecordedOperation::Put { key, .. }
                if key.starts_with(&loonfs_objectstore::keys::wal_prefix(&namespace_id)) =>
            {
                wal_puts += 1;
            }
            loonfs_test_support::stores::RecordedOperation::GetWithMetadata { key, .. }
            | loonfs_test_support::stores::RecordedOperation::CompareAndSwap { key, .. }
                if key == &loonfs_objectstore::keys::hint(&namespace_id) => {}
            other => panic!("unexpected operation after fold: {other:?}"),
        }
    }
    assert!(manifest_gets <= 1, "{operations:?}");
    assert_eq!(wal_puts, 1);
    writer.shutdown().await.expect("shutdown");
}

#[test]
fn runtime_publish_and_read_allow_multi_object_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let setup = open_runtime(object_store.clone(), "publish-tail");
    let measured_read = open_runtime(object_store.clone(), "publish-tail");
    let measured_publish = open_runtime(object_store, "publish-tail");

    setup
        .create_namespace_blocking(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("create namespace");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-a",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed first WAL object");
    setup
        .create_directory_blocking(
            &namespace_id,
            "/seed-b",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("seed second WAL object");

    measured_read
        .stat_path_blocking(&namespace_id, "/seed-a")
        .expect("read projects the visible WAL tail without a WAL object limit");
    measured_publish
        .create_directory_blocking(
            &namespace_id,
            "/should-succeed",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("publish projects the visible WAL tail without a WAL object limit");
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
        .create_namespace_blocking(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
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
fn a_cache_with_zero_limits_keeps_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime_with(object_store, "tail-cache-disabled-test", |builder| {
        builder.metadata_cache(
            MetadataCache::builder()
                .max_segment_bytes(0)
                .max_head_state_bytes(0)
                .build(),
        )
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    block_on(fs.writer.drain()).expect("finish hints");
    raw_store.reset_wal_get_count();
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read should project WAL tail");
    fs.get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should project WAL tail again");
    assert_eq!(raw_store.wal_get_count(), 10);
    let stats = fs.metadata_cache_stats();
    assert_eq!(stats.wal_tail_hits, 0);
    assert_eq!(stats.wal_tail_misses, 0);
}

/// Creates `first` and `other`, each holding one file of five bytes, so the
/// two namespaces' head state weighs the same.
fn two_namespaces_with_one_file(store: SharedObjectStore) -> (NamespaceId, NamespaceId) {
    let first = namespace_id("first");
    let other = namespace_id("other");
    let setup = open_runtime(store, "head-state-setup");
    for (namespace_id, bytes) in [(&first, b"first"), (&other, b"other")] {
        setup
            .create_namespace_blocking(
                namespace_id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .expect("create namespace");
        setup
            .put_file_bytes_blocking(
                namespace_id,
                "/file.txt",
                bytes,
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .expect("put file");
    }
    block_on(setup.writer.drain()).expect("finish hints");
    (first, other)
}

/// What one namespace's head state weighs after a cold read of its file:
/// the head anchor and the WAL-tail projection.
fn one_namespace_head_state_bytes() -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let shared_store = store(temp_dir.path());
    let (first, _) = two_namespaces_with_one_file(shared_store.clone());
    let fs = open_runtime(shared_store, "head-state-measure");
    fs.get_file_bytes_blocking(&first, "/file.txt")
        .expect("cold read");
    fs.metadata_cache_stats().head_state_bytes
}

#[test]
fn head_state_evicts_by_bytes_anchors_included() {
    // Room for one namespace's head state, not for two.
    let budget = one_namespace_head_state_bytes() * 5 / 4;
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(RuntimeStoreProbe::new(
        temp_dir.path(),
        &namespace_id("first"),
    ));
    let (first, other) = two_namespaces_with_one_file(raw_store.store());
    let fs = open_runtime_with(raw_store.store(), "head-state-budget", |builder| {
        builder.metadata_cache(
            MetadataCache::builder()
                .max_head_state_bytes(budget)
                .build(),
        )
    });

    fs.get_file_bytes_blocking(&first, "/file.txt")
        .expect("cache the first namespace's head state");
    fs.get_file_bytes_blocking(&other, "/file.txt")
        .expect("cache the other namespace's head state");
    let after_other = fs.metadata_cache_stats();
    assert!(after_other.head_state_evictions > 0);
    assert!(after_other.head_state_bytes <= budget);

    raw_store.reset_control_get_counts();
    let file = fs
        .get_file_bytes_blocking(&first, "/file.txt")
        .expect("reload the evicted head state");
    assert_eq!(file.bytes, b"first");
    // Discovery reads the starting hint and rechecks it after the final gap.
    assert_eq!(
        raw_store.hint_get_count(),
        2,
        "the oldest entry, the first namespace's anchor, was evicted"
    );
    assert_eq!(raw_store.wal_get_count(), 3);
    let after_reload = fs.metadata_cache_stats();
    assert_eq!(
        after_reload.wal_tail_inserts,
        after_other.wal_tail_inserts + 1
    );
    assert_eq!(
        after_reload.wal_tail_hits,
        after_other.wal_tail_hits + 1,
        "the read uses the projection its cold discovery inserted"
    );
    assert_eq!(after_reload.wal_tail_misses, after_other.wal_tail_misses);
}

#[tokio::test]
async fn every_head_stays_cached_past_sixty_four_namespaces_under_the_default_budget() {
    const NAMESPACES: usize = 80;

    let temp_dir = tempdir().expect("tempdir");
    let hints = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::family(DurableObjectFamily::Hint),
    ));
    let object_store: SharedObjectStore = hints.clone();
    let setup = open_runtime_async(object_store.clone(), "many-namespaces").await;
    let namespaces = (0..NAMESPACES)
        .map(|index| namespace_id(&format!("ns-{index:03}")))
        .collect::<Vec<_>>();
    for namespace_id in &namespaces {
        setup
            .create_namespace(
                namespace_id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
    }
    let reader = loonfs::LoonFs::builder_with_store(object_store)
        .read_only()
        .build()
        .await
        .expect("reader");
    let stat_every_root = || async {
        for namespace_id in &namespaces {
            reader
                .namespace(namespace_id)
                .get_path_entry("/", Default::default())
                .await
                .expect("stat the root");
        }
    };

    stat_every_root().await;
    hints.reset();
    stat_every_root().await;
    assert_eq!(
        hints.count(OperationClass::Read),
        0,
        "no namespace's head was dropped and loaded again"
    );
    assert_eq!(reader.metadata_cache().stats().head_state_evictions, 0);
}

#[test]
fn runtime_wal_tail_projection_cache_skips_oversized_projection() {
    // Half of a namespace's anchor and tail together holds the anchor but not
    // the tail, which outweighs it.
    let budget = one_namespace_head_state_bytes() / 2;
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("first");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let object_store: SharedObjectStore = recording.clone();
    let fs = open_runtime_with(object_store, "tail-oversized-test", |builder| {
        builder
            .manifest_revalidation_interval_ms(u64::MAX)
            .metadata_cache(
                MetadataCache::builder()
                    .max_head_state_bytes(budget)
                    .build(),
            )
    });
    let namespace = fs.reader.namespace(&namespace_id);

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/file.txt",
        b"first",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    block_on(fs.writer.drain()).expect("finish hints");
    recording.reset();
    let _view = block_on(namespace.read_view()).expect("view the seeded namespace");
    assert_wal_probe(recording.take(), &namespace_id, loonfs_api::WalNo(3));
    for _ in 0..2 {
        fs.get_file_bytes_blocking(&namespace_id, "/file.txt")
            .expect("read replays the uncached tail");
        let operations = recording.take();
        assert_eq!(operations.len(), 3);
        assert_wal_probe(
            operations[2..].to_vec(),
            &namespace_id,
            loonfs_api::WalNo(3),
        );
        // The bounded replay overlaps its reads, so they finish in either order.
        let mut replayed = operations[..2]
            .iter()
            .map(|operation| match operation {
                loonfs_test_support::stores::RecordedOperation::Get {
                    key,
                    range,
                    result_bytes,
                } => {
                    assert_eq!(*range, None);
                    assert!(*result_bytes > 0);
                    key.clone()
                }
                other => panic!("expected WAL object read, got {other:?}"),
            })
            .collect::<Vec<_>>();
        replayed.sort();
        assert_eq!(
            replayed,
            [1, 2].map(|wal_no| format!("namespaces/{namespace_id}/wal/{wal_no:020}.wal.zst"))
        );
    }
    let stats = fs.metadata_cache_stats();
    assert_eq!(stats.wal_tail_misses, 2);
    assert_eq!(stats.wal_tail_hits, 0);
    assert_eq!(stats.head_state_rejections, 3);
    assert_eq!(stats.head_state_evictions, 0);
    assert!(stats.head_state_bytes <= budget);
}

#[test]
fn wal_publication_conflict_recovers_and_reseeds_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let recording = Arc::new(RecordingStore::new(raw_store.store(), KeyPredicate::any()));
    let fs = open_runtime_with(recording.clone(), "tail-cache-stale-test", |builder| {
        builder.manifest_revalidation_interval_ms(u64::MAX)
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/docs",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create docs");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.fail_wal_publish();
    assert_core_error_kind(
        fs.create_directory_blocking(
            &namespace_id,
            "/stale",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        ),
        ErrorCode::StaleHead,
    );

    raw_store.allow_wal_publish();
    fs.create_directory_blocking(
        &namespace_id,
        "/after-stale",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("write succeeds after the WAL publication conflict");

    block_on(fs.writer.drain()).expect("finish hints");
    recording.reset();
    let before_read = fs.metadata_cache_stats();
    fs.stat_path_blocking(&namespace_id, "/after-stale")
        .expect("read after the recovered write");
    assert_wal_probe(recording.take(), &namespace_id, loonfs_api::WalNo(4));
    let after_read = fs.metadata_cache_stats();
    assert_eq!(after_read.wal_tail_misses, before_read.wal_tail_misses);
    assert_eq!(after_read.wal_tail_hits, before_read.wal_tail_hits + 1);
}

#[test]
fn stat_and_list_use_initial_manifest_without_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let fs = open_runtime_with(store(temp_dir.path()), "read-fallback-test", |builder| {
        builder.metrics_recorder(recorder.clone())
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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

    assert_eq!(latest_metadata_view_reads(&recorder), 2);
}

fn latest_metadata_view_reads(recorder: &DefaultMetricsRecorder) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.runtime_cache.latest_metadata_view_reads")
        .next()
        .expect("the runtime registers its view-read counter");
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

#[test]
fn stat_and_list_use_materialized_segments_after_checkpoint_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let fs = open_runtime_with(object_store, "read-materialized-test", |builder| {
        builder.metrics_recorder(recorder.clone())
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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

    assert_eq!(latest_metadata_view_reads(&recorder), 2);
    assert_eq!(raw_store.count(OperationClass::Read), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_materialized_stat_and_list_share_async_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let fs = open_runtime_with_async(
        store(temp_dir.path()),
        "concurrent-materialized-read-test",
        |builder| builder.metrics_recorder(recorder.clone()),
    )
    .await;

    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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

    assert_eq!(latest_metadata_view_reads(&recorder), 2);
}

#[test]
fn repeated_materialized_stat_uses_metadata_segment_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let fs = runtime(temp_dir.path(), "metadata-segment-cache-test");

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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
    block_on(fs.writer.drain()).expect("finish hints");
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let fs = open_runtime_with(store(temp_dir.path()), "materialized-reader", |builder| {
        builder.metrics_recorder(recorder.clone())
    });
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("first materialized stat");
    let after_first = fs.metadata_cache_stats();
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("second materialized stat");
    let after_second = fs.metadata_cache_stats();

    assert!(after_first.segment_inserts > 0);
    assert!(after_second.segment_hits > after_first.segment_hits);
    assert_eq!(latest_metadata_view_reads(&recorder), 2);
}

#[test]
fn a_cached_head_anchor_serves_materialization_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "control-cache-head-test");

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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

    assert_eq!(raw_store.hint_get_count(), 0);
}

#[test]
fn a_cached_head_anchor_probes_the_wal_after_an_external_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let reader = open_runtime_with(object_store.clone(), "control-cache-reader", |builder| {
        builder.manifest_revalidation_interval_ms(u64::MAX)
    });
    let writer = open_runtime(object_store, "control-cache-writer");

    writer
        .create_namespace_blocking(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
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
        .expect("prime the head anchor");
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("reuse the unchanged head anchor");
    assert_eq!(raw_store.hint_get_count(), 0);

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
        .expect("probe changed head");
    assert_eq!(raw_store.hint_get_count(), 0);
}

#[test]
fn root_stat_and_list_work_immediately_after_namespace_create() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "initial-manifest-read-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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
        .create_namespace_blocking(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
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

#[test]
fn an_installed_stored_block_cache_is_filled_and_then_serves_a_later_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let shared_store = store(temp_dir.path());
    let fs = open_runtime_with(shared_store.clone(), "stored-block-writer", |builder| {
        builder.stored_metadata_block_cache(stored_blocks.clone())
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
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

    // The writer's own reads are served from the state its batches seeded,
    // so a runtime that reads cold is what fills the stored cache.
    let filler = open_runtime_with(shared_store.clone(), "stored-block-filler", |builder| {
        builder.stored_metadata_block_cache(stored_blocks.clone())
    });
    let file = filler
        .get_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read file");
    assert_eq!(file.bytes, b"file");
    let entries = filler
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs");
    assert_eq!(entries.len(), 2);

    assert!(
        filler.metadata_cache_stats().segment_inserts > 0,
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

#[test]
fn metadata_upkeep_offers_nothing_to_the_local_block_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let stored_blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    // Every read probes for a successor manifest, so the read after each
    // fold reloads and reaches the cache whatever the wall clock did.
    let fs = open_runtime_with(store(temp_dir.path()), "maintenance-cold", |builder| {
        builder
            .stored_metadata_block_cache(stored_blocks.clone())
            .manifest_revalidation_interval_ms(0)
    });

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");

    // Each step folds the tail into one more delta run, and the default policy
    // admits a compaction unit once enough of them have piled up. Reads
    // the writes make on the way are outside every measured window.
    let mut compacted = false;
    for index in 0..16 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/file-{index:02}.txt"),
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
        fs.stat_path_blocking(&namespace_id, &format!("/docs/file-{index:02}.txt"))
            .expect("validate the published read state");
        block_on(fs.writer.drain()).expect("finish hints");
        let calls_before = stored_blocks.call_count();
        let step = fs
            .maintain_metadata_blocking(&namespace_id, metadata_options(1))
            .expect("maintenance pass");
        assert_eq!(
            stored_blocks.call_count(),
            calls_before,
            "a maintenance pass carries no segment cache, so it reaches neither cache tier"
        );
        fs.stat_path_blocking(&namespace_id, &format!("/docs/file-{index:02}.txt"))
            .expect("read folded file outside the maintenance window");
        if step.compaction == (CompactionStepOutcome::UnitPublished {}) {
            compacted = true;
            break;
        }
    }

    assert!(
        compacted,
        "the steps above should have published one compaction unit"
    );
    assert!(
        stored_blocks.call_count() > 0,
        "the reads around the steps must reach the cache for this to prove anything"
    );
}
