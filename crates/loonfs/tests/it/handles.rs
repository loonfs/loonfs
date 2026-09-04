#![allow(clippy::panic)]
// Handle integration tests use panic in helper assertions for precise diagnostics.

//! Purpose-specific handle coverage: builder contracts, the background-work
//! policy, background-shutdown semantics, and cross-handle reads. Each test drives every
//! handle from one runtime fixture, matching the runtime-ownership contract
//! the handles document.

use crate::common::collect_path_entries;
use loonfs::{
    maintenance_hint_relay, CommitId, CreateCheckpointOptions, CreateNamespaceOptions, ErrorCode,
    FsMaintenance, FsReader, FsWriter, GarbageCollectionJob, MaintenanceRegistry,
    MaintenanceRunner, ManifestNo, MetadataCompactionJob, MetadataMaintenanceJob,
    MetadataMaintenanceOptions, NamespaceId, PutFileOptions, RuntimeCacheConfig, RuntimeError,
    SharedObjectStore, StoreConfig,
};
use loonfs_core::test_support::append_wal_segments;
use loonfs_core::MutationContext;
use loonfs_objectstore::layout::DurableObjectFamily;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn store_config(root: &Path) -> StoreConfig {
    StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    }
}

fn wal_tail_segment_threshold() -> u64 {
    MetadataMaintenanceOptions::default()
        .max_wal_tail_segments
        .get()
}

fn wal_tail_segment_count_past_threshold() -> u64 {
    wal_tail_segment_threshold() + 1
}

fn writes_past_wal_tail_threshold() -> u32 {
    u32::try_from(wal_tail_segment_count_past_threshold())
        .expect("WAL tail threshold plus one should fit in u32")
}

async fn writer(root: &Path) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("handle-test-writer")
        .build()
        .await
        .expect("build writer")
}

async fn writer_with_runner(
    root: &Path,
    runtime_cache: RuntimeCacheConfig,
) -> (FsWriter, FsMaintenance, MaintenanceRunner) {
    let (observer, receiver) =
        maintenance_hint_relay(NonZeroUsize::new(64).expect("relay capacity is nonzero"));
    let writer = FsWriter::builder(store_config(root))
        .writer_id("handle-test-writer")
        .runtime_cache(runtime_cache)
        .maintenance_hint_observer(move |hint| observer(hint))
        .build()
        .await
        .expect("build writer");
    let maintenance = writer
        .maintenance_handle("handle-test-maintenance")
        .expect("build maintenance handle");
    let registry = MaintenanceRegistry::new();
    registry
        .register(Arc::new(MetadataMaintenanceJob::new(maintenance.clone())))
        .expect("metadata job");
    registry
        .register(Arc::new(MetadataCompactionJob::new(maintenance.clone())))
        .expect("metadata compaction job");
    registry
        .register(Arc::new(GarbageCollectionJob::new(maintenance.clone())))
        .expect("garbage collection job");
    let runner = MaintenanceRunner::builder(registry)
        .build()
        .expect("build runner");
    runner.attach_hints(receiver);
    (writer, maintenance, runner)
}

/// Leaves the tail exactly at the write-stop bound: every write here is
/// admitted, and the next one is not.
async fn fill_wal_tail_to_write_stop<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) {
    append_wal_segments(
        store,
        namespace_id,
        loonfs_core::limits::MAX_UNFLUSHED_WAL_SEGMENTS,
        &MutationContext {
            writer_id: loonfs_api::WriterId::parse("wal-tail-test-writer").expect("writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("fill WAL tail to write-stop bound");
}

#[test]
fn writer_reader_and_maintenance_share_a_namespace_through_store_config() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");

        // A reader derived from the writer shares its caches.
        let derived = writer.reader();
        let read = derived
            .get_file_bytes(&namespace_id, "/docs/hello.txt")
            .await
            .expect("read through derived reader");
        assert_eq!(read.bytes, b"hello");

        // A standalone reader opens its own store client from config and
        // still observes the write.
        let standalone = FsReader::builder(store_config(temp_dir.path()))
            .build()
            .await
            .expect("build standalone reader");
        let read = standalone
            .get_file_bytes(&namespace_id, "/docs/hello.txt")
            .await
            .expect("read through standalone reader");
        assert_eq!(read.bytes, b"hello");
        let entries = collect_path_entries(&standalone, &namespace_id, "/docs")
            .await
            .expect("list through standalone reader")
            .entries;
        assert_eq!(entries.len(), 1);

        // Admin inspects the same namespace through its own handle.
        let maintenance = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("namespace status");
        assert_eq!(status.namespace_id, namespace_id);
        assert_eq!(status.wal_tail_segments, 1);
        // Admin-driven work is observable through the maintenance handle's own
        // cache counters, like writer and reader work through theirs.
        let _ = maintenance.runtime_cache_stats();

        writer.shutdown().await.expect("shut down writer");
    });
}

#[test]
fn standalone_reader_builds_without_writer_identity() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");

        let reader = FsReader::builder(store_config(temp_dir.path()))
            .build()
            .await
            .expect("build standalone reader");
        // The reader serves the full read surface without an identity.
        let stat = reader
            .get_path_entry(&namespace_id, "/docs/hello.txt", Default::default())
            .await
            .expect("stat through standalone reader");
        assert_eq!(stat.size_bytes(), Some(5));
        let entries = collect_path_entries(&reader, &namespace_id, "/docs")
            .await
            .expect("list through standalone reader")
            .entries;
        assert_eq!(entries.len(), 1);

        // The only identity on the head is the writer's own label: a read
        // carries none and records none.
        let store = LocalFsStore::new(temp_dir.path()).expect("open store for inspection");
        let head = loonfs_core::control::load_namespace_head_control(&store, &namespace_id)
            .await
            .expect("load head")
            .state;
        let writer_block = head.writer.expect("head records the writer that published");
        assert_eq!(writer_block.writer_id.as_str(), "handle-test-writer");
        assert_ne!(
            writer_block.acquired_at_ms, 0,
            "the acquisition stamp is what tells two runs of one writer apart"
        );
    });
}

#[test]
fn a_writer_maintenance_handle_invalidates_shared_read_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let (writer, maintenance, runner) =
            writer_with_runner(temp_dir.path(), RuntimeCacheConfig::default()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let reader = writer.reader();
        for round in 0..writes_past_wal_tail_threshold() {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/docs/file-{round}.txt"),
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("put file");
        }
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("writer fold settles");
        runner.drain().await.expect("maintenance quiesces");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after the scheduled step");
        assert!(
            status.current_manifest_no.is_some(),
            "the scheduled step should have published a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the scheduled step should have bounded the tail: {status:?}"
        );

        // Reads and writes on the writer's own runtime see the state the
        // step left behind, with no stale-cache error in between.
        reader
            .get_path_entry(&namespace_id, "/docs/file-0.txt", Default::default())
            .await
            .expect("read after maintenance is served from revalidated caches");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/after-maintenance.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("writes continue against the post-maintenance head");
        reader
            .get_path_entry(
                &namespace_id,
                "/docs/after-maintenance.txt",
                Default::default(),
            )
            .await
            .expect("read after write on the shared core");

        writer.shutdown().await.expect("shut down writer");
        runner.shutdown().await.expect("shut down runner");
    });
}

#[test]
fn put_file_bytes_and_prepare_then_put_commit_equivalent_state() {
    let temp_dir = tempdir().expect("tempdir");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        let simple_namespace = NamespaceId::parse("simple-put").expect("valid simple namespace id");
        let prepared_namespace =
            NamespaceId::parse("prepared-put").expect("valid prepared namespace id");
        for namespace_id in [&simple_namespace, &prepared_namespace] {
            writer
                .create_namespace(namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");
        }
        let bytes = b"equivalent content";
        let commit_id = CommitId::parse("equivalent-put").expect("valid commit id");
        let options = PutFileOptions {
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: Some(commit_id.clone()),
                message: None,
            },
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        };

        let simple = writer
            .put_file_bytes(&simple_namespace, "/file.txt", bytes, options.clone())
            .await
            .expect("put file bytes");
        let prepared = writer
            .prepare_file_bytes(&prepared_namespace, bytes)
            .await
            .expect("prepare file bytes");
        let composed = writer
            .put_file_prepared(&prepared_namespace, "/file.txt", prepared, options)
            .await
            .expect("put prepared file");

        assert_eq!(simple.commit_id, commit_id);
        assert_eq!(composed.commit_id, commit_id);
        assert_eq!(simple.committed_seq, composed.committed_seq);

        let reader = writer.reader();
        let simple_stat = reader
            .get_path_entry(&simple_namespace, "/file.txt", Default::default())
            .await
            .expect("stat simple put");
        let prepared_stat = reader
            .get_path_entry(&prepared_namespace, "/file.txt", Default::default())
            .await
            .expect("stat prepared put");
        assert_eq!(simple_stat.revision_no(), prepared_stat.revision_no());
        assert_eq!(simple_stat.size_bytes(), prepared_stat.size_bytes());
        // The two paths staged their own content objects, so their
        // references name different objects and carry identical evidence.
        let simple_ref = simple_stat.content_ref().expect("simple put content ref");
        let prepared_ref = prepared_stat
            .content_ref()
            .expect("prepared put content ref");
        assert_ne!(simple_ref.content_id, prepared_ref.content_id);
        assert_eq!(simple_ref.size_bytes, prepared_ref.size_bytes);
        assert_eq!(simple_ref.checksum, prepared_ref.checksum);

        let simple_read = reader
            .get_file_bytes(&simple_namespace, "/file.txt")
            .await
            .expect("read simple put");
        let prepared_read = reader
            .get_file_bytes(&prepared_namespace, "/file.txt")
            .await
            .expect("read prepared put");
        assert_eq!(simple_read.bytes, bytes);
        assert_eq!(prepared_read.bytes, bytes);
    });
}

#[test]
fn manual_only_writer_folds_without_scheduling_maintenance() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        for round in 0..=(wal_tail_segment_threshold() * 2) {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/docs/file-{round}.txt"),
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("put file");
        }
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("settle the writer's fold");

        let maintenance = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after writes");
        assert_eq!(
            status.current_manifest_no,
            Some(ManifestNo(2)),
            "the writer should have folded twice without a maintenance runner: {status:?}"
        );
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the writer must keep its own tail below the fold threshold: {status:?}"
        );
    });
}

#[test]
fn a_writer_with_a_runner_maintains_what_it_touches() {
    for runtime_cache in [
        RuntimeCacheConfig::default(),
        RuntimeCacheConfig::disabled(),
    ] {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("demo");
        block_on(async {
            let (writer, maintenance, runner) =
                writer_with_runner(temp_dir.path(), runtime_cache).await;
            writer
                .create_namespace(&namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");

            writer
                .put_file_bytes(
                    &namespace_id,
                    "/docs/under-threshold.txt",
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("put file below the threshold");
            runner.drain().await.expect("nothing was due");
            let status = maintenance
                .get_namespace_diagnostics(&namespace_id)
                .await
                .expect("status below the threshold");
            assert_eq!(
                status.current_manifest_no, None,
                "a publish below the threshold must not step: {status:?}"
            );
            assert_eq!(status.wal_tail_segments, 1, "{status:?}");

            for round in 0..writes_past_wal_tail_threshold() {
                writer
                    .put_file_bytes(
                        &namespace_id,
                        &format!("/docs/file-{round}.txt"),
                        b"body",
                        PutFileOptions::new(loonfs_test_support::test_actor()),
                    )
                    .await
                    .expect("put file");
            }
            writer
                .wait_for_fold(&namespace_id)
                .await
                .expect("writer fold settles");
            runner.drain().await.expect("maintenance quiesces");

            let status = maintenance
                .get_namespace_diagnostics(&namespace_id)
                .await
                .expect("status after auto step");
            assert!(
                status.current_manifest_no.is_some(),
                "auto step should have published a manifest: {status:?}"
            );
            assert!(
                status.wal_tail_segments < wal_tail_segment_threshold(),
                "auto step should have bounded the tail: {status:?}"
            );
            writer.shutdown().await.expect("shut down writer");
            runner.shutdown().await.expect("shut down runner");
        });
    }
}

#[test]
fn a_runner_retries_a_failed_writer_fold_without_another_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let failing = Arc::new(FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::family(DurableObjectFamily::MetadataManifest),
            OperationClass::Put,
            InjectedError::PermissionDenied("injected manifest write failure".to_owned()),
        ));
        let (observer, receiver) =
            maintenance_hint_relay(NonZeroUsize::new(64).expect("relay capacity is nonzero"));
        let store: SharedObjectStore = failing.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("fold-retry-writer")
            .maintenance_hint_observer(move |hint| observer(hint))
            .build()
            .await
            .expect("build writer");
        let maintenance = writer
            .maintenance_handle("fold-retry-maintenance")
            .expect("build maintenance handle");
        let registry = MaintenanceRegistry::new();
        registry
            .register(Arc::new(MetadataMaintenanceJob::new(maintenance.clone())))
            .expect("metadata job");
        registry
            .register(Arc::new(MetadataCompactionJob::new(maintenance.clone())))
            .expect("metadata compaction job");
        registry
            .register(Arc::new(GarbageCollectionJob::new(maintenance.clone())))
            .expect("garbage collection job");
        let runner = MaintenanceRunner::builder(registry)
            .build()
            .expect("build runner");
        runner.attach_hints(receiver);

        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        append_wal_segments(
            failing.as_ref(),
            &namespace_id,
            wal_tail_segment_threshold() - 1,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse("fold-retry-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL tail below fold threshold");

        failing.fail_next(1);
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/cross-threshold.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("publish across the fold threshold");
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("writer fold settles");
        runner.drain().await.expect("maintenance retry quiesces");

        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after maintenance retry");
        assert!(
            status.current_manifest_no.is_some(),
            "the maintenance retry should have published a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the maintenance retry should have bounded the tail: {status:?}"
        );
        assert_eq!(
            failing.remaining(),
            0,
            "the one injected failure should have been consumed"
        );
        assert_eq!(
            failing.attempts(),
            2,
            "only the failed writer attempt and successful runner retry should write a manifest"
        );

        writer.shutdown().await.expect("shut down writer");
        runner.shutdown().await.expect("shut down runner");
    });
}

#[test]
fn a_runtime_publish_folds_a_preexisting_write_stopped_tail_and_lands() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let stalled = FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-stalled-writer")
            .build()
            .await
            .expect("build the writer that leaves the debt");
        stalled
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let tail_store = LocalFsStore::new(temp_dir.path()).expect("open tail store");
        fill_wal_tail_to_write_stop(&tail_store, &namespace_id).await;
        stalled
            .shutdown()
            .await
            .expect("shut down the first writer");

        let writer = writer(temp_dir.path()).await;
        let refused = writer
            .put_file_bytes(
                &namespace_id,
                "/write-stop/recovered.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("the write-stopped tail refuses the first publish");
        assert_eq!(refused.code(), ErrorCode::MaintenanceRequired);
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("settle the fold started by the refusal");
        writer
            .put_file_bytes(
                &namespace_id,
                "/write-stop/recovered.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("the retry lands after the fold");
        let maintenance = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after the folding publish");
        assert_eq!(status.wal_tail_segments, 1, "{status:?}");
        assert!(status.current_manifest_no.is_some(), "{status:?}");
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn a_failed_fold_preserves_the_write_stop_until_the_store_recovers() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let failing = Arc::new(FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::family(DurableObjectFamily::MetadataManifest),
            OperationClass::Put,
            InjectedError::PermissionDenied("manifest writes disabled".to_owned()),
        ));
        let store: SharedObjectStore = failing.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("fold-failure-writer")
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let seed_segments = wal_tail_segment_threshold() - 1;
        append_wal_segments(
            failing.as_ref(),
            &namespace_id,
            seed_segments,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse("fold-failure-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL tail below fold threshold");

        failing.fail_all();
        for round in 0..(loonfs_core::limits::MAX_UNFLUSHED_WAL_SEGMENTS - seed_segments) {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/failed-fold/file-{round}.txt"),
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("publishes below the write-stop bound continue after a failed fold");
        }
        let error = writer
            .put_file_bytes(
                &namespace_id,
                "/failed-fold/refused.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("the write-stop invariant still refuses the full tail");
        assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

        failing.clear();
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("settle the fold after the manifest store recovers");
        writer
            .put_file_bytes(
                &namespace_id,
                "/failed-fold/recovered.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("the recovered manifest store lets the retry land");
        let maintenance = FsMaintenance::builder_with_store(failing as SharedObjectStore)
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after fold recovery");
        assert_eq!(status.wal_tail_segments, 1, "{status:?}");
    });
}

#[test]
fn a_threshold_crossing_publish_returns_before_its_fold_completes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::family(DurableObjectFamily::MetadataManifest),
            OperationClass::Put,
        ));
        let writer = FsWriter::builder_with_store(blocking.clone())
            .writer_id("parked-fold-writer")
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        append_wal_segments(
            blocking.inner(),
            &namespace_id,
            wal_tail_segment_threshold() - 1,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse("parked-fold-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed the WAL tail below the fold threshold");

        blocking.block_next();
        let put = tokio::spawn({
            let writer = writer.clone();
            let namespace_id = namespace_id.clone();
            async move {
                writer
                    .put_file_bytes(
                        &namespace_id,
                        "/crossing.txt",
                        b"body",
                        PutFileOptions::new(loonfs_test_support::test_actor()),
                    )
                    .await
            }
        });
        blocking.wait_until_blocked().await;
        let published = match tokio::time::timeout(Duration::from_secs(1), put).await {
            Ok(published) => published,
            Err(error) => {
                blocking.release();
                panic!("the publish waited for its parked fold: {error}");
            }
        };
        published
            .expect("join the crossing publish")
            .expect("the crossing publish lands");

        blocking.release();
        writer
            .wait_for_fold(&namespace_id)
            .await
            .expect("settle the released fold");
        let maintenance = writer
            .maintenance_handle("parked-fold-inspection")
            .expect("build maintenance handle");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("inspect the folded namespace");
        assert!(status.current_manifest_no.is_some(), "{status:?}");
        writer.shutdown().await.expect("shut down writer");
    });
}

#[test]
fn a_shut_down_writer_refuses_mutations_and_keeps_reading() {
    // Shutdown is terminal for the write path only. Mutations are refused,
    // so nothing can cross the WAL threshold and schedule work the runner
    // is no longer around to run, while reads — which own no background
    // work — answer from durable state exactly as before.
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file before the shutdown");
        let tail_at_shutdown = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status before the shutdown")
            .wal_tail_segments;

        assert!(!writer.is_shutting_down());
        writer.shutdown().await.expect("shut down the writer");
        assert!(writer.is_shutting_down());

        let refused = writer
            .put_file_bytes(
                &namespace_id,
                "/docs/after.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("a mutation after shutdown must be refused");
        assert_eq!(refused.code(), ErrorCode::ShuttingDown);
        let read = writer
            .reader()
            .get_file_bytes(&namespace_id, "/docs/hello.txt")
            .await
            .expect("reads survive the writer's shutdown");
        assert_eq!(read.bytes, b"hello");

        let maintenance = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let status = maintenance
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after the shutdown");
        assert_eq!(
            status.current_manifest_no, None,
            "a shut-down writer must not schedule checkpoints: {status:?}"
        );
        assert_eq!(
            status.wal_tail_segments, tail_at_shutdown,
            "a refused mutation must leave the tail exactly as it was: {status:?}"
        );
    });
}

#[test]
fn builders_require_identity_and_a_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    match block_on(FsWriter::builder(store_config(temp_dir.path())).build()) {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error for missing writer_id, got {other:?}"),
        Ok(_) => panic!("writer_id must be required"),
    }
    match block_on(
        FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("   ")
            .build(),
    ) {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error for a blank writer_id, got {other:?}"),
        Ok(_) => panic!("a whitespace-only writer_id must be rejected"),
    }
    match block_on(FsMaintenance::builder(store_config(temp_dir.path())).build()) {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error for missing actor_id, got {other:?}"),
        Ok(_) => panic!("actor_id must be required"),
    }

    // Polling build() outside a Tokio runtime is a config error, not a panic.
    let outside_runtime = futures::executor::block_on(
        FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-writer")
            .build(),
    );
    match outside_runtime {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error outside a runtime, got {other:?}"),
        Ok(_) => panic!("build must require an owning runtime"),
    }

    let outside_runtime = MaintenanceRunner::builder(MaintenanceRegistry::new()).build();
    assert!(matches!(outside_runtime, Err(RuntimeError::Config(_))));
}

#[test]
fn maintenance_checkpoint_and_retention_are_explicit_one_shot_calls() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");

        let maintenance = FsMaintenance::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance");
        let checkpoint = maintenance
            .create_checkpoint(
                &namespace_id,
                CreateCheckpointOptions {
                    name: "handle-pin".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
            .expect("create checkpoint");
        assert!(checkpoint.manifest_no > ManifestNo(0));
        let retention = maintenance
            .advance_retention_floor(&namespace_id)
            .await
            .expect("advance retention");
        assert_eq!(retention.retention_floor_seq, checkpoint.checkpoint_seq);
    });
}
