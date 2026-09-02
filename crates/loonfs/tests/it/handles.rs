#![allow(clippy::panic)]
// Handle integration tests use panic in helper assertions for precise diagnostics.

//! Purpose-specific handle coverage: builder contracts, the background-work
//! policy, background-shutdown semantics, and cross-handle reads. Each test drives every
//! handle from one runtime fixture, matching the runtime-ownership contract
//! the handles document.

use crate::common::collect_path_entries;
use loonfs::{
    CommitId, CreateCheckpointOptions, CreateNamespaceOptions, ErrorCode, FsAdmin,
    FsBackgroundWork, FsReader, FsWriter, MaintenanceJobId, MaintenancePlan, ManifestNo,
    MetadataMaintenanceOptions, NamespaceId, PutFileOptions, RuntimeCacheConfig, RuntimeError,
    SharedObjectStore, StoreConfig,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_core::control::load_namespace_metadata_root_control;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{timeout, Duration};

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

async fn writer(root: &Path, background_work: FsBackgroundWork) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("handle-test-writer")
        .background_work(background_work)
        .build()
        .await
        .expect("build writer")
}

async fn fill_wal_tail_past_threshold(writer: &FsWriter, namespace_id: &NamespaceId) {
    for round in 0..writes_past_wal_tail_threshold() {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/docs/file-{round}.txt"),
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");
    }
}

/// Leaves the tail exactly at the write-stop bound: every write here is
/// admitted, and the next one is not.
async fn fill_wal_tail_to_write_stop(writer: &FsWriter, namespace_id: &NamespaceId) {
    let writes = u32::try_from(loonfs_core::limits::MAX_UNFLUSHED_WAL_SEGMENTS)
        .expect("the WAL write-stop bound should fit in u32");
    for round in 0..writes {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/write-stop/file-{round}.txt"),
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file up to the write-stop boundary");
    }
}

#[test]
fn a_threshold_crossing_during_an_active_step_still_bounds_the_tail() {
    // The interleaving behind the CI failures on the extraction stack: the
    // first crossing's step is mid-run when more publishes cross the
    // threshold. Their requests must defer and rerun the step — dropping
    // them leaves the tail unbounded when those were the last writes before
    // an idle period.
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        // The root is published by whichever step gets there first: a
        // create-if-absent for a namespace that has never flushed, a
        // compare-and-swap after that. Both are the publication this test
        // holds.
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::metadata_root(&namespace_id),
            OperationClass::Put,
        ));
        let store: SharedObjectStore = blocking.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("handle-test-writer")
            .background_work(FsBackgroundWork::Enabled)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        // The threshold-crossing publish spawns the step; the armed store
        // holds that step at its metadata root CAS.
        blocking.block_next();
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        blocking.wait_until_blocked().await;

        // A full second threshold's worth of publishes lands while the step
        // is held; every crossing defers to the running step.
        for round in 0..writes_past_wal_tail_threshold() {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/docs/held/file-{round}.txt"),
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("put file during held step");
        }

        blocking.release();
        writer
            .flush_background()
            .await
            .expect("background maintenance quiesces");

        let admin = FsAdmin::builder_with_store(blocking.clone() as SharedObjectStore)
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after deferred rerun");
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "deferred crossings must rerun the step and bound the tail: {status:?}"
        );
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn a_step_queued_at_the_global_cap_runs_without_another_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let active_namespace = namespace_id("active");
    let queued_namespace = namespace_id("queued");
    block_on(async {
        // The root is published by whichever step gets there first: a
        // create-if-absent for a namespace that has never flushed, a
        // compare-and-swap after that. Both are the publication this test
        // holds.
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::metadata_root(&active_namespace),
            OperationClass::Put,
        ));
        let store: SharedObjectStore = blocking.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("handle-test-writer")
            .background_work(FsBackgroundWork::Enabled)
            .max_concurrent_maintenance(1)
            .build()
            .await
            .expect("build writer");
        for namespace_id in [&active_namespace, &queued_namespace] {
            writer
                .create_namespace(namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");
        }

        blocking.block_next();
        fill_wal_tail_past_threshold(&writer, &active_namespace).await;
        blocking.wait_until_blocked().await;
        fill_wal_tail_past_threshold(&writer, &queued_namespace).await;

        blocking.release();
        writer
            .flush_background()
            .await
            .expect("queued maintenance quiesces");

        let admin = FsAdmin::builder_with_store(blocking.clone() as SharedObjectStore)
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&queued_namespace)
            .await
            .expect("queued namespace status");
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the queued step must run without another publish: {status:?}"
        );
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn a_write_stopped_namespace_queued_at_the_global_cap_unblocks_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let active_namespace = namespace_id("active");
    let write_stopped_namespace = namespace_id("write-stopped");
    block_on(async {
        // The root is published by whichever step gets there first: a
        // create-if-absent for a namespace that has never flushed, a
        // compare-and-swap after that. Both are the publication this test
        // holds.
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::metadata_root(&active_namespace),
            OperationClass::Put,
        ));
        let store: SharedObjectStore = blocking.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("handle-test-writer")
            .background_work(FsBackgroundWork::Enabled)
            .max_concurrent_maintenance(1)
            .build()
            .await
            .expect("build writer");
        for namespace_id in [&active_namespace, &write_stopped_namespace] {
            writer
                .create_namespace(namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");
        }

        blocking.block_next();
        fill_wal_tail_past_threshold(&writer, &active_namespace).await;
        blocking.wait_until_blocked().await;
        fill_wal_tail_to_write_stop(&writer, &write_stopped_namespace).await;
        let error = writer
            .put_file_bytes(
                &write_stopped_namespace,
                "/write-stop/rejected.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("writes against a tail at the hard limit must reject");
        assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

        blocking.release();
        writer
            .flush_background()
            .await
            .expect("queued maintenance quiesces");

        writer
            .put_file_bytes(
                &write_stopped_namespace,
                "/write-stop/rejected.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("the queued step must unblock writes without another publish");
        let admin = FsAdmin::builder_with_store(blocking.clone() as SharedObjectStore)
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&write_stopped_namespace)
            .await
            .expect("write-stopped namespace status");
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the queued step must flush the write-stopped tail before the retry: {status:?}"
        );
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn shutdown_clears_a_non_empty_maintenance_queue_without_spawning_it() {
    let temp_dir = tempdir().expect("tempdir");
    let active_namespace = namespace_id("active");
    let queued_namespace = namespace_id("queued");
    block_on(async {
        // The root is published by whichever step gets there first: a
        // create-if-absent for a namespace that has never flushed, a
        // compare-and-swap after that. Both are the publication this test
        // holds.
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::metadata_root(&active_namespace),
            OperationClass::Put,
        ));
        let store: SharedObjectStore = blocking.clone();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("handle-test-writer")
            .background_work(FsBackgroundWork::Enabled)
            .max_concurrent_maintenance(1)
            .build()
            .await
            .expect("build writer");
        for namespace_id in [&active_namespace, &queued_namespace] {
            writer
                .create_namespace(namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");
        }

        blocking.block_next();
        fill_wal_tail_past_threshold(&writer, &active_namespace).await;
        blocking.wait_until_blocked().await;
        fill_wal_tail_past_threshold(&writer, &queued_namespace).await;

        let mut shutdown = Box::pin(writer.shutdown());
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "shutdown must wait for the parked active step"
        );
        blocking.release();
        timeout(Duration::from_secs(10), shutdown)
            .await
            .expect("shutdown must not hang with a non-empty queue")
            .expect("shut down writer background work");

        let admin = FsAdmin::builder_with_store(blocking.clone() as SharedObjectStore)
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&queued_namespace)
            .await
            .expect("queued namespace status after shutdown");
        assert_eq!(
            status.current_manifest_no, None,
            "shutdown must clear queued work before the active step releases its permit"
        );

        // The shutdown that cleared the queue also closed the write path,
        // so nothing can cross the WAL threshold behind it.
        let refused = writer
            .put_file_bytes(
                &queued_namespace,
                "/after-close.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("a mutation after shutdown must be refused");
        assert_eq!(refused.code(), ErrorCode::ShuttingDown);
        // And the runner stays shut rather than reopening behind the drain:
        // a nudge is the one trigger left, and it must admit nothing.
        writer
            .maintenance()
            .nudge(MaintenanceJobId::METADATA, &queued_namespace);
        writer
            .flush_background()
            .await
            .expect("nothing may spawn after shutdown");
        let status = admin
            .get_namespace_diagnostics(&queued_namespace)
            .await
            .expect("queued namespace status after the post-shutdown nudge");
        assert_eq!(
            status.current_manifest_no, None,
            "post-shutdown nudges must not spawn maintenance"
        );
    });
}

#[test]
fn writer_reader_and_admin_share_a_namespace_through_store_config() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
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
        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("namespace status");
        assert_eq!(status.namespace_id, namespace_id);
        assert_eq!(status.wal_tail_segments, 1);
        // Admin-driven work is observable through the admin handle's own
        // cache counters, like writer and reader work through theirs.
        let _ = admin.runtime_cache_stats();

        // Only the writer owns background work, so only the writer has
        // anything to shut down.
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn standalone_reader_builds_without_writer_identity() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
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
        assert_eq!(writer_block.writer_id, "handle-test-writer");
        assert_ne!(
            writer_block.acquired_at_ms, 0,
            "the acquisition stamp is what tells two runs of one writer apart"
        );
    });
}

#[test]
fn admin_over_writer_core_invalidates_shared_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let reader = writer.reader();
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .flush_background()
            .await
            .expect("background maintenance quiesces");

        // The scheduled step ran: it published a manifest and bounded the
        // tail, both through the writer's own runtime.
        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
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

        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
    });
}

#[test]
fn put_file_bytes_and_prepare_then_put_commit_equivalent_state() {
    let temp_dir = tempdir().expect("tempdir");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
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
fn manual_only_writer_never_schedules_maintenance() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .flush_background()
            .await
            .expect("no background work to wait for");

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after writes");
        assert_eq!(
            status.current_manifest_no, None,
            "manual-only writer must not publish checkpoints: {status:?}"
        );
        assert!(
            status.wal_tail_segments >= wal_tail_segment_count_past_threshold(),
            "manual-only writer must leave the tail alone: {status:?}"
        );

        // Explicit admin maintenance bounds the tail the writer left.
        let step = admin
            .run_maintenance(&namespace_id, MaintenancePlan::metadata())
            .await
            .expect("explicit maintenance step")
            .metadata_maintenance
            .expect("metadata selected");
        assert_ne!(
            step.wal_flush,
            loonfs::WalFlushStepOutcome::NotNeeded,
            "step should act on the oversized tail"
        );
        let status = admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after explicit step");
        assert!(
            status.current_manifest_no.is_some(),
            "explicit step should publish a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "explicit step should bound the tail: {status:?}"
        );
    });
}

#[test]
fn enabled_writer_schedules_maintenance_on_its_owning_runtime() {
    for runtime_cache in [
        RuntimeCacheConfig::default(),
        RuntimeCacheConfig::disabled(),
    ] {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("demo");
        block_on(async {
            let writer = FsWriter::builder(store_config(temp_dir.path()))
                .writer_id("handle-test-writer")
                .background_work(FsBackgroundWork::Enabled)
                .runtime_cache(runtime_cache)
                .build()
                .await
                .expect("build writer");
            writer
                .create_namespace(&namespace_id, CreateNamespaceOptions::default())
                .await
                .expect("create namespace");

            let admin = FsAdmin::builder(store_config(temp_dir.path()))
                .actor_id("handle-test-admin")
                .build()
                .await
                .expect("build admin");
            writer
                .put_file_bytes(
                    &namespace_id,
                    "/docs/under-threshold.txt",
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("put file below the threshold");
            writer
                .flush_background()
                .await
                .expect("nothing was scheduled below the threshold");
            let status = admin
                .get_namespace_diagnostics(&namespace_id)
                .await
                .expect("status below the threshold");
            assert_eq!(
                status.current_manifest_no, None,
                "a publish below the threshold must not step: {status:?}"
            );
            assert_eq!(status.wal_tail_segments, 1, "{status:?}");

            fill_wal_tail_past_threshold(&writer, &namespace_id).await;
            writer
                .flush_background()
                .await
                .expect("background maintenance quiesces");

            let status = admin
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
            writer
                .shutdown()
                .await
                .expect("shut down writer background work");
        });
    }
}

#[test]
fn a_refused_publish_schedules_the_step_that_relieves_the_debt() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let stalled = FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-stalled-writer")
            .background_work(FsBackgroundWork::ManualOnly)
            .build()
            .await
            .expect("build the writer that leaves the debt");
        stalled
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        fill_wal_tail_to_write_stop(&stalled, &namespace_id).await;
        stalled
            .shutdown()
            .await
            .expect("shut down the first writer");

        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
        let refused = writer
            .put_file_bytes(
                &namespace_id,
                "/write-stop/refused.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("a tail at the write-stop bound refuses the publish");
        assert_eq!(refused.code(), ErrorCode::MaintenanceRequired);

        writer
            .flush_background()
            .await
            .expect("the step the refusal asked for settles");
        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("status after the refused publish");
        assert!(
            status.wal_tail_segments < wal_tail_segment_threshold(),
            "the refused publish must schedule the flush that unblocks it: {status:?}"
        );
        writer
            .put_file_bytes(
                &namespace_id,
                "/write-stop/recovered.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("the retry lands once the debt is cleared");
        writer
            .shutdown()
            .await
            .expect("shut down writer background work");
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
        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
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
        let tail_at_shutdown = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin")
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

        writer
            .flush_background()
            .await
            .expect("nothing scheduled after the shutdown");
        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
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
    match block_on(FsAdmin::builder(store_config(temp_dir.path())).build()) {
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
}

#[test]
fn writer_builder_rejects_zero_maintenance_concurrency() {
    let temp_dir = tempdir().expect("tempdir");
    for background_work in [FsBackgroundWork::Enabled, FsBackgroundWork::ManualOnly] {
        let result = block_on(
            FsWriter::builder(store_config(temp_dir.path()))
                .writer_id("handle-test-writer")
                .background_work(background_work)
                .max_concurrent_maintenance(0)
                .build(),
        );

        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("zero maintenance concurrency must be rejected"),
        };
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
}

#[test]
fn admin_checkpoint_and_retention_are_explicit_one_shot_calls() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
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

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let checkpoint = admin
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
        let retention = admin
            .run_maintenance(
                &namespace_id,
                MaintenancePlan {
                    advance_retention: true,
                    ..MaintenancePlan::default()
                },
            )
            .await
            .expect("advance retention");
        assert_eq!(retention.namespace_id, namespace_id);
        assert!(retention.retention.is_some());
    });
}

#[test]
fn enabled_writer_drains_reorganization_backlog_without_admin() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        for round in 0..9u32 {
            for file in 0..writes_past_wal_tail_threshold() {
                writer
                    .put_file_bytes(
                        &namespace_id,
                        &format!("/docs/round-{round}/file-{file}.txt"),
                        b"body",
                        PutFileOptions::new(loonfs_test_support::test_actor()),
                    )
                    .await
                    .expect("put file");
            }
            writer
                .flush_background()
                .await
                .expect("background steps finish");
        }

        let store = LocalFsStore::new(temp_dir.path()).expect("open store for inspection");
        let root = load_namespace_metadata_root_control(&store, &namespace_id)
            .await
            .expect("load metadata root");
        let manifest_key =
            metadata_manifest_object(&namespace_id, &root.state.manifest.manifest_object_id);
        let bytes = store
            .get(&manifest_key, None)
            .await
            .expect("read manifest")
            .expect("manifest exists");
        let manifest = decode_namespace_manifest_json(&bytes).expect("decode manifest");
        let delta_files = manifest
            .payload
            .segments
            .iter()
            .filter(|descriptor| descriptor.level == 0)
            .count();
        assert_eq!(
            delta_files, 0,
            "background steps drain the fold backlog to zero delta runs; \
             a leftover run means the drain stopped early"
        );
    });
}
