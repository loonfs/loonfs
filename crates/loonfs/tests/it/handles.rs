#![allow(clippy::panic)]
// Handle integration tests use panic in helper assertions for precise diagnostics.

//! Purpose-specific handle coverage: builder contracts, the background-work
//! policy, background-shutdown semantics, and cross-handle reads. Each test drives every
//! handle from one runtime fixture, matching the runtime-ownership contract
//! the handles document.

use crate::common::collect_path_entries;
use loonfs::{
    maintenance_hint_relay, Commit, CommitId, CreateDirectoryOptions, Error, ErrorCode,
    GarbageCollectionJob, LoonFs, Maintenance, MaintenanceRegistry, MaintenanceRunner, ManifestNo,
    MetadataCache, MetadataCompactionJob, MetadataMaintenanceJob, MetadataMaintenanceOptions,
    NamespaceId, PutFileOptions, SharedObjectStore, StoreConfig, Writable,
};
use loonfs_core::test_support::append_wal_objects;
use loonfs_core::MutationContext;
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

fn wal_tail_object_threshold() -> u64 {
    MetadataMaintenanceOptions::default()
        .max_wal_tail_objects
        .get()
}

fn wal_tail_object_count_past_threshold() -> u64 {
    wal_tail_object_threshold() + 1
}

fn writes_past_wal_tail_threshold() -> u32 {
    u32::try_from(wal_tail_object_count_past_threshold())
        .expect("WAL tail threshold plus one should fit in u32")
}

async fn writer(root: &Path) -> LoonFs<Writable> {
    LoonFs::builder(store_config(root))
        .writer_id("handle-test-writer")
        .build()
        .await
        .expect("build writer")
}

async fn writer_with_runner(
    root: &Path,
    metadata_cache: MetadataCache,
) -> (LoonFs<Writable>, Maintenance, MaintenanceRunner) {
    let (observer, receiver) =
        maintenance_hint_relay(NonZeroUsize::new(64).expect("relay capacity is nonzero"));
    let writer = LoonFs::builder(store_config(root))
        .writer_id("handle-test-writer")
        .metadata_cache(metadata_cache)
        .maintenance_hint_observer(move |hint| observer(hint))
        .build()
        .await
        .expect("build writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
        "handle-test-maintenance",
    ));
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
    let current = loonfs_core::control::load_namespace_read_state(store, namespace_id)
        .await
        .expect("tail state");
    append_wal_objects(
        store,
        namespace_id,
        loonfs_core::limits::MAX_UNFOLDED_WAL_OBJECTS
            - (current.wal_no.0 - current.folded_wal_no.0)
            - 1,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("wal-tail-test-writer").expect("writer id"),
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
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace
            .put_file(
                "/docs/hello.txt",
                b"hello",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");

        // A reader derived from the writer shares its caches.
        let derived = writer.read_only();
        let derived_namespace = derived.namespace(&namespace_id);
        let read = derived_namespace
            .read_file("/docs/hello.txt")
            .await
            .expect("read through derived reader");
        assert_eq!(read.bytes, b"hello");

        // A standalone reader opens its own store client from config and
        // still observes the write.
        let standalone = LoonFs::builder(store_config(temp_dir.path()))
            .read_only()
            .build()
            .await
            .expect("build standalone reader");
        let standalone_namespace = standalone.namespace(&namespace_id);
        let read = standalone_namespace
            .read_file("/docs/hello.txt")
            .await
            .expect("read through standalone reader");
        assert_eq!(read.bytes, b"hello");
        let entries = collect_path_entries(&standalone, &namespace_id, "/docs")
            .await
            .expect("list through standalone reader")
            .entries;
        assert_eq!(entries.len(), 1);

        // Maintenance inspects the same namespace through its own handle.
        let maintenance = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("namespace status");
        assert_eq!(status.namespace_id, namespace_id);
        assert_eq!(status.wal_tail_objects, 2);

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
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace_writer = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace_writer
            .put_file(
                "/docs/hello.txt",
                b"hello",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");

        let reader = LoonFs::builder(store_config(temp_dir.path()))
            .read_only()
            .build()
            .await
            .expect("build standalone reader");
        let namespace = reader.namespace(&namespace_id);
        // The reader serves the full read surface without an identity.
        let stat = namespace
            .stat("/docs/hello.txt")
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
        let head = loonfs_core::control::load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("load head");
        let writer_block = head.writer.expect("head records the writer that published");
        assert_eq!(writer_block.writer_id.as_str(), "handle-test-writer");
        assert_ne!(
            writer_block.acquired_at_ms, 0,
            "the acquisition stamp is what tells two runs of one writer apart"
        );
    });
}

#[test]
fn maintenance_invalidates_the_runtimes_shared_read_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let (writer, maintenance, runner) =
            writer_with_runner(temp_dir.path(), MetadataCache::default()).await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace_writer = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let reader = writer.read_only();
        let namespace = reader.namespace(&namespace_id);
        for round in 0..writes_past_wal_tail_threshold() {
            namespace_writer
                .put_file(
                    &format!("/docs/file-{round}.txt"),
                    b"body",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("put file");
        }
        namespace_writer
            .wait_for_fold()
            .await
            .expect("writer fold settles");
        runner.drain().await.expect("maintenance quiesces");
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after the scheduled step");
        assert!(
            status.current_manifest_no.is_some(),
            "the scheduled step should have published a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_objects < wal_tail_object_threshold(),
            "the scheduled step should have bounded the tail: {status:?}"
        );

        // Reads and writes on the writer's own runtime see the state the
        // step left behind, with no stale-cache error in between.
        namespace
            .stat("/docs/file-0.txt")
            .await
            .expect("read after maintenance is served from revalidated caches");
        namespace_writer
            .put_file(
                "/docs/after-maintenance.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("writes continue against the post-maintenance head");
        namespace
            .stat("/docs/after-maintenance.txt")
            .await
            .expect("read after write on the shared core");

        writer.shutdown().await.expect("shut down writer");
        runner.shutdown().await.expect("shut down runner");
    });
}

#[test]
fn a_plain_mutation_commits_what_its_options_form_commits_with_default_options() {
    let namespace_id = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    let plain_dir = tempdir().expect("tempdir");
    let options_dir = tempdir().expect("tempdir");
    block_on(async {
        let plain_writer = writer(plain_dir.path()).await;
        let options_writer = writer(options_dir.path()).await;
        for runtime in [&plain_writer, &options_writer] {
            runtime
                .create_namespace(&namespace_id, &actor)
                .await
                .expect("create namespace");
        }
        let plain = plain_writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .create_directory("/docs", &actor)
            .await
            .expect("plain create");
        let with_options = options_writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .create_directory_with_options("/docs", &actor, &CreateDirectoryOptions::default())
            .await
            .expect("create with default options");
        // Each call generates its own commit id, and each commit is stamped
        // with its own wall-clock time.
        assert_eq!(
            Commit {
                commit_id: with_options.commit_id.clone(),
                committed_at_ms: with_options.committed_at_ms,
                ..plain
            },
            with_options
        );
    });
}

#[test]
fn put_file_and_prepare_then_put_commit_equivalent_state() {
    let temp_dir = tempdir().expect("tempdir");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        let simple_namespace = NamespaceId::parse("simple-put").expect("valid simple namespace id");
        let prepared_namespace =
            NamespaceId::parse("prepared-put").expect("valid prepared namespace id");
        for namespace_id in [&simple_namespace, &prepared_namespace] {
            writer
                .create_namespace(namespace_id, &loonfs_test_support::test_actor())
                .await
                .expect("create namespace");
        }
        let bytes = b"equivalent content";
        let prepared_namespace_writer = writer
            .open_namespace(&prepared_namespace)
            .expect("open namespace");
        let simple_namespace_writer = writer
            .open_namespace(&simple_namespace)
            .expect("open namespace");
        let commit_id = CommitId::parse("equivalent-put").expect("valid commit id");
        let options = PutFileOptions {
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: Some(commit_id.clone()),
                message: None,
            },
            ..Default::default()
        };

        let simple = simple_namespace_writer
            .put_file_with_options(
                "/file.txt",
                bytes,
                &loonfs_test_support::test_actor(),
                &options,
            )
            .await
            .expect("put file bytes");
        let prepared = prepared_namespace_writer
            .prepare_content(bytes)
            .await
            .expect("prepare file bytes");
        let composed = prepared_namespace_writer
            .put_file_prepared_with_options(
                "/file.txt",
                prepared,
                &loonfs_test_support::test_actor(),
                &options,
            )
            .await
            .expect("put prepared file");

        assert_eq!(simple.commit_id, commit_id);
        assert_eq!(composed.commit_id, commit_id);
        assert_eq!(simple.committed_seq, composed.committed_seq);

        let reader = writer.read_only();
        let simple_namespace_reader = reader.namespace(&simple_namespace);
        let prepared_namespace_reader = reader.namespace(&prepared_namespace);
        let simple_stat = simple_namespace_reader
            .stat("/file.txt")
            .await
            .expect("stat simple put");
        let prepared_stat = prepared_namespace_reader
            .stat("/file.txt")
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

        let simple_read = simple_namespace_reader
            .read_file("/file.txt")
            .await
            .expect("read simple put");
        let prepared_read = prepared_namespace_reader
            .read_file("/file.txt")
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
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        for round in 0..=(wal_tail_object_threshold() * 2) {
            namespace
                .put_file(
                    &format!("/docs/file-{round}.txt"),
                    b"body",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("put file");
        }
        namespace
            .wait_for_fold()
            .await
            .expect("settle the writer's fold");

        let maintenance = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after writes");
        assert_eq!(
            status.current_manifest_no,
            Some(ManifestNo(4)),
            "the writer should have folded twice without a maintenance runner: {status:?}"
        );
        assert!(
            status.wal_tail_objects < wal_tail_object_threshold(),
            "the writer must keep its own tail below the fold threshold: {status:?}"
        );
    });
}

#[test]
fn a_writer_with_a_runner_maintains_what_it_touches() {
    for metadata_cache in [
        MetadataCache::default(),
        MetadataCache::builder()
            .max_segment_bytes(0)
            .max_head_state_bytes(0)
            .build(),
    ] {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("demo");
        block_on(async {
            let (writer, maintenance, runner) =
                writer_with_runner(temp_dir.path(), metadata_cache).await;
            writer
                .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
                .await
                .expect("create namespace");
            let namespace = writer
                .open_namespace(&namespace_id)
                .expect("open namespace");

            namespace
                .put_file(
                    "/docs/under-threshold.txt",
                    b"body",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("put file below the threshold");
            runner.drain().await.expect("nothing was due");
            let status = maintenance
                .diagnostics(&namespace_id)
                .await
                .expect("status below the threshold");
            assert_eq!(
                status.current_manifest_no,
                Some(ManifestNo(2)),
                "a publish below the threshold must not step: {status:?}"
            );
            assert_eq!(status.wal_tail_objects, 2, "{status:?}");

            for round in 0..writes_past_wal_tail_threshold() {
                namespace
                    .put_file(
                        &format!("/docs/file-{round}.txt"),
                        b"body",
                        &loonfs_test_support::test_actor(),
                    )
                    .await
                    .expect("put file");
            }
            namespace
                .wait_for_fold()
                .await
                .expect("writer fold settles");
            runner.drain().await.expect("maintenance quiesces");

            let status = maintenance
                .diagnostics(&namespace_id)
                .await
                .expect("status after auto step");
            assert!(
                status.current_manifest_no.is_some(),
                "auto step should have published a manifest: {status:?}"
            );
            assert!(
                status.wal_tail_objects < wal_tail_object_threshold(),
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
        let failing = Arc::new(FailStore::matching(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            crate::common::folded_manifest_put,
            InjectedError::PermissionDenied("injected manifest write failure".to_owned()),
        ));
        let (observer, receiver) =
            maintenance_hint_relay(NonZeroUsize::new(64).expect("relay capacity is nonzero"));
        let store: SharedObjectStore = failing.clone();
        let writer = LoonFs::builder_with_store(store)
            .writer_id("fold-retry-writer")
            .maintenance_hint_observer(move |hint| observer(hint))
            .build()
            .await
            .expect("build writer");
        let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
            "fold-retry-maintenance",
        ));
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
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        append_wal_objects(
            failing.as_ref(),
            &namespace_id,
            wal_tail_object_threshold() - 1,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("fold-retry-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL tail below fold threshold");

        failing.fail_next(1);
        namespace
            .put_file(
                "/docs/cross-threshold.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("publish across the fold threshold");
        namespace
            .wait_for_fold()
            .await
            .expect("writer fold settles");
        runner.drain().await.expect("maintenance retry quiesces");

        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after maintenance retry");
        assert!(
            status.current_manifest_no.is_some(),
            "the maintenance retry should have published a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_objects < wal_tail_object_threshold(),
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
        let stalled = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-stalled-writer")
            .build()
            .await
            .expect("build the writer that leaves the debt");
        stalled
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let stalled_namespace_writer = stalled
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let mut replay_options = CreateDirectoryOptions::default();
        replay_options.commit.commit_id =
            Some(CommitId::parse("before-write-stop").expect("commit id"));
        let original = stalled_namespace_writer
            .create_directory_with_options(
                "/before-write-stop",
                &loonfs_test_support::test_actor(),
                &replay_options,
            )
            .await
            .expect("land the commit before the ceiling");
        let tail_store = LocalFsStore::new(temp_dir.path()).expect("open tail store");
        fill_wal_tail_to_write_stop(&tail_store, &namespace_id).await;
        stalled
            .shutdown()
            .await
            .expect("shut down the first writer");

        let blocking = Arc::new(BlockingStore::matching(
            tail_store,
            crate::common::folded_manifest_put,
        ));
        let writer = LoonFs::builder_with_store(blocking.clone())
            .writer_id("handle-test-writer")
            .build()
            .await
            .expect("build writer");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        blocking.block_next();
        let refused = namespace
            .put_file(
                "/write-stop/recovered.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect_err("the write-stopped tail refuses the first publish");
        assert_eq!(refused.code(), ErrorCode::MaintenanceRequired);
        blocking.wait_until_blocked().await;
        let replay = namespace
            .create_directory_with_options(
                "/before-write-stop",
                &loonfs_test_support::test_actor(),
                &replay_options,
            )
            .await
            .expect("replay succeeds while the tail remains at the bound");
        assert_eq!(replay, original);
        blocking.release();
        namespace
            .wait_for_fold()
            .await
            .expect("settle the fold started by the refusal");
        namespace
            .put_file(
                "/write-stop/recovered.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("the retry lands after the fold");
        let maintenance = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after the folding publish");
        assert_eq!(status.wal_tail_objects, 1, "{status:?}");
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
        let failing = Arc::new(FailStore::matching(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            crate::common::folded_manifest_put,
            InjectedError::PermissionDenied("manifest writes disabled".to_owned()),
        ));
        let store: SharedObjectStore = failing.clone();
        let writer = LoonFs::builder_with_store(store)
            .writer_id("fold-failure-writer")
            .inline_content(loonfs::InlineContentPolicy {
                inline_content_threshold_bytes: None,
                ..Default::default()
            })
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let seed_wal_objects = wal_tail_object_threshold() - 1;
        append_wal_objects(
            failing.as_ref(),
            &namespace_id,
            seed_wal_objects,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("fold-failure-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL tail below fold threshold");

        failing.fail_all();
        for round in 0..(loonfs_core::limits::MAX_UNFOLDED_WAL_OBJECTS - seed_wal_objects - 2) {
            namespace
                .put_file(
                    &format!("/failed-fold/file-{round}.txt"),
                    b"body",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("publishes below the write-stop bound continue after a failed fold");
        }
        let error = namespace
            .put_file(
                "/failed-fold/refused.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect_err("the write-stop invariant still refuses the full tail");
        assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

        failing.clear();
        namespace
            .wait_for_fold()
            .await
            .expect("settle the fold after the manifest store recovers");
        namespace
            .put_file(
                "/failed-fold/recovered.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("the recovered manifest store lets the retry land");
        let maintenance = LoonFs::builder_with_store(failing as SharedObjectStore)
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after fold recovery");
        assert!(
            matches!(status.wal_tail_objects, 1 | 2),
            "the fold in flight at recovery may have begun one commit early: {status:?}"
        );
    });
}

#[test]
fn a_threshold_crossing_publish_returns_before_its_fold_completes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let blocking = Arc::new(BlockingStore::matching(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            crate::common::folded_manifest_put,
        ));
        let writer = LoonFs::builder_with_store(blocking.clone())
            .writer_id("parked-fold-writer")
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        append_wal_objects(
            blocking.inner(),
            &namespace_id,
            wal_tail_object_threshold() - 1,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("parked-fold-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed the WAL tail below the fold threshold");

        blocking.block_next();
        let put = tokio::spawn({
            let namespace = namespace.clone();
            async move {
                namespace
                    .put_file("/crossing.txt", b"body", &loonfs_test_support::test_actor())
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
        namespace
            .wait_for_fold()
            .await
            .expect("settle the released fold");
        let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
            "parked-fold-inspection",
        ));
        let status = maintenance
            .diagnostics(&namespace_id)
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
        let namespace = writer.namespace(&namespace_id);
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace_writer = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace_writer
            .put_file(
                "/docs/hello.txt",
                b"hello",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file before the shutdown");
        let tail_at_shutdown = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ))
            .diagnostics(&namespace_id)
            .await
            .expect("status before the shutdown")
            .wal_tail_objects;

        assert!(!writer.is_shutting_down());
        writer.shutdown().await.expect("shut down the writer");
        assert!(writer.is_shutting_down());

        let refused = namespace_writer
            .put_file(
                "/docs/after.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect_err("a mutation after shutdown must be refused");
        assert_eq!(refused.code(), ErrorCode::ShuttingDown);
        let read = namespace
            .read_file("/docs/hello.txt")
            .await
            .expect("reads survive the writer's shutdown");
        assert_eq!(read.bytes, b"hello");

        let maintenance = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let status = maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("status after the shutdown");
        assert_eq!(
            status.current_manifest_no,
            Some(ManifestNo(2)),
            "a shut-down writer must not schedule checkpoints: {status:?}"
        );
        assert_eq!(
            status.wal_tail_objects, tail_at_shutdown,
            "a refused mutation must leave the tail exactly as it was: {status:?}"
        );
    });
}

#[test]
fn builders_require_identity_and_a_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    match block_on(LoonFs::builder(store_config(temp_dir.path())).build()) {
        Err(Error::Config(_)) => {}
        Err(other) => panic!("expected config error for missing writer_id, got {other:?}"),
        Ok(_) => panic!("writer_id must be required"),
    }
    match block_on(
        LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("   ")
            .build(),
    ) {
        Err(Error::Config(_)) => {}
        Err(other) => panic!("expected config error for a blank writer_id, got {other:?}"),
        Ok(_) => panic!("a whitespace-only writer_id must be rejected"),
    }

    // Polling build() outside a Tokio runtime is a config error, not a panic.
    let outside_runtime = futures::executor::block_on(
        LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-writer")
            .build(),
    );
    match outside_runtime {
        Err(Error::Config(_)) => {}
        Err(other) => panic!("expected config error outside a runtime, got {other:?}"),
        Ok(_) => panic!("build must require an owning runtime"),
    }

    let outside_runtime = MaintenanceRunner::builder(MaintenanceRegistry::new()).build();
    assert!(matches!(outside_runtime, Err(Error::Config(_))));
}

#[test]
fn maintenance_checkpoint_and_retention_are_explicit_one_shot_calls() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace
            .put_file(
                "/docs/hello.txt",
                b"hello",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");

        let maintenance = LoonFs::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "handle-test-maintenance",
            ));
        let checkpoint = maintenance
            .create_checkpoint(&namespace_id, "handle-pin")
            .await
            .expect("create checkpoint");
        assert!(checkpoint.manifest_no > ManifestNo(0));
        let retention = maintenance
            .advance_retention_floor(&namespace_id)
            .await
            .expect("advance retention");
        assert_eq!(retention.retention_floor_seq, checkpoint.captured_seq);
    });
}

#[tokio::test]
async fn namespace_deletion_drops_cached_reads_and_schedules_gc_even_when_its_answer_is_lost() {
    use loonfs::{MaintenanceHint, MaintenanceJobId};
    use loonfs_core::limits::{GC_SAFETY_MARGIN_MS, NAMESPACE_RETIREMENT_GRACE_MS};
    use std::sync::Mutex;

    for lost_answer in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = namespace_id("delete-hint");
        let store = Arc::new(
            FailStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::manifest(&namespace_id),
                OperationClass::PutCreateIfAbsent,
                InjectedError::Transport("lost acknowledgement".to_owned()),
            )
            .apply_then_fail(),
        );
        let hints = Arc::new(Mutex::new(Vec::new()));
        let observed = hints.clone();
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id("delete-hint")
            .manifest_revalidation_interval_ms(u64::MAX)
            .maintenance_hint_observer(move |hint| observed.lock().expect("hints").push(hint))
            .build()
            .await
            .expect("writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("namespace");
        let namespace_writer = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace_writer
            .put_file("/file", b"data", &loonfs_test_support::test_actor())
            .await
            .expect("put");
        writer
            .maintenance(loonfs_test_support::ids::writer_id("delete-hint"))
            .fold_wal(&namespace_id)
            .await
            .expect("fold the tail so the tombstone is the only manifest put");
        let reader = writer.read_only();
        let namespace = reader.namespace(&namespace_id);
        namespace.stat("/file").await.expect("warm read");
        hints.lock().expect("hints").clear();
        if lost_answer {
            store.fail_next(1);
        }
        let deleted = namespace_writer.delete().await;
        assert_eq!(store.remaining(), 0);
        if let Err(error) = &deleted {
            assert!(lost_answer, "unexpected error: {error:?}");
            assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
        }
        let state = loonfs_core::control::load_namespace_read_state(store.as_ref(), &namespace_id)
            .await
            .expect("tombstone");
        let expected = state.status.deleted_at_ms().expect("deletion stamp")
            + loonfs::GcOptions::default()
                .grace_window_ms
                .max(NAMESPACE_RETIREMENT_GRACE_MS)
            + GC_SAFETY_MARGIN_MS;
        assert!(
            matches!(hints.lock().expect("hints").as_slice(), [MaintenanceHint::DueAt {
            namespace_id: actual_namespace, job: MaintenanceJobId::GC, not_before_ms,
        }] if actual_namespace == &namespace_id && *not_before_ms == expected)
        );
        assert_eq!(
            namespace
                .stat("/file")
                .await
                .expect_err("the cached view is dropped")
                .code(),
            ErrorCode::NamespaceDeleted
        );
        writer.shutdown().await.expect("shutdown");
    }
}
