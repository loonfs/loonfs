#![allow(clippy::panic)]
// Lifecycle diagnostics deliberately panic with the full unexpected outcome.

//! GrepWorker lifecycle, rebootstrap, query contracts, and GC boundaries.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ErrorCode, FsAdmin, FsWriter, GcConfig,
    GrepRequest, GrepResponse, NamespaceId, PutFileOptions, SharedObjectStore,
};
use loonfs_api::wire::control::CheckpointRecordLifecycle;
use loonfs_api::{ChangeSeq, IndexSegmentId};
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::{load_namespace_checkpoint_record_control, load_namespace_read_anchor};
use loonfs_core::{NamespaceEngine, RuntimeReadContext};
use loonfs_grep::keyspace::{
    manifest_key, manifests_prefix, namespace_prefix, root_key, segment_key,
};
use loonfs_grep::root::{
    advance_grep_root, encode_grep_manifest, encode_grep_root, load_grep_root, GrepIndexState,
    GrepLifecycle, GrepManifestEnvelope, GrepManifestId, GrepRootEnvelope, GrepRootPointer,
    GrepRootState,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepFoldOutcome, GrepIndexSnapshot, GrepService,
    GrepWorker, GREP_GC_GRACE_WINDOW_MS,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix, metadata_table_prefix,
    upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::sync::Semaphore;

fn request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    }
}

fn worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    GrepWorker::new(
        store.clone(),
        "grep-worker-tests",
        "grep-worker-tests-session",
        "grep-worker-tests/0.1",
    )
}

async fn read_context(store: &SharedObjectStore, namespace_id: &NamespaceId) -> RuntimeReadContext {
    let (head, root) = load_namespace_read_anchor(&**store, namespace_id)
        .await
        .expect("load read anchor");
    RuntimeReadContext {
        head: head.state,
        head_etag: head.identity.etag,
        manifest_id: root.state.manifest_id,
        manifest_object_id: root.state.manifest_object_id,
        table_cache: Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default())),
        tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 4,
            max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        })),
        catalog: None,
    }
}

async fn drive_worker_to_current(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..512 {
        let build = worker
            .build_step(namespace_id, policy)
            .await
            .expect("worker build step");
        let fold = worker
            .fold_step(namespace_id, policy)
            .await
            .expect("worker fold step");
        if matches!(build.outcome, GrepBuildOutcome::UpToDate { .. })
            && matches!(fold.outcome, GrepFoldOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("worker backlog must drain");
}

async fn new_query(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
) -> loonfs_core::Result<GrepResponse> {
    let engine = NamespaceEngine::builder(store.clone())
        .namespace_id(namespace_id.clone())
        .writer_id("new-query")
        .build()
        .expect("new query engine");
    let context = read_context(store, namespace_id).await;
    let view = engine.load_grep_view_with_runtime_context(&context).await?;
    let service = GrepService::new();
    let snapshot = GrepIndexSnapshot::from_grep_root(&**store, namespace_id, &service).await;
    service.query(grep_request, &snapshot, &view, store).await
}

#[derive(Debug)]
struct BlockingGrepRootCasStore {
    inner: SharedObjectStore,
    root_key: String,
    blocked_once: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}

impl BlockingGrepRootCasStore {
    fn new(inner: SharedObjectStore, root_key: String) -> Self {
        Self {
            inner,
            root_key,
            blocked_once: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    async fn wait_until_blocked(&self) {
        self.entered
            .acquire()
            .await
            .expect("blocking store remains open")
            .forget();
    }

    fn unblock(&self) {
        self.release.add_permits(1);
    }
}

#[async_trait]
impl ObjectStore for BlockingGrepRootCasStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let should_block = key == self.root_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && !self.blocked_once.swap(true, Ordering::SeqCst);
        if should_block {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("blocking store remains open")
                .forget();
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn normalize_namespace(mut response: GrepResponse, namespace_id: &NamespaceId) -> GrepResponse {
    response.namespace_id = namespace_id.clone();
    response
}

#[tokio::test]
async fn grep_worker_lifecycle_uses_and_releases_checkpointed_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-lifecycle").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("lifecycle-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..3u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/before-{index}.txt"),
                format!("checkpoint needle {index}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store);
    let enabled = worker.enable(&namespace_id).await.expect("enable");
    assert!(matches!(
        enabled,
        loonfs_grep::GrepEnableOutcome::Enabled { .. }
    ));
    let again = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable");
    assert!(matches!(
        again,
        loonfs_grep::GrepEnableOutcome::AlreadyEnabled { .. }
    ));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    let GrepLifecycle::Backfilling {
        checkpoint_id: Some(checkpoint_id),
        ..
    } = root.state().lifecycle()
    else {
        panic!(
            "enable must publish checkpointed backfill: {:?}",
            root.state()
        );
    };
    let checkpoint_id = checkpoint_id.clone();

    let policy = GramIndexBuildPolicy {
        max_files_per_step: 1,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(matches!(
        first.outcome,
        GrepBuildOutcome::Published {
            materialized: false,
            ..
        }
    ));
    let error = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect_err("backfill is not materialized");
    assert_eq!(error.code(), ErrorCode::NotSupported);

    drive_worker_to_current(&worker, &namespace_id, policy).await;
    let checkpoint =
        load_namespace_checkpoint_record_control(&*store, &namespace_id, &checkpoint_id)
            .await
            .expect("load checkpoint")
            .expect("released checkpoint record remains until core GC");
    assert_eq!(checkpoint.state, CheckpointRecordLifecycle::Released);
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("materialized query");
    assert_eq!(response.matches.len(), 3);
    let materialized_root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load materialized root")
        .expect("root exists");
    let materialized_segment = segment_key(
        &namespace_id,
        &materialized_root.state().segments()[0].segment_id,
    );

    assert_eq!(
        worker.disable(&namespace_id).await.expect("disable"),
        loonfs_grep::GrepDisableOutcome::Disabled
    );
    worker
        .garbage_collect_namespace(&namespace_id, u64::MAX)
        .await
        .expect("collect disabled segments");
    assert!(
        store
            .head(&materialized_segment)
            .await
            .expect("head disabled segment")
            .is_none(),
        "disable must leave segments for grep-owned GC"
    );
    let disabled_root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load disabled root")
        .expect("disabled root remains");
    assert!(matches!(
        disabled_root.state().lifecycle(),
        GrepLifecycle::Disabled
    ));
    assert_eq!(
        worker
            .disable(&namespace_id)
            .await
            .expect("idempotent disable"),
        loonfs_grep::GrepDisableOutcome::NotEnabled
    );
    let reenabled = worker.enable(&namespace_id).await.expect("re-enable");
    assert!(matches!(
        reenabled,
        loonfs_grep::GrepEnableOutcome::Enabled { .. }
    ));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load re-enabled root")
        .expect("root exists");
    assert!(root.state().segments().is_empty());
    assert!(matches!(
        root.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn retention_gap_and_vanished_checkpoint_restart_fresh_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-gap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("gap-admin")
        .build()
        .await
        .expect("admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store);
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/gap.txt",
            b"retention gap needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write after watermark");
    admin.flush_wal(&namespace_id).await.expect("flush wal");
    let floor = admin
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance retention");
    assert!(floor.retention_floor_seq > ChangeSeq(0));

    let restart = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("gap restart");
    assert!(matches!(
        restart.outcome,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load restarted root")
        .expect("root exists");
    let GrepLifecycle::Backfilling {
        checkpoint_id: Some(checkpoint_id),
        ..
    } = root.state().lifecycle()
    else {
        panic!("gap must restart checkpointed backfill");
    };
    admin
        .release_checkpoint(&namespace_id, checkpoint_id)
        .await
        .expect("remove checkpoint mid-backfill");
    let vanished = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("vanished checkpoint restart");
    assert!(matches!(
        vanished.outcome,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after rebootstrap");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn grep_root_lifecycle_pins_not_materialized_error_surface() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("error-surface").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("error-writer")
        .build()
        .await
        .expect("writer");
    let new_reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store);

    assert_not_materialized_error(
        "never enabled",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );
    worker.enable(&namespace_id).await.expect("enable");
    assert_not_materialized_error(
        "backfilling",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );
    worker.disable(&namespace_id).await.expect("disable");
    assert_not_materialized_error(
        "disabled",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );

    let missing_manifest_id =
        GrepManifestId::parse("1111111111111111111111111111111111111111111111111111111111111111")
            .expect("valid manifest id");
    write_pointer(&*store, &namespace_id, missing_manifest_id).await;
    assert_not_materialized_error(
        "missing manifest",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );

    let corrupt_manifest_id =
        GrepManifestId::parse("2222222222222222222222222222222222222222222222222222222222222222")
            .expect("valid manifest id");
    store
        .put_overwrite(
            &manifest_key(&namespace_id, &corrupt_manifest_id),
            Bytes::from_static(b"corrupt manifest"),
        )
        .await
        .expect("write corrupt manifest");
    write_pointer(&*store, &namespace_id, corrupt_manifest_id).await;
    assert_not_materialized_error(
        "corrupt manifest",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );

    store
        .put_overwrite(
            &root_key(&namespace_id),
            Bytes::from_static(b"corrupt pointer"),
        )
        .await
        .expect("write corrupt pointer");
    assert_not_materialized_error(
        "corrupt pointer",
        new_reader.grep(&namespace_id, &request("needle")).await,
    );
    writer.shutdown_background().await.expect("shutdown");
}

async fn write_pointer(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_id: GrepManifestId,
) {
    let envelope = GrepRootEnvelope::from_pointer(
        "grep-error-surface-test/0.1",
        GrepRootPointer::new(namespace_id.clone(), manifest_id),
    )
    .expect("build pointer");
    store
        .put_overwrite(
            &root_key(namespace_id),
            Bytes::from(encode_grep_root(&envelope).expect("encode pointer")),
        )
        .await
        .expect("write pointer");
}

fn assert_not_materialized_error(case: &str, result: loonfs::Result<GrepResponse>) {
    match result {
        Err(loonfs::Error::Core(error)) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `index.grams` is not materialized on this namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected not-materialized error for {case}, got {outcome:?}"),
    }
}

#[tokio::test]
async fn grep_worker_pins_fold_tail_and_pagination_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-results").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("worker-results-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store);
    let policy = GramIndexBuildPolicy {
        max_l0_runs: 2,
        max_mid_runs: 2,
        ..GramIndexBuildPolicy::default()
    };
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    for round in 0..6u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/file-{round}.txt"),
                format!("shared needle {round}\nshared needle again {round}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write indexed file");
        worker
            .build_step(&namespace_id, policy)
            .await
            .expect("new build");
        worker
            .fold_step(&namespace_id, policy)
            .await
            .expect("new fold");
    }
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/tail.txt",
            b"tail-only needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write tail file");

    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    let levels: BTreeSet<u32> = root
        .state()
        .segments()
        .iter()
        .map(|segment| segment.level)
        .collect();
    assert!(
        levels.contains(&1) && levels.contains(&2),
        "levels: {levels:?}"
    );

    let shared = new_query(&store, &namespace_id, &request("shared needle"))
        .await
        .expect("shared query");
    assert_eq!(shared.namespace_id, namespace_id);
    assert_eq!(shared.matches.len(), 12);
    assert!(shared.tail_scanned);
    assert!(shared.built_through_seq < shared.head_seq);
    assert!(shared.next_cursor.is_none());
    for found in &shared.matches {
        assert!(found.absolute_path.starts_with("/docs/file-"));
        assert!(matches!(found.line_number, 1 | 2));
        assert_eq!(
            found.byte_offset,
            if found.line_number == 1 { 0 } else { 16 }
        );
        assert!(found.line.starts_with("shared needle"));
        assert!(!found.line_truncated);
    }

    let tail = new_query(&store, &namespace_id, &request("tail-only needle"))
        .await
        .expect("tail query");
    assert_eq!(tail.matches.len(), 1);
    assert_eq!(tail.matches[0].absolute_path, "/tail.txt");
    assert_eq!(tail.matches[0].line_number, 1);
    assert_eq!(tail.matches[0].byte_offset, 0);
    assert_eq!(tail.matches[0].line, "tail-only needle");
    assert!(tail.tail_scanned);
    assert!(tail.next_cursor.is_none());

    let absent = new_query(&store, &namespace_id, &request("absent needle"))
        .await
        .expect("absent query");
    assert!(absent.matches.is_empty());
    assert!(absent.next_cursor.is_none());

    let mut page_request = request("shared needle");
    page_request.limit = Some(1);
    let mut found_matches = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    loop {
        let page = new_query(&store, &namespace_id, &page_request)
            .await
            .expect("query page");
        assert_eq!(page.namespace_id, namespace_id);
        assert_eq!(page.matches.len(), 1);
        let found = &page.matches[0];
        assert!(found_matches.insert((found.absolute_path.clone(), found.line_number)));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(cursors.insert(cursor.clone()));
        page_request.cursor = Some(cursor);
    }
    assert_eq!(found_matches.len(), 12);
    assert_eq!(cursors.len(), 11);
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn fork_of_grep_enabled_namespace_starts_unmaterialized_without_manifest_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let source = NamespaceId::parse("grep-fork-source").expect("source namespace");
    let target = NamespaceId::parse("grep-fork-target").expect("target namespace");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("fork-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&source, CreateNamespaceOptions::default())
        .await
        .expect("create source");
    writer
        .put_file_bytes(
            &source,
            "/source.txt",
            b"fork needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write source");
    let worker = worker(&store);
    worker.enable(&source).await.expect("enable source");
    drive_worker_to_current(&worker, &source, GramIndexBuildPolicy::default()).await;
    let source_root_before = load_grep_root(&*store, &source)
        .await
        .expect("load source root")
        .expect("source root exists")
        .state()
        .clone();

    writer
        .fork_namespace(&source, &target)
        .await
        .expect("fork source");

    assert!(
        load_grep_root(&*store, &target)
            .await
            .expect("load target root")
            .is_none(),
        "fork target must have no grep root until explicitly enabled"
    );
    let target_reader = writer.reader();
    assert_not_materialized_error(
        "fork target",
        target_reader.grep(&target, &request("needle")).await,
    );

    let (_, target_metadata_root) = load_namespace_read_anchor(&*store, &target)
        .await
        .expect("load target read anchor");
    let manifest_key = metadata_manifest_object(
        target.as_str(),
        &target_metadata_root.state.manifest_object_id,
    );
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read target manifest")
        .expect("target manifest exists");
    let document: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("decode target manifest JSON");
    let payload = document["payload"].as_object().expect("manifest payload");
    assert!(!payload.contains_key("index_files"));
    assert!(!payload.contains_key("features"));

    let source_root_after = load_grep_root(&*store, &source)
        .await
        .expect("reload source root")
        .expect("source root still exists")
        .state()
        .clone();
    assert_eq!(source_root_after, source_root_before);
    let source_response = new_query(&store, &source, &request("fork needle"))
        .await
        .expect("source query after fork");
    assert_eq!(source_response.matches.len(), 1);
    assert_eq!(source_response.matches[0].absolute_path, "/source.txt");
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn checkpoint_backfill_matches_incremental_worker_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let backfill_namespace = NamespaceId::parse("equiv-backfill").expect("namespace id");
    let incremental_namespace = NamespaceId::parse("equiv-incremental").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("equiv-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    for namespace_id in [&backfill_namespace, &incremental_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }
    let worker = worker(&store);
    worker
        .enable(&incremental_namespace)
        .await
        .expect("enable incremental");
    drive_worker_to_current(
        &worker,
        &incremental_namespace,
        GramIndexBuildPolicy::default(),
    )
    .await;

    for index in 0..5u32 {
        for namespace_id in [&backfill_namespace, &incremental_namespace] {
            writer
                .put_file_bytes(
                    namespace_id,
                    &format!("/file-{index}.txt"),
                    format!("equivalence needle {index}\n").as_bytes(),
                    PutFileOptions::default(),
                )
                .await
                .expect("write file");
        }
        drive_worker_to_current(
            &worker,
            &incremental_namespace,
            GramIndexBuildPolicy::default(),
        )
        .await;
    }
    worker
        .enable(&backfill_namespace)
        .await
        .expect("enable backfill");
    drive_worker_to_current(
        &worker,
        &backfill_namespace,
        GramIndexBuildPolicy {
            max_files_per_step: 2,
            ..GramIndexBuildPolicy::default()
        },
    )
    .await;

    let backfill = new_query(&store, &backfill_namespace, &request("needle"))
        .await
        .expect("backfill query");
    let incremental = new_query(&store, &incremental_namespace, &request("needle"))
        .await
        .expect("incremental query");
    assert_eq!(
        normalize_namespace(incremental, &backfill_namespace),
        backfill
    );
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn pointer_advance_heals_a_manifest_deleted_after_already_exists() {
    let temp_dir = tempdir().expect("tempdir");
    let base: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("manifest-heal").expect("namespace id");
    let writer = FsWriter::builder_with_store(base.clone())
        .writer_id("manifest-heal-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/needle.txt",
            b"verify and heal needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write indexed file");
    let initial_worker = worker(&base);
    initial_worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(
        &initial_worker,
        &namespace_id,
        GramIndexBuildPolicy::default(),
    )
    .await;
    writer.shutdown_background().await.expect("shutdown writer");

    let current = load_grep_root(&*base, &namespace_id)
        .await
        .expect("load current root")
        .expect("root exists");
    let current_state = current.state();
    let next = GrepRootState::new(
        namespace_id.clone(),
        current_state.lifecycle().clone(),
        GrepIndexState::new(
            current_state.index().built_through_seq,
            current_state.index().fold.clone(),
            current_state.index().next_run_ordinal + 1,
        ),
        current_state.segments().to_vec(),
    )
    .expect("valid successor state");
    let candidate = GrepManifestEnvelope::from_state("manifest-heal-test/0.1", next.clone())
        .expect("candidate manifest");
    let candidate_key = manifest_key(&namespace_id, candidate.manifest_id());
    base.put_if_absent(
        &candidate_key,
        Bytes::from(encode_grep_manifest(&candidate).expect("encode candidate")),
    )
    .await
    .expect("pre-land the content-addressed candidate");

    let blocking = Arc::new(BlockingGrepRootCasStore::new(
        base.clone(),
        root_key(&namespace_id),
    ));
    let store: SharedObjectStore = blocking.clone();
    let advance = advance_grep_root(&*store, &current, &next, "manifest-heal-test/0.1");
    let collect = async {
        blocking.wait_until_blocked().await;
        let report = worker(&store)
            .garbage_collect_namespace(&namespace_id, u64::MAX)
            .await;
        let candidate_after_gc = store.head(&candidate_key).await;
        blocking.unblock();
        (report, candidate_after_gc)
    };
    let (advanced, (report, candidate_after_gc)) = tokio::join!(advance, collect);

    assert!(
        report
            .expect("grep GC wins the manifest race")
            .deleted_other_objects
            >= 1
    );
    assert!(candidate_after_gc
        .expect("head candidate after GC")
        .is_none());
    advanced.expect("pointer advance verifies and heals its manifest");
    assert!(store
        .head(&candidate_key)
        .await
        .expect("head healed manifest")
        .is_some());
    let response = new_query(&store, &namespace_id, &request("verify and heal needle"))
        .await
        .expect("query follows the healed pointer");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/needle.txt");
}

#[derive(Debug)]
struct AgedMetadataStore {
    inner: LocalFsStore,
    listed_prefixes: Mutex<Vec<String>>,
}

impl AgedMetadataStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("local store"),
            listed_prefixes: Mutex::new(Vec::new()),
        }
    }

    fn age(mut metadata: ObjectMetadata) -> ObjectMetadata {
        metadata.last_modified_ms = Some(0);
        metadata
    }

    fn clear_listed_prefixes(&self) {
        self.listed_prefixes.lock().expect("prefix lock").clear();
    }

    fn listed_prefixes(&self) -> Vec<String> {
        self.listed_prefixes.lock().expect("prefix lock").clone()
    }
}

#[async_trait]
impl ObjectStore for AgedMetadataStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self.inner.head(key).await?.map(Self::age))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self.inner.get_with_metadata(key).await?.map(|mut body| {
            body.metadata = Self::age(body.metadata);
            body
        }))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await.map(Self::age)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.listed_prefixes
            .lock()
            .expect("prefix lock")
            .push(prefix.to_owned());
        self.inner.list_prefix_stream(prefix)
    }
}

#[tokio::test]
async fn grep_gc_retains_live_roots_reaps_deleted_namespaces_and_never_crosses_keyspaces() {
    let temp_dir = tempdir().expect("tempdir");
    let aged_store = Arc::new(AgedMetadataStore::new(temp_dir.path()));
    let store: SharedObjectStore = aged_store.clone();
    let live_namespace = NamespaceId::parse("gc-live").expect("namespace id");
    let deleted_namespace = NamespaceId::parse("gc-deleted").expect("namespace id");
    let corrupt_namespace = NamespaceId::parse("gc-corrupt").expect("namespace id");
    let absent_namespace = NamespaceId::parse("gc-absent").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gc-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("gc-admin")
        .build()
        .await
        .expect("admin");
    for namespace_id in [&live_namespace, &deleted_namespace, &corrupt_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                namespace_id,
                "/needle.txt",
                b"gc needle\n",
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store);
    for namespace_id in [&live_namespace, &deleted_namespace, &corrupt_namespace] {
        worker.enable(namespace_id).await.expect("enable");
        drive_worker_to_current(&worker, namespace_id, GramIndexBuildPolicy::default()).await;
    }
    let live_root = load_grep_root(&*store, &live_namespace)
        .await
        .expect("load live root")
        .expect("root exists");
    let live_segment_key =
        segment_key(&live_namespace, &live_root.state().segments()[0].segment_id);
    let live_manifest_key =
        manifest_key(&live_namespace, live_root.manifest_envelope().manifest_id());
    let orphan_key = segment_key(&live_namespace, &IndexSegmentId::generate());
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"orphan"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write orphan");
    let non_grep_key =
        format!("namespaces/{live_namespace}/metadata/tables/grep-gc-sentinel.sst.zst");
    store
        .put(
            &non_grep_key,
            Bytes::from_static(b"non-grep"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write non-grep sentinel");
    let absent_grep_key = segment_key(&absent_namespace, &IndexSegmentId::generate());
    store
        .put(
            &absent_grep_key,
            Bytes::from_static(b"absent-namespace grep state"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write absent namespace grep state");
    let corrupt_keys = store
        .list_prefix(&namespace_prefix(&corrupt_namespace))
        .await
        .expect("list corrupt namespace grep state");
    store
        .put_overwrite(
            &root_key(&corrupt_namespace),
            Bytes::from_static(b"corrupt root pointer"),
        )
        .await
        .expect("corrupt root pointer");

    writer
        .delete_namespace(&deleted_namespace, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    let live_report = worker
        .garbage_collect_namespace(&live_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect live namespace");
    let deleted_report = worker
        .garbage_collect_namespace(&deleted_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect deleted namespace");
    let corrupt_report = worker
        .garbage_collect_namespace(&corrupt_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect corrupt namespace");
    let absent_report = worker
        .garbage_collect_namespace(&absent_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect absent namespace");
    assert!(live_report.deleted_segments >= 1);
    assert!(deleted_report.namespace_reaped);
    assert!(absent_report.namespace_reaped);
    assert!(corrupt_report.namespace_degraded);
    assert!(store
        .head(&live_segment_key)
        .await
        .expect("head live")
        .is_some());
    assert!(store
        .head(&live_manifest_key)
        .await
        .expect("head live manifest")
        .is_some());
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&live_namespace))
            .await
            .expect("list retained live manifests"),
        vec![live_manifest_key],
        "only the root-referenced manifest remains live"
    );
    assert!(store
        .head(&orphan_key)
        .await
        .expect("head orphan")
        .is_none());
    assert!(store
        .list_prefix(&namespace_prefix(&deleted_namespace))
        .await
        .expect("list deleted grep prefix")
        .is_empty());
    assert!(store
        .list_prefix(&namespace_prefix(&absent_namespace))
        .await
        .expect("list absent grep prefix")
        .is_empty());
    for key in corrupt_keys {
        assert!(
            store
                .head(&key)
                .await
                .expect("head degraded-retained key")
                .is_some(),
            "corrupt live grep state must degrade to retention for `{key}`"
        );
    }
    assert!(store
        .head(&non_grep_key)
        .await
        .expect("head sentinel")
        .is_some());

    let core_only_grep_key = segment_key(&live_namespace, &IndexSegmentId::generate());
    store
        .put(
            &core_only_grep_key,
            Bytes::from_static(b"core-must-ignore"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write grep sentinel");
    aged_store.clear_listed_prefixes();
    admin
        .gc_namespace(&live_namespace, &GcConfig::default())
        .await
        .expect("core gc");
    assert_eq!(
        aged_store.listed_prefixes(),
        vec![
            checkpoint_prefix(live_namespace.as_str()),
            wal_segment_prefix(live_namespace.as_str()),
            metadata_table_prefix(live_namespace.as_str()),
            metadata_manifest_prefix(live_namespace.as_str()),
            checkpoint_prefix(live_namespace.as_str()),
            upload_session_prefix(live_namespace.as_str()),
        ],
        "core GC must list only its six core prefixes"
    );
    assert!(
        store
            .head(&core_only_grep_key)
            .await
            .expect("head grep sentinel")
            .is_some(),
        "core GC must not learn grep keys"
    );
    writer.shutdown_background().await.expect("shutdown");
}
