#![allow(clippy::panic)]
// Cross-world and lifecycle diagnostics deliberately panic with both outcomes.

//! GrepWorker lifecycle, rebootstrap, GC, and old/new cross-world equivalence.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ErrorCode, FsAdmin, FsWriter, GcConfig,
    GramIndexBuildPolicy, GrepRequest, GrepResponse, NamespaceId, PutFileOptions,
    SharedObjectStore,
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
use loonfs_grep::keyspace::{namespace_prefix, segment_key};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{
    GrepBuildOutcome, GrepFoldOutcome, GrepIndexSnapshot, GrepService, GrepWorker,
    GREP_GC_GRACE_WINDOW_MS,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

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

async fn drive_old_step(engine: &NamespaceEngine<SharedObjectStore>, policy: GramIndexBuildPolicy) {
    engine
        .build_grams_index_step(policy, None)
        .await
        .expect("old build step");
    engine
        .fold_grams_index_step(policy, None)
        .await
        .expect("old fold step");
}

async fn old_query(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
) -> loonfs_core::Result<GrepResponse> {
    let engine = NamespaceEngine::builder(store.clone())
        .namespace_id(namespace_id.clone())
        .writer_id("old-query")
        .build()
        .expect("old query engine");
    engine
        .grep_with_runtime_context(grep_request, &read_context(store, namespace_id).await)
        .await
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
    let root = load_grep_root(&**store, namespace_id)
        .await
        .map_err(|error| loonfs_core::Error::NamespaceCorrupt(error.to_string()))?;
    let snapshot = GrepIndexSnapshot::from_grep_root(root.as_ref().map(|root| root.state()));
    GrepService::new()
        .query(grep_request, &snapshot, &view, store)
        .await
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
        .garbage_collect(u64::MAX)
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
async fn grep_root_lifecycle_matches_old_not_materialized_error_surface() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let old_namespace = NamespaceId::parse("error-old").expect("namespace id");
    let new_namespace = NamespaceId::parse("error-new").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("error-writer")
        .build()
        .await
        .expect("writer");
    let new_reader = writer.reader();
    for namespace_id in [&old_namespace, &new_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }
    let old_engine = NamespaceEngine::builder(store.clone())
        .namespace_id(old_namespace.clone())
        .writer_id("error-old-index")
        .build()
        .expect("old engine");
    let worker = worker(&store);

    assert_same_runtime_error(
        "never enabled",
        old_query(&store, &old_namespace, &request("needle")).await,
        new_reader.grep(&new_namespace, &request("needle")).await,
    );
    old_engine.enable_grams_index().await.expect("old enable");
    worker.enable(&new_namespace).await.expect("new enable");
    assert_same_runtime_error(
        "backfilling",
        old_query(&store, &old_namespace, &request("needle")).await,
        new_reader.grep(&new_namespace, &request("needle")).await,
    );
    old_engine.disable_grams_index().await.expect("old disable");
    worker.disable(&new_namespace).await.expect("new disable");
    assert_same_runtime_error(
        "disabled",
        old_query(&store, &old_namespace, &request("needle")).await,
        new_reader.grep(&new_namespace, &request("needle")).await,
    );
    writer.shutdown_background().await.expect("shutdown");
}

fn assert_same_runtime_error(
    case: &str,
    old: loonfs_core::Result<GrepResponse>,
    new: loonfs::Result<GrepResponse>,
) {
    match (old, new) {
        (Err(old), Err(loonfs::Error::Core(new))) => {
            assert_eq!(old.code(), ErrorCode::NotSupported, "old code for {case}");
            assert_eq!(new.code(), old.code(), "new code for {case}");
            assert_eq!(new.to_string(), old.to_string(), "error text for {case}");
        }
        outcomes => panic!("expected matching errors for {case}, got {outcomes:?}"),
    }
}

#[tokio::test]
async fn grep_worker_matches_old_pipeline_across_folds_tail_and_pagination() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let old_namespace = NamespaceId::parse("cross-old").expect("namespace id");
    let new_namespace = NamespaceId::parse("cross-new").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("cross-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    for namespace_id in [&old_namespace, &new_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }
    let old_engine = NamespaceEngine::builder(store.clone())
        .namespace_id(old_namespace.clone())
        .writer_id("cross-old-index")
        .build()
        .expect("old engine");
    let worker = worker(&store);
    let policy = GramIndexBuildPolicy {
        max_l0_runs: 2,
        max_mid_runs: 2,
        ..GramIndexBuildPolicy::default()
    };
    old_engine.enable_grams_index().await.expect("old enable");
    worker.enable(&new_namespace).await.expect("new enable");
    drive_old_step(&old_engine, policy).await;
    drive_worker_to_current(&worker, &new_namespace, policy).await;

    for round in 0..6u32 {
        for namespace_id in [&old_namespace, &new_namespace] {
            writer
                .put_file_bytes(
                    namespace_id,
                    &format!("/docs/file-{round}.txt"),
                    format!("shared needle {round}\nshared needle again {round}\n").as_bytes(),
                    PutFileOptions::default(),
                )
                .await
                .expect("write indexed file");
        }
        drive_old_step(&old_engine, policy).await;
        worker
            .build_step(&new_namespace, policy)
            .await
            .expect("new build");
        worker
            .fold_step(&new_namespace, policy)
            .await
            .expect("new fold");
    }
    drive_worker_to_current(&worker, &new_namespace, policy).await;

    for namespace_id in [&old_namespace, &new_namespace] {
        writer
            .put_file_bytes(
                namespace_id,
                "/tail.txt",
                b"tail-only needle\n",
                PutFileOptions::default(),
            )
            .await
            .expect("write tail file");
    }

    let root = load_grep_root(&*store, &new_namespace)
        .await
        .expect("load new root")
        .expect("new root exists");
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

    for pattern in ["shared needle", "tail-only needle", "absent needle"] {
        let old = old_query(&store, &old_namespace, &request(pattern))
            .await
            .expect("old query");
        let new = new_query(&store, &new_namespace, &request(pattern))
            .await
            .expect("new query");
        assert_eq!(normalize_namespace(new, &old_namespace), old, "{pattern}");
    }

    let mut old_request = request("shared needle");
    let mut new_request = old_request.clone();
    old_request.limit = Some(1);
    new_request.limit = Some(1);
    loop {
        let old = old_query(&store, &old_namespace, &old_request)
            .await
            .expect("old page");
        let new = new_query(&store, &new_namespace, &new_request)
            .await
            .expect("new page");
        assert_eq!(normalize_namespace(new.clone(), &old_namespace), old);
        match (old.next_cursor, new.next_cursor) {
            (Some(old_cursor), Some(new_cursor)) => {
                old_request.cursor = Some(old_cursor);
                new_request.cursor = Some(new_cursor);
            }
            (None, None) => break,
            cursors => panic!("pagination diverged: {cursors:?}"),
        }
    }
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

#[derive(Debug)]
struct AgedMetadataStore {
    inner: LocalFsStore,
}

impl AgedMetadataStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("local store"),
        }
    }

    fn age(mut metadata: ObjectMetadata) -> ObjectMetadata {
        metadata.last_modified_ms = Some(0);
        metadata
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
        self.inner.list_prefix_stream(prefix)
    }
}

#[tokio::test]
async fn grep_gc_retains_live_roots_reaps_deleted_namespaces_and_never_crosses_keyspaces() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(AgedMetadataStore::new(temp_dir.path()));
    let live_namespace = NamespaceId::parse("gc-live").expect("namespace id");
    let deleted_namespace = NamespaceId::parse("gc-deleted").expect("namespace id");
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
    for namespace_id in [&live_namespace, &deleted_namespace] {
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
    for namespace_id in [&live_namespace, &deleted_namespace] {
        worker.enable(namespace_id).await.expect("enable");
        drive_worker_to_current(&worker, namespace_id, GramIndexBuildPolicy::default()).await;
    }
    let live_root = load_grep_root(&*store, &live_namespace)
        .await
        .expect("load live root")
        .expect("root exists");
    let live_segment_key =
        segment_key(&live_namespace, &live_root.state().segments()[0].segment_id);
    let orphan_key = segment_key(&live_namespace, &IndexSegmentId::generate());
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"orphan"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write orphan");
    let non_grep_key = format!("namespaces/{live_namespace}/metadata/indexes/sentinel.sst");
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

    writer
        .delete_namespace(&deleted_namespace, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    let report = worker
        .garbage_collect(GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("grep gc");
    assert!(report.deleted_segments >= 1);
    assert!(store
        .head(&live_segment_key)
        .await
        .expect("head live")
        .is_some());
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
    admin
        .gc_namespace(&live_namespace, &GcConfig::default())
        .await
        .expect("core gc");
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
