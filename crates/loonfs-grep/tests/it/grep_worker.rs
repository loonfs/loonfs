#![allow(clippy::panic)]
// Lifecycle diagnostics deliberately panic with the full unexpected outcome.

//! GrepWorker lifecycle, rebootstrap, query contracts, and GC boundaries.

use crate::common::{control, default_page_limit, grep_with, page_limit, GrepHost};
use bytes::Bytes;
use loonfs::{
    CoreError, CreateNamespaceOptions, DeleteNamespaceOptions, ErrorCode, FsAdmin, FsReader,
    FsWriter, GcConfig, MaintenancePlan, MetadataMaintenanceOptions, NamespaceId, PutFileOptions,
    RuntimeError, SharedObjectStore,
};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus};
use loonfs_api::{
    sha256_digest, AbsolutePath, ChangeSeq, EffectiveLimit, GrepRequest, GrepResponse,
    IndexSegmentId, PageRequest, PaginationPolicy, RunNo, MAX_PUBLIC_INTEGER,
};
use loonfs_grep::keyspace::{
    grep_prefix, manifest_key, manifests_prefix, root_key, segment_key, segments_prefix,
};
use loonfs_grep::root::{
    advance_grep_root, encode_grep_root, load_grep_root, GrepIndexState, GrepIndexStatus,
    GrepManifestObjectId, GrepManifestState, GrepRootEnvelope, GrepRootPointer,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepGcOptions, GrepGcReport,
    GrepReorganizeOutcome, GrepService, GrepWorker, GREP_GC_GRACE_WINDOW_MS,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, checkpoint_record, metadata_compaction_prefix, metadata_manifest_object,
    metadata_manifest_prefix, metadata_segment_prefix, upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::nonzero_usize;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    OperationContext, OperationKind, RecordedOperation, RecordingStore,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        allow_stale: false,
        allow_scan: false,
    }
}

async fn worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    GrepHost::new(store, "grep-worker-tests").await.worker
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
        let reorganize = worker
            .reorganize_step(namespace_id, policy)
            .await
            .expect("worker reorganize step");
        if matches!(build, GrepBuildOutcome::UpToDate { .. })
            && matches!(reorganize, GrepReorganizeOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("worker backlog must drain");
}

/// A cold query: a fresh reader and a fresh grep service, so nothing an
/// earlier query decoded can answer this one.
async fn new_query(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
) -> loonfs_grep::Result<GrepResponse> {
    new_query_page(store, namespace_id, grep_request, default_page_limit()).await
}

async fn new_query_page(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
    limit: EffectiveLimit,
) -> loonfs_grep::Result<GrepResponse> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("new query reader");
    grep_with(
        &GrepService::new(),
        &reader,
        store,
        namespace_id,
        grep_request,
        limit,
    )
    .await
}

async fn flush_wal_and_advance_retention(admin: &FsAdmin, namespace_id: &NamespaceId) -> ChangeSeq {
    admin
        .run_maintenance(
            namespace_id,
            MaintenancePlan {
                metadata: Some(MetadataMaintenanceOptions {
                    max_wal_tail_segments: std::num::NonZeroU64::MIN,
                }),
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("flush wal");
    admin
        .run_maintenance(
            namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("advance retention")
        .retention
        .expect("retention selected")
        .retention_floor_seq
}

fn normalize_namespace(mut response: GrepResponse, namespace_id: &NamespaceId) -> GrepResponse {
    response.namespace_id = namespace_id.clone();
    response
}

#[tokio::test]
async fn grep_query_keeps_its_pinned_head_when_a_matching_file_commits_mid_query() {
    let temp_dir = tempdir().expect("tempdir");
    let base = Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let base_store: SharedObjectStore = base.clone();
    let namespace_id = NamespaceId::parse("query-pin").expect("namespace id");
    let writer = FsWriter::builder_with_store(base_store.clone())
        .writer_id("query-pin-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    GrepHost::new(&base_store, "query-pin-index")
        .await
        .enable_grep_index(&namespace_id)
        .await
        .expect("enable empty index");

    let blocking = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::exact(root_key(&namespace_id)),
        OperationClass::GetWithMetadata,
    ));
    let query_store: SharedObjectStore = blocking.clone();
    blocking.block_next();
    let grep_request = request("mid-query needle");
    let query = new_query(&query_store, &namespace_id, &grep_request);
    let publish = async {
        blocking.wait_until_blocked().await;
        let committed = writer
            .put_file_bytes(
                &namespace_id,
                "/later.txt",
                b"mid-query needle\n",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await;
        blocking.release();
        committed
    };
    let (pinned_response, committed) = tokio::join!(query, publish);
    let committed = committed.expect("publish matching file while query is paused");
    let pinned_response = pinned_response.expect("pinned query completes");

    assert!(pinned_response.matches.is_empty());
    assert!(pinned_response.head_seq < committed.committed_seq);

    let latest = new_query(&query_store, &namespace_id, &grep_request)
        .await
        .expect("later query sees committed file");
    assert_eq!(latest.head_seq, committed.committed_seq);
    assert_eq!(latest.matches.len(), 1);
    assert_eq!(latest.matches[0].path, "/later.txt");
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
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
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
    let GrepIndexStatus::Backfilling { checkpoint_id, .. } = root.manifest_state().status() else {
        panic!(
            "enable must publish checkpointed backfill: {:?}",
            root.manifest_state()
        );
    };
    let checkpoint_id = checkpoint_id.clone();

    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(matches!(first, GrepBuildOutcome::Published { .. }));
    let error = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect_err("backfill is not materialized");
    assert_eq!(error.code(), ErrorCode::NotSupported);

    drive_worker_to_current(&worker, &namespace_id, policy).await;
    let checkpoint = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("released checkpoint record remains until core GC");
    assert!(matches!(
        checkpoint.status,
        CheckpointStatus::Released { .. }
    ));
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
        &materialized_root.manifest_state().segments()[0].segment_id,
    );

    assert_eq!(
        worker.disable(&namespace_id).await.expect("disable"),
        loonfs_grep::GrepDisableOutcome::Disabled
    );
    worker
        .garbage_collect_namespace(&namespace_id, u64::MAX, &GrepGcOptions::default())
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
        disabled_root.manifest_state().status(),
        GrepIndexStatus::Disabled {}
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
    assert!(root.manifest_state().segments().is_empty());
    assert!(matches!(
        root.manifest_state().status(),
        GrepIndexStatus::Backfilling { .. }
    ));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exhausted_run_numbers_fail_as_server_errors_without_writing_the_root() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("run-number-limit").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("run-number-limit-writer")
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
            "/initial.txt",
            b"initial run number boundary needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write initial file");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable grep");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let current = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load current root")
        .expect("current root");
    let current_state = current.manifest_state();
    let maximum_state = GrepManifestState::new(
        namespace_id.clone(),
        current_state.status().clone(),
        GrepIndexState {
            reorganize: current_state.index().reorganize.clone(),
            next_run_no: RunNo(MAX_PUBLIC_INTEGER),
        },
        current_state.segments().to_vec(),
    )
    .expect("valid root at the public maximum");
    let maximum = advance_grep_root(&*store, &current, &maximum_state)
        .await
        .expect("install root at the public maximum");

    writer
        .put_file_bytes(
            &namespace_id,
            "/incremental.txt",
            b"incremental run number boundary needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write incremental file");
    let pointer_before = store
        .get(&root_key(&namespace_id), None)
        .await
        .expect("read root pointer before failure")
        .expect("root pointer exists");
    let manifests_before = store
        .list_prefix(&manifests_prefix(&namespace_id))
        .await
        .expect("list manifests before failure");
    let segments_before = store
        .list_prefix(&segments_prefix(&namespace_id))
        .await
        .expect("list segments before failure");

    for error in [
        worker
            .build_step(&namespace_id, GramIndexBuildPolicy::default())
            .await
            .expect_err("a build cannot allocate a run above the public maximum"),
        worker
            .reorganize_step(
                &namespace_id,
                GramIndexBuildPolicy {
                    max_delta_runs: NonZeroUsize::MIN,
                    ..GramIndexBuildPolicy::default()
                },
            )
            .await
            .expect_err("a reorganization cannot allocate above the public maximum"),
    ] {
        assert_eq!(error.code(), ErrorCode::ServerError);
        assert!(matches!(
            error,
            GrepError::Runtime(RuntimeError::Core(CoreError::Internal(message)))
                if message.contains("run number must be an integer")
        ));
    }

    let after = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root after failures")
        .expect("root remains");
    assert_eq!(after.manifest_object_id(), maximum.manifest_object_id());
    assert_eq!(
        after.manifest_state().index().next_run_no,
        RunNo(MAX_PUBLIC_INTEGER)
    );
    assert_eq!(
        store
            .get(&root_key(&namespace_id), None)
            .await
            .expect("read root pointer after failure")
            .expect("root pointer remains"),
        pointer_before
    );
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&namespace_id))
            .await
            .expect("list manifests after failures"),
        manifests_before
    );
    assert_eq!(
        store
            .list_prefix(&segments_prefix(&namespace_id))
            .await
            .expect("list segments after failures"),
        segments_before,
        "the range check must run before writing a segment"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enable_creates_no_checkpoint_when_the_root_load_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("enable-root-failure").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("enable-root-failure-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "enable-root-failure-admin").await;
    let grep_root_key = root_key(&namespace_id);
    let root_loads = Arc::new(AtomicUsize::new(0));
    let observed_root_loads = Arc::clone(&root_loads);
    let failing_store = Arc::new(FailStore::matching(
        store.clone(),
        move |context: &OperationContext<'_>| {
            context.key() == grep_root_key
                && matches!(context.kind(), OperationKind::GetWithMetadata)
                && observed_root_loads.fetch_add(1, Ordering::SeqCst) == 0
        },
        InjectedError::Transport("injected grep-root reload failure".to_owned()),
    ));
    failing_store.fail_all();
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.admin.clone(),
        Arc::clone(&host.block_cache),
    );

    let error = worker
        .enable(&namespace_id)
        .await
        .expect_err("the root load fails before checkpoint creation");
    assert!(matches!(error, GrepError::StoreUnavailable { .. }));
    assert_eq!(failing_store.attempts(), 1);

    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = host.admin.list_checkpoints_pager(&namespace_id, request);
    let checkpoints = pager
        .next()
        .await
        .expect("a fresh pager has one page")
        .expect("list checkpoints after failed enable");
    assert!(
        checkpoints.checkpoints.is_empty(),
        "a failure before root publication must not leave an active checkpoint"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enable_retains_its_checkpoint_when_the_root_write_result_is_ambiguous() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("ambiguous-enable").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("ambiguous-enable-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "ambiguous-enable-admin").await;
    let grep_root_key = root_key(&namespace_id);
    let failing_store = Arc::new(
        FailStore::matching(
            store.clone(),
            move |context: &OperationContext<'_>| {
                context.key() == grep_root_key
                    && matches!(
                        context.kind(),
                        OperationKind::Put {
                            mode: PutMode::CreateIfAbsent,
                            ..
                        }
                    )
            },
            InjectedError::Transport("injected failure after root publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.admin.clone(),
        Arc::clone(&host.block_cache),
    );

    let error = worker
        .enable(&namespace_id)
        .await
        .expect_err("root publication acknowledgement fails");
    assert!(matches!(error, GrepError::StoreUnavailable { .. }));
    assert_eq!(failing_store.attempts(), 1);

    assert_fresh_backfill_attempt(&store, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restart_retains_its_checkpoint_when_the_root_write_result_is_ambiguous() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("ambiguous-restart").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("ambiguous-restart-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "ambiguous-restart-admin").await;
    host.worker
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    let previous_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    host.admin
        .release_checkpoint(&namespace_id, &previous_checkpoint_id)
        .await
        .expect("make the current backfill restart");

    let grep_root_key = root_key(&namespace_id);
    let failing_store = Arc::new(
        FailStore::matching(
            store.clone(),
            move |context: &OperationContext<'_>| {
                context.key() == grep_root_key
                    && matches!(
                        context.kind(),
                        OperationKind::Put {
                            mode: PutMode::CompareAndSwap { .. },
                            ..
                        }
                    )
            },
            InjectedError::Transport("injected failure after root publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.admin.clone(),
        Arc::clone(&host.block_cache),
    );

    let error = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect_err("restart publication acknowledgement fails");
    assert!(matches!(error, GrepError::StoreUnavailable { .. }));
    assert_eq!(failing_store.attempts(), 1);

    let checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_ne!(checkpoint_id, previous_checkpoint_id);
    writer.shutdown().await.expect("shutdown");
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
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/gap.txt",
            b"retention gap needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write after watermark");
    let retention_floor_seq = flush_wal_and_advance_retention(&admin, &namespace_id).await;
    assert!(retention_floor_seq > ChangeSeq(0));

    // The feed can no longer reach the watermark, which the worker must
    // read as "my basis is gone" and answer with a whole new attempt.
    let restart = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("gap restart");
    assert!(matches!(
        restart,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    let gap_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;

    // The second trigger: the pinned checkpoint stops pinning its basis
    // mid-backfill. The enumeration says so out loud instead of quietly
    // answering current state, and the worker starts over again.
    admin
        .release_checkpoint(&namespace_id, &gap_checkpoint_id)
        .await
        .expect("remove checkpoint mid-backfill");
    let vanished = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("vanished checkpoint restart");
    assert!(matches!(
        vanished,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    // A released record is finished for good, so the new attempt takes a
    // pin of its own: a new id, pinning its basis, with the walk starting
    // from nothing.
    let fresh_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_ne!(fresh_checkpoint_id, gap_checkpoint_id);

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after rebootstrap");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retention_passing_a_backfill_checkpoint_never_serves_a_partial_query() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-handoff-gap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("handoff-gap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("handoff-gap-admin")
        .build()
        .await
        .expect("admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..2u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/before-{index}.txt"),
                format!("handoff needle before {index}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write checkpointed file");
    }

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    let target_seq = match worker
        .lifecycle(&namespace_id)
        .await
        .expect("backfill lifecycle")
    {
        GrepIndexStatus::Backfilling { target_seq, .. } => target_seq,
        status => panic!("newly enabled grep must be backfilling, got {status:?}"),
    };
    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(matches!(
        first,
        GrepBuildOutcome::Published {
            indexed_revisions: 1,
            ..
        }
    ));

    writer
        .put_file_bytes(
            &namespace_id,
            "/during.txt",
            b"handoff needle during\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write during backfill");
    let retention_floor_seq = flush_wal_and_advance_retention(&admin, &namespace_id).await;
    assert!(retention_floor_seq > target_seq);

    // Checkpoint pages remain readable after retention passes their basis,
    // so the final page may still publish the snapshot watermark as active.
    // The change-feed boundary is authoritative: a query at that watermark
    // must fail explicitly instead of treating the missing tail as empty.
    let completed = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("final backfill page");
    assert!(matches!(
        completed,
        GrepBuildOutcome::Published {
            built_through_seq,
            ..
        } if built_through_seq == target_seq
    ));
    let error = new_query(&store, &namespace_id, &request("handoff needle"))
        .await
        .expect_err("an expired handoff cursor cannot answer a query");
    assert_eq!(error.code(), ErrorCode::RebootstrapRequired);

    let restarted = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("restart expired handoff");
    assert!(matches!(
        restarted,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let response = new_query(&store, &namespace_id, &request("handoff needle"))
        .await
        .expect("query after fresh backfill");
    assert_eq!(
        matched_paths(&response)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "/before-0.txt".to_owned(),
            "/before-1.txt".to_owned(),
            "/during.txt".to_owned(),
        ])
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_expired_but_unreleased_backfill_pin_keeps_enumerating() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-expiry").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("expiry-writer")
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
            "/expiring.txt",
            b"expiring needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    let checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;

    // Age the pin out from under the backfill without releasing it.
    let key = checkpoint_record(&namespace_id, &checkpoint_id);
    let mut record = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("backfill checkpoint record");
    assert!(
        record.owner.expires_at_ms().is_some(),
        "the backfill pin carries a ttl"
    );
    let CheckpointOwner::User { expires_at_ms, .. } = &mut record.owner else {
        panic!("the backfill pin is user-owned");
    };
    *expires_at_ms = Some(record.created_at_ms);
    let expired = loonfs_api::wire::control::CheckpointRecordEnvelope::from_state(
        loonfs_api::wire::control::ControlObjectKind::CheckpointRecord,
        record,
    )
    .expect("expired record envelope");
    store
        .put_overwrite(
            &key,
            Bytes::from(
                loonfs_api::wire::control::encode_control_object(&expired).expect("encode record"),
            ),
        )
        .await
        .expect("write the expired record");

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let finished = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("the record survives until core GC reaps it");
    assert!(
        matches!(finished.status, CheckpointStatus::Released { .. }),
        "the attempt completed the backfill using that pin and then released it"
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after a backfill on an expired pin");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown().await.expect("shutdown");
}

/// Asserts the namespace's grep root is at the start of a backfill attempt
/// — nothing indexed, nothing walked, an active checkpoint pinning its basis
/// — and returns the checkpoint that attempt holds.
async fn assert_fresh_backfill_attempt(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> loonfs_api::CheckpointId {
    let root = load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    let GrepIndexStatus::Backfilling {
        cursor_inode_id,
        checkpoint_id,
        ..
    } = root.manifest_state().status()
    else {
        panic!(
            "expected a checkpointed backfill: {:?}",
            root.manifest_state()
        );
    };
    assert_eq!(
        *cursor_inode_id, None,
        "a fresh backfill starts the walk from the beginning"
    );
    assert!(
        root.manifest_state().segments().is_empty(),
        "a rebootstrap discards the incomplete projection"
    );
    let record = control::checkpoint_record(store, namespace_id, checkpoint_id)
        .await
        .expect("the new attempt's checkpoint record exists");
    assert_eq!(
        record.status,
        CheckpointStatus::Active {},
        "a fresh backfill attempt must hold a checkpoint that still pins its basis"
    );
    checkpoint_id.clone()
}

/// Every grep segment the namespace's root names, for asserting that an
/// event changed no postings.
async fn grep_segment_ids(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<IndexSegmentId> {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .manifest_state()
        .segments()
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect()
}

async fn grep_built_through_seq(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .manifest_state()
        .status()
        .active_watermark()
        .expect("an active grep root has a watermark")
        .built_through_seq()
}

fn matched_paths(response: &GrepResponse) -> Vec<String> {
    response
        .matches
        .iter()
        .map(|found| found.path.as_str().to_owned())
        .collect()
}

#[tokio::test]
async fn commits_during_backfill_are_indexed_once_by_the_feed_phase() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("backfill-overlap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("overlap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..4u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/before-{index}.txt"),
                format!("overlap needle before {index}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    // One file per step, so the backfill is genuinely partway through the
    // checkpointed file set when the commits below land.
    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(
        matches!(
            first,
            GrepBuildOutcome::Published {
                indexed_revisions: 1,
                ..
            }
        ),
        "one file per step must leave the backfill unfinished: {:?}",
        first
    );

    // Commits strictly after the pinned sequence: one new file, and a
    // replacement of a file the checkpoint already pinned.
    writer
        .put_file_bytes(
            &namespace_id,
            "/during.txt",
            b"overlap needle during\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write during backfill");
    writer
        .put_file_bytes(
            &namespace_id,
            "/before-0.txt",
            b"overlap needle replaced\n",
            PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace a checkpointed file during backfill");

    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let response = new_query(&store, &namespace_id, &request("overlap needle"))
        .await
        .expect("query after backfill and catch-up");
    let paths = matched_paths(&response);
    assert_eq!(
        paths,
        vec![
            "/before-0.txt",
            "/before-1.txt",
            "/before-2.txt",
            "/before-3.txt",
            "/during.txt",
        ],
        "every file matches exactly once across both phases"
    );

    let replaced = new_query(&store, &namespace_id, &request("needle replaced"))
        .await
        .expect("query the replacing revision");
    assert_eq!(matched_paths(&replaced), vec!["/before-0.txt"]);
    let superseded = new_query(&store, &namespace_id, &request("needle before 0"))
        .await
        .expect("query the superseded revision");
    assert!(
        superseded.matches.is_empty(),
        "a revision the checkpoint pinned but the feed replaced must not match"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_move_reindexes_nothing_and_answers_the_new_path() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("move-no-reindex").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("move-writer")
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
            "/docs/note.txt",
            b"moved needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let built_before = grep_built_through_seq(&store, &namespace_id).await;

    let moved = writer
        .move_path(
            &namespace_id,
            "/docs/note.txt",
            "/docs/renamed.txt",
            loonfs::MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move the indexed file");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "a move must write no new postings"
    );
    let built_after = grep_built_through_seq(&store, &namespace_id).await;
    assert!(
        built_after > built_before && built_after >= moved.committed_seq,
        "the watermark still advances past a move: {built_before:?} -> {built_after:?}"
    );
    let response = new_query(&store, &namespace_id, &request("moved needle"))
        .await
        .expect("query after the move");
    assert_eq!(matched_paths(&response), vec!["/docs/renamed.txt"]);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_recursive_delete_hides_matches_and_an_undelete_rebuild_restores_them() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("delete-undelete").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("delete-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for name in ["a", "b"] {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/{name}.txt"),
                format!("subtree needle {name}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let docs_inode_id = reader
        .get_path_entry(&namespace_id, "/docs", Default::default())
        .await
        .expect("stat the directory")
        .inode_id;
    assert_eq!(
        new_query(&store, &namespace_id, &request("subtree needle"))
            .await
            .expect("query before the delete")
            .matches
            .len(),
        2
    );

    let deleted = writer
        .delete_path(
            &namespace_id,
            "/docs",
            loonfs::DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                ..loonfs::DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete the subtree");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let hidden = new_query(&store, &namespace_id, &request("subtree needle"))
        .await
        .expect("query after the delete");
    assert!(
        hidden.matches.is_empty(),
        "a deleted subtree's files must verify away: {:?}",
        matched_paths(&hidden)
    );
    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "a delete must write no new postings"
    );

    writer
        .undelete(
            &namespace_id,
            docs_inode_id,
            deleted.committed_seq,
            Some("/docs"),
            loonfs::UndeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("undelete the subtree");
    let restart = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("restart after undelete");
    assert!(matches!(
        restart,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let restored = new_query(&store, &namespace_id, &request("subtree needle"))
        .await
        .expect("query after the undelete");
    assert_eq!(
        matched_paths(&restored),
        vec!["/docs/a.txt", "/docs/b.txt"],
        "the fresh checkpoint must index the restored subtree"
    );
    assert_ne!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "an undelete must replace the old projection"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn undeleting_a_subtree_hidden_from_backfill_restarts_the_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("undelete-after-backfill").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("undelete-backfill-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for name in ["a", "b"] {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/{name}.txt"),
                format!("restored needle {name}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write file before delete");
    }
    let docs_inode_id = reader
        .get_path_entry(&namespace_id, "/docs", Default::default())
        .await
        .expect("stat docs before delete")
        .inode_id;
    let deleted = writer
        .delete_path(
            &namespace_id,
            "/docs",
            loonfs::DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                ..loonfs::DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete subtree before backfill");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    assert!(
        new_query(&store, &namespace_id, &request("restored needle"))
            .await
            .expect("query hidden tree")
            .matches
            .is_empty()
    );

    writer
        .undelete(
            &namespace_id,
            docs_inode_id,
            deleted.committed_seq,
            Some("/docs"),
            loonfs::UndeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("undelete subtree after backfill");

    let exact_error = new_query(&store, &namespace_id, &request("restored needle"))
        .await
        .expect_err("an exact query cannot project an unseen restored subtree");
    assert_eq!(exact_error.code(), ErrorCode::IndexLagging);

    let mut stale_request = request("restored needle");
    stale_request.allow_stale = true;
    let stale = new_query(&store, &namespace_id, &stale_request)
        .await
        .expect("indexed-only query across undelete");
    assert!(!stale.tail_scanned);
    assert!(stale.matches.is_empty());

    let restart = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("restart after undelete");
    assert!(matches!(
        restart,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let restored = new_query(&store, &namespace_id, &request("restored needle"))
        .await
        .expect("query rebuilt restored tree");
    assert_eq!(matched_paths(&restored), vec!["/docs/a.txt", "/docs/b.txt"]);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failing_worker_step_never_blocks_a_concurrent_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-isolation").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("isolation-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    store
        .put_overwrite(
            &root_key(&namespace_id),
            Bytes::from_static(b"corrupt pointer"),
        )
        .await
        .expect("poison the grep root");

    let (build, commit) = tokio::join!(
        worker.build_step(&namespace_id, GramIndexBuildPolicy::default()),
        writer.put_file_bytes(
            &namespace_id,
            "/during-failure.txt",
            b"isolated needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        ),
    );
    let error = build.expect_err("an unreadable grep root fails the step");
    assert_eq!(error.code(), ErrorCode::IndexCorrupt);
    commit.expect("the filesystem commit is unaffected by grep");
    let read = reader
        .get_file_bytes(&namespace_id, "/during-failure.txt")
        .await
        .expect("the committed file is readable");
    assert_eq!(read.bytes, b"isolated needle\n");
    writer.shutdown().await.expect("shutdown");
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
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;

    assert_not_enabled_error(
        "never enabled",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    worker.enable(&namespace_id).await.expect("enable");
    assert_backfilling_error(
        "backfilling",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    worker.disable(&namespace_id).await.expect("disable");
    assert_not_enabled_error(
        "disabled",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    let missing_manifest_object_id =
        GrepManifestObjectId::parse("gmf_11111111111111111111111111111111")
            .expect("valid manifest object id");
    write_pointer(
        &*store,
        &namespace_id,
        missing_manifest_object_id,
        sha256_digest(b"whatever the absent manifest would have carried"),
    )
    .await;
    assert_corrupt_index_error(
        "missing manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    let corrupt_manifest_object_id =
        GrepManifestObjectId::parse("gmf_22222222222222222222222222222222")
            .expect("valid manifest object id");
    store
        .put_overwrite(
            &manifest_key(&namespace_id, &corrupt_manifest_object_id),
            Bytes::from_static(b"corrupt manifest"),
        )
        .await
        .expect("write corrupt manifest");
    write_pointer(
        &*store,
        &namespace_id,
        corrupt_manifest_object_id,
        sha256_digest(b"corrupt manifest"),
    )
    .await;
    assert_corrupt_index_error(
        "corrupt manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    store
        .put_overwrite(
            &root_key(&namespace_id),
            Bytes::from_static(b"corrupt pointer"),
        )
        .await
        .expect("write corrupt pointer");
    assert_corrupt_index_error(
        "corrupt pointer",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn backfilling_root_without_checkpoint_id_is_index_corrupt() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("missing-backfill-checkpoint").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("missing-checkpoint-writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load backfilling root")
        .expect("backfilling root exists");
    let manifest_bytes = store
        .get(
            &manifest_key(&namespace_id, root.manifest_object_id()),
            None,
        )
        .await
        .expect("read backfilling manifest")
        .expect("backfilling manifest exists");
    let (corrupt_manifest_object_id, corrupt_payload_checksum) =
        write_manifest_without_checkpoint_id(&*store, &namespace_id, &manifest_bytes).await;
    write_pointer(
        &*store,
        &namespace_id,
        corrupt_manifest_object_id,
        corrupt_payload_checksum,
    )
    .await;

    assert_corrupt_index_error(
        "backfilling manifest missing checkpoint id",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown().await.expect("shutdown");
}

async fn write_manifest_without_checkpoint_id(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_bytes: &[u8],
) -> (GrepManifestObjectId, String) {
    let mut document: serde_json::Value =
        serde_json::from_slice(manifest_bytes).expect("decode valid manifest document");
    document["payload"]["status"]
        .as_object_mut()
        .expect("backfilling status is an object")
        .remove("checkpoint_id")
        .expect("valid backfilling manifest carries checkpoint id");
    let payload_bytes =
        serde_json::to_vec(&document["payload"]).expect("encode corrupt manifest payload");
    let payload_checksum = sha256_digest(&payload_bytes);
    document["payload_checksum"] = serde_json::Value::String(payload_checksum.clone());
    let manifest_object_id = GrepManifestObjectId::generate();
    store
        .put_overwrite(
            &manifest_key(namespace_id, &manifest_object_id),
            Bytes::from(serde_json::to_vec(&document).expect("encode corrupt manifest document")),
        )
        .await
        .expect("write manifest missing checkpoint id");
    (manifest_object_id, payload_checksum)
}

async fn write_pointer(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_object_id: GrepManifestObjectId,
    manifest_payload_checksum: String,
) {
    let envelope = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
        namespace_id.clone(),
        manifest_object_id,
        manifest_payload_checksum,
    ))
    .expect("build pointer");
    store
        .put_overwrite(
            &root_key(namespace_id),
            Bytes::from(encode_grep_root(&envelope).expect("encode pointer")),
        )
        .await
        .expect("write pointer");
}

#[tokio::test]
async fn planless_scan_covers_wal_revisions_at_or_below_index_watermark() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("scan-gap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("scan-gap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/only-in-wal.txt",
            b"x\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write WAL-only file");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let head = control::head(&store, &namespace_id).await;
    let metadata_root = control::metadata_root(&store, &namespace_id).await;
    assert!(
        metadata_root.manifest.manifest_head_seq < head.seq,
        "the WAL-only revision must sit past metadata materialization"
    );
    let grep_root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    assert_eq!(
        grep_root
            .manifest_state()
            .status()
            .active_watermark()
            .map(|resume| (resume.built_through_seq(), resume.next_event_index())),
        Some((head.seq, 0)),
        "the independent worker can advance past metadata materialization"
    );

    let mut scan = request("x");
    scan.allow_scan = true;
    let response = new_query(&store, &namespace_id, &scan)
        .await
        .expect("plan-less scan");
    assert_eq!(
        response.matches.len(),
        1,
        "scan must cover the WAL-only revision"
    );
    assert_eq!(response.matches[0].path.as_str(), "/only-in-wal.txt");

    writer.shutdown().await.expect("shutdown");
}

fn assert_not_enabled_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::NotEnabled) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `query.grep` is not enabled on this namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected not-enabled error for {case}, got {outcome:?}"),
    }
}

fn assert_backfilling_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::Backfilling) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `query.grep` is enabled but its backfill has not completed on this \
                 namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected backfilling error for {case}, got {outcome:?}"),
    }
}

fn assert_corrupt_index_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::CorruptIndex { .. }) => {
            assert_eq!(error.code(), ErrorCode::IndexCorrupt, "code for {case}");
            assert!(
                error
                    .to_string()
                    .contains("disable and re-enable grep to rebuild it"),
                "error text for {case}: {error}"
            );
        }
        outcome => panic!("expected corrupt-index error for {case}, got {outcome:?}"),
    }
}

#[tokio::test]
async fn grep_worker_pins_reorganized_tail_and_pagination_results() {
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
    let worker = worker(&store).await;
    let policy = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(2),
        max_mid_runs: nonzero_usize(2),
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
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write indexed file");
        worker
            .build_step(&namespace_id, policy)
            .await
            .expect("new build");
        worker
            .reorganize_step(&namespace_id, policy)
            .await
            .expect("new reorganization");
    }
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/tail.txt",
            b"tail-only needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write tail file");

    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    let levels: BTreeSet<u32> = root
        .manifest_state()
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
        assert!(found.path.starts_with("/docs/file-"));
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
    assert_eq!(tail.matches[0].path, "/tail.txt");
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

    let mut missing_scope = request("shared needle");
    missing_scope.path_prefix = Some(AbsolutePath::parse("/missing").expect("scope path"));
    let error = new_query(&store, &namespace_id, &missing_scope)
        .await
        .expect_err("a missing scope must remain a missing path");
    assert_eq!(error.code(), ErrorCode::PathNotFound);

    let mut page_request = request("shared needle");
    let mut found_matches = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    loop {
        let page = new_query_page(&store, &namespace_id, &page_request, page_limit(1))
            .await
            .expect("query page");
        assert_eq!(page.namespace_id, namespace_id);
        assert_eq!(page.matches.len(), 1);
        let found = &page.matches[0];
        assert!(found_matches.insert((found.path.clone(), found.line_number)));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(cursors.insert(cursor.clone()));
        page_request.cursor = Some(cursor);
    }
    assert_eq!(found_matches.len(), 12);
    assert_eq!(cursors.len(), 11);
    writer.shutdown().await.expect("shutdown");
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write source");
    let worker = worker(&store).await;
    worker.enable(&source).await.expect("enable source");
    drive_worker_to_current(&worker, &source, GramIndexBuildPolicy::default()).await;
    let source_root_before = load_grep_root(&*store, &source)
        .await
        .expect("load source root")
        .expect("source root exists")
        .manifest_state()
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
    assert_not_enabled_error(
        "fork target",
        new_query(&store, &target, &request("needle")).await,
    );

    // A fresh fork target has published no manifest of its own: its basis
    // is the source manifest its head authorizes.
    let target_basis = control::head(&store, &target)
        .await
        .fork_basis
        .expect("a fork target has a basis manifest");
    let manifest_key = metadata_manifest_object(
        &target_basis.manifest.owner_namespace_id,
        &target_basis.manifest.manifest_object_id,
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
        .manifest_state()
        .clone();
    assert_eq!(source_root_after, source_root_before);
    let source_response = new_query(&store, &source, &request("fork needle"))
        .await
        .expect("source query after fork");
    assert_eq!(source_response.matches.len(), 1);
    assert_eq!(source_response.matches[0].path, "/source.txt");
    writer.shutdown().await.expect("shutdown");
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
    let worker = worker(&store).await;
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
                    PutFileOptions::new(loonfs_test_support::test_actor()),
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
            max_files_per_step: nonzero_usize(2),
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
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_publication_in_flight_keeps_its_candidate_through_a_collection_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let base: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("manifest-race").expect("namespace id");
    let writer = FsWriter::builder_with_store(base.clone())
        .writer_id("manifest-race-writer")
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
            b"publication race needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write indexed file");
    let initial_worker = worker(&base).await;
    initial_worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(
        &initial_worker,
        &namespace_id,
        GramIndexBuildPolicy::default(),
    )
    .await;
    writer.shutdown().await.expect("shutdown writer");

    let current = load_grep_root(&*base, &namespace_id)
        .await
        .expect("load current root")
        .expect("root exists");
    let current_state = current.manifest_state();
    let next = GrepManifestState::new(
        namespace_id.clone(),
        current_state.status().clone(),
        GrepIndexState {
            reorganize: current_state.index().reorganize.clone(),
            next_run_no: RunNo(current_state.index().next_run_no.0 + 1),
        },
        current_state.segments().to_vec(),
    )
    .expect("valid successor state");
    let manifests_before = base
        .list_prefix(&manifests_prefix(&namespace_id))
        .await
        .expect("list manifests before the advance");
    // A clock in the store's own domain: the moment the state this
    // publication builds on became durable. Everything the publication
    // itself writes is stamped at or after it.
    let published_at_ms = base
        .head(&root_key(&namespace_id))
        .await
        .expect("head the installed pointer")
        .expect("an enabled namespace has a pointer")
        .last_modified_ms
        .expect("the local store stamps its objects");

    let blocking = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::exact(root_key(&namespace_id)),
        OperationClass::CompareAndSwap,
    ));
    blocking.block_next();
    let store: SharedObjectStore = blocking.clone();
    let advance = advance_grep_root(&*store, &current, &next);
    let collect = async {
        blocking.wait_until_blocked().await;
        let report = worker(&store)
            .await
            .garbage_collect_namespace(&namespace_id, published_at_ms, &GrepGcOptions::default())
            .await;
        blocking.release();
        report
    };
    let (advanced, report) = tokio::join!(advance, collect);

    let report = report.expect("collection pass runs mid-publication");
    assert_eq!(
        report.deleted_other_objects, 0,
        "nothing the publication wrote is old enough to collect"
    );
    assert!(
        report.retained_candidates >= 1,
        "the unreferenced candidate was examined and kept"
    );
    let advanced = advanced.expect("pointer advance completes");
    let candidate_key = manifest_key(&namespace_id, advanced.manifest_object_id());
    assert!(
        !manifests_before.contains(&candidate_key),
        "the candidate claimed an object no earlier publication wrote"
    );
    assert!(store
        .head(&candidate_key)
        .await
        .expect("head published manifest")
        .is_some());
    let response = new_query(&store, &namespace_id, &request("publication race needle"))
        .await
        .expect("query follows the published pointer");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].path, "/needle.txt");

    // And the retention above was the grace window, not an inert pass: past
    // it, the superseded manifest goes and the published one stays.
    let aged = worker(&store)
        .await
        .garbage_collect_namespace(
            &namespace_id,
            published_at_ms + GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions::default(),
        )
        .await
        .expect("collection pass after the grace window");
    assert!(aged.deleted_other_objects >= 1);
    assert!(store
        .head(&candidate_key)
        .await
        .expect("head published manifest")
        .is_some());
}

#[tokio::test]
async fn grep_gc_retains_live_roots_reaps_deleted_namespaces_and_never_crosses_keyspaces() {
    let temp_dir = tempdir().expect("tempdir");
    let aged_store = Arc::new(RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(temp_dir.path()).expect("local store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    ));
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
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    for namespace_id in [&live_namespace, &deleted_namespace, &corrupt_namespace] {
        worker.enable(namespace_id).await.expect("enable");
        drive_worker_to_current(&worker, namespace_id, GramIndexBuildPolicy::default()).await;
    }
    let live_root = load_grep_root(&*store, &live_namespace)
        .await
        .expect("load live root")
        .expect("root exists");
    let live_segment_key = segment_key(
        &live_namespace,
        &live_root.manifest_state().segments()[0].segment_id,
    );
    let live_manifest_key = manifest_key(&live_namespace, live_root.manifest_object_id());
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
        format!("namespaces/{live_namespace}/metadata/segments/grep-gc-sentinel.sst.zst");
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
        .list_prefix(&grep_prefix(&corrupt_namespace))
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
    let enable_error = worker
        .enable(&deleted_namespace)
        .await
        .expect_err("a deleted namespace cannot enable grep");
    assert_eq!(enable_error.code(), ErrorCode::NamespaceDeleted);
    let live_report = worker
        .garbage_collect_namespace(
            &live_namespace,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions::default(),
        )
        .await
        .expect("collect live namespace");
    let deleted_report = worker
        .garbage_collect_namespace(
            &deleted_namespace,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions::default(),
        )
        .await
        .expect("collect deleted namespace");
    let corrupt_report = worker
        .garbage_collect_namespace(
            &corrupt_namespace,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions::default(),
        )
        .await
        .expect("collect corrupt namespace");
    let absent_report = worker
        .garbage_collect_namespace(
            &absent_namespace,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions::default(),
        )
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
        .list_prefix(&grep_prefix(&deleted_namespace))
        .await
        .expect("list deleted grep prefix")
        .is_empty());
    assert!(store
        .list_prefix(&grep_prefix(&absent_namespace))
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
    aged_store.reset();
    admin
        .gc_namespace(&live_namespace, &GcConfig::default())
        .await
        .expect("core gc");
    let listed_prefixes: Vec<_> = aged_store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect();
    assert_eq!(
        listed_prefixes,
        vec![
            // Marking lists the records it roots from, then the manifests
            // it ages to find its reference manifest.
            checkpoint_prefix(&live_namespace),
            metadata_manifest_prefix(&live_namespace),
            wal_segment_prefix(&live_namespace),
            metadata_segment_prefix(&live_namespace),
            metadata_compaction_prefix(&live_namespace),
            metadata_manifest_prefix(&live_namespace),
            checkpoint_prefix(&live_namespace),
            upload_session_prefix(&live_namespace),
        ],
        "core GC must list only its own core prefixes"
    );
    assert!(
        store
            .head(&core_only_grep_key)
            .await
            .expect("head grep sentinel")
            .is_some(),
        "core GC must not learn grep keys"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Seeds one namespace with an index plus a fixed set of aged orphans, so
/// two stores can be built identically and collected differently.
async fn seed_collectable_namespace(root: &Path, namespace_id: &NamespaceId) -> SharedObjectStore {
    let store: SharedObjectStore = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(root).expect("local store"),
        KeyPredicate::any(),
    ));
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gc-budget-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..4u32 {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/file-{index}.txt"),
                format!("budget needle {index}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    worker.enable(namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, namespace_id, GramIndexBuildPolicy::default()).await;
    for orphan_key in orphan_keys(namespace_id) {
        store
            .put(
                &orphan_key,
                Bytes::from_static(b"orphan"),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write orphan");
    }
    writer.shutdown().await.expect("shutdown");
    store
}

/// Aged, unreferenced segment keys with fixed ids, so two independently
/// seeded stores hold the same collectable garbage under the same names —
/// the live segment and manifest object ids are minted and cannot match.
fn orphan_keys(namespace_id: &NamespaceId) -> Vec<String> {
    (0..6u8)
        .map(|index| {
            let orphan =
                IndexSegmentId::parse(format!("idx_{index:032x}")).expect("orphan segment id");
            segment_key(namespace_id, &orphan)
        })
        .collect()
}

#[tokio::test]
async fn a_budgeted_grep_collection_walks_everything_an_unbudgeted_one_does() {
    let namespace_id = NamespaceId::parse("gc-budget").expect("namespace id");
    let whole_dir = tempdir().expect("tempdir");
    let paged_dir = tempdir().expect("tempdir");
    let whole_store = seed_collectable_namespace(whole_dir.path(), &namespace_id).await;
    let paged_store = seed_collectable_namespace(paged_dir.path(), &namespace_id).await;
    let now_ms = GREP_GC_GRACE_WINDOW_MS + 1;
    let orphans = orphan_keys(&namespace_id);
    let whole_before = whole_store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("list whole-store keys");
    let paged_before = paged_store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("list paged-store keys");
    // The live segment and manifest object ids are minted and differ
    // between the two stores; what must match is how many keys there are
    // and which of them are collectable.
    assert_eq!(
        whole_before.len(),
        paged_before.len(),
        "the two stores must start alike for the comparison to mean anything"
    );
    for orphan in &orphans {
        assert!(whole_before.contains(orphan) && paged_before.contains(orphan));
    }

    let whole = worker(&whole_store)
        .await
        .garbage_collect_namespace(&namespace_id, now_ms, &GrepGcOptions::default())
        .await
        .expect("collect the whole prefix");
    assert_eq!(whole.next_cursor, None);
    assert!(whole.deleted_segments >= 6, "{whole:?}");

    let paged_worker = worker(&paged_store).await;
    let mut paged = GrepGcReport::default();
    let mut request = GrepGcOptions {
        max_objects: Some(1),
        cursor: None,
    };
    let mut passes = 0;
    loop {
        let pass = paged_worker
            .garbage_collect_namespace(&namespace_id, now_ms, &request)
            .await
            .expect("collect one page");
        passes += 1;
        assert!(passes < 256, "the cursor loop must terminate");
        paged.deleted_segments += pass.deleted_segments;
        paged.deleted_other_objects += pass.deleted_other_objects;
        paged.retained_candidates += pass.retained_candidates;
        paged.namespace_reaped |= pass.namespace_reaped;
        paged.namespace_degraded |= pass.namespace_degraded;
        let Some(next_cursor) = pass.next_cursor else {
            break;
        };
        request.cursor = Some(next_cursor);
    }
    assert!(
        passes > 1,
        "a one-read budget must stop the pass before the prefix ends"
    );

    assert_eq!(
        (
            paged.deleted_segments,
            paged.deleted_other_objects,
            paged.retained_candidates,
            paged.namespace_reaped,
            paged.namespace_degraded,
        ),
        (
            whole.deleted_segments,
            whole.deleted_other_objects,
            whole.retained_candidates,
            whole.namespace_reaped,
            whole.namespace_degraded,
        ),
        "resuming under a budget must decide exactly what one pass decides"
    );
    let whole_after = whole_store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("list surviving whole-store keys");
    let paged_after = paged_store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("list surviving paged-store keys");
    assert_eq!(
        whole_after.len(),
        paged_after.len(),
        "the two collections must leave the same amount behind"
    );
    for orphan in &orphans {
        assert!(
            !whole_after.contains(orphan) && !paged_after.contains(orphan),
            "both collections must reach `{orphan}`"
        );
    }
}

#[tokio::test]
async fn the_collection_budget_shares_reverification_across_a_candidate_chunk() {
    let namespace_id = NamespaceId::parse("gc-charge").expect("namespace id");
    let temp_dir = tempdir().expect("tempdir");
    let store = seed_collectable_namespace(temp_dir.path(), &namespace_id).await;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gc-charge-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .delete_namespace(&namespace_id, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    writer.shutdown().await.expect("shutdown");
    let keys_before = store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("list keys");
    assert!(keys_before.len() > 2, "{keys_before:?}");

    // Stop the pass before it can reclaim every key.
    const BUDGET: u64 = 7;
    let pass = worker(&store)
        .await
        .garbage_collect_namespace(
            &namespace_id,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions {
                max_objects: Some(BUDGET),
                cursor: None,
            },
        )
        .await
        .expect("collect under a seven-read budget");
    let reclaimed = pass.deleted_segments + pass.deleted_other_objects;
    assert!(
        reclaimed > BUDGET / 3,
        "one liveness read must authorize a chunk of candidate decisions: {pass:?}"
    );
    assert!(pass.next_cursor.is_some(), "{pass:?}");
    assert_eq!(
        store
            .list_prefix(&grep_prefix(&namespace_id))
            .await
            .expect("list keys after the bounded pass")
            .len(),
        keys_before.len() - usize::try_from(reclaimed).expect("a reclaimed count fits a usize"),
        "the pass deleted exactly the keys it reported"
    );
}

#[tokio::test]
async fn a_budget_too_small_for_one_key_still_advances_the_cursor() {
    let namespace_id = NamespaceId::parse("gc-progress").expect("namespace id");
    let temp_dir = tempdir().expect("tempdir");
    let store = seed_collectable_namespace(temp_dir.path(), &namespace_id).await;
    let worker = worker(&store).await;
    let first = worker
        .garbage_collect_namespace(
            &namespace_id,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions {
                max_objects: Some(1),
                cursor: None,
            },
        )
        .await
        .expect("collect under a one-read budget");
    let cursor = first.next_cursor.expect("a stopped pass resumes somewhere");
    let second = worker
        .garbage_collect_namespace(
            &namespace_id,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions {
                max_objects: Some(1),
                cursor: Some(cursor.clone()),
            },
        )
        .await
        .expect("collect the next page");
    assert_ne!(
        second.next_cursor.as_ref(),
        Some(&cursor),
        "a resumed pass must not hand back the position it was given"
    );
}

#[tokio::test]
async fn a_collection_cursor_is_refused_outside_the_namespace_that_minted_it() {
    let namespace_id = NamespaceId::parse("gc-cursor").expect("namespace id");
    let other_namespace = NamespaceId::parse("gc-cursor-other").expect("namespace id");
    let temp_dir = tempdir().expect("tempdir");
    let store = seed_collectable_namespace(temp_dir.path(), &namespace_id).await;
    let worker = worker(&store).await;
    let cursor = worker
        .garbage_collect_namespace(
            &namespace_id,
            GREP_GC_GRACE_WINDOW_MS + 1,
            &GrepGcOptions {
                max_objects: Some(1),
                cursor: None,
            },
        )
        .await
        .expect("collect one page")
        .next_cursor
        .expect("a stopped pass carries a resume cursor");

    for (namespace, token) in [
        (&other_namespace, cursor.clone()),
        (&namespace_id, "not-a-cursor".to_owned()),
    ] {
        let error = worker
            .garbage_collect_namespace(
                namespace,
                GREP_GC_GRACE_WINDOW_MS + 1,
                &GrepGcOptions {
                    max_objects: None,
                    cursor: Some(token),
                },
            )
            .await
            .expect_err("a foreign or malformed cursor is refused");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
}

#[tokio::test]
async fn a_backfilling_root_never_reports_a_built_through_sequence() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("enable-honesty").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("enable-honesty-writer")
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
            "/note.txt",
            b"honest needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    let worker = worker(&store).await;

    let loonfs_grep::GrepEnableOutcome::Enabled { state } =
        worker.enable(&namespace_id).await.expect("enable")
    else {
        panic!("a fresh enable publishes a backfill");
    };
    let backfilling = loonfs_api::v0::GrepIndexLifecycle::from(&state);
    assert_eq!(
        backfilling,
        loonfs_api::v0::GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(1),
            cursor_inode_id: None,
            checkpoint_id: match &state {
                GrepIndexStatus::Backfilling { checkpoint_id, .. } => checkpoint_id.clone(),
                other => panic!("expected a backfill: {other:?}"),
            },
        }
    );
    let rendered = serde_json::to_string(&backfilling).expect("serialize the reported lifecycle");
    assert!(
        !rendered.contains("built_through_seq"),
        "a backfill must not report a watermark anywhere: {rendered}"
    );
    assert_eq!(
        worker
            .lifecycle(&namespace_id)
            .await
            .expect("read lifecycle"),
        state,
        "the enable response reports the lifecycle it published"
    );

    // Re-enabling an active root reports the same phase, still without a
    // watermark.
    let loonfs_grep::GrepEnableOutcome::AlreadyEnabled { state: again } = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable")
    else {
        panic!("re-enabling an active root reports it as already enabled");
    };
    assert_eq!(again, state);

    // Once the walk finishes, the API reports the root as active with the
    // target it reached as its own watermark, and no target field survives.
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let active = loonfs_api::v0::GrepIndexLifecycle::from(
        &worker
            .lifecycle(&namespace_id)
            .await
            .expect("read completed lifecycle"),
    );
    assert_eq!(
        active,
        loonfs_api::v0::GrepIndexLifecycle::Active {
            built_through_seq: ChangeSeq(1),
            next_event_index: 0,
        }
    );
    assert!(!serde_json::to_string(&active)
        .expect("serialize the active API lifecycle")
        .contains("target_seq"));
    writer.shutdown().await.expect("shutdown");
}
