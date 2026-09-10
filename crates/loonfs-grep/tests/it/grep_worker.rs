#![allow(clippy::panic)]
// Lifecycle diagnostics deliberately panic with the full unexpected outcome.

//! GrepWorker lifecycle, rebootstrap, query contracts, and GC boundaries.

use crate::common::{control, default_page_limit, grep_with, page_limit, GrepHost};
use bytes::Bytes;
use loonfs::{
    CoreError, CreateNamespaceOptions, DeleteNamespaceOptions, ErrorCode, FsMaintenance, FsReader,
    FsWriter, MetadataMaintenanceOptions, NamespaceId, PutFileOptions, RuntimeError,
    SharedObjectStore,
};
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{
    sha256_digest, AbsolutePath, ChangeSeq, EffectiveLimit, GrepRequest, GrepResponse,
    IndexSegmentId, PageRequest, PaginationPolicy, RunNo, MAX_PUBLIC_INTEGER,
};
use loonfs_grep::keyspace::{
    grep_prefix, hint_key, manifest_key, manifests_prefix, segment_key, segments_prefix,
};
use loonfs_grep::manifest::{
    encode_grep_hint, load_current_grep_manifest, publish_grep_manifest, GrepHint, GrepIndexState,
    GrepIndexStatus, GrepManifestState,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepService,
    GrepWorker, GREP_GC_GRACE_WINDOW_MS,
};
use loonfs_objectstore::keys::{checkpoint_record, metadata_manifest_object};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::nonzero_usize;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    OperationContext, OperationKind, RecordedOperation, RecordingStore,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
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
        &GrepService::default(),
        &reader,
        store,
        namespace_id,
        grep_request,
        limit,
    )
    .await
}

async fn flush_wal_and_advance_retention(
    maintenance: &FsMaintenance,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    maintenance
        .maintain_metadata(
            namespace_id,
            MetadataMaintenanceOptions {
                max_wal_tail_segments: std::num::NonZeroU64::MIN,
                ..Default::default()
            },
        )
        .await
        .expect("flush wal");
    maintenance
        .advance_retention_floor(namespace_id)
        .await
        .expect("advance retention")
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
        KeyPredicate::exact(hint_key(&namespace_id)),
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
    let manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load manifest")
        .expect("manifest exists");
    let GrepIndexStatus::Backfilling { checkpoint_id, .. } = manifest.manifest_state().status()
    else {
        panic!(
            "enable must publish checkpointed backfill: {:?}",
            manifest.manifest_state()
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
    assert!(
        control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
            .await
            .is_none()
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("materialized query");
    assert_eq!(response.matches.len(), 3);
    let materialized_manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load materialized manifest")
        .expect("manifest exists");
    let materialized_segment = segment_key(
        &namespace_id,
        &materialized_manifest.manifest_state().segments()[0].segment_id,
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
    let disabled_manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load disabled manifest")
        .expect("disabled manifest remains");
    assert!(matches!(
        disabled_manifest.manifest_state().status(),
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
    let manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load re-enabled manifest")
        .expect("manifest exists");
    assert!(manifest.manifest_state().segments().is_empty());
    assert!(matches!(
        manifest.manifest_state().status(),
        GrepIndexStatus::Backfilling { .. }
    ));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exhausted_run_numbers_fail_as_server_errors_without_writing_the_manifest() {
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

    let current = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load current manifest")
        .expect("current manifest");
    let current_state = current.manifest_state();
    let maximum_state = GrepManifestState::new(
        namespace_id.clone(),
        current.manifest_no().successor().expect("next number"),
        current_state.status().clone(),
        GrepIndexState {
            reorganize: current_state.index().reorganize.clone(),
            next_run_no: RunNo(MAX_PUBLIC_INTEGER),
        },
        current_state.segments().to_vec(),
    )
    .expect("valid manifest at the public maximum");
    let timer = loonfs_objectstore::timing::StdMonotonicTimer::default();
    let maximum = publish_grep_manifest(&*store, Some(&current), &maximum_state, &timer, 0)
        .await
        .expect("install manifest at the public maximum");

    writer
        .put_file_bytes(
            &namespace_id,
            "/incremental.txt",
            b"incremental run number boundary needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write incremental file");
    let hint_before = store
        .get(&hint_key(&namespace_id), None)
        .await
        .expect("read hint before failure")
        .expect("hint exists");
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

    let after = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load manifest after failures")
        .expect("manifest remains");
    assert_eq!(after.manifest_no(), maximum.manifest_no());
    assert_eq!(
        after.manifest_state().index().next_run_no,
        RunNo(MAX_PUBLIC_INTEGER)
    );
    assert_eq!(
        store
            .get(&hint_key(&namespace_id), None)
            .await
            .expect("read hint after failure")
            .expect("hint remains"),
        hint_before
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
async fn enable_creates_no_checkpoint_when_the_manifest_load_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("enable-manifest-failure").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("enable-manifest-failure-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "enable-manifest-failure-maintenance").await;
    let grep_hint_key = hint_key(&namespace_id);
    let manifest_loads = Arc::new(AtomicUsize::new(0));
    let observed_manifest_loads = Arc::clone(&manifest_loads);
    let failing_store = Arc::new(FailStore::matching(
        store.clone(),
        move |context: &OperationContext<'_>| {
            context.key() == grep_hint_key
                && matches!(context.kind(), OperationKind::GetWithMetadata)
                && observed_manifest_loads.fetch_add(1, Ordering::SeqCst) == 0
        },
        InjectedError::Transport("injected grep-manifest reload failure".to_owned()),
    ));
    failing_store.fail_all();
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.maintenance.clone(),
        Arc::clone(&host.block_cache),
    );

    let error = worker
        .enable(&namespace_id)
        .await
        .expect_err("the manifest load fails before checkpoint creation");
    assert!(matches!(error, GrepError::StoreUnavailable { .. }));
    assert_eq!(failing_store.attempts(), 1);

    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = host
        .maintenance
        .list_checkpoints_pager(&namespace_id, request);
    let checkpoints = pager
        .next()
        .await
        .expect("a fresh pager has one page")
        .expect("list checkpoints after failed enable");
    assert!(
        checkpoints.checkpoints.is_empty(),
        "a failure before manifest publication must not leave an active checkpoint"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enable_retains_its_checkpoint_when_the_manifest_write_result_is_ambiguous() {
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
    let host = GrepHost::new(&store, "ambiguous-enable-maintenance").await;
    let grep_manifest_key = manifest_key(&namespace_id, &loonfs_api::ManifestNo(1));
    let failing_store = Arc::new(
        FailStore::matching(
            store.clone(),
            move |context: &OperationContext<'_>| {
                context.key() == grep_manifest_key
                    && matches!(
                        context.kind(),
                        OperationKind::Put {
                            mode: PutMode::CreateIfAbsent,
                            ..
                        }
                    )
            },
            InjectedError::Transport("injected failure after manifest publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.maintenance.clone(),
        Arc::clone(&host.block_cache),
    );

    let error = worker
        .enable(&namespace_id)
        .await
        .expect_err("manifest publication acknowledgement fails");
    assert!(matches!(error, GrepError::StoreUnavailable { .. }));
    assert_eq!(failing_store.attempts(), 1);

    assert_fresh_backfill_attempt(&store, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restart_retains_its_checkpoint_when_the_manifest_write_result_is_ambiguous() {
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
    let host = GrepHost::new(&store, "ambiguous-restart-maintenance").await;
    host.worker
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    let previous_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    host.maintenance
        .release_checkpoint(&namespace_id, &previous_checkpoint_id)
        .await
        .expect("make the current backfill restart");

    let grep_manifest_key = manifest_key(&namespace_id, &loonfs_api::ManifestNo(2));
    let failing_store = Arc::new(
        FailStore::matching(
            store.clone(),
            move |context: &OperationContext<'_>| {
                context.key() == grep_manifest_key
                    && matches!(
                        context.kind(),
                        OperationKind::Put {
                            mode: PutMode::CreateIfAbsent,
                            ..
                        }
                    )
            },
            InjectedError::Transport("injected failure after manifest publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let worker = GrepWorker::with_block_cache(
        failing_store.clone(),
        host.reader.clone(),
        host.maintenance.clone(),
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
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("gap-maintenance")
        .build()
        .await
        .expect("maintenance");
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
    let retention_floor_seq = flush_wal_and_advance_retention(&maintenance, &namespace_id).await;
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
    maintenance
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
    // A deleted record is finished for good, so the new attempt takes a
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
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("handoff-gap-maintenance")
        .build()
        .await
        .expect("maintenance");
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
    let retention_floor_seq = flush_wal_and_advance_retention(&maintenance, &namespace_id).await;
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
async fn an_expired_backfill_pin_keeps_enumerating_until_deleted() {
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
    let expired = record;
    store
        .put_overwrite(
            &key,
            Bytes::from(
                loonfs_api::wire::control::encode_control_state(
                    loonfs_api::wire::control::ControlObjectKind::CheckpointRecord,
                    &expired,
                )
                .expect("encode record"),
            ),
        )
        .await
        .expect("write the expired record");

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    assert!(
        control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
            .await
            .is_none()
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after a backfill on an expired pin");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown().await.expect("shutdown");
}

/// Asserts the namespace's grep manifest is at the start of a backfill attempt
/// — nothing indexed, nothing walked, an active checkpoint pinning its basis
/// — and returns the checkpoint that attempt holds.
async fn assert_fresh_backfill_attempt(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> loonfs_api::CheckpointId {
    let manifest = load_current_grep_manifest(&**store, namespace_id)
        .await
        .expect("load grep manifest")
        .expect("grep manifest exists");
    let GrepIndexStatus::Backfilling {
        cursor_inode_id,
        checkpoint_id,
        ..
    } = manifest.manifest_state().status()
    else {
        panic!(
            "expected a checkpointed backfill: {:?}",
            manifest.manifest_state()
        );
    };
    assert_eq!(
        *cursor_inode_id, None,
        "a fresh backfill starts the walk from the beginning"
    );
    assert!(
        manifest.manifest_state().segments().is_empty(),
        "a rebootstrap discards the incomplete projection"
    );
    assert!(
        control::checkpoint_record(store, namespace_id, checkpoint_id)
            .await
            .is_some()
    );
    checkpoint_id.clone()
}

/// Every grep segment the namespace's manifest names, for asserting that an
/// event changed no postings.
async fn grep_segment_ids(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<IndexSegmentId> {
    load_current_grep_manifest(&**store, namespace_id)
        .await
        .expect("load grep manifest")
        .expect("grep manifest exists")
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
    load_current_grep_manifest(&**store, namespace_id)
        .await
        .expect("load grep manifest")
        .expect("grep manifest exists")
        .manifest_state()
        .status()
        .active_watermark()
        .expect("an active grep manifest has a watermark")
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
            &hint_key(&namespace_id),
            Bytes::from_static(b"corrupt hint"),
        )
        .await
        .expect("poison the grep manifest");

    let (build, commit) = tokio::join!(
        worker.build_step(&namespace_id, GramIndexBuildPolicy::default()),
        writer.put_file_bytes(
            &namespace_id,
            "/during-failure.txt",
            b"isolated needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        ),
    );
    let error = build.expect_err("an unreadable grep manifest fails the step");
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
async fn grep_manifest_lifecycle_pins_not_materialized_error_surface() {
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

    let missing_manifest_no = loonfs_api::ManifestNo(20);
    write_hint(&*store, &namespace_id, missing_manifest_no).await;
    assert_corrupt_index_error(
        "missing manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    let corrupt_manifest_no = loonfs_api::ManifestNo(21);
    store
        .put_overwrite(
            &manifest_key(&namespace_id, &corrupt_manifest_no),
            Bytes::from_static(b"corrupt manifest"),
        )
        .await
        .expect("write corrupt manifest");
    write_hint(&*store, &namespace_id, corrupt_manifest_no).await;
    assert_corrupt_index_error(
        "corrupt manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    store
        .put_overwrite(
            &hint_key(&namespace_id),
            Bytes::from_static(b"corrupt hint"),
        )
        .await
        .expect("write corrupt hint");
    assert_corrupt_index_error(
        "corrupt hint",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn backfilling_manifest_without_checkpoint_id_is_index_corrupt() {
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
    let manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load backfilling manifest")
        .expect("backfilling manifest exists");
    let manifest_bytes = store
        .get(&manifest_key(&namespace_id, &manifest.manifest_no()), None)
        .await
        .expect("read backfilling manifest")
        .expect("backfilling manifest exists");
    let corrupt_manifest_no =
        write_manifest_without_checkpoint_id(&*store, &namespace_id, &manifest_bytes).await;
    write_hint(&*store, &namespace_id, corrupt_manifest_no).await;

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
) -> loonfs_api::ManifestNo {
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
    let manifest_no = loonfs_api::ManifestNo(22);
    document["payload"]["manifest_no"] = serde_json::json!(manifest_no);
    let payload = serde_json::to_vec(&document["payload"]).expect("payload");
    document["payload_checksum"] = serde_json::json!(sha256_digest(&payload));
    store
        .put_overwrite(
            &manifest_key(namespace_id, &manifest_no),
            Bytes::from(serde_json::to_vec(&document).expect("encode corrupt manifest document")),
        )
        .await
        .expect("write manifest missing checkpoint id");
    manifest_no
}

async fn write_hint(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_api::ManifestNo,
) {
    let envelope = encode_grep_hint(GrepHint {
        namespace_id: namespace_id.clone(),
        manifest_no,
    })
    .expect("build hint")
    .into_envelope();
    store
        .put_overwrite(
            &hint_key(namespace_id),
            Bytes::from(
                encode_grep_hint(envelope.payload().clone())
                    .expect("encode hint")
                    .into_bytes(),
            ),
        )
        .await
        .expect("write hint");
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
    let metadata_manifest = control::metadata_manifest(&store, &namespace_id).await;
    assert!(
        metadata_manifest.manifest.manifest_head_seq < head.seq,
        "the WAL-only revision must sit past metadata materialization"
    );
    let grep_manifest = loonfs_grep::manifest::load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load grep manifest")
        .expect("grep manifest exists");
    assert_eq!(
        grep_manifest
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

    let manifest = load_current_grep_manifest(&*store, &namespace_id)
        .await
        .expect("load manifest")
        .expect("manifest exists");
    let levels: BTreeSet<u32> = manifest
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
    let source_manifest_before = load_current_grep_manifest(&*store, &source)
        .await
        .expect("load source manifest")
        .expect("source manifest exists")
        .manifest_state()
        .clone();

    writer
        .fork_namespace(&source, &target)
        .await
        .expect("fork source");

    assert!(
        load_current_grep_manifest(&*store, &target)
            .await
            .expect("load target manifest")
            .is_none(),
        "fork target must have no grep manifest until explicitly enabled"
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
        &target_basis.manifest.manifest_no,
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

    let source_manifest_after = load_current_grep_manifest(&*store, &source)
        .await
        .expect("reload source manifest")
        .expect("source manifest still exists")
        .manifest_state()
        .clone();
    assert_eq!(source_manifest_after, source_manifest_before);
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
async fn a_backfilling_manifest_never_reports_a_built_through_sequence() {
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

    // Re-enabling an active manifest reports the same phase, still without a
    // watermark.
    let loonfs_grep::GrepEnableOutcome::AlreadyEnabled { state: again } = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable")
    else {
        panic!("re-enabling an active manifest reports it as already enabled");
    };
    assert_eq!(again, state);

    // Once the walk finishes, the API reports the manifest as active with the
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

#[tokio::test]
async fn enable_disable_and_cached_queries_use_numbered_publication() {
    use loonfs_api::ManifestNo;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("numbered-query").expect("namespace");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(grep_prefix(&namespace_id)),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("numbered-grep-tests")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("namespace");
    let host = GrepHost::new(&store, "numbered-query").await;
    host.worker.enable(&namespace_id).await.expect("enable");
    let writes: Vec<_> = recording
        .take()
        .into_iter()
        .filter(|operation| matches!(operation, RecordedOperation::Put { .. }))
        .collect();
    assert_eq!(writes.len(), 2);
    for (operation, expected_key) in writes.iter().zip([
        hint_key(&namespace_id),
        manifest_key(&namespace_id, &ManifestNo(1)),
    ]) {
        assert!(
            matches!(operation, RecordedOperation::Put { key, mode: PutMode::CreateIfAbsent, .. } if key == &expected_key)
        );
    }
    host.worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("empty backfill");
    let grep_request = request("needle");
    grep_with(
        &host.service,
        &host.reader,
        &store,
        &namespace_id,
        &grep_request,
        default_page_limit(),
    )
    .await
    .expect("cold query");
    recording.reset();
    grep_with(
        &host.service,
        &host.reader,
        &store,
        &namespace_id,
        &grep_request,
        default_page_limit(),
    )
    .await
    .expect("warm query");
    assert_eq!(
        recording.take(),
        vec![RecordedOperation::Head {
            key: manifest_key(&namespace_id, &ManifestNo(3))
        }]
    );
    host.worker.disable(&namespace_id).await.expect("disable");
    assert_eq!(
        load_current_grep_manifest(&*store, &namespace_id)
            .await
            .expect("discover")
            .expect("manifest")
            .manifest_no(),
        ManifestNo(3)
    );
    assert!(matches!(
        grep_with(
            &host.service,
            &host.reader,
            &store,
            &namespace_id,
            &grep_request,
            default_page_limit()
        )
        .await,
        Err(GrepError::NotEnabled)
    ));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn gc_preserves_discovery_and_applies_successor_and_segment_age_rules() {
    use loonfs::UNREFERENCED_SEGMENT_MIN_AGE_MS;
    use loonfs_api::ManifestNo;
    use loonfs_grep::manifest::encode_grep_manifest;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("numbered-gc").expect("namespace");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let aged = MetadataMapStore::aged(
        base.clone(),
        KeyPredicate::prefix(grep_prefix(&namespace_id)),
    );
    let young_successor = MetadataMapStore::new(
        aged,
        KeyPredicate::exact(manifest_key(&namespace_id, &ManifestNo(2))),
        |mut metadata| {
            metadata.last_modified_ms =
                Some(UNREFERENCED_SEGMENT_MIN_AGE_MS - GREP_GC_GRACE_WINDOW_MS + 1);
            metadata
        },
    );
    let recording = Arc::new(RecordingStore::new(young_successor, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("numbered-grep-tests")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("namespace");
    let obsolete = crate::golden_formats::segment_ref(1, 1, 0, 0);
    let live = crate::golden_formats::segment_ref(2, 2, 0, 0);
    for segment in [&obsolete, &live] {
        store
            .put_if_absent(
                &segment_key(&namespace_id, &segment.segment_id),
                Bytes::from_static(b"segment"),
            )
            .await
            .expect("segment");
    }
    for number in 1..=4 {
        let segments = if number <= 2 {
            vec![obsolete.clone()]
        } else {
            vec![live.clone()]
        };
        let state = GrepManifestState::new(
            namespace_id.clone(),
            ManifestNo(number),
            GrepIndexStatus::Active {
                built_through_seq: ChangeSeq(0),
                next_event_index: 0,
            },
            GrepIndexState {
                reorganize: None,
                next_run_no: RunNo(3),
            },
            segments,
        )
        .expect("manifest state");
        store
            .put_if_absent(
                &manifest_key(&namespace_id, &ManifestNo(number)),
                Bytes::from(
                    encode_grep_manifest(state)
                        .expect("encode manifest")
                        .into_bytes(),
                ),
            )
            .await
            .expect("manifest");
    }
    write_hint(&*store, &namespace_id, ManifestNo(2)).await;
    let collector = worker(&store).await;
    recording.reset();
    let young = collector
        .garbage_collect_namespace(&namespace_id, UNREFERENCED_SEGMENT_MIN_AGE_MS)
        .await
        .expect("young pass");
    assert_eq!(
        (young.deleted_segments, young.deleted_other_objects),
        (0, 0)
    );
    assert_eq!(recording.counts().deletes, 0);
    let aged = collector
        .garbage_collect_namespace(&namespace_id, UNREFERENCED_SEGMENT_MIN_AGE_MS + 1)
        .await
        .expect("aged pass");
    assert_eq!((aged.deleted_segments, aged.deleted_other_objects), (1, 1));
    assert!(store
        .head(&manifest_key(&namespace_id, &ManifestNo(1)))
        .await
        .expect("head")
        .is_none());
    for number in 2..=4 {
        assert!(store
            .head(&manifest_key(&namespace_id, &ManifestNo(number)))
            .await
            .expect("head")
            .is_some());
    }
    assert!(store
        .head(&segment_key(&namespace_id, &live.segment_id))
        .await
        .expect("head")
        .is_some());
    let failing = Arc::new(FailStore::new(
        store.clone(),
        KeyPredicate::exact(hint_key(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("unreadable hint".to_owned()),
    ));
    failing.fail_all();
    let failing_store: SharedObjectStore = failing;
    recording.reset();
    assert!(matches!(
        worker(&failing_store)
            .await
            .garbage_collect_namespace(&namespace_id, u64::MAX)
            .await,
        Err(GrepError::StoreUnavailable { .. })
    ));
    assert_eq!(recording.counts().deletes, 0);
    let unknown = format!("{}unrecognized", segments_prefix(&namespace_id));
    store
        .put_if_absent(&unknown, Bytes::from_static(b"unknown"))
        .await
        .expect("unknown key");
    store
        .put_overwrite(
            &hint_key(&namespace_id),
            Bytes::from_static(b"invalid hint"),
        )
        .await
        .expect("corrupt hint");
    recording.reset();
    assert!(matches!(
        collector
            .garbage_collect_namespace(&namespace_id, u64::MAX)
            .await,
        Err(GrepError::CorruptIndex { .. })
    ));
    assert_eq!(recording.counts().deletes, 0);
    writer
        .delete_namespace(&namespace_id, DeleteNamespaceOptions::default())
        .await
        .expect("tombstone");
    let core_keys: Vec<_> = store
        .list_prefix(&format!("namespaces/{namespace_id}/"))
        .await
        .expect("core keys")
        .into_iter()
        .filter(|key| !key.starts_with(&grep_prefix(&namespace_id)))
        .collect();
    let reaped = collector
        .garbage_collect_namespace(&namespace_id, u64::MAX)
        .await
        .expect("reap tombstone");
    assert!(reaped.namespace_reaped);
    assert!(store
        .list_prefix(&grep_prefix(&namespace_id))
        .await
        .expect("grep keys")
        .is_empty());
    assert_eq!(
        store
            .list_prefix(&format!("namespaces/{namespace_id}/"))
            .await
            .expect("core keys"),
        core_keys
    );
    let absent = NamespaceId::parse("absent-gc").expect("namespace");
    store
        .put_if_absent(
            &format!("{}leftover", grep_prefix(&absent)),
            Bytes::from_static(b"leftover"),
        )
        .await
        .expect("absent extension");
    assert!(
        worker(&store)
            .await
            .garbage_collect_namespace(&absent, u64::MAX)
            .await
            .expect("reap absent")
            .namespace_reaped
    );
    writer.shutdown().await.expect("shutdown");
}
