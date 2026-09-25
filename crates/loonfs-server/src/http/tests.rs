//! Host configuration, operational routes, and shutdown contracts.

#![allow(clippy::panic)]

mod filesystem_app;

use super::serve::{build_handles, serve_on};
use super::{app, AppOptions, AppState};
use crate::config::RuntimeCacheConfigOverrides;
use crate::{ServerConfig, StoreConfig};
use async_trait::async_trait;
use axum::http::StatusCode;
use loonfs::{
    CreateNamespaceOptions, FsMaintenance, FsReader, FsWriter, MaintenanceCancellation,
    MaintenanceConclusion, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceRunReport, PutFileOptions, SharedObjectStore, StoredMetadataBlockCache, TraceMode,
    TraceStoreKind,
};
use loonfs_api::{
    CapabilityDocument, ChangeSeq, GrepRequest, NamespaceId, PaginationPolicy,
    API_GROUP_FILESYSTEM_V0, API_GROUP_QUERY_V0,
};
use loonfs_client::{Client, ClientConfig};
use loonfs_grep::keyspace::hint_key as grep_hint_key;
use loonfs_grep::manifest::load_current_grep_manifest;
use loonfs_grep::{GrepWorker, NamespaceReads};
use loonfs_http::HttpMetrics;
use loonfs_objectstore::{local_fs_store::LocalFsStore, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, OperationContext, OperationKind,
};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn build_handles_installs_jsonl_object_store_metrics_recorder() {
    let store_dir = tempdir().expect("store tempdir");
    let metrics_dir = tempdir().expect("metrics tempdir");
    let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(store_dir.path(), "server-writer");
    let metrics_path = metrics_dir.path().join("object-store.ndjson");

    {
        let (writer, _reader, _maintenance) = build_handles(
            &config,
            store,
            &HttpMetrics::new(),
            Some(metrics_path.clone().into_os_string()),
            None,
            None,
        )
        .await
        .expect("build handles");
        writer
            .create_namespace(
                &namespace_id("metrics"),
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create namespace");
    }

    let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
    assert!(!jsonl.is_empty());
    assert!(!jsonl.contains("namespaces/metrics"));
}

#[tokio::test]
async fn app_validates_directly_built_configs() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(temp_dir.path(), "app-validate-writer");
    config.max_concurrent_uploads = 0;
    match app(config, AppOptions::default()).await {
        Err(crate::config::ServerConfigError::InvalidField { field, .. }) => {
            assert_eq!(field, "max_concurrent_uploads");
        }
        Err(other) => panic!("expected invalid field error, got {other:?}"),
        Ok(_) => panic!("app must reject a zero upload bound"),
    }
}

#[tokio::test]
async fn a_server_without_the_table_builds_no_local_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "no-local-cache-writer");

    let (_router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    assert!(state.local_cache.is_none());
    let rendered = super::metrics::render(&state.binding.metrics, None, 0, 0);
    assert!(!rendered.contains("loonfs_local_cache_"));
}

#[tokio::test]
async fn runtime_and_grep_cache_metrics_render_from_the_recorder() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "cache-metrics-writer");

    let (_router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    let rendered = super::metrics::render(&state.binding.metrics, None, 0, 0);

    for name in [
        "loonfs_runtime_cache_latest_metadata_view_reads_total",
        "loonfs_runtime_cache_snapshot_view_reads_total",
        "loonfs_metadata_segment_cache_gets_total",
        "loonfs_metadata_segment_cache_inserts_total",
        "loonfs_metadata_segment_cache_evictions_total",
        "loonfs_metadata_segment_cache_filter_skips_total",
        "loonfs_metadata_segment_cache_filter_false_positives_total",
        "loonfs_wal_tail_projection_cache_gets_total",
        "loonfs_wal_tail_projection_cache_inserts_total",
        "loonfs_wal_tail_projection_cache_evictions_total",
        "loonfs_wal_tail_projection_cache_evicted_rows_total",
        "loonfs_wal_tail_projection_cache_evicted_decoded_bytes_total",
        "loonfs_wal_tail_projection_cache_rejections_total",
        "loonfs_wal_tail_projection_cache_rejected_rows_total",
        "loonfs_wal_tail_projection_cache_rejected_decoded_bytes_total",
        "loonfs_grep_block_cache_gets_total",
        "loonfs_grep_block_cache_inserts_total",
        "loonfs_grep_block_cache_evictions_total",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {name} counter\n")),
            "missing counter `{name}`"
        );
    }
    for name in [
        "loonfs_wal_tail_projection_cache_retained_rows",
        "loonfs_wal_tail_projection_cache_retained_decoded_bytes",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {name} gauge\n")),
            "missing gauge `{name}`"
        );
    }
    assert!(!rendered.contains("loonfs_cache_metadata_segment_cache_hits"));
}

#[tokio::test]
async fn a_configured_local_cache_is_built_and_scraped() {
    let temp_dir = tempdir().expect("tempdir");
    let cache_dir = tempdir().expect("cache tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "local-cache-writer");
    config.local_cache = Some(test_local_cache_config(cache_dir.path()));

    let (_router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    let local_cache = state.local_cache.clone().expect("a local cache");
    let rendered = super::metrics::render(
        &state.binding.metrics,
        Some(local_cache.foyer_stats()),
        0,
        0,
    );
    assert!(rendered.contains("loonfs_local_cache_memory_capacity_bytes 4194304\n"));
    assert!(rendered.contains("loonfs_local_cache_disk_capacity_bytes 134217728\n"));
    assert!(rendered.contains("loonfs_local_cache_queue_buffer_overflows 0\n"));
    assert!(rendered.contains("loonfs_local_cache_queue_channel_overflows 0\n"));

    local_cache.close().await.expect("close local cache");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_closes_the_local_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let cache_dir = tempdir().expect("cache tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "shutdown-local-cache-writer");
    config.local_cache = Some(test_local_cache_config(cache_dir.path()));
    let shutdown_deadline_ms = config.shutdown_deadline_ms;

    let (router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    let local_cache = state.local_cache.clone().expect("a local cache");
    assert!(!local_cache.is_closed());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        state.writer.clone(),
        state.runner.clone(),
        state.local_cache.clone(),
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    assert!(
        local_cache.is_closed(),
        "the graceful path closes the cache before it returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
// The sleeping handler keeps one connection in flight past the drain budget.
async fn graceful_shutdown_abandons_requests_at_the_deadline_and_settles_the_writer() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = test_runtime(store, "shutdown-deadline-writer").await;
    let handler_started = Arc::new(tokio::sync::Notify::new());
    let handler_signal = handler_started.clone();
    let router = axum::Router::new().route(
        "/slow",
        axum::routing::get(move || {
            let handler_signal = handler_signal.clone();
            async move {
                handler_signal.notify_one();
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                "late"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown_deadline_ms = 25;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        writer.clone(),
        None,
        None,
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let client = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect client");
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("send slow request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read slow response");
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        handler_started.notified(),
    )
    .await
    .expect("slow handler starts");

    shutdown_tx.send(()).expect("trigger shutdown");
    tokio::time::timeout(
        std::time::Duration::from_millis(shutdown_deadline_ms + 1_000),
        server,
    )
    .await
    .expect("serve_and_settle returns within the drain deadline plus slack")
    .expect("join server task")
    .expect("shutdown settles the writer");
    assert!(writer.is_shutting_down(), "writer shutdown completed");

    client.abort();
}

fn test_local_cache_config(root: &Path) -> crate::config::LocalCacheConfig {
    crate::config::LocalCacheConfig {
        path: root.display().to_string(),
        memory_bytes: 4 * 1024 * 1024,
        disk_bytes: 128 * 1024 * 1024,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_requests_and_settles_the_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(temp_dir.path(), "shutdown-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_on(listener, config, async move {
        let _ = shutdown_rx.await;
    }));

    // The server accepts work while running.
    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    client
        .create_namespace(
            &namespace_id("demo"),
            &loonfs_test_support::test_actor(),
            loonfs_api::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace over http");

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    // The listener is closed once serve returns.
    assert!(
        std::net::TcpStream::connect(addr).is_err(),
        "listener should refuse connections after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_runner_shutdown_drains_an_active_grep_step() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-shutdown");
    let blocking_store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::exact(grep_hint_key(&namespace_id)),
        OperationClass::GetWithMetadata,
    ));
    let store = blocking_store.clone() as SharedObjectStore;
    let writer = test_runtime(store.clone(), "grep-shutdown-seed").await;
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    grep_worker(&store, "grep-shutdown-enable")
        .await
        .enable(&namespace_id)
        .await
        .expect("enable grep");

    blocking_store.block_next();
    let config = test_config(temp_dir.path(), "grep-shutdown-server");
    let (_router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    state
        .binding
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    blocking_store.wait_until_blocked().await;

    let shutdown = tokio::runtime::Handle::current().spawn({
        let runner = state.runner.clone().expect("automatic runner");
        async move { runner.shutdown().await }
    });
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the active bounded grep step"
    );
    blocking_store.release();
    shutdown
        .await
        .expect("join shutdown")
        .expect("drain grep step");
    state.writer.shutdown().await.expect("shutdown writer");
}

/// A job that does nothing but count the steps the runner admitted for it.
///
/// It is registered on the server's own writer, so it queues, waits for a
/// permit, and is shut down through exactly the admission every other job
/// goes through. Counting is the whole point: an ordinary step's work is
/// object-store traffic, and the question this test asks is whether any of
/// it is issued at all once a shutdown has begun.
struct StepCountingJob {
    id: MaintenanceJobId,
    steps: Arc<AtomicUsize>,
}

#[async_trait]
impl MaintenanceJob for StepCountingJob {
    fn id(&self) -> MaintenanceJobId {
        self.id
    }

    async fn run(
        &self,
        _namespace_id: &NamespaceId,
        _cancellation: &MaintenanceCancellation,
    ) -> loonfs::Result<MaintenanceRunReport> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        // Idle rather than progressed: a requeueing step would never let
        // the control settle below.
        Ok(MaintenanceRunReport::concluded(MaintenanceConclusion::Idle))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> loonfs::Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

#[tokio::test]
async fn shutdown_closes_maintenance_admission_before_draining_publications() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("shutdown-order");
    let blocking = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        data_wal_put_for(&namespace_id),
    ));
    let config = test_config(temp_dir.path(), "shutdown-order-server");
    let (_router, state) = app(
        config,
        options_with_store(blocking.clone() as SharedObjectStore),
    )
    .await
    .expect("build app");
    state
        .writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");

    let steps = Arc::new(AtomicUsize::new(0));
    let job = MaintenanceJobId::new("shutdown-order-probe");
    state
        .jobs
        .register(Arc::new(StepCountingJob {
            id: job,
            steps: Arc::clone(&steps),
        }))
        .expect("register the counting job");

    // The control. Without it, a later count of zero would prove only that
    // this job never ran under any conditions.
    let runner = state.runner.clone().expect("automatic runner");
    runner.handle().nudge(job, &namespace_id);
    runner.drain().await.expect("settle the admitted run");
    let admitted_while_serving = steps.load(Ordering::SeqCst);
    assert_eq!(
        admitted_while_serving, 1,
        "a nudge on a serving deployment admits one step"
    );

    // Park a publication so the shutdown's publication drain is still
    // pending when its first poll returns — the window the runner would
    // otherwise keep admitting into.
    blocking.block_next();
    let put = tokio::spawn({
        let writer = state.writer.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer
                .put_file_bytes(
                    &namespace_id,
                    "/parked.txt",
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
        }
    });
    blocking.wait_until_blocked().await;

    state.writer.close_admission_for_shutdown();
    runner.close_admission();
    let mut shutdown = Box::pin(state.writer.shutdown());
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "the parked publication must keep the shutdown pending"
    );
    // Everything after this point is the drain window.
    runner.handle().nudge(job, &namespace_id);

    blocking.release();
    put.await
        .expect("join the parked put")
        .expect("the released put succeeds");
    // Releasing the put also lets it publish, which fires the publish
    // observer's own nudge — the production path into this same window.
    shutdown
        .await
        .expect("the shutdown settles with its queue discarded");
    runner.shutdown().await.expect("runner shutdown");

    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "no maintenance pass may be admitted once the shutdown has begun"
    );
    // And the runner stays shut rather than reopening behind the drain.
    runner.handle().nudge(job, &namespace_id);
    runner.drain().await.expect("a shut runner is settled");
    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "a nudge after the shutdown must admit nothing either"
    );
}

#[tokio::test]
async fn a_namespace_advance_nudges_the_enabled_namespaces_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "grep-observer-server");
    let (_router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    let namespace_id = namespace_id("grep-observer");
    state
        .writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    state
        .binding
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    state
        .binding
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    state
        .runner
        .as_ref()
        .expect("automatic runner")
        .drain()
        .await
        .expect("settle the backfill");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(0),
        "an empty namespace's backfill completes at its own head"
    );

    // The publish is the only trigger from here on: nothing below nudges.
    state
        .writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"observer-driven needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish file");
    state
        .runner
        .as_ref()
        .expect("automatic runner")
        .drain()
        .await
        .expect("settle the observer-driven step");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(1),
        "the publish observer is what carried the index to the new head"
    );
    let request = GrepRequest {
        pattern: "observer-driven needle".to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        allow_stale: false,
        allow_scan: false,
    };
    let service = state
        .binding
        .grep_service
        .as_ref()
        .expect("a query-serving app carries a grep service");
    let store = state.writer.object_store();
    let reads = NamespaceReads::new(&state.binding.reader, &namespace_id);
    let response = service
        .query(
            &request,
            PaginationPolicy::default()
                .resolve_limit(None)
                .expect("the pagination policy accepts its own default"),
            &reads,
            &store,
        )
        .await
        .expect("grep caught-up index");
    assert_eq!(response.matches.len(), 1);
    state.writer.shutdown().await.expect("drain the writer");
}

/// What the index's steps published, read where an operator reads it.
async fn built_through_seq(state: &AppState, namespace_id: &NamespaceId) -> ChangeSeq {
    load_current_grep_manifest(&*state.writer.object_store(), namespace_id)
        .await
        .expect("load grep manifest")
        .expect("an enabled namespace has a grep manifest")
        .manifest_state()
        .status()
        .active_watermark()
        .expect("an active grep manifest has a watermark")
        .built_through_seq()
}

#[tokio::test]
async fn every_route_except_health_and_readiness_requires_authorization() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let router = app(
        test_config(temp_dir.path(), "server-writer"),
        options_with_store(store),
    )
    .await
    .expect("build app")
    .0;
    let protected_routes = [
        ("GET", "/metrics"),
        ("GET", "/v0/capabilities"),
        ("POST", "/v0/namespaces"),
        ("GET", "/v0/namespaces/demo"),
        ("DELETE", "/v0/namespaces/demo"),
        ("POST", "/v0/namespaces/demo/forks"),
        ("GET", "/v0/namespaces/demo/snapshots"),
        ("POST", "/v0/namespaces/demo/snapshots"),
        ("POST", "/v0/namespaces/demo/snapshots/pin_test/extend"),
        ("DELETE", "/v0/namespaces/demo/snapshots/pin_test"),
        ("GET", "/v0/maintenance/namespaces/demo/diagnostics"),
        ("GET", "/v0/namespaces/demo/filesystem/entries"),
        ("GET", "/v0/namespaces/demo/filesystem/entry"),
        ("GET", "/v0/namespaces/demo/filesystem/content"),
        ("POST", "/v0/namespaces/demo/filesystem/downloads"),
        ("GET", "/v0/namespaces/demo/grep"),
        ("GET", "/v0/maintenance/namespaces/demo/grep/index"),
        ("POST", "/v0/maintenance/namespaces/demo/grep/index/enable"),
        ("POST", "/v0/maintenance/namespaces/demo/grep/index/disable"),
        ("GET", "/v0/namespaces/demo/filesystem/revisions"),
        ("GET", "/v0/namespaces/demo/inodes/ino_1"),
        ("GET", "/v0/namespaces/demo/inodes/ino_1/children"),
        ("GET", "/v0/namespaces/demo/inodes/ino_1/revisions"),
        (
            "GET",
            "/v0/namespaces/demo/inodes/ino_1/revisions/1/content",
        ),
        (
            "POST",
            "/v0/namespaces/demo/inodes/ino_1/revisions/1/downloads",
        ),
        ("GET", "/v0/namespaces/demo/filesystem/trash"),
        ("POST", "/v0/namespaces/demo/commits"),
        ("POST", "/v0/namespaces/demo/uploads"),
        ("PUT", "/v0/namespaces/demo/uploads/upl_test/content"),
        ("POST", "/v0/namespaces/demo/uploads/upl_test/parts"),
        ("POST", "/v0/namespaces/demo/uploads/upl_test/complete"),
        ("POST", "/v0/namespaces/demo/uploads/upl_test/abort"),
        ("GET", "/v0/namespaces/demo/uploads/upl_test"),
        ("GET", "/v0/namespaces/demo/changes"),
        ("GET", "/v0/maintenance/namespaces/demo/checkpoints"),
        ("POST", "/v0/maintenance/namespaces/demo/checkpoints"),
        (
            "DELETE",
            "/v0/maintenance/namespaces/demo/checkpoints/pin_test",
        ),
        ("POST", "/v0/maintenance/namespaces/demo/runs"),
        ("POST", "/v0/maintenance/store/probe"),
    ];

    for (method, uri) in protected_routes {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .expect("route request"),
            )
            .await
            .expect("route response");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }

    for uri in ["/health", "/readiness"] {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .expect("public request"),
            )
            .await
            .expect("public response");
        assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hidden_maintenance_surface_keeps_filesystem_and_query_routes_served() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "hidden-maintenance-writer");
    config.maintenance = crate::config::MaintenanceMode::MaintainOnly;
    let (router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    assert!(state.runner.is_some());

    let capabilities_response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/v0/capabilities")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .header("Loonfs-Actor", "test-actor")
                .body(axum::body::Body::empty())
                .expect("capabilities request"),
        )
        .await
        .expect("capabilities response");
    assert_eq!(capabilities_response.status(), StatusCode::OK);
    let capabilities_body = axum::body::to_bytes(capabilities_response.into_body(), usize::MAX)
        .await
        .expect("capabilities body");
    let capabilities: CapabilityDocument =
        serde_json::from_slice(&capabilities_body).expect("capability document");
    assert_eq!(
        capabilities.api_groups,
        vec![
            API_GROUP_FILESYSTEM_V0.to_owned(),
            API_GROUP_QUERY_V0.to_owned(),
        ]
    );
    assert!(!capabilities
        .features
        .keys()
        .any(|feature| feature.starts_with("maintenance.")));
    capabilities
        .validate()
        .expect("the capability document is well formed");

    let diagnostics_response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/v0/maintenance/namespaces/hidden/diagnostics")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .header("Loonfs-Actor", "test-actor")
                .body(axum::body::Body::empty())
                .expect("diagnostics request"),
        )
        .await
        .expect("diagnostics response");
    assert_eq!(diagnostics_response.status(), StatusCode::NOT_FOUND);
    let diagnostics_body = axum::body::to_bytes(diagnostics_response.into_body(), usize::MAX)
        .await
        .expect("diagnostics body");
    let diagnostics_error: serde_json::Value =
        serde_json::from_slice(&diagnostics_body).expect("diagnostics error");
    assert_eq!(diagnostics_error["code"], "route_not_found");
    assert!(diagnostics_error.get("feature").is_none());

    let namespace_id = namespace_id("hidden");
    state
        .writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let commit_response = router
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::POST)
                .uri("/v0/namespaces/hidden/commits")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .header("Loonfs-Actor", "test-actor")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    r#"{
                        "commit_id":"hidden-maintenance-commit",
                        "operations":[{"kind":"create_directory","path":"/docs"}]
                    }"#,
                ))
                .expect("commit request"),
        )
        .await
        .expect("commit response");
    assert_eq!(commit_response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
// Monotonic time is used only to bound this shutdown check.
async fn shutdown_keeps_readiness_reachable_until_an_active_request_finishes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn get(addr: std::net::SocketAddr, path: &str) -> Vec<u8> {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to serving listener");
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("send request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        response
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "readiness-shutdown-writer");
    config.shutdown_deadline_ms = 1_000;
    let shutdown_deadline_ms = config.shutdown_deadline_ms;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app(config, options_with_store(store))
        .await
        .expect("build app");
    let slow_started = Arc::new(tokio::sync::Notify::new());
    let slow_release = Arc::new(tokio::sync::Notify::new());
    let router = router.route(
        "/slow",
        axum::routing::get({
            let slow_started = Arc::clone(&slow_started);
            let slow_release = Arc::clone(&slow_release);
            move || {
                let slow_started = Arc::clone(&slow_started);
                let slow_release = Arc::clone(&slow_release);
                async move {
                    slow_started.notify_one();
                    slow_release.notified().await;
                    "slow request finished"
                }
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        state.writer.clone(),
        state.runner.clone(),
        None,
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let slow = tokio::spawn(get(addr, "/slow"));
    tokio::time::timeout(std::time::Duration::from_secs(1), slow_started.notified())
        .await
        .expect("slow request starts");

    let shutdown_started = tokio::time::Instant::now();
    shutdown_tx.send(()).expect("trigger shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !state.writer.is_shutting_down() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown closes admission");

    let readiness = get(addr, "/readiness").await;
    let readiness = String::from_utf8(readiness).expect("readiness is utf-8");
    assert!(
        readiness.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
        "readiness returns 503 during drain: {readiness}"
    );
    assert!(
        readiness.contains("\"code\":\"shutting_down\""),
        "readiness names the shutdown: {readiness}"
    );
    assert!(
        readiness.contains("retry-after: 1\r\n"),
        "readiness tells callers when to retry: {readiness}"
    );
    assert!(
        !slow.is_finished(),
        "the original request is still in flight"
    );
    assert!(
        !server.is_finished(),
        "the listener remains serving while the request drains"
    );

    slow_release.notify_one();
    let slow = tokio::time::timeout(std::time::Duration::from_secs(1), slow)
        .await
        .expect("slow request completes")
        .expect("join slow request");
    let slow = String::from_utf8(slow).expect("slow response is utf-8");
    assert!(slow.starts_with("HTTP/1.1 200 OK\r\n"), "{slow}");
    assert!(slow.contains("slow request finished"), "{slow}");

    tokio::time::timeout(
        std::time::Duration::from_millis(shutdown_deadline_ms),
        server,
    )
    .await
    .expect("serve_and_settle finishes within the configured deadline")
    .expect("join server task")
    .expect("shutdown settles the server");
    assert!(
        shutdown_started.elapsed() < std::time::Duration::from_millis(shutdown_deadline_ms),
        "shutdown finishes inside its configured budget"
    );
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the listener stops accepting after the slow request finishes"
    );
}

fn options_with_store(store: SharedObjectStore) -> AppOptions {
    AppOptions {
        store: Some(store),
        direct_transfers: None,
    }
}

fn data_wal_put_for(
    namespace_id: &NamespaceId,
) -> impl Fn(&OperationContext<'_>) -> bool + Send + Sync + 'static {
    let prefix = loonfs_objectstore::keys::wal_segment_prefix(namespace_id);
    move |operation| match operation.kind() {
        OperationKind::Put {
            bytes,
            mode: PutMode::CreateIfAbsent,
        } if operation.key().starts_with(&prefix) => {
            loonfs_api::wire::wal::decode_wal_segment_envelope_zstd(bytes)
                .is_ok_and(|envelope| !envelope.payload().records.is_empty())
        }
        _ => false,
    }
}

fn test_config(root: &Path, writer_id: &str) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: writer_id.to_owned(),
        max_writer_sessions: loonfs::DEFAULT_MAX_WRITER_SESSIONS,
        max_concurrent_folds: loonfs::DEFAULT_MAX_CONCURRENT_FOLDS,
        publication: Default::default(),
        inline_content: Default::default(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        local_cache: None,
        grep: crate::config::GrepConfig {
            mode: crate::config::GrepMode::ServeAndMaintain,
            ..crate::config::GrepConfig::default()
        },
        maintenance: crate::config::MaintenanceMode::ServeAndMaintain,
        min_publish_interval_ms: 0,
        request_deadline_ms: 60_000,
        shutdown_deadline_ms: 600_000,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        snapshot_max_ttl_ms: 86_400_000,
        snapshot_max_lifetime_ms: 604_800_000,
        snapshot_max_live_per_namespace: 16,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: root.display().to_string(),
            key_prefix: Some("http-tests".to_owned()),
        },
    }
}

async fn test_runtime(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

/// A worker composed the way the server composes its own: grep's keyspace
/// on the given store, its filesystem reads and checkpoints on handles over
/// the same store.
async fn grep_worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id(actor)
        .build()
        .await
        .expect("build maintenance");
    GrepWorker::new(store.clone(), reader, maintenance)
}
