//! Host configuration, operational routes, and shutdown contracts.

mod admission;
mod composition;

use super::serve::{build_handles, serve_on, RuntimeHandles, Settlement};
use super::{app, AppConfig};
use crate::{AuthConfig, ServerConfig, StoreConfig};
use axum::http::StatusCode;
use loonfs::metrics::MetricValue;
use loonfs::{ConfiguredObjectStoreKind, LoonFs, SharedObjectStore, TraceMode, Writable};
use loonfs_client::{Client, ClientConfig};
use loonfs_http::HttpMetrics;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS;
use loonfs_types::{CapabilityDocument, API_GROUP_FILESYSTEM_V0, API_GROUP_QUERY_V0};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn build_handles_installs_jsonl_object_store_metrics_recorder() {
    let store_dir = tempdir().expect("store tempdir");
    let metrics_dir = tempdir().expect("metrics tempdir");
    let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(store_dir.path(), "server-writer")
        .validate()
        .expect("valid config");
    let metrics_path = metrics_dir.path().join("object-store.ndjson");

    {
        let RuntimeHandles { runtime, .. } = build_handles(
            &config.config,
            &config.memory,
            config.maintenance_writer_id.clone(),
            store,
            &HttpMetrics::new(),
            Some(metrics_path.clone().into_os_string()),
        )
        .await
        .expect("build handles");
        runtime
            .create_namespace(&namespace_id("metrics"), &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
    }

    let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
    assert!(!jsonl.is_empty());
    assert!(!jsonl.contains("namespaces/metrics"));
}

/// Checks that a scrape renders every counter and gauge of the metadata
/// cache and the grep cache exactly once, so the recorder's snapshot
/// doesn't include a second copy of any of them.
#[tokio::test]
async fn runtime_and_grep_cache_metrics_render_from_the_cache_stats() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "cache-metrics-writer");

    let (_router, state) = app(
        config.validate().expect("validate the test config"),
        options_with_store(store),
    )
    .await
    .expect("build app");
    assert!(
        state.grep_block_cache.is_some(),
        "the test config serves grep"
    );
    let rendered = super::metrics::render(&state);

    for name in [
        "loonfs_head_cache_gets_total",
        "loonfs_head_cache_evictions_total",
        "loonfs_head_cache_evicted_decoded_bytes_total",
        "loonfs_head_cache_rejections_total",
        "loonfs_head_cache_rejected_decoded_bytes_total",
        "loonfs_segment_cache_gets_total",
        "loonfs_segment_cache_inserts_total",
        "loonfs_segment_cache_evictions_total",
        "loonfs_segment_cache_rejections_total",
        "loonfs_segment_cache_filter_skips_total",
        "loonfs_segment_cache_filter_false_positives_total",
        "loonfs_manifest_cache_gets_total",
        "loonfs_manifest_cache_inserts_total",
        "loonfs_manifest_cache_evictions_total",
        "loonfs_manifest_cache_rejections_total",
        "loonfs_wal_tail_cache_gets_total",
        "loonfs_wal_tail_cache_inserts_total",
        "loonfs_wal_tail_cache_evictions_total",
        "loonfs_wal_tail_cache_evicted_decoded_bytes_total",
        "loonfs_wal_tail_cache_rejections_total",
        "loonfs_wal_tail_cache_rejected_decoded_bytes_total",
        "loonfs_content_object_cache_gets_total",
        "loonfs_content_object_cache_inserts_total",
        "loonfs_content_object_cache_evictions_total",
        "loonfs_grep_cache_gets_total",
        "loonfs_grep_cache_inserts_total",
        "loonfs_grep_cache_evictions_total",
    ] {
        assert_eq!(
            rendered
                .matches(&format!("# TYPE {name} counter\n"))
                .count(),
            1,
            "counter `{name}`"
        );
    }
    for name in [
        "loonfs_segment_cache_retained_decoded_bytes",
        "loonfs_manifest_cache_retained_decoded_bytes",
        "loonfs_head_cache_retained_decoded_bytes",
        "loonfs_wal_tail_cache_retained_decoded_bytes",
        "loonfs_content_object_cache_retained_decoded_bytes",
        "loonfs_grep_cache_retained_decoded_bytes",
    ] {
        assert_eq!(
            rendered.matches(&format!("# TYPE {name} gauge\n")).count(),
            1,
            "gauge `{name}`"
        );
    }
    assert!(!rendered.contains("loonfs_cache_metadata_segment_cache_hits"));
    assert!(!rendered.contains("loonfs_runtime_cache_"));
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
        Settlement {
            runtime: writer.clone(),
            sweep: None,
            deadline_ms: shutdown_deadline_ms,
        },
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
    loonfs_test_support::clock::bounded(
        std::time::Duration::from_secs(1),
        handler_started.notified(),
    )
    .await
    .expect("slow handler starts");

    shutdown_tx.send(()).expect("trigger shutdown");
    loonfs_test_support::clock::bounded(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_requests_and_settles_the_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(temp_dir.path(), "shutdown-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_on(
        listener,
        config.validate().expect("validate the test config"),
        async move {
            let _ = shutdown_rx.await;
        },
    ));

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
        .create_namespace(&namespace_id("demo"), &loonfs_test_support::test_actor())
        .await
        .expect("create namespace over http");

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    // The listener is closed once serve returns.
    std::net::TcpStream::connect(addr)
        .expect_err("listener should refuse connections after shutdown");
}

/// Writes one file and folds it, so each call leaves one more delta run.
async fn write_and_fold(writer: &LoonFs<Writable>, namespace_id: &loonfs::NamespaceId, path: &str) {
    writer
        .open_namespace(namespace_id)
        .expect("open the namespace")
        .put_file(path, b"body", &loonfs_test_support::test_actor())
        .await
        .expect("write a file");
    writer
        .maintenance(writer_id("seed-maintenance"))
        .fold_wal(namespace_id)
        .await
        .expect("fold the file");
}

#[tokio::test]
async fn a_sweep_stops_between_namespaces_and_cancels_streaming_on_shutdown() {
    let temp_dir = tempdir().expect("tempdir");
    let seed = test_runtime(
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")),
        "sweep-shutdown-seed",
    )
    .await;
    let compacts = namespace_id("a-compacts");
    seed.create_namespace(&compacts, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    for index in 0..=DEFAULT_MAX_DELTA_RUNS {
        write_and_fold(&seed, &compacts, &format!("/file-{index}")).await;
    }
    let later = ["b-later", "c-later"].map(namespace_id);
    for namespace_id in &later {
        seed.create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create the namespace");
    }
    seed.shutdown().await.expect("stop the seed writer");

    let store = Arc::new(RecordingStore::new(
        BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::metadata_segment(),
            OperationSelector::Put,
        ),
        KeyPredicate::any(),
    ));
    let mut config = test_config(temp_dir.path(), "sweep-shutdown-server");
    config.max_concurrent_maintenance = std::num::NonZeroUsize::MIN;
    let (router, state) = app(
        config.validate().expect("validate the test config"),
        options_with_store(store.clone()),
    )
    .await
    .expect("build app");
    store.inner().block_next();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        Settlement {
            runtime: state.binding.runtime.clone(),
            sweep: state.sweep.clone(),
            deadline_ms: 60_000,
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    loonfs_test_support::clock::bounded(
        std::time::Duration::from_secs(10),
        store.inner().wait_until_blocked(),
    )
    .await
    .expect("the sweep's compaction unit reaches its first segment write");

    shutdown_tx.send(()).expect("trigger shutdown");
    while !state.binding.runtime.is_shutting_down() {
        tokio::task::yield_now().await;
    }
    store.inner().release();
    loonfs_test_support::clock::bounded(std::time::Duration::from_secs(10), server)
        .await
        .expect("shutdown waits only for the compaction's next block")
        .expect("join the server task")
        .expect("shutdown settles the sweep and the runtime");

    let snapshot = state.binding.metrics.snapshot();
    let cancelled = snapshot
        .by_name("loonfs.maintenance.compactions")
        .find(|entry| entry.labels == [("outcome", "cancelled")])
        .map(|entry| entry.value.clone());
    assert_eq!(cancelled, Some(MetricValue::Counter(1)));
    for namespace_id in &later {
        let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
        assert!(
            !store
                .snapshot()
                .iter()
                .any(|operation| operation.key().starts_with(&prefix)),
            "no visit starts after shutdown, so `{namespace_id}` is never read"
        );
    }
}

#[tokio::test]
async fn shutdown_abandons_a_parked_sweep_visit_at_its_deadline_and_settles_the_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("parked-collection");
    // The first pass collects garbage, and collection lists pins first.
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::pin_prefix(&namespace_id)),
        OperationSelector::List,
    ));
    let shutdown_deadline_ms = 25;
    let mut config = test_config(temp_dir.path(), "parked-sweep-server");
    config.shutdown_deadline_ms = shutdown_deadline_ms;
    let (router, state) = app(
        config.validate().expect("validate the test config"),
        options_with_store(store.clone()),
    )
    .await
    .expect("build app");
    state
        .binding
        .runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    store.block_next();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        Settlement {
            runtime: state.binding.runtime.clone(),
            sweep: state.sweep.clone(),
            deadline_ms: shutdown_deadline_ms,
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    loonfs_test_support::clock::bounded(
        std::time::Duration::from_secs(10),
        store.wait_until_blocked(),
    )
    .await
    .expect("the sweep's collection reaches the parked pin listing");

    shutdown_tx.send(()).expect("trigger shutdown");
    loonfs_test_support::clock::bounded(std::time::Duration::from_secs(10), server)
        .await
        .expect("shutdown does not wait for a parked visit past its deadline")
        .expect("join the server task")
        .expect("an abandoned visit does not fail the shutdown");
    assert!(state.binding.runtime.is_shutting_down());
    store.release();
}

#[tokio::test]
async fn every_route_except_health_and_readiness_requires_authorization() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let router = app(
        test_config(temp_dir.path(), "server-writer")
            .validate()
            .expect("validate the test config"),
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
        (
            "GET",
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
    config.maintenance = crate::config::JobMode::MaintainOnly;
    let (router, state) = app(
        config.validate().expect("validate the test config"),
        options_with_store(store),
    )
    .await
    .expect("build app");
    assert!(state.sweep.is_some());

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
        .binding
        .runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
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
    let (router, state) = app(
        config.validate().expect("validate the test config"),
        options_with_store(store),
    )
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
        Settlement {
            runtime: state.binding.runtime.clone(),
            sweep: state.sweep.clone(),
            deadline_ms: shutdown_deadline_ms,
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let slow = tokio::spawn(get(addr, "/slow"));
    loonfs_test_support::clock::bounded(std::time::Duration::from_secs(1), slow_started.notified())
        .await
        .expect("slow request starts");

    let shutdown_started = tokio::time::Instant::now();
    shutdown_tx.send(()).expect("trigger shutdown");
    loonfs_test_support::clock::bounded(std::time::Duration::from_secs(1), async {
        while !state.binding.runtime.is_shutting_down() {
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
    let slow = loonfs_test_support::clock::bounded(std::time::Duration::from_secs(1), slow)
        .await
        .expect("slow request completes")
        .expect("join slow request");
    let slow = String::from_utf8(slow).expect("slow response is utf-8");
    assert!(slow.starts_with("HTTP/1.1 200 OK\r\n"), "{slow}");
    assert!(slow.contains("slow request finished"), "{slow}");

    loonfs_test_support::clock::bounded(
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
    tokio::net::TcpStream::connect(addr)
        .await
        .expect_err("the listener stops accepting after the slow request finishes");
}

fn options_with_store(store: SharedObjectStore) -> AppConfig {
    AppConfig {
        store: Some(store),
        direct_transfers: None,
    }
}

fn test_config(root: &Path, writer_id: &str) -> ServerConfig {
    let mut config: ServerConfig = toml::from_str(
        r#"
bind = "127.0.0.1:0"
writer_id = "unused"

[store]
kind = "local_fs"
root = "unused"
"#,
    )
    .expect("parse the test config");
    config.auth = Some(AuthConfig::BearerToken {
        token: "test-token".into(),
    });
    config.content_token_secret = "test-content-token-secret".into();
    config.writer_id = loonfs_test_support::ids::writer_id(writer_id);
    config.grep.mode = crate::config::JobMode::ServeAndMaintain;
    config.min_publish_interval_ms = 0;
    config.store = StoreConfig::LocalFs(LocalFsStoreConfig {
        root: root.display().to_string(),
        key_prefix: Some("http-tests".to_owned()),
    });
    config
}

async fn test_runtime(store: SharedObjectStore, writer_id: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id(writer_id))
        .config(loonfs::RuntimeConfig {
            trace_mode: TraceMode::Remote,
            ..loonfs::RuntimeConfig::default()
        })
        .store_kind(ConfiguredObjectStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}
