#![allow(clippy::panic)]
//! In-process HTTP mode matrix for grep serving and worker ownership.

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions};
use loonfs_api::{
    ApiError, CapabilityDocument, GrepRequest, GrepResponse, NamespaceId, FEATURE_QUERY_GREP,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepWorker, GrepWorkerConfig, GrepWorkerLoop,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_server::{
    app, GrepConfig, GrepMode, RuntimeCacheConfigOverrides, ServerConfig, StoreConfig,
};
use serde::de::DeserializeOwned;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn disabled_mode_returns_not_supported_and_omits_grep_capabilities() {
    let temp_dir = tempdir().expect("store tempdir");
    let (_store, _writer, namespace_id) = seed_namespace(temp_dir.path(), "disabled").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::Disabled))
        .await
        .expect("build app");

    let capabilities: CapabilityDocument =
        response_json(send(&router, Method::GET, "/v0/config", None).await).await;
    assert!(!capabilities.features.contains_key(FEATURE_QUERY_GREP));
    for limit in grep_limits() {
        assert!(!capabilities.limits.contains_key(limit));
    }

    for path in [
        format!("/v0/namespaces/{namespace_id}/query/grep"),
        format!("/v0/admin/namespaces/{namespace_id}/index/grams/enable"),
        format!("/v0/admin/namespaces/{namespace_id}/index/grams/disable"),
    ] {
        let response = send(&router, Method::POST, &path, None).await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let error: ApiError = response_json(response).await;
        assert_eq!(error.code, "not_supported");
        assert_eq!(error.feature.as_deref(), Some(FEATURE_QUERY_GREP));
    }
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn embedded_mode_registers_fresh_enablement_without_waiting_for_rescan() {
    let temp_dir = tempdir().expect("store tempdir");
    let (_store, writer, namespace_id) = seed_namespace(temp_dir.path(), "embedded").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::Embedded))
        .await
        .expect("build app");
    wait_for_worker_opportunity().await;
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"automatic needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");

    let capabilities: CapabilityDocument =
        response_json(send(&router, Method::GET, "/v0/config", None).await).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    for limit in grep_limits() {
        assert!(capabilities.limits.contains_key(limit));
    }

    let response = wait_for_grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/note.txt");
    assert_eq!(response.built_through_seq, response.head_seq);
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn standalone_worker_rediscovers_server_enablement_within_one_rescan() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "rediscovered").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::ServeOnly))
        .await
        .expect("build app");

    let external_worker = GrepWorker::new(
        store.clone(),
        "standalone-rediscovery-worker",
        "standalone-rediscovery-session",
        "standalone-rediscovery/0.1",
    );
    let worker_loop = GrepWorkerLoop::new(
        external_worker,
        store,
        GrepWorkerConfig {
            step_interval_ms: 5,
            gc_interval_ms: 60_000,
            rescan_interval_ms: 10,
            ..GrepWorkerConfig::default()
        },
    );
    let shutdown = worker_loop.shutdown_handle();
    let worker_task = tokio::spawn(worker_loop.run());
    wait_for_worker_opportunity().await;

    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"rediscovered needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");

    let response = wait_for_grep(&router, &namespace_id, "rediscovered needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/note.txt");

    shutdown.request_shutdown();
    worker_task.await.expect("standalone worker loop joins");
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn serve_only_mode_requires_an_external_worker_to_advance_the_watermark() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "serve-only").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::ServeOnly))
        .await
        .expect("build app");
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"external needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");

    wait_for_worker_opportunity().await;
    let before = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("enabled grep root");
    assert_eq!(before.state().index().built_through_seq.0, 0);
    assert!(matches!(
        before.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));

    let worker = GrepWorker::new(
        store,
        "external-grep-worker",
        "external-grep-worker-session",
        "external-grep-worker/0.1",
    );
    for _ in 0..8 {
        let build = worker
            .build_step(&namespace_id, GramIndexBuildPolicy::default())
            .await
            .expect("external build step");
        worker
            .fold_step(&namespace_id, GramIndexBuildPolicy::default())
            .await
            .expect("external fold step");
        if matches!(
            build.outcome,
            GrepBuildOutcome::UpToDate {
                built_through_seq: loonfs_api::ChangeSeq(1)
            }
        ) {
            break;
        }
    }

    let response = grep(&router, &namespace_id, "external needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.built_through_seq.0, 1);
    lifecycle.shutdown().await.expect("drain lifecycle");
}

async fn seed_namespace(root: &Path, name: &str) -> (SharedObjectStore, FsWriter, NamespaceId) {
    let store = Arc::new(LocalFsStore::new(root).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id(format!("grep-mode-seed-{name}"))
        .writer_version("grep-mode-tests/0.1")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    (store, writer, namespace_id)
}

fn test_config(store_root: &Path, mode: GrepMode) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: format!("grep-mode-{mode:?}"),
        writer_version: "grep-mode-tests/0.1".to_owned(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: GrepConfig {
            mode,
            step_interval_ms: 5,
            gc_interval_ms: 25,
            rescan_interval_ms: 60_000,
            ..GrepConfig::default()
        },
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 1024 * 1024,
        max_download_bytes: 1024 * 1024,
        max_commit_body_bytes: 1024 * 1024,
        max_concurrent_uploads: 2,
        max_concurrent_downloads: 2,
        max_concurrent_maintenance: 2,
        allow_unauthenticated_remote: false,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: None,
        },
    }
}

async fn enable_grep(router: &Router, namespace_id: &NamespaceId) -> StatusCode {
    send(
        router,
        Method::POST,
        &format!("/v0/admin/namespaces/{namespace_id}/index/grams/enable"),
        None,
    )
    .await
    .status()
}

async fn grep(router: &Router, namespace_id: &NamespaceId, pattern: &str) -> GrepResponse {
    let request = GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    };
    response_json(
        send(
            router,
            Method::POST,
            &format!("/v0/namespaces/{namespace_id}/query/grep"),
            Some(serde_json::to_vec(&request).expect("serialize grep request")),
        )
        .await,
    )
    .await
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<Vec<u8>>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    router
        .clone()
        .oneshot(
            request
                .body(body.map_or_else(Body::empty, Body::from))
                .expect("request"),
        )
        .await
        .expect("route request")
}

async fn response_json<T: DeserializeOwned>(response: axum::response::Response) -> T {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&bytes).expect("decode response JSON")
}

fn grep_limits() -> [&'static str; 4] {
    [
        LIMIT_QUERY_GREP_DEFAULT,
        LIMIT_QUERY_GREP_MAX,
        LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
        LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    ]
}

#[allow(clippy::disallowed_methods)]
async fn wait_for_grep(router: &Router, namespace_id: &NamespaceId, pattern: &str) -> GrepResponse {
    // The real background timer is the behavior under test; the bounded poll only observes it.
    for _ in 0..200 {
        let response = send(
            router,
            Method::POST,
            &format!("/v0/namespaces/{namespace_id}/query/grep"),
            Some(
                serde_json::to_vec(&GrepRequest {
                    pattern: pattern.to_owned(),
                    case_insensitive: false,
                    path_prefix: None,
                    cursor: None,
                    limit: None,
                    allow_stale: false,
                    allow_scan: false,
                })
                .expect("serialize grep request"),
            ),
        )
        .await;
        if response.status() == StatusCode::OK {
            let response: GrepResponse = response_json(response).await;
            if !response.matches.is_empty() && response.built_through_seq == response.head_seq {
                return response;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("grep worker did not make the file searchable");
}

#[allow(clippy::disallowed_methods)]
async fn wait_for_worker_opportunity() {
    // Five step intervals let an embedded/standalone loop finish startup work,
    // and are long enough to prove serve_only did not spawn a loop.
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
}
