#![allow(clippy::panic)]
//! In-process HTTP mode matrix for grep serving and worker ownership.

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions};
use loonfs_api::v0::GrepGcResponse;
use loonfs_api::{
    ApiError, CapabilityDocument, GrepRequest, GrepResponse, NamespaceId, FEATURE_QUERY_GREP,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepDriverParked, GrepWorker};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_server::{
    app, GrepConfig, GrepMode, RuntimeCacheConfigOverrides, ServerConfig, StoreConfig,
};
use serde::de::DeserializeOwned;
use std::num::NonZeroUsize;
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
        response_json(send(&router, Method::GET, "/v0/capabilities", None).await).await;
    assert!(!capabilities.features.contains_key(FEATURE_QUERY_GREP));
    for limit in grep_limits() {
        assert!(!capabilities.limits.contains_key(limit));
    }

    for path in [
        format!("/v0/namespaces/{namespace_id}/query/grep"),
        format!("/v0/admin/namespaces/{namespace_id}/grep/index/enable"),
        format!("/v0/admin/namespaces/{namespace_id}/grep/index/disable"),
        format!("/v0/admin/namespaces/{namespace_id}/grep/index/gc"),
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
async fn embedded_mode_enable_query_nudge_disable_and_reenable_are_per_namespace() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "embedded").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::Embedded))
        .await
        .expect("build app");
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&namespace_id).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(0)
        })
    );
    assert!(lifecycle.grep_driver_running(&namespace_id));
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
        response_json(send(&router, Method::GET, "/v0/capabilities", None).await).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    for limit in grep_limits() {
        assert!(capabilities.limits.contains_key(limit));
    }

    let response = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/note.txt");
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&namespace_id).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(1)
        })
    );
    let caught_up = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(caught_up.built_through_seq, caught_up.head_seq);

    assert_eq!(disable_grep(&router, &namespace_id).await, StatusCode::OK);
    assert!(!lifecycle.grep_driver_running(&namespace_id));
    let disabled = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load disabled root")
        .expect("disabled root");
    assert!(matches!(
        disabled.state().lifecycle(),
        GrepLifecycle::Disabled
    ));
    let gc: GrepGcResponse = response_json(
        send(
            &router,
            Method::POST,
            &format!("/v0/admin/namespaces/{namespace_id}/grep/index/gc"),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(gc.namespace_id, namespace_id);

    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&namespace_id).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(1)
        })
    );
    let reenabled = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(reenabled.matches.len(), 1);
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn first_query_after_restart_resumes_stale_and_mid_backfill_namespaces() {
    let temp_dir = tempdir().expect("store tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("restart-seed")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let stale = NamespaceId::parse("restart-stale").expect("namespace id");
    let backfill = NamespaceId::parse("restart-backfill").expect("namespace id");
    for namespace_id in [&stale, &backfill] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }
    writer
        .put_file_bytes(
            &stale,
            "/indexed.txt",
            b"indexed before restart\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write indexed file");
    for index in 0..3 {
        writer
            .put_file_bytes(
                &backfill,
                &format!("/backfill-{index}.txt"),
                format!("mid-backfill needle {index}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write backfill file");
    }

    let worker = GrepWorker::new(
        store.clone(),
        "restart-worker",
        "restart-worker-session",
        "restart-worker/0.1",
    );
    worker.enable(&stale).await.expect("enable stale namespace");
    drive_worker_to_current(&worker, &stale, GramIndexBuildPolicy::default()).await;
    writer
        .put_file_bytes(
            &stale,
            "/tail.txt",
            b"stale steady needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write unindexed tail");

    worker
        .enable(&backfill)
        .await
        .expect("enable backfill namespace");
    worker
        .build_step(
            &backfill,
            GramIndexBuildPolicy {
                max_files_per_step: NonZeroUsize::MIN,
                ..GramIndexBuildPolicy::default()
            },
        )
        .await
        .expect("leave mid-backfill root");
    let root = load_grep_root(&*store, &backfill)
        .await
        .expect("load root")
        .expect("backfill root");
    assert!(matches!(
        root.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));
    writer.shutdown_background().await.expect("shutdown writer");
    drop(writer);
    drop(worker);
    drop(store);

    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::Embedded))
        .await
        .expect("reopen app");
    let stale_response = grep(&router, &stale, "stale steady needle").await;
    assert_eq!(stale_response.matches.len(), 1);
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&stale).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(2)
        })
    );

    let not_materialized = send(
        &router,
        Method::POST,
        &format!("/v0/namespaces/{backfill}/query/grep"),
        Some(
            serde_json::to_vec(&grep_request("mid-backfill needle"))
                .expect("serialize grep request"),
        ),
    )
    .await;
    assert!(
        matches!(
            not_materialized.status(),
            StatusCode::OK | StatusCode::NOT_IMPLEMENTED
        ),
        "first touch either observes backfill or its concurrently completed root"
    );
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&backfill).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(3)
        })
    );
    let resumed = grep(&router, &backfill, "mid-backfill needle").await;
    assert_eq!(resumed.matches.len(), 3);
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

    assert!(!lifecycle.grep_driver_running(&namespace_id));
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
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let response = grep(&router, &namespace_id, "external needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.built_through_seq.0, 1);
    assert!(!lifecycle.grep_driver_running(&namespace_id));
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
        &format!("/v0/admin/namespaces/{namespace_id}/grep/index/enable"),
        None,
    )
    .await
    .status()
}

async fn disable_grep(router: &Router, namespace_id: &NamespaceId) -> StatusCode {
    send(
        router,
        Method::POST,
        &format!("/v0/admin/namespaces/{namespace_id}/grep/index/disable"),
        None,
    )
    .await
    .status()
}

async fn grep(router: &Router, namespace_id: &NamespaceId, pattern: &str) -> GrepResponse {
    let request = grep_request(pattern);
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

fn grep_request(pattern: &str) -> GrepRequest {
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

async fn drive_worker_to_current(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..64 {
        let build = worker
            .build_step(namespace_id, policy)
            .await
            .expect("build step");
        let fold = worker
            .fold_step(namespace_id, policy)
            .await
            .expect("fold step");
        if matches!(build.outcome, GrepBuildOutcome::UpToDate { .. })
            && matches!(fold.outcome, loonfs_grep::GrepFoldOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("grep worker did not catch up");
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
