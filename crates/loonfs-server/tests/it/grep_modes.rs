#![allow(clippy::panic)]
//! In-process HTTP matrix for the four things a deployment can do about
//! grep: answer searches, keep the index built, both, or neither.

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions};
use loonfs::{FsAdmin, FsReader};
use loonfs_api::v0::GrepGcResponse;
use loonfs_api::{
    ApiError, CapabilityDocument, ChangeSeq, GrepRequest, GrepResponse, NamespaceId,
    FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, PROFILE_QUERY_V0,
};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepWorker};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_server::{
    app, GrepConfig, GrepMode, RuntimeCacheConfigOverrides, ServerConfig, ServerLifecycle,
    StoreConfig,
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
    assert!(
        !capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0),
        "a deployment that answers `not_supported` on every query route must not advertise \
         the plane"
    );
    for limit in grep_limits() {
        assert!(!capabilities.limits.contains_key(limit));
    }
    assert!(!lifecycle.maintains_grep_index());

    for path in grep_paths(&namespace_id) {
        let response = send(&router, Method::POST, &path, None).await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let error: ApiError = response_json(response).await;
        assert_eq!(error.code, "not_supported");
        assert_eq!(error.feature.as_deref(), Some(FEATURE_QUERY_GREP));
    }
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn serving_and_maintaining_enables_queries_nudges_and_disables_per_namespace() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "both").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::ServeAndMaintain))
        .await
        .expect("build app");
    assert!(lifecycle.maintains_grep_index());
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(0));

    // The file lands through a writer of its own, so nothing in this server
    // observed the publish: the index stays where it was until a request
    // touches the namespace again.
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"automatic needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(0));

    let capabilities: CapabilityDocument =
        response_json(send(&router, Method::GET, "/v0/capabilities", None).await).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    assert!(capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0));
    for limit in grep_limits() {
        assert!(capabilities.limits.contains_key(limit));
    }
    assert_served_document_covers_the_spec_example(&capabilities);

    // The first search over a namespace whose index trails answers from the
    // exhaustive tail and nudges the index at the same time.
    let response = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/note.txt");
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let caught_up = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(caught_up.matches.len(), 1);
    assert_eq!(caught_up.built_through_seq, caught_up.head_seq);

    // Disabling is one durable compare-and-swap; the runner discovers it.
    assert_eq!(disable_grep(&router, &namespace_id).await, StatusCode::OK);
    let disabled = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load disabled root")
        .expect("disabled root");
    assert!(matches!(
        disabled.state().lifecycle(),
        GrepLifecycle::Disabled
    ));
    settle(&lifecycle).await;
    assert!(
        matches!(
            load_grep_root(&*store, &namespace_id)
                .await
                .expect("reload disabled root")
                .expect("disabled root")
                .state()
                .lifecycle(),
            GrepLifecycle::Disabled
        ),
        "no step may resurrect a root the operator disabled"
    );

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
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
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

    let worker = grep_worker(&store, "restart-worker").await;
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

    // Nothing has nudged either namespace in this process: the first search
    // is what re-admits the index that trails its head.
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::ServeAndMaintain))
        .await
        .expect("reopen app");
    let stale_response = grep(&router, &stale, "stale steady needle").await;
    assert_eq!(stale_response.matches.len(), 1);
    settle(&lifecycle).await;
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    assert_eq!(watermark(&store, &stale).await, ChangeSeq(2));

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
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &backfill).await, ChangeSeq(3));
    let resumed = grep(&router, &backfill, "mid-backfill needle").await;
    assert_eq!(resumed.matches.len(), 3);
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn serve_only_answers_searches_over_an_index_it_refuses_to_administer() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "serve-only").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::ServeOnly))
        .await
        .expect("build app");
    assert!(!lifecycle.maintains_grep_index());

    // Every route that would mutate a grep root belongs where the index is
    // maintained, so this deployment refuses all three.
    for path in admin_grep_paths(&namespace_id) {
        let response = send(&router, Method::POST, &path, None).await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let error: ApiError = response_json(response).await;
        assert_eq!(error.code, "not_supported");
        assert!(
            error.message.contains("does not maintain"),
            "{}",
            error.message
        );
    }

    let worker = grep_worker(&store, "external-grep-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"external needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");
    settle(&lifecycle).await;
    assert_eq!(
        watermark(&store, &namespace_id).await,
        ChangeSeq(0),
        "a deployment that maintains nothing must not advance the watermark"
    );

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = grep(&router, &namespace_id, "external needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.built_through_seq, ChangeSeq(1));
    lifecycle.shutdown().await.expect("drain lifecycle");
}

#[tokio::test]
async fn maintain_only_keeps_the_index_built_without_serving_searches() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "maintain-only").await;
    let (router, lifecycle) = app(test_config(temp_dir.path(), GrepMode::MaintainOnly))
        .await
        .expect("build app");
    assert!(lifecycle.maintains_grep_index());

    let capabilities: CapabilityDocument =
        response_json(send(&router, Method::GET, "/v0/capabilities", None).await).await;
    assert!(
        !capabilities.features.contains_key(FEATURE_QUERY_GREP),
        "a deployment that answers no searches must not advertise that it does"
    );
    let refused = send(
        &router,
        Method::POST,
        &format!("/v0/namespaces/{namespace_id}/query/grep"),
        Some(serde_json::to_vec(&grep_request("needle")).expect("serialize grep request")),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::NOT_IMPLEMENTED);
    let error: ApiError = response_json(refused).await;
    assert!(
        error.message.contains("does not serve grep queries"),
        "{}",
        error.message
    );

    // The index itself is this deployment's job: enabling it here admits the
    // backfill, and the runner carries it to the namespace's head.
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"unserved needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    settle(&lifecycle).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("maintained root");
    assert!(matches!(root.state().lifecycle(), GrepLifecycle::Steady));
    assert!(
        !root.state().segments().is_empty(),
        "the index this deployment maintains holds real segments"
    );
    lifecycle.shutdown().await.expect("drain lifecycle");
}

async fn seed_namespace(root: &Path, name: &str) -> (SharedObjectStore, FsWriter, NamespaceId) {
    let store = Arc::new(LocalFsStore::new(root).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id(format!("grep-mode-seed-{name}"))
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
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: GrepConfig {
            mode,
            ..GrepConfig::default()
        },
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 1024 * 1024,
        max_download_bytes: 1024 * 1024,
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

/// Waits for every maintenance step this deployment admitted to settle, so
/// the durable state read next is the state those steps left.
async fn settle(lifecycle: &ServerLifecycle) {
    lifecycle
        .wait_for_maintenance()
        .await
        .expect("settle maintenance");
}

/// The sequence this namespace's index is built through, or `None` when it
/// has no grep root at all.
async fn watermark(store: &SharedObjectStore, namespace_id: &NamespaceId) -> ChangeSeq {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("an enabled namespace has a grep root")
        .state()
        .index()
        .built_through_seq
}

fn grep_paths(namespace_id: &NamespaceId) -> Vec<String> {
    let mut paths = vec![format!("/v0/namespaces/{namespace_id}/query/grep")];
    paths.extend(admin_grep_paths(namespace_id));
    paths
}

fn admin_grep_paths(namespace_id: &NamespaceId) -> Vec<String> {
    ["enable", "disable", "gc"]
        .into_iter()
        .map(|action| format!("/v0/admin/namespaces/{namespace_id}/grep/index/{action}"))
        .collect()
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
            .reorganize_step(namespace_id, policy)
            .await
            .expect("fold step");
        if matches!(build.outcome, GrepBuildOutcome::UpToDate { .. })
            && matches!(
                fold.outcome,
                loonfs_grep::GrepReorganizeOutcome::NotNeeded { .. }
            )
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

const API_SPEC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/api.md");

/// A grep worker over the same handles the server composes.
async fn grep_worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store.clone(), reader, admin)
}

/// The api.md section 2.1 example describes a reference deployment: the
/// runtime's core and admin planes plus the query plane the server composes
/// from `loonfs-grep`. `loonfs`'s `capability_conformance` pins the
/// runtime's half; this pins the merged document a served deployment
/// answers with. A deployment adds its own limits on top, so the example is
/// a subset rather than an equality.
fn assert_served_document_covers_the_spec_example(served: &CapabilityDocument) {
    let spec = std::fs::read_to_string(API_SPEC_PATH).expect("read docs/specs/api.md");
    let example = spec
        .split("### 2.1")
        .nth(1)
        .expect("api.md section 2.1")
        .split("### 2.2")
        .next()
        .expect("section end")
        .split("```json")
        .nth(1)
        .expect("capability example block")
        .split("```")
        .next()
        .expect("fenced block end");
    let mut expected: CapabilityDocument =
        serde_json::from_str(example).expect("spec capability example parses");
    // The direct transports are properties of the configured store, not of
    // what this process composes: a store that cannot presign has their keys
    // removed rather than advertised `false`. This deployment's local-fs
    // store cannot, so they are out of scope for this comparison.
    let mut served_features = served.features.clone();
    for feature in [FEATURE_UPLOADS_DIRECT_PUT, FEATURE_UPLOADS_DIRECT_MULTIPART] {
        expected.features.remove(feature);
        served_features.remove(feature);
    }

    served.validate().expect("served document is well-formed");
    assert_eq!(served.protocol_version, expected.protocol_version);
    assert_eq!(
        served.profiles, expected.profiles,
        "the served profiles drifted from the api.md section 2.1 example"
    );
    assert_eq!(
        served_features, expected.features,
        "the served features drifted from the api.md section 2.1 example"
    );
    for (limit, value) in &expected.limits {
        assert_eq!(
            served.limits.get(limit),
            Some(value),
            "the served `{limit}` limit drifted from the api.md section 2.1 example"
        );
    }
}

fn grep_limits() -> [&'static str; 4] {
    [
        LIMIT_QUERY_GREP_DEFAULT,
        LIMIT_QUERY_GREP_MAX,
        LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
        LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    ]
}
