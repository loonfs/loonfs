//! The writable namespace handles the host holds across requests.

use super::*;
use axum::http::Method;
use tower::ServiceExt;

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: String,
) -> (StatusCode, Bytes) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .header("Loonfs-Actor", "namespace-writers")
                .body(axum::body::Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, body)
}

async fn create_directory(router: &axum::Router, namespace_id: &NamespaceId, path: &str) {
    let body = serde_json::json!({
        "commit_id": format!("create{}", path.replace('/', "-")),
        "operations": [{"kind": "create_directory", "path": path}],
    });
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    assert_eq!(
        send(router, Method::POST, &uri, body.to_string()).await.0,
        StatusCode::OK
    );
}

async fn create_direct_put_upload(
    router: &axum::Router,
    namespace_id: &NamespaceId,
) -> (StatusCode, String) {
    let uri = format!("/v0/namespaces/{namespace_id}/uploads");
    let body = serde_json::json!({"mode": "direct_put", "size_bytes": 5});
    let (status, body) = send(router, Method::POST, &uri, body.to_string()).await;
    let error: loonfs_api::ApiError = serde_json::from_slice(&body).expect("error body");
    (status, error.code)
}

async fn host(writer_id: &str) -> (tempfile::TempDir, axum::Router, BindingState) {
    let directory = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(directory.path(), writer_id),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    (directory, router, state)
}

async fn create_namespace(state: &BindingState, namespace_id: &NamespaceId) {
    state
        .runtime
        .create_namespace(
            namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
}

async fn writer_epoch(state: &BindingState, namespace_id: &NamespaceId) -> u64 {
    loonfs::control::load_namespace_read_state(state.probe_store.as_ref(), namespace_id)
        .await
        .expect("read head")
        .writer_epoch
        .0
}

#[tokio::test]
async fn two_commits_to_one_namespace_use_one_writer_epoch() {
    let (_directory, router, state) = host("one-epoch-host").await;
    let namespace_id = namespace_id("one-epoch");
    create_namespace(&state, &namespace_id).await;
    let created = writer_epoch(&state, &namespace_id).await;

    create_directory(&router, &namespace_id, "/first").await;
    create_directory(&router, &namespace_id, "/second").await;

    assert_eq!(writer_epoch(&state, &namespace_id).await, created + 1);
}

#[tokio::test]
async fn a_deleted_namespace_leaves_no_writer_handle_in_the_host() {
    let (_directory, router, state) = host("delete-host").await;
    let deleted = namespace_id("deleted");
    let kept = namespace_id("kept");
    for namespace_id in [&deleted, &kept] {
        create_namespace(&state, namespace_id).await;
        create_directory(&router, namespace_id, "/written").await;
    }

    let uri = format!("/v0/namespaces/{deleted}");
    assert_eq!(
        send(&router, Method::DELETE, &uri, String::new()).await.0,
        StatusCode::OK
    );

    let closed = state.namespaces.close(&deleted).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
    let closed = state.namespaces.close(&kept).await.expect("close");
    assert!(closed.is_some(), "the host holds the written namespace");
}

#[tokio::test]
async fn a_request_for_a_missing_namespace_leaves_no_writer_handle_in_the_host() {
    let (_directory, router, state) = host("missing-host").await;
    let missing = namespace_id("missing");
    let body = serde_json::json!({
        "commit_id": "create-missing",
        "operations": [{"kind": "create_directory", "path": "/missing"}],
    });
    let uri = format!("/v0/namespaces/{missing}/commits");
    assert_eq!(
        send(&router, Method::POST, &uri, body.to_string()).await.0,
        StatusCode::NOT_FOUND
    );

    let closed = state.namespaces.close(&missing).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
}

#[tokio::test]
async fn a_rejected_upload_for_a_missing_namespace_answers_not_found_and_leaves_no_writer_handle() {
    let (_directory, router, state) = host("missing-upload-host").await;
    let missing = namespace_id("missing");

    assert_eq!(
        create_direct_put_upload(&router, &missing).await,
        (
            StatusCode::NOT_FOUND,
            ErrorCode::NamespaceNotFound.as_str().to_owned()
        )
    );

    let closed = state.namespaces.close(&missing).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
}

#[tokio::test]
async fn a_rejected_upload_for_an_existing_namespace_keeps_its_writer_handle() {
    let (_directory, router, state) = host("existing-upload-host").await;
    let existing = namespace_id("existing");
    create_namespace(&state, &existing).await;

    // The local-storage fixture cannot presign, so it refuses `direct_put`.
    assert_eq!(
        create_direct_put_upload(&router, &existing).await,
        (
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::NotSupported.as_str().to_owned()
        )
    );

    let closed = state.namespaces.close(&existing).await.expect("close");
    assert!(closed.is_some(), "the host holds the existing namespace");
}
