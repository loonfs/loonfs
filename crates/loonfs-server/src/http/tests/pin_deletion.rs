//! Checkpoint and snapshot deletion through the router.

use super::*;
use axum::http::Method;
use loonfs_api::CheckpointId;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn request(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON response"),
    )
}

#[tokio::test]
async fn delete_routes_require_the_owner_and_delete_each_pin_once() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_config(directory.path(), "pin-deletion-writer");
    config.maintenance = crate::config::MaintenanceMode::ServeOnly;
    config.grep.mode = crate::config::GrepMode::Disabled;
    let (router, state) = app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let namespace_id = namespace_id("pins");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("namespace");
    let checkpoints = format!("/v0/maintenance/namespaces/{namespace_id}/checkpoints");
    let snapshots = format!("/v0/namespaces/{namespace_id}/snapshots");
    for (collection, other_collection, id_field, missing_code) in [
        (
            &checkpoints,
            &snapshots,
            "checkpoint_id",
            ErrorCode::CheckpointNotFound,
        ),
        (
            &snapshots,
            &checkpoints,
            "snapshot_id",
            ErrorCode::SnapshotNotFound,
        ),
    ] {
        let (status, created) = request(
            &router,
            Method::POST,
            collection,
            json!({"name": "view", "ttl_ms": 10_000}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let checkpoint_id = CheckpointId::parse(created[id_field].as_str().expect("pin id"))
            .expect("checkpoint id");
        let uri = format!("{collection}/{checkpoint_id}");
        for (method, path, expected_status, error_code) in [
            (
                Method::DELETE,
                format!("{other_collection}/{checkpoint_id}"),
                StatusCode::BAD_REQUEST,
                Some(ErrorCode::InvalidRequest),
            ),
            (
                Method::POST,
                format!("{uri}/release"),
                StatusCode::NOT_FOUND,
                Some(ErrorCode::RouteNotFound),
            ),
            (Method::DELETE, uri.clone(), StatusCode::OK, None),
            (
                Method::DELETE,
                uri.clone(),
                StatusCode::NOT_FOUND,
                Some(missing_code),
            ),
        ] {
            store.reset();
            let (status, body) = request(&router, method, &path, json!({})).await;
            assert_eq!(status, expected_status, "{path}: {body}");
            if let Some(code) = error_code {
                assert_eq!(body["code"], code.as_str());
                assert_eq!(store.counts().deletes, 0);
            } else {
                assert_eq!(
                    body,
                    json!({"namespace_id": namespace_id, (id_field): checkpoint_id})
                );
                assert_eq!(store.counts().deletes, 1);
            }
            assert_eq!(store.counts().puts, 0);
        }
        if id_field == "snapshot_id" {
            store.reset();
            let (status, body) = request(
                &router,
                Method::POST,
                &format!("{uri}/extend"),
                json!({"ttl_ms": 10_000}),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["code"], missing_code.as_str());
            assert_eq!(store.counts().puts, 0);
            assert_eq!(store.counts().deletes, 0);
        }
    }
}
