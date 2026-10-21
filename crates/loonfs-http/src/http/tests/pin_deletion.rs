//! Snapshot deletion and the checkpoint inventory's read-only routes through
//! the router.

use super::*;
use axum::http::Method;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::PinId;
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

/// Checks that deleting a snapshot deletes its pin once and that repeating
/// the delete succeeds without a write, that the checkpoint inventory has
/// no create or delete route, and that a deleted snapshot can't be extended.
#[tokio::test]
async fn snapshot_deletes_are_idempotent_and_the_checkpoint_inventory_is_read_only() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_options(directory.path(), "pin-deletion-writer");
    config.binding.serves_maintenance = true;
    config.grep = JobMode::Disabled;
    let (router, state) = test_app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let namespace_id = namespace_id("pins");
    state
        .runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let checkpoints = format!("/v0/maintenance/namespaces/{namespace_id}/checkpoints");
    let snapshots = format!("/v0/namespaces/{namespace_id}/snapshots");
    let (status, created) = request(
        &router,
        Method::POST,
        &snapshots,
        json!({"name": "view", "ttl_ms": 10_000}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let snapshot_id =
        PinId::parse(created["snapshot_id"].as_str().expect("pin id")).expect("snapshot id");
    let uri = format!("{snapshots}/{snapshot_id}");

    for (method, path) in [
        (Method::POST, checkpoints.clone()),
        (Method::DELETE, format!("{checkpoints}/{snapshot_id}")),
    ] {
        store.reset();
        let (status, body) = request(&router, method, &path, json!({"name": "view"})).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{path}: {body}");
        assert_eq!(body["code"], ErrorCode::MethodNotAllowed.as_str());
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }

    for expected_deletes in [1, 0] {
        store.reset();
        let (status, body) = request(&router, Method::DELETE, &uri, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert_eq!(
            body,
            json!({"namespace_id": namespace_id, "snapshot_id": snapshot_id})
        );
        assert_eq!(store.counts().deletes, expected_deletes);
        assert_eq!(store.counts().puts, 0);
    }

    store.reset();
    let (status, body) = request(
        &router,
        Method::POST,
        &format!("{uri}/extend"),
        json!({"ttl_ms": 10_000}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], ErrorCode::SnapshotNotFound.as_str());
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

/// Checks that a snapshot without an expiry can still be deleted after its
/// namespace is deleted, and that the ID of a fork pin returns
/// `snapshot_not_found` and leaves the pin in place.
#[tokio::test]
async fn a_snapshot_on_a_deleted_namespace_stays_deletable() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_options(directory.path(), "deleted-pin-writer");
    config.binding.serves_maintenance = true;
    config.grep = JobMode::Disabled;
    let (router, state) = test_app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let namespace_id = namespace_id("deleted-pins");
    let actor = loonfs_test_support::test_actor();
    state
        .runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    state
        .runtime
        .fork_namespace(
            &namespace_id,
            &self::namespace_id("deleted-pins-fork"),
            &actor,
        )
        .await
        .expect("fork");
    let snapshots = format!("/v0/namespaces/{namespace_id}/snapshots");
    let (status, created) =
        request(&router, Method::POST, &snapshots, json!({"name": "kept"})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert!(created.get("expires_at_ms").is_none(), "{created}");
    let snapshot_id = created["snapshot_id"].as_str().expect("pin id").to_owned();
    let (status, listed) = request(
        &router,
        Method::GET,
        &format!("/v0/maintenance/namespaces/{namespace_id}/checkpoints"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let fork_pin_id = listed["checkpoints"]
        .as_array()
        .expect("checkpoints")
        .iter()
        .find(|checkpoint| checkpoint["owner"]["kind"] == "fork")
        .expect("fork pin")["checkpoint_id"]
        .as_str()
        .expect("fork pin id")
        .to_owned();
    let (status, body) = request(
        &router,
        Method::DELETE,
        &format!("/v0/namespaces/{namespace_id}"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    store.reset();
    let (status, body) = request(
        &router,
        Method::DELETE,
        &format!("{snapshots}/{fork_pin_id}"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], ErrorCode::SnapshotNotFound.as_str());
    assert_eq!(store.counts().deletes, 0);
    let (status, body) = request(
        &router,
        Method::DELETE,
        &format!("{snapshots}/{snapshot_id}"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(store.counts().deletes, 1);
    let (status, body) = request(
        &router,
        Method::DELETE,
        &format!("/v0/namespaces/never-created/snapshots/{snapshot_id}"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], ErrorCode::NamespaceNotFound.as_str());
}
