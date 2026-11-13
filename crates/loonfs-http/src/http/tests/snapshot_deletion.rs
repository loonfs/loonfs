//! Snapshot deletion and the snapshot inventory's read-only routes through
//! the router.

use super::fixtures;
use super::*;
use axum::http::Method;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::SnapshotId;
use serde_json::{json, Value};

async fn request(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = fixtures::request(method.as_str(), uri, Some(&body.to_string()), &[]);
    let (status, _, bytes) = fixtures::send(router, request).await;
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON response"),
    )
}

/// Checks that deleting a snapshot writes its expiry once and that repeating
/// the delete succeeds without a write, that the snapshot inventory has
/// no create or delete route, and that a deleted snapshot can't be extended.
#[tokio::test]
async fn snapshot_deletes_are_idempotent_and_the_snapshot_inventory_is_read_only() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_options(directory.path(), "snapshot-deletion-writer");
    config.http.serves_maintenance = true;
    config.grep = JobMode::Disabled;
    let (router, state) = test_app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let namespace_id = namespace_id("snapshots");
    state
        .runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let snapshot_records = format!("/v0/maintenance/namespaces/{namespace_id}/snapshots");
    let snapshots = format!("/v0/namespaces/{namespace_id}/snapshots");
    let (status, created) = request(
        &router,
        Method::POST,
        &snapshots,
        json!({"name": "view", "ttl_ms": 10_000}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let snapshot_id = SnapshotId::parse(created["snapshot_id"].as_str().expect("snapshot id"))
        .expect("snapshot id");
    let uri = format!("{snapshots}/{snapshot_id}");

    for (method, path) in [
        (Method::POST, snapshot_records.clone()),
        (Method::DELETE, format!("{snapshot_records}/{snapshot_id}")),
    ] {
        store.reset();
        let (status, body) = request(&router, method, &path, json!({"name": "view"})).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{path}: {body}");
        assert_eq!(body["code"], ErrorCode::MethodNotAllowed.as_str());
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }

    for expected_writes in [1, 0] {
        store.reset();
        let (status, body) = request(&router, Method::DELETE, &uri, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert_eq!(
            body,
            json!({"namespace_id": namespace_id, "snapshot_id": snapshot_id})
        );
        assert_eq!(store.counts().compare_and_swaps, expected_writes);
        assert_eq!(store.counts().deletes, 0);
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
/// namespace is deleted, and that the ID of a fork snapshot returns
/// `snapshot_not_found` and leaves the snapshot in place.
#[tokio::test]
async fn a_snapshot_on_a_deleted_namespace_stays_deletable() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_options(directory.path(), "deleted-snapshot-writer");
    config.http.serves_maintenance = true;
    config.grep = JobMode::Disabled;
    let (router, state) = test_app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let namespace_id = namespace_id("deleted-snapshots");
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
            &self::namespace_id("deleted-snapshots-fork"),
            &actor,
        )
        .await
        .expect("fork");
    let snapshots = format!("/v0/namespaces/{namespace_id}/snapshots");
    let (status, created) =
        request(&router, Method::POST, &snapshots, json!({"name": "kept"})).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert!(created.get("expires_at_ms").is_none(), "{created}");
    let snapshot_id = created["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_owned();
    let (status, listed) = request(
        &router,
        Method::GET,
        &format!("/v0/maintenance/namespaces/{namespace_id}/snapshots"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let fork_snapshot_id = listed["snapshots"]
        .as_array()
        .expect("snapshots")
        .iter()
        .find(|snapshot| snapshot["owner"]["kind"] == "fork")
        .expect("fork snapshot")["snapshot_id"]
        .as_str()
        .expect("fork snapshot id")
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
        &format!("{snapshots}/{fork_snapshot_id}"),
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
    assert_eq!(store.counts().compare_and_swaps, 1);
    assert_eq!(store.counts().deletes, 0);
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
