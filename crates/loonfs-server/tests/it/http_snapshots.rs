//! HTTP snapshot lifecycle operations and the checkpoint inventory.

use crate::common::http_split_support::test_config;
use crate::common::start_server;
use loonfs_server::JobMode;
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    ChangeSeq, DeleteSnapshotResponse, ErrorResponse, ListSnapshotsResponse, NamespaceId, Snapshot,
};
use serde::de::DeserializeOwned;
use tempfile::tempdir;

type ApiResult<T> = Result<T, (u16, Box<ErrorResponse>)>;

fn create_snapshot(
    server_url: &str,
    namespace: &str,
    name: &str,
    ttl_ms: Option<u64>,
) -> ApiResult<Snapshot> {
    let body = match ttl_ms {
        Some(ttl_ms) => serde_json::json!({"name": name, "ttl_ms": ttl_ms}),
        None => serde_json::json!({"name": name}),
    };
    post_json_body(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots"),
        body,
    )
}

fn list_snapshots(server_url: &str, namespace: &str) -> ApiResult<ListSnapshotsResponse> {
    retry_result_on_macos_teardown_einval(|| {
        decode_response(
            raw_agent()
                .get(&format!("{server_url}/v0/namespaces/{namespace}/snapshots"))
                .set("authorization", "Bearer test-token")
                .call(),
        )
    })
}

fn extend_snapshot(
    server_url: &str,
    namespace: &str,
    snapshot_id: &str,
    ttl_ms: u64,
) -> ApiResult<Snapshot> {
    post_json_body(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots/{snapshot_id}/extend"),
        serde_json::json!({"ttl_ms": ttl_ms}),
    )
}

fn delete_snapshot(
    server_url: &str,
    namespace: &str,
    snapshot_id: &str,
) -> ApiResult<DeleteSnapshotResponse> {
    delete_json(&format!(
        "{server_url}/v0/namespaces/{namespace}/snapshots/{snapshot_id}"
    ))
}

fn delete_json<T: DeserializeOwned>(url: &str) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        decode_response(
            raw_agent()
                .delete(url)
                .set("authorization", "Bearer test-token")
                .call(),
        )
    })
}

fn post_json_body<T: DeserializeOwned>(url: &str, body: serde_json::Value) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        decode_response(
            raw_agent()
                .post(url)
                .set("authorization", "Bearer test-token")
                .send_json(body.clone()),
        )
    })
}

fn decode_response<T: DeserializeOwned>(
    result: Result<ureq::Response, ureq::Error>,
) -> ApiResult<T> {
    match result {
        Ok(response) => Ok(serde_json::from_reader(response.into_reader())
            .unwrap_or_else(|error| panic!("decode success response: {error}"))),
        Err(ureq::Error::Status(status, response)) => Err((
            status,
            Box::new(
                serde_json::from_reader(response.into_reader())
                    .unwrap_or_else(|error| panic!("decode error response: {error}")),
            ),
        )),
        Err(ureq::Error::Transport(error)) => panic!("HTTP transport failed: {error}"),
    }
}

/// Checks that a snapshot is created with its expiry, listed, extended
/// without moving its expiry earlier, and deleted, and that repeating the
/// delete returns the same response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_are_listed_extended_and_deleted_idempotently() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "snapshot-lifecycle",
        "snapshot-lifecycle",
    );
    let harness = start_server(config).await;
    let namespace = namespace_id("lifecycle");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let before_create = loonfs::current_time_ms();
    let created = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "report",
        Some(5_000),
    )
    .expect("create snapshot");
    let after_create = loonfs::current_time_ms();
    assert_eq!(created.namespace_id, namespace);
    assert_eq!(created.name, "report");
    assert_eq!(created.captured_seq, ChangeSeq(0));
    assert!(created.created_at_ms >= before_create);
    assert!(created.created_at_ms <= after_create);
    let expires_at_ms = created
        .expires_at_ms
        .expect("a snapshot with a TTL expires");
    assert!(expires_at_ms >= before_create + 5_000);
    assert!(expires_at_ms <= after_create + 5_000);
    let diagnostics = harness
        .client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read snapshot diagnostics");
    assert_eq!(diagnostics.live_snapshots, 1);

    let listed = list_snapshots(&harness.server_url, namespace.as_str()).expect("list snapshot");
    assert_eq!(listed.snapshots, vec![created.clone()]);

    let extended = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        20_000,
    )
    .expect("extend snapshot");
    assert!(extended.expires_at_ms > created.expires_at_ms);
    let shortened = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        1_000,
    )
    .expect("a shorter extension");
    assert_eq!(
        shortened.expires_at_ms, extended.expires_at_ms,
        "an extension never moves the expiry earlier"
    );

    for attempt in 0..2 {
        let deleted = delete_snapshot(
            &harness.server_url,
            namespace.as_str(),
            created.snapshot_id.as_str(),
        )
        .unwrap_or_else(|error| panic!("delete attempt {attempt}: {error:?}"));
        assert_eq!(
            deleted,
            DeleteSnapshotResponse {
                namespace_id: namespace.clone(),
                snapshot_id: created.snapshot_id.clone(),
            }
        );
    }
    assert!(list_snapshots(&harness.server_url, namespace.as_str())
        .expect("list after delete")
        .snapshots
        .is_empty());
    let diagnostics = harness
        .client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read diagnostics after delete");
    assert_eq!(diagnostics.live_snapshots, 0);
    let (status, error) = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        20_000,
    )
    .expect_err("deleted snapshot cannot extend");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");

    harness.server.abort();
}

/// Checks that snapshot routes reject a zero TTL, an empty name, and a
/// malformed ID, that extending an unknown snapshot returns
/// `snapshot_not_found`, and that deleting it succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_validate_names_ttls_and_ids() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "snapshot-validation",
        "snapshot-validation",
    );
    let harness = start_server(config).await;
    let namespace = namespace_id("validation");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let (status, error) = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "invalid-ttl",
        Some(0),
    )
    .expect_err("a zero ttl must fail");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("/ttl_ms"));
    let (status, error) = create_snapshot(&harness.server_url, namespace.as_str(), "", Some(500))
        .expect_err("empty name must fail");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");

    let (status, error) =
        extend_snapshot(&harness.server_url, namespace.as_str(), "malformed", 500)
            .expect_err("malformed snapshot id must fail");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");

    let unknown = "pin_00000000000000000001-ffffffffffffffff";
    let (status, error) = extend_snapshot(&harness.server_url, namespace.as_str(), unknown, 500)
        .expect_err("unknown snapshot must fail extension");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");
    delete_snapshot(&harness.server_url, namespace.as_str(), unknown)
        .expect("deleting an unknown snapshot succeeds");

    harness.server.abort();
}

/// Checks that the snapshot listing and the checkpoint inventory list the
/// same snapshot as one pin, that the inventory also lists fork pins, and
/// that the snapshot routes refuse a fork pin's ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_and_the_checkpoint_inventory_list_the_same_pin() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "snapshot-inventory",
        "snapshot-inventory",
    );
    config.maintenance = JobMode::ServeOnly;
    let harness = start_server(config).await;
    let namespace = namespace_id("inventory");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let expiring = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "brief",
        Some(60_000),
    )
    .expect("create snapshot");
    let unexpiring = create_snapshot(&harness.server_url, namespace.as_str(), "kept", None)
        .expect("create a snapshot without an expiry");
    assert_eq!(unexpiring.expires_at_ms, None);
    harness
        .client
        .fork_namespace(
            &namespace,
            &namespace_id("inventory-fork"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("fork namespace");

    let checkpoints: serde_json::Value = retry_result_on_macos_teardown_einval(|| {
        decode_response(
            raw_agent()
                .get(&format!(
                    "{}/v0/maintenance/namespaces/{}/checkpoints",
                    harness.server_url, namespace
                ))
                .set("authorization", "Bearer test-token")
                .call(),
        )
    })
    .expect("list checkpoints");
    let checkpoints = checkpoints["checkpoints"]
        .as_array()
        .expect("checkpoint list is an array");
    assert_eq!(checkpoints.len(), 3);
    let listed_snapshot = checkpoints
        .iter()
        .find(|item| item["checkpoint_id"] == expiring.snapshot_id.as_str())
        .expect("the snapshot is in the checkpoint inventory");
    assert_eq!(
        listed_snapshot["owner"],
        serde_json::json!({"kind": "user", "name": "brief"})
    );
    assert_eq!(
        listed_snapshot["expires_at_ms"],
        serde_json::json!(expiring.expires_at_ms)
    );
    let fork_pin = checkpoints
        .iter()
        .find(|item| item["owner"]["kind"] == "fork")
        .expect("the fork pin is in the checkpoint inventory")["checkpoint_id"]
        .as_str()
        .expect("fork pin id")
        .to_owned();
    let mut snapshots = list_snapshots(&harness.server_url, namespace.as_str())
        .expect("list snapshots")
        .snapshots;
    snapshots.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(snapshots, vec![expiring, unexpiring]);

    let (status, error) = delete_snapshot(&harness.server_url, namespace.as_str(), &fork_pin)
        .expect_err("the snapshot delete refuses a fork pin");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");
    let (status, error) = extend_snapshot(&harness.server_url, namespace.as_str(), &fork_pin, 500)
        .expect_err("the snapshot extension refuses a fork pin");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");

    harness.server.abort();
}

#[tokio::test]
async fn snapshot_lifecycle_round_trips_through_the_client() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "client-snapshot-lifecycle",
        "client-snapshot-lifecycle",
    );
    let harness = start_server(config).await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let created = harness
        .client
        .create_snapshot(&namespace, "first", std::num::NonZeroU64::new(5_000))
        .await
        .expect("create snapshot");
    assert_eq!(created.namespace_id, namespace);
    assert_eq!(created.name, "first");
    assert_eq!(created.captured_seq, ChangeSeq(0));

    let mut pager = harness.client.list_snapshots(&namespace);
    assert_eq!(
        pager.collect_up_to(10).await.expect("list snapshots"),
        vec![created.clone()]
    );

    let extended = harness
        .client
        .extend_snapshot(
            &namespace,
            &created.snapshot_id,
            std::num::NonZeroU64::new(10_000).expect("a nonzero ttl"),
        )
        .await
        .expect("extend snapshot");
    assert!(extended.expires_at_ms > created.expires_at_ms);

    harness
        .client
        .delete_snapshot(&namespace, &created.snapshot_id)
        .await
        .expect("delete snapshot");
    assert!(harness
        .client
        .list_snapshots(&namespace)
        .page()
        .await
        .expect("list deleted snapshots")
        .snapshots
        .is_empty());
    harness.server.abort();
}

#[tokio::test]
async fn snapshot_and_checkpoint_listings_reject_each_others_cursors() {
    use loonfs_client::ClientError;
    use loonfs_test_support::ids::page_limit;

    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "snapshot-cursor-kind",
        "snapshot-cursor-kind",
    );
    let harness = start_server(config).await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    for name in ["first", "second"] {
        harness
            .client
            .create_snapshot(&namespace, name, std::num::NonZeroU64::new(5_000))
            .await
            .expect("create snapshot");
    }
    let snapshot_cursor = harness
        .client
        .list_snapshots(&namespace)
        .page_limit(page_limit(1))
        .page()
        .await
        .expect("first snapshot page")
        .next_cursor
        .expect("snapshot cursor");
    let resumed = harness
        .client
        .list_snapshots(&namespace)
        .page_limit(page_limit(1))
        .cursor(Some(snapshot_cursor.clone()))
        .page()
        .await
        .expect("a snapshot cursor resumes the snapshot listing");
    assert_eq!(resumed.snapshots.len(), 1);
    let checkpoint_cursor = harness
        .client
        .list_checkpoints(&namespace)
        .page_limit(page_limit(1))
        .page()
        .await
        .expect("first checkpoint page")
        .next_cursor
        .expect("checkpoint cursor");

    let rejections = [
        harness
            .client
            .list_snapshots(&namespace)
            .page_limit(page_limit(1))
            .cursor(Some(checkpoint_cursor))
            .page()
            .await
            .map(drop),
        harness
            .client
            .list_checkpoints(&namespace)
            .page_limit(page_limit(1))
            .cursor(Some(snapshot_cursor))
            .page()
            .await
            .map(drop),
    ];
    for rejection in rejections {
        match rejection {
            Err(ClientError::Api {
                status,
                code,
                param,
                ..
            }) => {
                assert_eq!((status, code.as_str()), (400, "invalid_request"));
                assert_eq!(param.as_deref(), Some("cursor"));
            }
            other => panic!("expected an invalid cursor, got {other:?}"),
        }
    }
    harness.server.abort();
}

#[tokio::test]
async fn snapshot_and_checkpoint_retrieves_return_their_listed_rows() {
    use loonfs_client::ClientError;

    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "pin-retrieve",
        "pin-retrieve",
    );
    let harness = start_server(config).await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let created = harness
        .client
        .create_snapshot(&namespace, "first", std::num::NonZeroU64::new(5_000))
        .await
        .expect("create snapshot");

    let listed = harness
        .client
        .list_snapshots(&namespace)
        .page()
        .await
        .expect("list snapshots");
    let retrieved = harness
        .client
        .get_snapshot(&namespace, &created.snapshot_id)
        .await
        .expect("retrieve snapshot");
    assert_eq!(listed.snapshots, vec![retrieved]);
    let checkpoints = harness
        .client
        .list_checkpoints(&namespace)
        .page()
        .await
        .expect("list checkpoints");
    let checkpoint = harness
        .client
        .get_checkpoint(&namespace, &created.snapshot_id)
        .await
        .expect("retrieve checkpoint");
    assert_eq!(checkpoints.checkpoints, vec![checkpoint]);

    harness
        .client
        .delete_snapshot(&namespace, &created.snapshot_id)
        .await
        .expect("delete snapshot");
    let code = |error: ClientError| match error {
        ClientError::Api { status, code, .. } => (status, code),
        other => panic!("expected an API error, got {other:?}"),
    };
    assert_eq!(
        code(
            harness
                .client
                .get_snapshot(&namespace, &created.snapshot_id)
                .await
                .expect_err("a deleted snapshot is not found")
        ),
        (404, "snapshot_not_found".to_owned())
    );
    assert_eq!(
        code(
            harness
                .client
                .get_checkpoint(&namespace, &created.snapshot_id)
                .await
                .expect_err("a deleted pin is not found")
        ),
        (404, "snapshot_not_found".to_owned())
    );
    harness.server.abort();
}
