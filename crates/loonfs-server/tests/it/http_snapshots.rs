//! HTTP read-snapshot lifecycle operations.

use crate::common::http_split_support::test_config;
use crate::common::start_server;
use loonfs_client::{NamespacePath, PutFileOptions, ReadFileOptions, StatOptions};
use loonfs_server::JobMode;
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::{first_page, namespace_id};
use loonfs_types::{
    ChangeSeq, CreateCheckpointRequest, DeleteSnapshotResponse, DestinationBehavior, ErrorResponse,
    ListSnapshotsResponse, NamespaceId, Snapshot,
};
use serde::de::DeserializeOwned;
use tempfile::tempdir;

type ApiResult<T> = Result<T, (u16, Box<ErrorResponse>)>;

fn create_snapshot(
    server_url: &str,
    namespace: &str,
    name: &str,
    ttl_ms: u64,
) -> ApiResult<Snapshot> {
    post_json_body(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots"),
        serde_json::json!({"name": name, "ttl_ms": ttl_ms}),
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

fn delete_checkpoint(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> ApiResult<loonfs_types::DeleteCheckpointResponse> {
    delete_json(&format!(
        "{server_url}/v0/maintenance/namespaces/{namespace}/checkpoints/{checkpoint_id}"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_lifecycle_is_live_extendable_and_releasable() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "snapshot-lifecycle",
        "snapshot-lifecycle",
    );
    config.snapshot_max_ttl_ms = 20_000;
    config.snapshot_max_age_ms = 20_000;
    let max_age_ms = config.snapshot_max_age_ms;
    let harness = start_server(config).await;
    let namespace = namespace_id("lifecycle");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let before_create = loonfs::current_time_ms();
    let created = create_snapshot(&harness.server_url, namespace.as_str(), "report", 5_000)
        .expect("create snapshot");
    let after_create = loonfs::current_time_ms();
    assert_eq!(created.namespace_id, namespace);
    assert_eq!(created.name, "report");
    assert_eq!(created.captured_seq, ChangeSeq(0));
    assert!(created.created_at_ms >= before_create);
    assert!(created.created_at_ms <= after_create);
    assert!(created.expires_at_ms >= before_create + 5_000);
    assert!(created.expires_at_ms <= after_create + 5_000);
    let diagnostics = harness
        .client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read snapshot diagnostics");
    assert_eq!(diagnostics.live_snapshots, 1);
    assert_eq!(diagnostics.live_checkpoints, 0);

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
    assert_eq!(extended.expires_at_ms, extended.created_at_ms + max_age_ms);
    let repeated = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        20_000,
    )
    .expect("repeat snapshot extension");
    assert_eq!(repeated.expires_at_ms, extended.expires_at_ms);

    let deleted = delete_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
    )
    .expect("delete snapshot");
    assert_eq!(
        deleted,
        DeleteSnapshotResponse {
            namespace_id: namespace.clone(),
            snapshot_id: created.snapshot_id.clone(),
        }
    );
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
    let (status, error) = delete_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
    )
    .expect_err("repeat delete");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_validate_names_ttls_and_ids() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "snapshot-validation",
        "snapshot-validation",
    );
    config.snapshot_max_ttl_ms = 1_000;
    config.snapshot_max_age_ms = 2_000;
    let harness = start_server(config).await;
    let namespace = namespace_id("validation");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    for ttl_ms in [0, 1_001] {
        let (status, error) = create_snapshot(
            &harness.server_url,
            namespace.as_str(),
            "invalid-ttl",
            ttl_ms,
        )
        .expect_err("invalid ttl must fail");
        assert_eq!(status, 400);
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains("snapshot.max_ttl_ms"));
    }
    let (status, error) = create_snapshot(&harness.server_url, namespace.as_str(), "", 500)
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
    let (status, error) = delete_snapshot(&harness.server_url, namespace.as_str(), unknown)
        .expect_err("unknown snapshot delete");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_enforce_quota_and_delete_frees_a_slot() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "snapshot-quota",
        "snapshot-quota",
    );
    config.snapshot_max_live_per_namespace = 1;
    let harness = start_server(config).await;
    let namespace = namespace_id("quota");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let first = create_snapshot(&harness.server_url, namespace.as_str(), "first", 10_000)
        .expect("create first snapshot");
    let (status, error) =
        create_snapshot(&harness.server_url, namespace.as_str(), "second", 10_000)
            .expect_err("quota must refuse second snapshot");
    assert_eq!(status, 409);
    assert_eq!(error.code, "snapshot_quota_exceeded");
    delete_snapshot(
        &harness.server_url,
        namespace.as_str(),
        first.snapshot_id.as_str(),
    )
    .expect("delete first snapshot");
    create_snapshot(&harness.server_url, namespace.as_str(), "second", 10_000)
        .expect("deleted snapshot frees quota");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_keep_owner_operations_and_listings_separate() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "snapshot-separation",
        "snapshot-separation",
    );
    config.maintenance = JobMode::ServeOnly;
    let harness = start_server(config).await;
    let namespace = namespace_id("separation");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let snapshot = create_snapshot(&harness.server_url, namespace.as_str(), "brief", 60_000)
        .expect("create snapshot");
    let checkpoint = harness
        .client
        .create_checkpoint(
            &namespace,
            &CreateCheckpointRequest {
                name: "operator".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("create user checkpoint");

    let (status, error) = delete_checkpoint(
        &harness.server_url,
        namespace.as_str(),
        snapshot.snapshot_id.as_str(),
    )
    .expect_err("maintenance delete must refuse snapshot");
    assert_eq!(status, 404);
    assert_eq!(error.code, "checkpoint_not_found");
    let (status, error) = delete_snapshot(
        &harness.server_url,
        namespace.as_str(),
        checkpoint.checkpoint_id.as_str(),
    )
    .expect_err("snapshot delete must refuse user checkpoint");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");

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
    assert_eq!(checkpoints.len(), 2);
    let listed_snapshot = checkpoints
        .iter()
        .find(|item| item["owner"]["kind"] == "snapshot")
        .expect("snapshot checkpoint is listed");
    assert_eq!(
        listed_snapshot["owner"],
        serde_json::json!({"kind": "snapshot", "name": "brief"})
    );
    assert_eq!(
        listed_snapshot["expires_at_ms"],
        serde_json::json!(snapshot.expires_at_ms)
    );
    assert_eq!(
        list_snapshots(&harness.server_url, namespace.as_str())
            .expect("list live snapshots")
            .snapshots,
        vec![snapshot.clone()]
    );

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
        .create_snapshot(&namespace, "first", 5_000)
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
        .extend_snapshot(&namespace, &created.snapshot_id, 10_000)
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
        .page(first_page())
        .await
        .expect("list deleted snapshots")
        .snapshots
        .is_empty());
    harness.server.abort();
}

#[tokio::test]
async fn snapshot_file_read_returns_the_captured_state() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        "client-snapshot-read",
        "client-snapshot-read",
    );
    let harness = start_server(config).await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    let path = NamespacePath::parse("demo", "/report.txt").expect("namespace path");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    harness
        .client
        .put_file(&path, b"captured", &loonfs_test_support::test_actor())
        .await
        .expect("write captured content");
    let snapshot = harness
        .client
        .create_snapshot(&namespace, "captured", 10_000)
        .await
        .expect("create snapshot");

    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..Default::default()
    };
    harness
        .client
        .put_file_with_options(
            &path,
            b"current",
            &loonfs_test_support::test_actor(),
            &replace,
        )
        .await
        .expect("replace content");

    let entry = harness
        .client
        .stat_with_options(
            &path,
            &StatOptions {
                snapshot_id: Some(snapshot.snapshot_id.clone()),
                ..StatOptions::default()
            },
        )
        .await
        .expect("snapshot stat");
    assert_eq!(entry.head_seq, snapshot.captured_seq);
    let bytes = harness
        .client
        .read_file_with_options(
            &path,
            &ReadFileOptions {
                snapshot_id: Some(snapshot.snapshot_id),
            },
        )
        .await
        .expect("snapshot content");
    assert_eq!(bytes, b"captured");
    assert_eq!(
        harness
            .client
            .read_file(&path)
            .await
            .expect("current content"),
        b"current"
    );
    harness.server.abort();
}

#[tokio::test]
async fn snapshot_and_checkpoint_listings_reject_each_others_cursors() {
    use loonfs_client::ClientError;
    use loonfs_test_support::ids::page_limit;
    use loonfs_types::PageRequest;

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
            .create_snapshot(&namespace, name, 5_000)
            .await
            .expect("create snapshot");
    }
    let first_of = |cursor| PageRequest {
        limit: page_limit(1),
        cursor,
    };
    let snapshot_cursor = harness
        .client
        .list_snapshots(&namespace)
        .page(first_of(None))
        .await
        .expect("first snapshot page")
        .next_cursor
        .expect("snapshot cursor");
    let resumed = harness
        .client
        .list_snapshots(&namespace)
        .page(first_of(Some(snapshot_cursor.clone())))
        .await
        .expect("a snapshot cursor resumes the snapshot listing");
    assert_eq!(resumed.snapshots.len(), 1);
    let checkpoint_cursor = harness
        .client
        .list_checkpoints(&namespace)
        .page(first_of(None))
        .await
        .expect("first checkpoint page")
        .next_cursor
        .expect("checkpoint cursor");

    let rejections = [
        harness
            .client
            .list_snapshots(&namespace)
            .page(first_of(Some(checkpoint_cursor)))
            .await
            .map(drop),
        harness
            .client
            .list_checkpoints(&namespace)
            .page(first_of(Some(snapshot_cursor)))
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
        .create_snapshot(&namespace, "first", 5_000)
        .await
        .expect("create snapshot");

    let listed = harness
        .client
        .list_snapshots(&namespace)
        .page(first_page())
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
        .page(first_page())
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
        (404, "checkpoint_not_found".to_owned())
    );
    harness.server.abort();
}
