//! HTTP read-snapshot lifecycle operations.

#![allow(clippy::panic)]

use crate::common::http_split_support::test_config;
use crate::common::{collect_checkpoints, start_server};
use loonfs_api::{
    ApiError, ChangeSeq, CheckpointOwnerSummary, CreateCheckpointRequest, ListSnapshotsResponse,
    ReleaseSnapshotResponse, SnapshotSummary,
};
use loonfs_server::MaintenanceMode;
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
use serde::de::DeserializeOwned;
use tempfile::tempdir;

type ApiResult<T> = Result<T, (u16, Box<ApiError>)>;

fn create_snapshot(
    server_url: &str,
    namespace: &str,
    name: &str,
    ttl_ms: u64,
) -> ApiResult<SnapshotSummary> {
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
) -> ApiResult<SnapshotSummary> {
    post_json_body(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots/{snapshot_id}/extend"),
        serde_json::json!({"ttl_ms": ttl_ms}),
    )
}

fn release_snapshot(
    server_url: &str,
    namespace: &str,
    snapshot_id: &str,
) -> ApiResult<ReleaseSnapshotResponse> {
    post_json(&format!(
        "{server_url}/v0/namespaces/{namespace}/snapshots/{snapshot_id}/release"
    ))
}

fn release_checkpoint(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> ApiResult<loonfs_api::ReleaseCheckpointResponse> {
    post_json(&format!(
        "{server_url}/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release"
    ))
}

fn post_json<T: DeserializeOwned>(url: &str) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        decode_response(
            raw_agent()
                .post(url)
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
    config.snapshot_max_lifetime_ms = 20_000;
    let max_lifetime_ms = config.snapshot_max_lifetime_ms;
    let harness = start_server(config).await;
    let namespace = namespace_id("lifecycle");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let before_create = loonfs::current_time_ms().expect("current time");
    let created = create_snapshot(&harness.server_url, namespace.as_str(), "report", 5_000)
        .expect("create snapshot");
    let after_create = loonfs::current_time_ms().expect("current time");
    assert_eq!(created.namespace_id, namespace);
    assert_eq!(created.name, "report");
    assert_eq!(created.head_seq, ChangeSeq(0));
    assert!(created.created_at_ms >= before_create);
    assert!(created.created_at_ms <= after_create);
    assert!(created.expires_at_ms >= before_create + 5_000);
    assert!(created.expires_at_ms <= after_create + 5_000);

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
    assert_eq!(
        extended.expires_at_ms,
        extended.created_at_ms + max_lifetime_ms
    );
    let repeated = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        20_000,
    )
    .expect("repeat snapshot extension");
    assert_eq!(repeated.expires_at_ms, extended.expires_at_ms);

    let released = release_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
    )
    .expect("release snapshot");
    assert_eq!(
        released,
        ReleaseSnapshotResponse {
            namespace_id: namespace.clone(),
            snapshot_id: created.snapshot_id.clone(),
        }
    );
    assert!(list_snapshots(&harness.server_url, namespace.as_str())
        .expect("list after release")
        .snapshots
        .is_empty());
    assert_eq!(
        release_snapshot(
            &harness.server_url,
            namespace.as_str(),
            created.snapshot_id.as_str(),
        )
        .expect("repeat release"),
        released
    );
    let (status, error) = extend_snapshot(
        &harness.server_url,
        namespace.as_str(),
        created.snapshot_id.as_str(),
        20_000,
    )
    .expect_err("released snapshot cannot extend");
    assert_eq!(status, 410);
    assert_eq!(error.code, "snapshot_gone");
    assert!(error.message.contains("released"));

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
    config.snapshot_max_lifetime_ms = 2_000;
    let harness = start_server(config).await;
    let namespace = namespace_id("validation");
    harness
        .client
        .create_namespace(&namespace)
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

    let unknown = "chk_ffffffffffffffffffffffffffffffff";
    let (status, error) = extend_snapshot(&harness.server_url, namespace.as_str(), unknown, 500)
        .expect_err("unknown snapshot must fail extension");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");
    let released = release_snapshot(&harness.server_url, namespace.as_str(), unknown)
        .expect("unknown snapshot release is idempotent");
    assert_eq!(released.snapshot_id.as_str(), unknown);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_snapshots_enforce_quota_and_release_frees_a_slot() {
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
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let first = create_snapshot(&harness.server_url, namespace.as_str(), "first", 10_000)
        .expect("create first snapshot");
    let (status, error) =
        create_snapshot(&harness.server_url, namespace.as_str(), "second", 10_000)
            .expect_err("quota must refuse second snapshot");
    assert_eq!(status, 409);
    assert_eq!(error.code, "snapshot_quota_exceeded");
    release_snapshot(
        &harness.server_url,
        namespace.as_str(),
        first.snapshot_id.as_str(),
    )
    .expect("release first snapshot");
    create_snapshot(&harness.server_url, namespace.as_str(), "second", 10_000)
        .expect("released snapshot frees quota");

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
    config.maintenance = MaintenanceMode::Manual;
    let harness = start_server(config).await;
    let namespace = namespace_id("separation");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let snapshot = create_snapshot(&harness.server_url, namespace.as_str(), "brief", 500)
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

    let (status, error) = release_checkpoint(
        &harness.server_url,
        namespace.as_str(),
        snapshot.snapshot_id.as_str(),
    )
    .expect_err("admin release must refuse snapshot");
    assert_eq!(status, 400);
    assert!(error.message.contains("snapshot release operation"));
    let (status, error) = release_snapshot(
        &harness.server_url,
        namespace.as_str(),
        checkpoint.checkpoint_id.as_str(),
    )
    .expect_err("snapshot release must refuse user checkpoint");
    assert_eq!(status, 400);
    assert!(error.message.contains("checkpoint release operation"));

    let checkpoints = collect_checkpoints(&harness.client, &namespace)
        .await
        .expect("list checkpoints");
    assert_eq!(checkpoints.checkpoints.len(), 2);
    assert!(checkpoints.checkpoints.iter().any(|item| {
        matches!(
            &item.owner,
            CheckpointOwnerSummary::Snapshot { name, .. } if name == "brief"
        )
    }));
    assert_eq!(
        list_snapshots(&harness.server_url, namespace.as_str())
            .expect("list live snapshots")
            .snapshots,
        vec![snapshot.clone()]
    );

    harness.server.abort();
}
