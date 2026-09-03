//! HTTP checkpoint, retention, garbage collection, and maintenance operations.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::{collect_checkpoints, start_server};
use bytes::Bytes;
use loonfs_api::{
    ApiError, ChangeSeq, Checkpoint, CheckpointId, CheckpointOwnerSummary, ManifestNo,
    ReleaseCheckpointResponse,
};
use loonfs_client::{ClientError, NamespacePath};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ConfiguredObjectStore, ObjectStore};
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

type ApiResult<T> = Result<T, Box<ApiError>>;

fn post_checkpoint(server_url: &str, namespace: &str) -> ApiResult<Checkpoint> {
    post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/checkpoints"),
        "test-token",
        serde_json::json!({ "name": "nightly" }),
    )
}

fn post_checkpoint_release(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> ApiResult<loonfs_api::ReleaseCheckpointResponse> {
    post_maintenance_json(
        &format!(
            "{server_url}/v0/maintenance/namespaces/{namespace}/checkpoints/{checkpoint_id}/release"
        ),
        "test-token",
    )
}

fn post_gc(server_url: &str, namespace: &str) -> ApiResult<loonfs_api::GcResponse> {
    post_gc_with(server_url, namespace, serde_json::json!({}))
}

fn upkeep(
    response: &loonfs_api::MaintenanceRunResponse,
) -> &loonfs_api::MetadataMaintenanceResponse {
    let loonfs_api::MaintenanceRunResponse::Metadata(metadata) = response else {
        panic!("metadata request returned a different response")
    };
    metadata
}

fn retention_floor(response: loonfs_api::MaintenanceRunResponse) -> ChangeSeq {
    let loonfs_api::MaintenanceRunResponse::Retention(retention) = response else {
        panic!("retention request returned a different response")
    };
    retention.retention_floor_seq
}

fn post_gc_with(
    server_url: &str,
    namespace: &str,
    gc: serde_json::Value,
) -> ApiResult<loonfs_api::GcResponse> {
    let mut request = gc;
    request
        .as_object_mut()
        .expect("GC options are an object")
        .insert("kind".to_owned(), serde_json::json!("gc"));
    let response: ApiResult<loonfs_api::MaintenanceRunResponse> = post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
        request,
    );
    response.map(|response| {
        let loonfs_api::MaintenanceRunResponse::Gc(gc) = response else {
            panic!("GC request returned a different response")
        };
        gc
    })
}

fn post_maintenance_step(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_api::MaintenanceRunResponse> {
    post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
        serde_json::json!({ "kind": "metadata" }),
    )
}

fn post_missing_maintenance_body(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_api::MaintenanceRunResponse> {
    post_maintenance_json(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
    )
}

fn post_retention_advance(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_api::MaintenanceRunResponse> {
    post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
        serde_json::json!({ "kind": "retention" }),
    )
}

fn post_maintenance_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        let request = raw_agent()
            .post(url)
            .set("authorization", &format!("Bearer {auth_token}"));
        decode_maintenance_response(request.call())
    })
}

fn post_maintenance_json_body<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
    body: serde_json::Value,
) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        let request = raw_agent()
            .post(url)
            .set("authorization", &format!("Bearer {auth_token}"));
        decode_maintenance_response(request.send_json(body.clone()))
    })
}

fn decode_maintenance_response<T: serde::de::DeserializeOwned>(
    result: Result<ureq::Response, ureq::Error>,
) -> ApiResult<T> {
    match result {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| {
            Box::new(ApiError {
                code: "invalid_json".to_owned(),
                feature: None,
                message: err.to_string(),
                param: None,
                request_id: None,
                details: None,
            })
        }),
        Err(ureq::Error::Status(_, response)) => Err(Box::new(
            serde_json::from_reader::<_, ApiError>(response.into_reader()).unwrap_or_else(|err| {
                ApiError {
                    code: "invalid_json".to_owned(),
                    feature: None,
                    message: err.to_string(),
                    param: None,
                    request_id: None,
                    details: None,
                }
            }),
        )),
        Err(ureq::Error::Transport(error)) => Err(Box::new(ApiError {
            code: "transport".to_owned(),
            feature: None,
            message: error.to_string(),
            param: None,
            request_id: None,
            details: None,
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_checkpoint_and_retention_are_idempotent_and_soft() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-maintenance",
        "http-maintenance",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    let namespace = namespace_id("demo");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    client
        .put_file_bytes(&target, b"hello maintenance\n", &replace_file_options())
        .await
        .expect("write file");

    let first = post_checkpoint(&server_url, namespace.as_str()).expect("first checkpoint");
    assert!(CheckpointId::parse(first.checkpoint_id.as_str()).is_ok());
    assert_eq!(
        first.owner,
        CheckpointOwnerSummary::User {
            name: "nightly".to_owned()
        }
    );
    assert_eq!(first.checkpoint_seq, ChangeSeq(1));
    assert_eq!(first.manifest_no, ManifestNo(1));
    let listed = collect_checkpoints(&client, &namespace)
        .await
        .expect("list first checkpoint");
    assert_eq!(listed.checkpoints, vec![first.clone()]);

    // A second checkpoint at the same head creates a new record.
    let repeated = post_checkpoint(&server_url, namespace.as_str()).expect("repeat checkpoint");
    assert_ne!(repeated.checkpoint_id, first.checkpoint_id);
    assert_eq!(repeated.namespace_id, first.namespace_id);
    assert_eq!(repeated.owner, first.owner);
    assert_eq!(repeated.checkpoint_seq, first.checkpoint_seq);
    assert_eq!(repeated.manifest_no, first.manifest_no);
    assert_eq!(repeated.expires_at_ms, first.expires_at_ms);
    assert!(repeated.created_at_ms >= first.created_at_ms);
    client
        .fork_namespace(&namespace, &namespace_id("fork"))
        .await
        .expect("fork namespace");
    let diagnostics = client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read checkpoint diagnostics");
    assert_eq!(diagnostics.live_snapshots, 0);
    assert_eq!(diagnostics.live_checkpoints, 2);

    // Releasing a checkpoint twice returns the same result.
    let released = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect("release checkpoint");
    assert_eq!(
        released,
        ReleaseCheckpointResponse {
            namespace_id: namespace.clone(),
            checkpoint_id: first.checkpoint_id.clone(),
        }
    );
    let released_again = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect("repeat release");
    assert_eq!(released_again, released);
    let diagnostics = client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read diagnostics after release");
    assert_eq!(diagnostics.live_checkpoints, 1);
    let bogus_release =
        post_checkpoint_release(&server_url, namespace.as_str(), "not-a-checkpoint-id")
            .expect_err("malformed checkpoint id");
    assert_eq!(bogus_release.code, "invalid_request");

    // Reject grace periods below the safety minimum.
    let unsafe_gc = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "grace_window_ms": 1 }),
    );
    let unsafe_gc = unsafe_gc.expect_err("sub-minimum grace window is rejected");
    assert_eq!(unsafe_gc.code, "invalid_request");
    assert!(unsafe_gc.message.contains("derived safety minimum"));

    let advanced = retention_floor(
        post_retention_advance(&server_url, namespace.as_str()).expect("advance retention"),
    );
    assert_eq!(advanced, ChangeSeq(1));

    // Both calls reach the same floor.
    let repeated =
        post_retention_advance(&server_url, namespace.as_str()).expect("repeat retention");
    assert_eq!(retention_floor(repeated), advanced);

    let bytes = client
        .get_file_bytes(&target, &Default::default())
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello maintenance\n");

    match client
        .list_changes(&namespace, ChangeSeq(0), &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
        other => panic!("expected rebootstrap_required, got {other:?}"),
    }

    let empty = client
        .list_changes(&namespace, ChangeSeq(1), &Default::default())
        .await
        .expect("changes after floor");
    assert_eq!(empty.changes, Vec::new());

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_gc_is_explicit_and_retains_young_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-gc",
        "http-maintenance-gc",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    let namespace = namespace_id("demo");
    client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .put_file_bytes(&target, b"hello gc\n", &replace_file_options())
        .await
        .expect("write file");
    post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");

    // Resume a bounded pass with its returned cursor.
    let bounded = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "max_objects": 7 }),
    )
    .expect("bounded gc pass");
    let cursor = bounded.next_cursor.expect("more candidate families remain");
    let resumed = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "max_objects": 7, "cursor": cursor }),
    )
    .expect("resumed gc pass");
    assert!(resumed.next_cursor.is_some());

    // Objects inside the grace window remain readable.
    let report = post_gc(&server_url, namespace.as_str()).expect("gc pass");
    assert_eq!(report.deleted.wal_segments, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.deleted.checkpoint_records, 0);
    assert!(!report.retention_degraded);
    assert!(report.next_cursor.is_none());

    let bytes = client
        .get_file_bytes(&target, &Default::default())
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello gc\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_step_reports_outcomes_not_errors() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-step",
        "http-maintenance-step",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    let namespace = namespace_id("demo");
    client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .put_file_bytes(&target, b"hello step\n", &replace_file_options())
        .await
        .expect("write file");

    let empty = post_missing_maintenance_body(&server_url, namespace.as_str())
        .expect_err("a missing body is refused");
    assert_eq!(empty.code, "invalid_request");

    let idle = post_maintenance_step(&server_url, namespace.as_str()).expect("idle step");
    assert_eq!(
        upkeep(&idle).wal_flush,
        loonfs_api::WalFlushStepOutcome::NotNeeded
    );

    let forced = client
        .run_maintenance(
            &namespace,
            &loonfs_api::MaintenanceRunRequest::Metadata(loonfs_api::MetadataMaintenanceRequest {
                max_wal_tail_segments: Some(1),
            }),
        )
        .await
        .expect("forced step");
    assert_eq!(
        upkeep(&forced).wal_flush,
        loonfs_api::WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1),
        }
    );
    assert_eq!(
        upkeep(&forced).reorganize,
        loonfs_api::ReorganizeStepOutcome::NotNeeded
    );
    let retention = client
        .run_maintenance(
            &namespace,
            &loonfs_api::MaintenanceRunRequest::Retention(loonfs_api::AdvanceRetentionRequest {}),
        )
        .await
        .expect("advance retention");
    assert_eq!(retention_floor(retention), ChangeSeq(1));
    let gc = post_gc(&server_url, namespace.as_str()).expect("GC run");
    assert_eq!(gc.deleted.wal_segments, 0);
    assert!(!gc.retention_degraded);

    let bytes = client
        .get_file_bytes(&target, &Default::default())
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello step\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_retention_advance_uses_initial_manifest_after_create() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-maintenance-missing-checkpoint",
        "http-maintenance-missing-checkpoint",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    let namespace = namespace_id("demo");
    client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let advanced = retention_floor(
        post_retention_advance(&server_url, namespace.as_str()).expect("advance retention"),
    );
    assert_eq!(advanced, ChangeSeq(0));

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_checkpoint_manifest_consumption_is_strict_when_manifest_is_corrupted() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let harness = start_server(test_config(
        store_root.clone(),
        "loonfs-server-maintenance-corrupt",
        "http-maintenance-corrupt",
    ))
    .await;
    // A new server must read the corrupted manifest; the first server has a valid cached snapshot.
    let cold = start_server(test_config(
        store_root,
        "loonfs-server-cold-reader",
        "http-maintenance-corrupt",
    ))
    .await;
    let client = harness.client.clone();
    let cold_client = cold.client.clone();
    let server_url = harness.server_url.clone();
    let store_root = harness
        .store_root
        .clone()
        .expect("local test server has a store root");
    let store_key_prefix = harness.store_key_prefix.clone();

    let namespace = namespace_id("demo");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    client
        .put_file_bytes(&target, b"hello\n", &replace_file_options())
        .await
        .expect("write file");
    post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");

    let store = ConfiguredObjectStore::local_fs(&store_root, store_key_prefix.as_deref())
        .expect("construct store")
        .into_shared();
    let root = loonfs::control::load_namespace_metadata_root_control(&store, &namespace)
        .await
        .expect("metadata root");
    store
        .put_overwrite(
            &metadata_manifest_object(&namespace, &root.state.manifest.manifest_object_id),
            Bytes::from_static(br#"{"bad":"json"}"#),
        )
        .await
        .expect("corrupt manifest");

    match cold_client
        .get_path_entry(&target, &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
        other => panic!("expected namespace_corrupt, got {other:?}"),
    }
    client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("warm server reads from its pinned head-plus-manifest pair");

    harness.server.abort();
    cold.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_store_probe_reports_unique_successes_from_the_configured_store() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-probe",
        "http-maintenance-probe",
    ))
    .await;

    let probe: loonfs_api::v0::StoreProbeResponse = post_maintenance_json_body(
        &format!("{}/v0/maintenance/store/probe", harness.server_url),
        "test-token",
        serde_json::json!({}),
    )
    .expect("probe the configured store");

    assert!(probe.run_id.starts_with("probe_"));
    assert!(!probe.checks.is_empty(), "the probe must report its work");
    let names: std::collections::BTreeSet<&str> = probe
        .checks
        .iter()
        .map(|check| check.name.as_str())
        .collect();
    assert_eq!(
        names.len(),
        probe.checks.len(),
        "the serialized report must not repeat check names"
    );
    for check in &probe.checks {
        assert_ne!(
            check.outcome,
            loonfs_api::v0::StoreProbeCheckOutcome::Failed,
            "the local filesystem store should honour every contract check: {check:?}"
        );
        assert_eq!(check.message, None);
    }

    // The probe removes its temporary objects.
    let store = ConfiguredObjectStore::local_fs(
        harness.store_root.as_ref().expect("local-fs test store"),
        harness.store_key_prefix.as_deref(),
    )
    .expect("open the test store")
    .into_shared();
    assert!(store
        .list_prefix("probe-runs/")
        .await
        .expect("list the probe prefix")
        .is_empty());

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_maintenance_store_probe_requires_a_token_and_accepts_a_bodyless_request() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-probe-auth",
        "http-maintenance-probe-auth",
    ))
    .await;
    let url = format!("{}/v0/maintenance/store/probe", harness.server_url);

    let unauthorized: ApiResult<loonfs_api::v0::StoreProbeResponse> =
        post_maintenance_json_body(&url, "wrong-token", serde_json::json!({}));
    assert_eq!(
        unauthorized.expect_err("a wrong token is refused").code,
        "unauthorized"
    );

    // An absent body is treated as an empty object.
    let bodyless: loonfs_api::v0::StoreProbeResponse =
        post_maintenance_json(&url, "test-token").expect("probe with no body");
    assert!(
        !bodyless.checks.is_empty(),
        "a bodyless request runs the probe rather than selecting nothing"
    );

    harness.server.abort();
}
