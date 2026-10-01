//! HTTP checkpoint, retention, garbage collection, and maintenance operations.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::{collect_checkpoints, start_server};
use bytes::Bytes;
use loonfs_client::{ClientError, NamespacePath};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ConfiguredObjectStore, ObjectStore};
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::{first_page, namespace_id};
use loonfs_types::{
    ApiError, ChangeSeq, Checkpoint, CheckpointOwnerSummary, DeleteCheckpointResponse, ManifestNo,
    PinId,
};
use tempfile::tempdir;

type ApiResult<T> = Result<T, Box<ApiError>>;

fn post_checkpoint(server_url: &str, namespace: &str) -> ApiResult<Checkpoint> {
    post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/checkpoints"),
        "test-token",
        serde_json::json!({ "name": "nightly" }),
    )
}

fn delete_checkpoint(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> ApiResult<loonfs_types::DeleteCheckpointResponse> {
    retry_result_on_macos_teardown_einval(|| {
        decode_maintenance_response(
            raw_agent()
                .delete(&format!(
                    "{server_url}/v0/maintenance/namespaces/{namespace}/checkpoints/{checkpoint_id}"
                ))
                .set("authorization", "Bearer test-token")
                .call(),
        )
    })
}

fn post_gc(server_url: &str, namespace: &str) -> ApiResult<loonfs_types::GcResponse> {
    post_gc_with(server_url, namespace, serde_json::json!({}))
}

fn upkeep(
    response: &loonfs_types::RunMaintenanceResponse,
) -> &loonfs_types::MetadataMaintenanceResponse {
    let loonfs_types::RunMaintenanceResponse::Metadata(metadata) = response else {
        panic!("metadata request returned a different response")
    };
    metadata
}

fn retention_floor(response: loonfs_types::RunMaintenanceResponse) -> ChangeSeq {
    let loonfs_types::RunMaintenanceResponse::Retention(retention) = response else {
        panic!("retention request returned a different response")
    };
    retention.retention_floor_seq
}

fn post_gc_with(
    server_url: &str,
    namespace: &str,
    gc: serde_json::Value,
) -> ApiResult<loonfs_types::GcResponse> {
    let mut request = gc;
    request
        .as_object_mut()
        .expect("GC options are an object")
        .insert("kind".to_owned(), serde_json::json!("gc"));
    let response: ApiResult<loonfs_types::RunMaintenanceResponse> = post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
        request,
    );
    response.map(|response| {
        let loonfs_types::RunMaintenanceResponse::Gc(gc) = response else {
            panic!("GC request returned a different response")
        };
        gc
    })
}

fn post_metadata_run(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_types::RunMaintenanceResponse> {
    post_maintenance_json_body(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
        serde_json::json!({ "kind": "metadata" }),
    )
}

fn post_missing_maintenance_body(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_types::RunMaintenanceResponse> {
    post_maintenance_json(
        &format!("{server_url}/v0/maintenance/namespaces/{namespace}/runs"),
        "test-token",
    )
}

fn post_retention_advance(
    server_url: &str,
    namespace: &str,
) -> ApiResult<loonfs_types::RunMaintenanceResponse> {
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
        .create_namespace(
            &namespace,
            &loonfs_test_support::test_actor(),
            loonfs_types::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace");
    client
        .put_file_with_options(
            &target,
            b"hello maintenance\n",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("write file");

    let first = post_checkpoint(&server_url, namespace.as_str()).expect("first checkpoint");
    assert!(PinId::parse(first.checkpoint_id.as_str()).is_ok());
    assert_eq!(
        first.owner,
        CheckpointOwnerSummary::User {
            name: "nightly".to_owned()
        }
    );
    assert_eq!(first.captured_seq, ChangeSeq(1));
    assert_eq!(first.manifest_no, ManifestNo(3));
    let listed = collect_checkpoints(&client, &namespace)
        .await
        .expect("list first checkpoint");
    assert_eq!(listed.checkpoints, vec![first.clone()]);

    // A second checkpoint at the same head creates a new record.
    let repeated = post_checkpoint(&server_url, namespace.as_str()).expect("repeat checkpoint");
    assert_ne!(repeated.checkpoint_id, first.checkpoint_id);
    assert_eq!(repeated.namespace_id, first.namespace_id);
    assert_eq!(repeated.owner, first.owner);
    assert_eq!(repeated.captured_seq, first.captured_seq);
    assert_eq!(repeated.manifest_no, first.manifest_no);
    assert_eq!(repeated.expires_at_ms, first.expires_at_ms);
    assert!(repeated.created_at_ms >= first.created_at_ms);
    client
        .fork_namespace(
            &namespace,
            &namespace_id("fork"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("fork namespace");
    let diagnostics = client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read checkpoint diagnostics");
    assert_eq!(diagnostics.live_snapshots, 0);
    assert_eq!(diagnostics.live_checkpoints, 2);

    let deleted = delete_checkpoint(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect("delete checkpoint");
    assert_eq!(
        deleted,
        DeleteCheckpointResponse {
            namespace_id: namespace.clone(),
            checkpoint_id: first.checkpoint_id.clone(),
        }
    );
    let deleted_again = delete_checkpoint(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect_err("repeat delete");
    assert_eq!(deleted_again.code, "checkpoint_not_found");
    let diagnostics = client
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("read diagnostics after delete");
    assert_eq!(diagnostics.live_checkpoints, 1);
    let bogus_delete = delete_checkpoint(&server_url, namespace.as_str(), "not-a-checkpoint-id")
        .expect_err("malformed checkpoint id");
    assert_eq!(bogus_delete.code, "invalid_request");

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

    let bytes = client.read_file(&target).await.expect("read file");
    assert_eq!(bytes, b"hello maintenance\n");

    match client
        .list_changes(&namespace, ChangeSeq(0))
        .page(first_page())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
        other => panic!("expected rebootstrap_required, got {other:?}"),
    }

    let empty = client
        .list_changes(&namespace, ChangeSeq(1))
        .page(first_page())
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
        .create_namespace(
            &namespace,
            &loonfs_test_support::test_actor(),
            loonfs_types::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .put_file_with_options(
            &target,
            b"hello gc\n",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("write file");
    post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");

    let refused = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "cursor": "obsolete" }),
    )
    .expect_err("GC requests reject continuation tokens");
    assert_eq!(refused.code, "invalid_request");

    // Objects inside the grace window remain readable.
    let report = post_gc(&server_url, namespace.as_str()).expect("gc pass");
    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );

    let bytes = client.read_file(&target).await.expect("read file");
    assert_eq!(bytes, b"hello gc\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_metadata_run_reports_outcomes_not_errors() {
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
        .create_namespace(
            &namespace,
            &loonfs_test_support::test_actor(),
            loonfs_types::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
    client
        .put_file_with_options(
            &target,
            b"hello step\n",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("write file");

    let empty = post_missing_maintenance_body(&server_url, namespace.as_str())
        .expect_err("a missing body is refused");
    assert_eq!(empty.code, "invalid_request");

    let idle = post_metadata_run(&server_url, namespace.as_str()).expect("idle step");
    assert_eq!(
        upkeep(&idle).wal_fold,
        loonfs_types::WalFoldStepOutcome::NotNeeded
    );

    let forced = client
        .run_maintenance(
            &namespace,
            &loonfs_types::RunMaintenanceRequest::Metadata(
                loonfs_types::MetadataMaintenanceRequest {
                    max_wal_tail_objects: Some(1),
                },
            ),
            None,
        )
        .await
        .expect("forced step");
    assert_eq!(
        upkeep(&forced).wal_fold,
        loonfs_types::WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(1),
        }
    );
    assert_eq!(
        upkeep(&forced).compaction,
        loonfs_types::CompactionStepOutcome::NotNeeded {}
    );
    let retention = client
        .run_maintenance(
            &namespace,
            &loonfs_types::RunMaintenanceRequest::Retention(
                loonfs_types::AdvanceRetentionRequest {},
            ),
            None,
        )
        .await
        .expect("advance retention");
    assert_eq!(retention_floor(retention), ChangeSeq(1));
    let gc = post_gc(&server_url, namespace.as_str()).expect("GC run");
    assert_eq!(gc.deleted.wal_objects, 0);

    let bytes = client.read_file(&target).await.expect("read file");
    assert_eq!(bytes, b"hello step\n");

    harness.server.abort();
}

#[derive(Debug)]
struct FixedWallClock(u64);

impl loonfs::WallClock for FixedWallClock {
    fn now_ms(&self) -> Result<u64, loonfs::CoreError> {
        Ok(self.0)
    }
}

#[tokio::test]
async fn http_metadata_run_folds_an_idle_tail_unless_the_server_turns_the_idle_rule_off() {
    let default_idle_ms = loonfs::MetadataMaintenanceOptions::default().idle_fold_after_ms;
    for (idle_fold_after_ms, expected) in [
        (0, loonfs_types::WalFoldStepOutcome::NotNeeded),
        (
            default_idle_ms,
            loonfs_types::WalFoldStepOutcome::Folded {
                manifest_head_seq: ChangeSeq(1),
            },
        ),
    ] {
        let temp_dir = tempdir().expect("tempdir");
        let store_root = temp_dir.path().join("store");
        let key_prefix = "http-maintenance-idle";
        let store = ConfiguredObjectStore::local_fs(&store_root, Some(key_prefix))
            .expect("construct store")
            .into_shared();
        // The server reads commit age on its own clock, so a commit stamped
        // by a writer whose clock is long past is idle on arrival.
        let writer = loonfs::LoonFs::builder_with_store(store)
            .writer_id("departed-writer")
            .wall_clock(std::sync::Arc::new(FixedWallClock(1_750_000_000_000)))
            .build()
            .await
            .expect("writer");
        let namespace = namespace_id("idle");
        writer
            .create_namespace(&namespace, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
        namespace_writer
            .put_file("/file.txt", b"body", &loonfs_test_support::test_actor())
            .await
            .expect("write file");
        writer.shutdown().await.expect("writer shutdown");

        let harness = start_server(loonfs_server::ServerConfig {
            idle_fold_after_ms,
            maintenance: loonfs_server::MaintenanceMode::ServeOnly,
            ..test_config(store_root, "loonfs-server-idle", key_prefix)
        })
        .await;
        let response = harness
            .client
            .run_maintenance(
                &namespace,
                &loonfs_types::RunMaintenanceRequest::Metadata(
                    loonfs_types::MetadataMaintenanceRequest::default(),
                ),
                None,
            )
            .await
            .expect("metadata run");
        assert_eq!(
            upkeep(&response).wal_fold,
            expected,
            "idle_fold_after_ms = {idle_fold_after_ms}"
        );
        harness.server.abort();
    }
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
        .create_namespace(
            &namespace,
            &loonfs_test_support::test_actor(),
            loonfs_types::NamespaceAccess::unrestricted(),
        )
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
    let mut warm_config = test_config(
        store_root.clone(),
        "loonfs-server-maintenance-corrupt",
        "http-maintenance-corrupt",
    );
    warm_config.maintenance = loonfs_server::MaintenanceMode::ServeOnly;
    let harness = start_server(warm_config).await;
    // A new server must read the corrupted manifest; the first server has a valid cached snapshot.
    let mut cold_config = test_config(
        store_root,
        "loonfs-server-cold-reader",
        "http-maintenance-corrupt",
    );
    cold_config.maintenance = loonfs_server::MaintenanceMode::ServeOnly;
    let cold = start_server(cold_config).await;
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
        .create_namespace(
            &namespace,
            &loonfs_test_support::test_actor(),
            loonfs_types::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace");
    client
        .put_file_with_options(
            &target,
            b"hello\n",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("write file");
    post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");
    client
        .stat(&target)
        .await
        .expect("warm the first server after checkpoint maintenance");

    let store = ConfiguredObjectStore::local_fs(&store_root, store_key_prefix.as_deref())
        .expect("construct store")
        .into_shared();
    let root = loonfs::control::load_namespace_current_manifest(&store, &namespace)
        .await
        .expect("metadata root");
    store
        .put_overwrite(
            &metadata_manifest_object(&namespace, &root.state.manifest().manifest_no),
            Bytes::from_static(br#"{"bad":"json"}"#),
        )
        .await
        .expect("corrupt manifest");

    match cold_client.stat(&target).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
        other => panic!("expected namespace_corrupt, got {other:?}"),
    }
    client
        .stat(&target)
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

    let probe: loonfs_types::api::v0::StoreProbeResponse = post_maintenance_json_body(
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
            loonfs_types::api::v0::StoreProbeCheckOutcome::Failed,
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

    let unauthorized: ApiResult<loonfs_types::api::v0::StoreProbeResponse> =
        post_maintenance_json_body(&url, "wrong-token", serde_json::json!({}));
    assert_eq!(
        unauthorized.expect_err("a wrong token is refused").code,
        "unauthorized"
    );

    // An absent body is treated as an empty object.
    let bodyless: loonfs_types::api::v0::StoreProbeResponse =
        post_maintenance_json(&url, "test-token").expect("probe with no body");
    assert!(
        !bodyless.checks.is_empty(),
        "a bodyless request runs the probe rather than selecting nothing"
    );

    harness.server.abort();
}
