//! HTTP checkpoint, retention, garbage collection, and maintenance operations.

use crate::common::http_split_support::*;
use crate::common::start_server;
use bytes::Bytes;
use loonfs_api::{
    ApiError, ChangeSeq, CheckpointId, CheckpointOwnerSummary, CreateCheckpointResponse,
    ManifestId, ReleaseCheckpointResponse,
};
use loonfs_client::{ClientError, NamespacePath};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ConfiguredObjectStore, ObjectStore};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

fn post_checkpoint(
    server_url: &str,
    namespace: &str,
) -> Result<CreateCheckpointResponse, ApiError> {
    post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/checkpoints"),
        "test-token",
        serde_json::json!({ "name": "nightly" }),
    )
}

fn post_checkpoint_release(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> Result<loonfs_api::ReleaseCheckpointResponse, ApiError> {
    post_admin_json(
        &format!(
            "{server_url}/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release"
        ),
        "test-token",
    )
}

fn post_gc(server_url: &str, namespace: &str) -> Result<loonfs_api::GcResponse, ApiError> {
    post_gc_with(server_url, namespace, serde_json::json!({}))
}

/// The upkeep report a step that selected metadata is obliged to carry.
fn upkeep(step: &loonfs_api::MaintenanceStepResponse) -> &loonfs_api::MetadataMaintenanceResponse {
    step.metadata
        .as_ref()
        .expect("a step selecting metadata upkeep reports it")
}

/// The floor a step that selected the retention advance is obliged to report.
fn retention_floor(step: loonfs_api::MaintenanceStepResponse) -> ChangeSeq {
    step.retention
        .expect("a step selecting the retention advance reports it")
        .retention_floor_seq
}

/// One garbage-collection pass, as the collapsed surface runs it: a
/// maintenance step selecting `gc` and nothing else.
fn post_gc_with(
    server_url: &str,
    namespace: &str,
    gc: serde_json::Value,
) -> Result<loonfs_api::GcResponse, ApiError> {
    let step: Result<loonfs_api::MaintenanceStepResponse, ApiError> = post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
        serde_json::json!({ "gc": gc }),
    );
    step.map(|step| {
        step.gc
            .expect("a step selecting collection reports its pass")
    })
}

/// One metadata-upkeep step at the server's default threshold.
fn post_maintenance_step(
    server_url: &str,
    namespace: &str,
) -> Result<loonfs_api::MaintenanceStepResponse, ApiError> {
    post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
        serde_json::json!({ "metadata": {} }),
    )
}

/// A step body that selects nothing, which the route refuses.
fn post_empty_maintenance_step(
    server_url: &str,
    namespace: &str,
) -> Result<loonfs_api::MaintenanceStepResponse, ApiError> {
    post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
        serde_json::json!({}),
    )
}

/// One retention-floor advance, as the collapsed surface runs it.
fn post_retention_advance(
    server_url: &str,
    namespace: &str,
) -> Result<loonfs_api::MaintenanceStepResponse, ApiError> {
    post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
        serde_json::json!({ "advance_retention": true }),
    )
}

fn post_admin_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> Result<T, ApiError> {
    let request = raw_agent()
        .post(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    decode_admin_response(request.call())
}

fn post_admin_json_body<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
    body: serde_json::Value,
) -> Result<T, ApiError> {
    let request = raw_agent()
        .post(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    decode_admin_response(request.send_json(body))
}

fn decode_admin_response<T: serde::de::DeserializeOwned>(
    result: Result<ureq::Response, ureq::Error>,
) -> Result<T, ApiError> {
    match result {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        }),
        Err(ureq::Error::Status(_, response)) => Err(serde_json::from_reader::<_, ApiError>(
            response.into_reader(),
        )
        .unwrap_or_else(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        })),
        Err(ureq::Error::Transport(error)) => Err(ApiError {
            code: "transport".to_owned(),
            feature: None,
            message: error.to_string(),
            request_id: None,
            details: None,
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_checkpoint_and_retention_are_idempotent_and_soft() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-admin",
        "http-admin",
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
        .put_file_bytes(&target, b"hello admin\n", &replace_file_options())
        .await
        .expect("write file");

    let first = post_checkpoint(&server_url, namespace.as_str()).expect("first checkpoint");
    assert!(CheckpointId::parse(first.checkpoint.checkpoint_id.as_str()).is_ok());
    assert_eq!(
        first.checkpoint.owner,
        CheckpointOwnerSummary::User {
            name: "nightly".to_owned()
        }
    );
    assert_eq!(first.checkpoint.checkpoint_seq, ChangeSeq(1));
    assert_eq!(first.checkpoint.manifest_id, ManifestId(1));
    let listed = client
        .list_checkpoints_all(&namespace)
        .await
        .expect("list first checkpoint");
    assert_eq!(listed.checkpoints, vec![first.checkpoint.clone()]);

    // Creating again over the same head is a second pin, not a second name
    // for the first: a new record under a new id, over the same basis.
    let repeated = post_checkpoint(&server_url, namespace.as_str()).expect("repeat checkpoint");
    assert_ne!(
        repeated.checkpoint.checkpoint_id,
        first.checkpoint.checkpoint_id
    );
    assert_eq!(repeated.namespace_id, first.namespace_id);
    assert_eq!(repeated.checkpoint.owner, first.checkpoint.owner);
    assert_eq!(
        repeated.checkpoint.checkpoint_seq,
        first.checkpoint.checkpoint_seq
    );
    assert_eq!(
        repeated.checkpoint.manifest_id,
        first.checkpoint.manifest_id
    );
    assert_eq!(
        repeated.checkpoint.expires_at_ms,
        first.checkpoint.expires_at_ms
    );
    assert!(repeated.checkpoint.created_at_ms >= first.checkpoint.created_at_ms);

    // Release is idempotent: the first call flips the record one way, and
    // the repeat observes the settled end state.
    let released = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint.checkpoint_id.as_str(),
    )
    .expect("release checkpoint");
    assert_eq!(
        released,
        ReleaseCheckpointResponse {
            namespace_id: namespace.clone(),
            checkpoint_id: first.checkpoint.checkpoint_id.clone(),
        }
    );
    let released_again = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint.checkpoint_id.as_str(),
    )
    .expect("repeat release");
    assert_eq!(released_again, released);
    let bogus_release =
        post_checkpoint_release(&server_url, namespace.as_str(), "not-a-checkpoint-id")
            .expect_err("malformed checkpoint id");
    assert_eq!(bogus_release.code, "invalid_request");

    // The GC grace window's derived safety floor is enforced at the API:
    // a sub-minimum override is rejected, not honored.
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

    // Idempotent in the floor it lands on. `status_before` legitimately
    // differs: the first call observed the floor it then advanced past.
    let repeated =
        post_retention_advance(&server_url, namespace.as_str()).expect("repeat retention");
    assert_eq!(repeated.status_before.retention_floor_seq, advanced);
    assert_eq!(retention_floor(repeated), advanced);

    let bytes = client.get_file_bytes(&target).await.expect("read file");
    assert_eq!(bytes, b"hello admin\n");

    match client.list_changes(&namespace, ChangeSeq(0), None).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
        other => unreachable!("expected rebootstrap_required, got {other:?}"),
    }

    let empty = client
        .list_changes(&namespace, ChangeSeq(1), None)
        .await
        .expect("changes after floor");
    assert_eq!(empty.changes, Vec::new());

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_gc_is_explicit_and_retains_young_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-gc",
        "http-admin-gc",
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

    // A bounded pass returns an opaque cursor, and the route accepts it
    // on the next invocation without carrying any safety state. The budget
    // covers the roots this namespace marks before it walks, so what is
    // left over is what bounds the walk.
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

    // A freshly written namespace sits entirely inside the grace
    // window: the unbounded pass completes, deletes nothing, and reads
    // keep working.
    let report = post_gc(&server_url, namespace.as_str()).expect("gc pass");
    assert_eq!(report.deleted_wal_segments, 0);
    assert_eq!(report.deleted_metadata_tables, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert_eq!(report.deleted_checkpoint_records, 0);
    assert!(!report.degraded_retention);
    assert!(report.next_cursor.is_none());

    let bytes = client.get_file_bytes(&target).await.expect("read file");
    assert_eq!(bytes, b"hello gc\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_maintenance_step_reports_outcomes_not_errors() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-step",
        "http-admin-step",
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

    // A body that names no action is not a step.
    let empty = post_empty_maintenance_step(&server_url, namespace.as_str())
        .expect_err("a body selecting nothing is refused");
    assert_eq!(empty.code, "invalid_request");
    assert!(empty.message.contains("at least one action"));

    // One WAL segment sits far below the default threshold.
    let idle = post_maintenance_step(&server_url, namespace.as_str()).expect("idle step");
    assert_eq!(idle.namespace_id, namespace);
    assert_eq!(idle.status_before.wal_tail_segments, 1);
    assert_eq!(
        upkeep(&idle).wal_flush,
        loonfs_api::WalFlushStepOutcome::NotNeeded
    );
    // Unnamed actions report nothing at all.
    assert!(idle.gc.is_none());
    assert!(idle.retention.is_none());

    // Forcing the threshold to one segment flushes the WAL tail, and the
    // named retention and collection actions run behind it.
    let forced: loonfs_api::MaintenanceStepResponse = client
        .maintenance_step(
            &namespace,
            &loonfs_api::MaintenanceStepRequest {
                metadata: Some(loonfs_api::MetadataMaintenanceRequest {
                    max_wal_tail_segments: Some(1),
                }),
                advance_retention: true,
                gc: Some(loonfs_api::GcRequest::default()),
            },
        )
        .await
        .expect("forced step");
    assert_eq!(
        upkeep(&forced).wal_flush,
        loonfs_api::WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1),
        }
    );
    // The floor is reported because the step named the advance, and it never
    // goes backwards.
    assert!(retention_floor(forced.clone()) >= forced.status_before.retention_floor_seq);
    assert_eq!(
        upkeep(&forced).reorganize,
        loonfs_api::ReorganizeStepOutcome::NotNeeded
    );
    let gc = forced.gc.clone().expect("gc report present when opted in");
    assert_eq!(gc.deleted_wal_segments, 0);
    assert!(!gc.degraded_retention);

    let bytes = client.get_file_bytes(&target).await.expect("read file");
    assert_eq!(bytes, b"hello step\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_retention_advance_uses_initial_manifest_after_create() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-admin-missing-checkpoint",
        "http-admin-missing-checkpoint",
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
        "loonfs-server-admin-corrupt",
        "http-admin-corrupt",
    ))
    .await;
    // A second server on the same store reads cold: it must consume the
    // manifest and notice the corruption. The first server's reads are
    // pinned to the head-plus-manifest pair its own publish seeded, which
    // stays valid without touching the corrupted object.
    let cold = start_server(test_config(
        store_root,
        "loonfs-server-cold-reader",
        "http-admin-corrupt",
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
            &metadata_manifest_object(&namespace, &root.state.manifest_object_id),
            Bytes::from_static(br#"{"bad":"json"}"#),
        )
        .await
        .expect("corrupt manifest");

    match cold_client.stat_path(&target, &Default::default()).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
        other => unreachable!("expected namespace_corrupt, got {other:?}"),
    }
    // The warm server keeps serving its pinned pair; the corruption is
    // surfaced by whoever actually consumes the manifest.
    client
        .stat_path(&target, &Default::default())
        .await
        .expect("warm server reads from its pinned head-plus-manifest pair");

    harness.server.abort();
    cold.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_store_probe_reports_unique_successes_from_the_configured_store() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-probe",
        "http-admin-probe",
    ))
    .await;

    let probe: loonfs_api::v0::StoreProbeResponse = post_admin_json_body(
        &format!("{}/v0/admin/store/probe", harness.server_url),
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

    // A probe leaves the store as it found it: emptying the run's scratch
    // prefix is its own last check, and nothing else here wrote there.
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
async fn http_admin_store_probe_requires_a_token_and_accepts_a_bodyless_request() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-probe-auth",
        "http-admin-probe-auth",
    ))
    .await;
    let url = format!("{}/v0/admin/store/probe", harness.server_url);

    let unauthorized: Result<loonfs_api::v0::StoreProbeResponse, ApiError> =
        post_admin_json_body(&url, "wrong-token", serde_json::json!({}));
    assert_eq!(
        unauthorized.expect_err("a wrong token is refused").code,
        "unauthorized"
    );

    // Body handling matches the maintenance-step route: an absent body is
    // the same request as `{}`.
    let bodyless: loonfs_api::v0::StoreProbeResponse =
        post_admin_json(&url, "test-token").expect("probe with no body");
    assert_eq!(bodyless.checks.len(), 14);

    harness.server.abort();
}
