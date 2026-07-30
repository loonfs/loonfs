//! HTTP checkpoint, retention, garbage collection, and maintenance operations.

use crate::common::http_split_support::*;
use crate::common::start_server;
use bytes::Bytes;
use loonfs_api::{ApiError, ChangeSeq, CheckpointId, CreateCheckpointResponse, ManifestId};
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

/// One garbage-collection pass, as the collapsed surface runs it: a
/// maintenance step restricted to `gc`.
fn post_gc_with(
    server_url: &str,
    namespace: &str,
    gc: serde_json::Value,
) -> Result<loonfs_api::GcResponse, ApiError> {
    let step: Result<loonfs_api::MaintenanceStepResponse, ApiError> = post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
        serde_json::json!({ "only": "gc", "gc": gc }),
    );
    step.map(|step| step.gc.expect("gc report present when the step opted in"))
}

fn post_maintenance_step(
    server_url: &str,
    namespace: &str,
) -> Result<loonfs_api::MaintenanceStepResponse, ApiError> {
    post_admin_json(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/step"),
        "test-token",
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
        serde_json::json!({ "only": "retention" }),
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
    assert!(CheckpointId::parse(first.checkpoint_id.as_str()).is_ok());
    assert_eq!(first.checkpoint_seq, ChangeSeq(1));
    assert_eq!(first.manifest_id, ManifestId(1));
    assert_eq!(first.current_manifest_id, Some(first.manifest_id));

    // Creating again over the same head is a second pin, not a second name
    // for the first: a new record under a new id, over the same basis.
    let repeated = post_checkpoint(&server_url, namespace.as_str()).expect("repeat checkpoint");
    assert_ne!(repeated.checkpoint_id, first.checkpoint_id);
    assert_eq!(
        CreateCheckpointResponse {
            checkpoint_id: first.checkpoint_id.clone(),
            ..repeated.clone()
        },
        first
    );

    // Release is idempotent: the first call flips the record one way, and
    // the repeat observes the settled end state.
    let released = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect("release checkpoint");
    assert!(released.was_active);
    let released_again = post_checkpoint_release(
        &server_url,
        namespace.as_str(),
        first.checkpoint_id.as_str(),
    )
    .expect("repeat release");
    assert!(!released_again.was_active);
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

    let advanced =
        post_retention_advance(&server_url, namespace.as_str()).expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));

    // Idempotent in the floor it lands on. `status_before` legitimately
    // differs: the first call observed the floor it then advanced past.
    let repeated_advance =
        post_retention_advance(&server_url, namespace.as_str()).expect("repeat retention");
    assert_eq!(
        repeated_advance.retention_floor_seq,
        advanced.retention_floor_seq
    );
    assert_eq!(
        repeated_advance.status_before.retention_floor_seq,
        advanced.retention_floor_seq
    );

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
    // on the next invocation without carrying any safety state.
    let bounded = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "max_objects": 1 }),
    )
    .expect("bounded gc pass");
    let cursor = bounded.next_cursor.expect("more candidate families remain");
    let resumed = post_gc_with(
        &server_url,
        namespace.as_str(),
        serde_json::json!({ "max_objects": 1, "cursor": cursor }),
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

    // One WAL segment sits far below the default threshold.
    let idle = post_maintenance_step(&server_url, namespace.as_str()).expect("idle step");
    assert_eq!(idle.namespace_id, namespace);
    assert_eq!(idle.status_before.wal_tail_segments, 1);
    assert_eq!(idle.wal_flush, loonfs_api::WalFlushStepOutcome::NotNeeded);
    assert!(idle.gc.is_none());

    // Forcing the threshold to one segment flushes the WAL tail
    // and runs the opted-in GC pass.
    let forced: loonfs_api::MaintenanceStepResponse = client
        .maintenance_step(
            &namespace,
            &loonfs_api::MaintenanceStepRequest {
                max_wal_tail_segments: Some(1),
                retention: None,
                gc: Some(loonfs_api::GcRequest::default()),
                only: None,
            },
        )
        .await
        .expect("forced step");
    assert_eq!(
        forced.wal_flush,
        loonfs_api::WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1),
        }
    );
    // Retention runs inside the step now, so the floor is reported whether or
    // not it moved, and it never goes backwards.
    assert!(forced.retention_floor_seq >= forced.status_before.retention_floor_seq);
    assert_eq!(
        forced.reorganize,
        loonfs_api::ReorganizeStepOutcome::NotNeeded
    );
    let gc = forced.gc.expect("gc report present when opted in");
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

    let advanced =
        post_retention_advance(&server_url, namespace.as_str()).expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, ChangeSeq(0));

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
        .expect("construct store");
    let root = loonfs::control::load_namespace_metadata_root_control(&store, &namespace)
        .await
        .expect("metadata root");
    store
        .put_overwrite(
            &metadata_manifest_object(namespace.as_str(), &root.state.manifest_object_id),
            Bytes::from_static(br#"{"bad":"json"}"#),
        )
        .await
        .expect("corrupt manifest");

    match cold_client.stat_path(&target).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
        other => unreachable!("expected namespace_corrupt, got {other:?}"),
    }
    // The warm server keeps serving its pinned pair; the corruption is
    // surfaced by whoever actually consumes the manifest.
    client
        .stat_path(&target)
        .await
        .expect("warm server reads from its pinned head-plus-manifest pair");

    harness.server.abort();
    cold.server.abort();
}
