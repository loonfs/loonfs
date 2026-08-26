#![allow(clippy::panic)]
// HTTP smoke helpers panic in unexpected match arms for precise diagnostics.

use super::error::{status_for_core_error_code, ServedErrorCode};
use super::{
    app_with_store, app_with_store_and_state, build_handles_with_metrics_jsonl_path,
    request_log_severity, AppState, RequestLogSeverity, SharedObjectStore,
};
use crate::config::RuntimeCacheConfigOverrides;
use crate::{ServerConfig, StoreConfig};
use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::StatusCode;
use futures::stream::{BoxStream, StreamExt};
use loonfs::{
    CreateNamespaceOptions, DeleteOptions, FsAdmin, FsReader, FsWriter, MaintenanceJob,
    MaintenanceJobId, MaintenanceProbe, MaintenanceStepConclusion, MaintenanceStepReport,
    PutFileOptions, StoredMetadataBlockCache, TraceMode, TraceStoreKind,
};
use loonfs_api::ErrorCode;
use loonfs_api::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, GrepRequest, NamespaceId,
    PaginationPolicy, FEATURE_QUERY_GREP,
};
use loonfs_client::{Client, ClientConfig, ClientError, MoveOptions, NamespacePath};
use loonfs_grep::keyspace::{manifest_key as grep_manifest_key, root_key as grep_root_key};
use loonfs_grep::root::{
    encode_grep_root, load_grep_root, GrepManifestObjectId, GrepRootEnvelope, GrepRootPointer,
};
use loonfs_grep::{GrepWorker, NamespaceReads};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;

const API_SPEC_NON_ERROR_CODE_TOKENS: &[&str] = &[
    "aborted_at_ms",
    "active_acquired_at_ms",
    "active_writer",
    "active_writer_epoch",
    "actual_attributes_revision_no",
    "actual_deletion_seq",
    "actual_head_seq",
    "actual_revision_no",
    "after_seq",
    "allow_scan",
    "allow_stale",
    "already_published",
    "attributes_changed",
    "attributes_revision_no",
    "attributes_updated_at_ms",
    "attributes_updated_by",
    "bearer_auth",
    "binding_generation",
    "budget_exhausted",
    "built_through_seq",
    "checkpoint_id",
    "checkpoint_not_releasable",
    "checkpoint_records",
    "checkpoint_seq",
    "commit_id",
    "committed_by",
    "committed_at_ms",
    "committed_fingerprint",
    "committed_seq",
    "compaction_at_capacity",
    "compaction_required",
    "compaction_running",
    "compaction_started",
    "complete_upload_prepared",
    "completed_at_ms",
    "content_changed",
    "content_reclamation_deferred",
    "content_objects",
    "content_ref",
    "content_scan_deferred",
    "content_store_id",
    "content_tokens",
    "created_by",
    "created_at_ms",
    "current_manifest_no",
    "cursor_inode_id",
    "degraded_roots",
    "deletion_seq",
    "deleted_at_ms",
    "deleted_binding",
    "deleted_by",
    "destination_exists",
    "directory_created",
    "direct_multipart",
    "direct_put",
    "display_name",
    "expected_attributes_revision_no",
    "expected_deletion_seq",
    "expected_head_seq",
    "expected_inode_id",
    "expected_revision_no",
    "expires_at_ms",
    "fenced_writer_epoch",
    "file_created",
    "from_display_name",
    "from_parent_inode_id",
    "grace_window_ms",
    "head_drift",
    "head_seq",
    "include_attributes",
    "inode_id",
    "inode_kind",
    "manifest_no",
    "max_objects",
    "max_wal_tail_segments",
    "metadata_maintenance",
    "metadata_segments",
    "missing_basis",
    "name_key",
    "namespace_id",
    "new_namespace_id",
    "next_after_seq",
    "next_cursor",
    "next_event_index",
    "next_reclamation_at_ms",
    "next_run_no",
    "no_provider_timestamp",
    "no_reference_manifest",
    "no_replace",
    "not_idempotent",
    "not_needed",
    "operation_id",
    "operation_index",
    "operation_kind",
    "operation_part",
    "parent_inode_id",
    "part_size_bytes",
    "path_prefix",
    "prepare_content_ref",
    "prepare_file_bytes",
    "protocol_version",
    "put_file",
    "put_file_prepared",
    "request_deadline_ms",
    "request_id",
    "released_checkpoints",
    "retained_candidates",
    "retention_degraded",
    "retention_floor_seq",
    "retries_exhausted",
    "reorganize_pending",
    "revision_committed_at_ms",
    "revision_committed_by",
    "revision_no",
    "root_advanced",
    "run_id",
    "service_proxied",
    "size_bytes",
    "checksum",
    "checksum_algorithm",
    "target_namespace_id",
    "target_seq",
    "to_display_name",
    "to_parent_inode_id",
    "ttl_ms",
    "unrecognized_key",
    "update_attributes",
    "wal_segments",
    "wal_tail_segments",
    "within_grace_window",
    "updated_at_ms",
    "updated_by",
    "upload_id",
    "upload_session_undecided",
    "upload_session_window",
    "upload_sessions",
    "content_token",
    "wal_flush",
];

fn replace_file_options() -> PutFileOptions {
    PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::new(loonfs_test_support::test_actor())
    }
}

#[test]
fn error_status_mapping_matches_the_api_spec_table() {
    let spec = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/api.md"
    ))
    .expect("read docs/specs/api.md");
    let table = spec
        .split("The full registry")
        .nth(1)
        .expect("api.md error registry intro")
        .split("Precondition failures surface")
        .next()
        .expect("api.md error registry end");

    let mut documented = std::collections::BTreeMap::new();
    for line in table.lines() {
        let Some(rest) = line.strip_prefix("| `") else {
            continue;
        };
        let mut cells = rest.split(" | ");
        let code = cells
            .next()
            .expect("code cell")
            .trim_end_matches('`')
            .to_owned();
        let status: u16 = cells
            .next()
            .expect("status cell")
            .trim()
            .parse()
            .expect("numeric status cell");
        documented.insert(code, status);
    }

    for code in ErrorCode::ALL {
        let documented_status = documented.remove(code.as_str()).unwrap_or_else(|| {
            panic!(
                "`{}` is registered in loonfs-api but missing from the api.md error table",
                code.as_str()
            )
        });
        assert_eq!(
            status_for_core_error_code(code).as_u16(),
            documented_status,
            "served status for `{}` disagrees with the api.md error table",
            code.as_str()
        );
    }
    assert!(
        documented.is_empty(),
        "api.md documents codes this build does not register: {documented:?}"
    );
    assert_api_spec_error_codes_are_registered(&spec);
}

#[test]
fn request_log_severity_uses_typed_codes_and_status_only_as_a_fallback() {
    assert_eq!(
        request_log_severity(
            StatusCode::NOT_IMPLEMENTED,
            Some(ServedErrorCode(ErrorCode::NotSupported)),
        ),
        RequestLogSeverity::Debug
    );
    assert_eq!(
        request_log_severity(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(ServedErrorCode(ErrorCode::ServerError)),
        ),
        RequestLogSeverity::Error
    );
    assert_eq!(
        request_log_severity(
            StatusCode::SERVICE_UNAVAILABLE,
            Some(ServedErrorCode(ErrorCode::ServerBusy)),
        ),
        RequestLogSeverity::Warn
    );
    assert_eq!(
        request_log_severity(StatusCode::NOT_IMPLEMENTED, None),
        RequestLogSeverity::Error
    );
    assert_eq!(
        request_log_severity(StatusCode::UNAUTHORIZED, None),
        RequestLogSeverity::Warn
    );
    assert_eq!(
        request_log_severity(StatusCode::FORBIDDEN, None),
        RequestLogSeverity::Warn
    );
    assert_eq!(
        request_log_severity(StatusCode::NOT_FOUND, None),
        RequestLogSeverity::Debug
    );
}

#[test]
fn registered_limit_keys_match_the_api_spec_table() {
    let spec = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/api.md"
    ))
    .expect("read docs/specs/api.md");
    let table = spec
        .split("Registered limit keys:")
        .nth(1)
        .expect("api.md registered-limit table intro")
        .split("### 2.2 Feature registry")
        .next()
        .expect("api.md registered-limit table end");
    let documented: std::collections::BTreeSet<_> = table
        .lines()
        .filter_map(|line| line.strip_prefix("| `"))
        .filter_map(|line| line.split('`').next())
        .map(ToOwned::to_owned)
        .collect();

    let capability_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../loonfs-api/src/capability.rs"
    ));
    let registered: std::collections::BTreeSet<_> = capability_source
        .split("pub const LIMIT_")
        .skip(1)
        .filter_map(|declaration| declaration.split(';').next())
        .filter_map(|declaration| declaration.split_once('"').map(|(_, value)| value))
        .filter_map(|value| value.split('"').next())
        .map(ToOwned::to_owned)
        .collect();

    assert_eq!(documented, registered);
}

#[test]
fn error_detail_fields_match_the_api_spec_table() {
    let spec = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/api.md"
    ))
    .expect("read docs/specs/api.md");
    let table = spec
        .split("The codes that populate it:")
        .nth(1)
        .expect("api.md error-details table intro")
        .split("One code exists specifically")
        .next()
        .expect("api.md error-details table end");
    let documented: std::collections::BTreeSet<_> = table
        .lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| line.trim_matches('|').split('|').nth(1))
        .flat_map(|fields| fields.split('`').skip(1).step_by(2))
        .map(ToOwned::to_owned)
        .collect();

    let operations_source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../loonfs-api/src/v0/operations.rs"
    ));
    let details_body = operations_source
        .split("pub struct ErrorDetails {")
        .nth(1)
        .expect("ErrorDetails declaration")
        .split("\n}")
        .next()
        .expect("ErrorDetails body");
    let registered: std::collections::BTreeSet<_> = details_body
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub "))
        .filter_map(|field| field.split(':').next())
        .map(ToOwned::to_owned)
        .collect();

    assert_eq!(documented, registered);
}

fn assert_api_spec_error_codes_are_registered(spec: &str) {
    let openapi: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/openapi.json"
    )))
    .expect("decode docs/specs/openapi.json");
    let operation_ids: std::collections::BTreeSet<_> = openapi["paths"]
        .as_object()
        .expect("OpenAPI paths object")
        .values()
        .flat_map(|path| {
            path.as_object()
                .expect("OpenAPI path item")
                .values()
                .filter_map(|operation| operation.get("operationId")?.as_str())
        })
        .collect();

    for token in spec
        .split('`')
        .skip(1)
        .step_by(2)
        .filter(|token| is_snake_case_token(token))
    {
        if API_SPEC_NON_ERROR_CODE_TOKENS.contains(&token) || operation_ids.contains(token) {
            continue;
        }
        // Valid inode IDs are examples, not error codes.
        if loonfs_api::public_inode_id::decode(token).is_ok() {
            continue;
        }
        assert!(
            ErrorCode::parse(token).is_some(),
            "api.md uses unregistered error-code-shaped token `{token}`"
        );
    }
}

fn is_snake_case_token(token: &str) -> bool {
    token.contains('_')
        && token.starts_with(|character: char| character.is_ascii_lowercase())
        && !token.ends_with('_')
        && !token.contains("__")
        && token
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
use loonfs_test_support::http::{raw_agent, retry_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, BufferWatchStore, FailStore, InjectedError, KeyPredicate, OperationClass,
    OperationContext, OperationKind,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

const POISON_PROVIDER_DETAIL: &str = "<Error>AccessDenied</Error> \
    arn:aws:iam::123456789012:role/private-role private-bucket \
    namespaces/customer-a/head.json x-amz-request-id=provider-request \
    x-amz-id-2=provider-host-id AKIAEXAMPLE";

fn assert_provider_markers_absent(rendered: &str) {
    for marker in [
        "<Error>AccessDenied</Error>",
        "arn:aws:iam::123456789012:role/private-role",
        "private-bucket",
        "namespaces/customer-a/head.json",
        "x-amz-request-id=provider-request",
        "x-amz-id-2=provider-host-id",
        "AKIAEXAMPLE",
    ] {
        assert!(
            !rendered.contains(marker),
            "API body leaked {marker}: {rendered}"
        );
    }
}

#[derive(Debug)]
struct PoisonProviderStore;

impl PoisonProviderStore {
    fn denied(key: &str) -> ObjectStoreError {
        ObjectStoreError::PermissionDenied {
            object_key: key.to_owned(),
            message: POISON_PROVIDER_DETAIL.to_owned(),
        }
    }
}

#[async_trait]
impl ObjectStore for PoisonProviderStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Err(Self::denied(key))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Err(Self::denied(key))
    }

    async fn get(
        &self,
        key: &str,
        _range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        Err(Self::denied(key))
    }

    async fn put(
        &self,
        key: &str,
        _bytes: Bytes,
        _mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Err(Self::denied(key))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        Err(Self::denied(key))
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        _start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        Box::pin(futures::stream::once(std::future::ready(Err(
            Self::denied(prefix),
        ))))
    }
}

#[tokio::test]
async fn provider_failure_is_projected_in_the_remote_api_envelope() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let router = app_with_store(
        test_config(temp_dir.path(), "provider-hygiene-writer"),
        Arc::new(PoisonProviderStore),
    )
    .await
    .expect("build app");
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::POST)
                .uri("/v0/namespaces")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(r#"{"namespace_id":"customer-a"}"#))
                .expect("create namespace request"),
        )
        .await
        .expect("create namespace response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let request_id = response
        .headers()
        .get(super::REQUEST_ID_HEADER)
        .expect("request id header")
        .to_str()
        .expect("request id header is text")
        .to_owned();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read provider failure body");
    let rendered = String::from_utf8(body.to_vec()).expect("API body is UTF-8");

    assert_provider_markers_absent(&rendered);
    assert!(rendered.contains(
        "object-store permission denied; verify that the selected credentials can access the configured bucket and key prefix"
    ));
    assert!(rendered.contains(&request_id), "{rendered}");
}

#[tokio::test]
async fn provider_failure_is_projected_in_the_presign_api_envelope() {
    use axum::response::IntoResponse;

    let response = super::REQUEST_ID
        .scope("req_presign_hygiene".to_owned(), async {
            super::handlers_uploads::presign_issuer_error(PoisonProviderStore::denied(
                "namespaces/customer-a/head.json",
            ))
            .into_response()
        })
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read presign failure body");
    let rendered = String::from_utf8(body.to_vec()).expect("API body is UTF-8");

    assert_provider_markers_absent(&rendered);
    assert!(rendered.contains(
        "object-store permission denied; verify that the selected credentials can access the configured bucket and key prefix"
    ));
    assert!(rendered.contains("req_presign_hygiene"), "{rendered}");
}

#[tokio::test]
async fn invalid_presign_content_is_a_bad_request() {
    use axum::response::IntoResponse;

    let response = super::handlers_uploads::presign_issuer_error(
        ObjectStoreError::InvalidContentRef("invalid checksum".to_owned()),
    )
    .into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read presign error body");
    let rendered = String::from_utf8(body.to_vec()).expect("API body is UTF-8");
    assert!(rendered.contains("invalid_request"), "{rendered}");
    assert!(rendered.contains("/content"), "{rendered}");
}

#[derive(Debug)]
struct StaleHeadOnceStore {
    inner: LocalFsStore,
    head_key: String,
    armed: AtomicBool,
}

impl StaleHeadOnceStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("construct local store"),
            head_key: wal_head(namespace_id),
            armed: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl ObjectStore for StaleHeadOnceStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(mode, PutMode::CompareAndSwap { .. })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            if let Some(existing) = self.inner.get(key, None).await? {
                let _ = self.inner.put_overwrite(key, existing).await?;
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

#[tokio::test]
async fn build_handles_installs_jsonl_object_store_metrics_recorder() {
    let store_dir = tempdir().expect("store tempdir");
    let metrics_dir = tempdir().expect("metrics tempdir");
    let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(store_dir.path(), "server-writer");
    let metrics_path = metrics_dir.path().join("object-store.ndjson");

    {
        let (writer, _reader, _admin) = build_handles_with_metrics_jsonl_path(
            &config,
            store,
            Some(metrics_path.clone().into_os_string()),
        )
        .await
        .expect("build handles");
        writer
            .create_namespace(&namespace_id("metrics"), CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }

    let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
    assert!(!jsonl.is_empty());
    assert!(!jsonl.contains("namespaces/metrics"));
}

#[tokio::test]
async fn app_validates_directly_built_configs() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(temp_dir.path(), "app-validate-writer");
    config.max_concurrent_uploads = 0;
    match super::app(config).await {
        Err(crate::config::ServerConfigError::InvalidField { field, .. }) => {
            assert_eq!(field, "max_concurrent_uploads");
        }
        Err(other) => panic!("expected invalid field error, got {other:?}"),
        Ok(_) => panic!("app must reject a zero upload bound"),
    }
}

#[tokio::test]
async fn admin_namespace_diagnostics_route_answers_storage_fields() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let (router, state) = app_with_store_and_state(
        test_config(temp_dir.path(), "namespace-diagnostics-route-writer"),
        store,
    )
    .await
    .expect("build app");
    let namespace_id = namespace_id("diagnostics");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    state
        .writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"diagnostic tail",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish one WAL segment");

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/v0/admin/namespaces/diagnostics/diagnostics")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .body(axum::body::Body::empty())
                .expect("diagnostics request"),
        )
        .await
        .expect("diagnostics response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("diagnostics body");
    let diagnostics: loonfs_api::NamespaceDiagnostics =
        serde_json::from_slice(&body).expect("namespace diagnostics response");
    assert_eq!(diagnostics.namespace_id, namespace_id);
    assert_eq!(diagnostics.head_seq, ChangeSeq(1));
    assert_eq!(diagnostics.retention_floor_seq, ChangeSeq(0));
    assert_eq!(diagnostics.current_manifest_no, None);
    assert_eq!(diagnostics.wal_tail_segments, 1);

    state.writer.shutdown().await.expect("shutdown writer");
}

#[tokio::test]
#[allow(clippy::disallowed_methods)]
// The sleeping handler is the controlled work the deadline cancels.
async fn request_deadline_answers_503_and_leaves_fast_handlers_untouched() {
    use tower::ServiceExt;

    let request_deadline_ms = 10;
    let router = axum::Router::new()
        .route(
            "/slow",
            axum::routing::get(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                "slow"
            }),
        )
        .route("/fast", axum::routing::get(|| async { "fast" }))
        .route_layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                super::with_request_deadline(request_deadline_ms, request, next)
            },
        ))
        .layer(axum::middleware::from_fn(super::with_request_id));

    let slow = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/slow")
                .body(axum::body::Body::empty())
                .expect("slow request"),
        )
        .await
        .expect("slow response");
    assert_eq!(slow.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    // Do not advertise an automatic retry when the cancelled work may still
    // complete.
    assert!(slow
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .is_none());
    let request_id = slow
        .headers()
        .get(super::REQUEST_ID_HEADER)
        .expect("request id header")
        .to_str()
        .expect("request id header text")
        .to_owned();
    let body = axum::body::to_bytes(slow.into_body(), usize::MAX)
        .await
        .expect("deadline body");
    let error: loonfs_api::ApiError = serde_json::from_slice(&body).expect("deadline envelope");
    assert_eq!(error.code, ErrorCode::DeadlineExceeded.as_str());
    assert_eq!(error.request_id.as_deref(), Some(request_id.as_str()));
    assert!(error.message.contains("request_deadline_ms"));
    assert!(error.message.contains("10"));

    let fast = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/fast")
                .body(axum::body::Body::empty())
                .expect("fast request"),
        )
        .await
        .expect("fast response");
    assert_eq!(fast.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(fast.into_body(), usize::MAX)
        .await
        .expect("fast body");
    assert_eq!(&body[..], b"fast");
}

#[tokio::test]
async fn deadline_exemptions_name_served_routes_and_cover_both_content_spellings() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let router = app_with_store(
        test_config(temp_dir.path(), "deadline-exempt-route-writer"),
        store,
    )
    .await
    .expect("build app");

    for content_route in [
        "/v0/namespaces/{namespace_id}/filesystem/content",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
    ] {
        assert!(
            super::DEADLINE_EXEMPT_ROUTES.contains(&content_route),
            "content route `{content_route}` is not deadline exempt"
        );
    }

    for route in super::DEADLINE_EXEMPT_ROUTES {
        let uri = route
            .replace("{namespace_id}", "deadline-exempt")
            .replace("{upload_id}", "upl_deadline_exempt")
            .replace("{inode_id}", "ino_1")
            .replace("{revision_no}", "1");
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(&uri)
                    .body(axum::body::Body::empty())
                    .expect("route request"),
            )
            .await
            .expect("route response");
        assert_ne!(
            response.status(),
            axum::http::StatusCode::NOT_FOUND,
            "deadline exemption `{route}` does not match a served route"
        );
    }
}

#[tokio::test]
async fn a_server_without_the_table_builds_no_local_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "no-local-cache-writer");

    let (_router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    assert!(state.local_cache.is_none());
    let rendered = state.metrics.render(None, 0, 0);
    assert!(!rendered.contains("loonfs_local_cache_"));
}

#[tokio::test]
async fn runtime_and_grep_cache_metrics_render_from_the_recorder() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "cache-metrics-writer");

    let (_router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let rendered = state.metrics.render(None, 0, 0);

    for name in [
        "loonfs_runtime_cache_latest_metadata_view_reads_total",
        "loonfs_metadata_segment_cache_gets_total",
        "loonfs_metadata_segment_cache_inserts_total",
        "loonfs_metadata_segment_cache_evictions_total",
        "loonfs_metadata_segment_cache_filter_skips_total",
        "loonfs_metadata_segment_cache_filter_false_positives_total",
        "loonfs_wal_tail_projection_cache_gets_total",
        "loonfs_wal_tail_projection_cache_inserts_total",
        "loonfs_wal_tail_projection_cache_evictions_total",
        "loonfs_wal_tail_projection_cache_evicted_rows_total",
        "loonfs_wal_tail_projection_cache_evicted_decoded_bytes_total",
        "loonfs_wal_tail_projection_cache_rejections_total",
        "loonfs_wal_tail_projection_cache_rejected_rows_total",
        "loonfs_wal_tail_projection_cache_rejected_decoded_bytes_total",
        "loonfs_grep_block_cache_gets_total",
        "loonfs_grep_block_cache_inserts_total",
        "loonfs_grep_block_cache_evictions_total",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {name} counter\n")),
            "missing counter `{name}`"
        );
    }
    for name in [
        "loonfs_wal_tail_projection_cache_retained_rows",
        "loonfs_wal_tail_projection_cache_retained_decoded_bytes",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {name} gauge\n")),
            "missing gauge `{name}`"
        );
    }
    assert!(!rendered.contains("loonfs_cache_metadata_segment_cache_hits"));
}

#[tokio::test]
async fn a_configured_local_cache_is_built_and_scraped() {
    let temp_dir = tempdir().expect("tempdir");
    let cache_dir = tempdir().expect("cache tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "local-cache-writer");
    config.local_cache = Some(test_local_cache_config(cache_dir.path()));

    let (_router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let local_cache = state.local_cache.clone().expect("a local cache");
    let rendered = state.metrics.render(Some(local_cache.foyer_stats()), 0, 0);
    assert!(rendered.contains("loonfs_local_cache_memory_capacity_bytes 4194304\n"));
    assert!(rendered.contains("loonfs_local_cache_disk_capacity_bytes 134217728\n"));
    assert!(rendered.contains("loonfs_local_cache_queue_buffer_overflows 0\n"));
    assert!(rendered.contains("loonfs_local_cache_queue_channel_overflows 0\n"));

    local_cache.close().await.expect("close local cache");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_closes_the_local_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let cache_dir = tempdir().expect("cache tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "shutdown-local-cache-writer");
    config.local_cache = Some(test_local_cache_config(cache_dir.path()));
    let shutdown_deadline_ms = config.shutdown_deadline_ms;

    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let local_cache = state.local_cache.clone().expect("a local cache");
    assert!(!local_cache.is_closed());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        state.writer.clone(),
        state.local_cache.clone(),
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    assert!(
        local_cache.is_closed(),
        "the graceful path closes the cache before it returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
// The sleeping handler keeps one connection in flight past the drain budget.
async fn graceful_shutdown_abandons_requests_at_the_deadline_and_settles_the_writer() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = test_runtime(store, "shutdown-deadline-writer").await;
    let handler_started = Arc::new(tokio::sync::Notify::new());
    let handler_signal = handler_started.clone();
    let router = axum::Router::new().route(
        "/slow",
        axum::routing::get(move || {
            let handler_signal = handler_signal.clone();
            async move {
                handler_signal.notify_one();
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                "late"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown_deadline_ms = 25;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        writer.clone(),
        None,
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let client = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect client");
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("send slow request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read slow response");
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        handler_started.notified(),
    )
    .await
    .expect("slow handler starts");

    shutdown_tx.send(()).expect("trigger shutdown");
    tokio::time::timeout(
        std::time::Duration::from_millis(shutdown_deadline_ms + 1_000),
        server,
    )
    .await
    .expect("serve_and_settle returns within the drain deadline plus slack")
    .expect("join server task")
    .expect("shutdown settles the writer");
    assert!(writer.is_shutting_down(), "writer shutdown completed");

    client.abort();
}

fn test_local_cache_config(root: &Path) -> crate::config::LocalCacheConfig {
    crate::config::LocalCacheConfig {
        path: root.display().to_string(),
        memory_bytes: 4 * 1024 * 1024,
        disk_bytes: 128 * 1024 * 1024,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_requests_and_settles_the_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(temp_dir.path(), "shutdown-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve_on(listener, config, async move {
        let _ = shutdown_rx.await;
    }));

    // The server accepts work while running.
    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace over http");

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    // The listener is closed once serve returns.
    assert!(
        std::net::TcpStream::connect(addr).is_err(),
        "listener should refuse connections after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_shutdown_drains_an_active_grep_step() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-shutdown");
    let blocking_store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::exact(grep_root_key(&namespace_id)),
        OperationClass::GetWithMetadata,
    ));
    let store = blocking_store.clone() as SharedObjectStore;
    let writer = test_runtime(store.clone(), "grep-shutdown-seed").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    grep_worker(&store, "grep-shutdown-enable")
        .await
        .enable(&namespace_id)
        .await
        .expect("enable grep");

    blocking_store.block_next();
    let config = test_config(temp_dir.path(), "grep-shutdown-server");
    let (_router, state) = super::app_with_store_and_direct_transfers(config, store, None)
        .await
        .expect("build app");
    state
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    blocking_store.wait_until_blocked().await;

    let shutdown = tokio::runtime::Handle::current().spawn({
        let writer = state.writer.clone();
        async move { writer.shutdown().await }
    });
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the active bounded grep step"
    );
    blocking_store.release();
    shutdown
        .await
        .expect("join shutdown")
        .expect("drain grep step");
}

/// A job that does nothing but count the steps the runner admitted for it.
///
/// It is registered on the server's own writer, so it queues, waits for a
/// permit, and is shut down through exactly the admission every other job
/// goes through. Counting is the whole point: an ordinary step's work is
/// object-store traffic, and the question this test asks is whether any of
/// it is issued at all once a shutdown has begun.
struct StepCountingJob {
    id: MaintenanceJobId,
    steps: Arc<AtomicUsize>,
}

#[async_trait]
impl MaintenanceJob for StepCountingJob {
    fn id(&self) -> MaintenanceJobId {
        self.id
    }

    async fn step(
        &self,
        _namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> loonfs::Result<MaintenanceStepReport> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        // Idle rather than progressed: a requeueing step would never let
        // the control settle below.
        Ok(MaintenanceStepReport::concluded(
            MaintenanceStepConclusion::Idle,
        ))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> loonfs::Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

#[tokio::test]
async fn shutdown_closes_maintenance_admission_before_draining_publications() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("shutdown-order");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
    ));
    let config = test_config(temp_dir.path(), "shutdown-order-server");
    let (_router, state) = super::app_with_store_and_direct_transfers(
        config,
        blocking.clone() as SharedObjectStore,
        None,
    )
    .await
    .expect("build app");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let steps = Arc::new(AtomicUsize::new(0));
    let job = MaintenanceJobId::new("shutdown-order-probe");
    state
        .writer
        .register_maintenance_job(Arc::new(StepCountingJob {
            id: job,
            steps: Arc::clone(&steps),
        }))
        .expect("register the counting job");

    // The control. Without it, a later count of zero would prove only that
    // this job never ran under any conditions.
    state.writer.maintenance().nudge(job, &namespace_id);
    state
        .writer
        .flush_background()
        .await
        .expect("settle the admitted step");
    let admitted_while_serving = steps.load(Ordering::SeqCst);
    assert_eq!(
        admitted_while_serving, 1,
        "a nudge on a serving deployment admits one step"
    );

    // Park a publication so the shutdown's publication drain is still
    // pending when its first poll returns — the window the runner would
    // otherwise keep admitting into.
    blocking.block_next();
    let put = tokio::spawn({
        let writer = state.writer.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer
                .put_file_bytes(
                    &namespace_id,
                    "/parked.txt",
                    b"body",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
        }
    });
    blocking.wait_until_blocked().await;

    let mut shutdown = Box::pin(state.writer.shutdown());
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "the parked publication must keep the shutdown pending"
    );
    // Everything after this point is the drain window.
    state.writer.maintenance().nudge(job, &namespace_id);

    blocking.release();
    put.await
        .expect("join the parked put")
        .expect("the released put succeeds");
    // Releasing the put also lets it publish, which fires the publish
    // observer's own nudge — the production path into this same window.
    shutdown
        .await
        .expect("the shutdown settles with its queue discarded");

    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "no maintenance step may be admitted once the shutdown has begun"
    );
    // And the runner stays shut rather than reopening behind the drain.
    state.writer.maintenance().nudge(job, &namespace_id);
    state
        .writer
        .flush_background()
        .await
        .expect("a shut runner has nothing left to settle");
    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "a nudge after the shutdown must admit nothing either"
    );
}

#[tokio::test]
async fn a_namespace_advance_nudges_the_enabled_namespaces_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "grep-observer-server");
    let (_router, state) = super::app_with_store_and_direct_transfers(config, store, None)
        .await
        .expect("build app");
    let namespace_id = namespace_id("grep-observer");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    state
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    state
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    state
        .writer
        .flush_background()
        .await
        .expect("settle the backfill");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(0),
        "an empty namespace's backfill completes at its own head"
    );

    // The publish is the only trigger from here on: nothing below nudges.
    state
        .writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"observer-driven needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish file");
    state
        .writer
        .flush_background()
        .await
        .expect("settle the observer-driven step");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(1),
        "the publish observer is what carried the index to the new head"
    );
    let request = GrepRequest {
        pattern: "observer-driven needle".to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        allow_stale: false,
        allow_scan: false,
    };
    let service = state
        .grep_service
        .as_ref()
        .expect("a query-serving app carries a grep service");
    let store = state.writer.object_store();
    let reads = NamespaceReads::new(&state.reader, &namespace_id);
    let response = service
        .query(
            &request,
            PaginationPolicy::default()
                .resolve_limit(None)
                .expect("the pagination policy accepts its own default"),
            &reads,
            &store,
        )
        .await
        .expect("grep caught-up index");
    assert_eq!(response.matches.len(), 1);
    state.writer.shutdown().await.expect("drain the writer");
}

/// What the index's steps published, read where an operator reads it.
async fn built_through_seq(state: &AppState, namespace_id: &NamespaceId) -> ChangeSeq {
    load_grep_root(&*state.writer.object_store(), namespace_id)
        .await
        .expect("load grep root")
        .expect("an enabled namespace has a grep root")
        .manifest_state()
        .status()
        .active_watermark()
        .expect("an active grep root has a watermark")
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_disabled_root_is_not_materialized_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-disabled");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    let worker = grep_error_worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable grep");
    worker.disable(&namespace_id).await.expect("disable grep");
    writer.shutdown().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "disabled-server").await;
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding, None);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        501,
        ErrorCode::NotSupported,
        "not enabled",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_mid_backfill_is_not_materialized_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-backfill");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    grep_error_worker(&store)
        .await
        .enable(&namespace_id)
        .await
        .expect("leave grep backfilling");
    writer.shutdown().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "backfill-server").await;
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding, None);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        501,
        ErrorCode::NotSupported,
        "backfill has not completed",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_store_outage_is_provider_failure_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-error-store");
    let fault_store = Arc::new(FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::exact(grep_root_key(&namespace_id)),
        OperationClass::GetWithMetadata,
        InjectedError::Transport("injected grep-root outage".to_owned()),
    ));
    let store = fault_store.clone() as SharedObjectStore;
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    writer.shutdown().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "store-server").await;
    fault_store.fail_next(1);
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding, None);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        500,
        ErrorCode::ServerError,
        "object-store request failed",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_unreadable_roots_are_index_corrupt_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let corrupt_pointer = namespace_id("grep-error-pointer");
    let missing_manifest = namespace_id("grep-error-missing-manifest");
    let corrupt_manifest = namespace_id("grep-error-manifest");
    let identity_mismatch = namespace_id("grep-error-identity");

    let writer = test_runtime(store.clone(), "grep-error-seed").await;
    for namespace_id in [
        &corrupt_pointer,
        &missing_manifest,
        &corrupt_manifest,
        &identity_mismatch,
    ] {
        seed_grep_error_namespace_on(&writer, namespace_id).await;
    }

    store
        .put_overwrite(
            &grep_root_key(&corrupt_pointer),
            Bytes::from_static(b"corrupt grep pointer"),
        )
        .await
        .expect("write corrupt grep pointer");

    write_grep_pointer(
        &*store,
        &missing_manifest,
        missing_manifest.clone(),
        GrepManifestObjectId::parse("gmf_11111111111111111111111111111111")
            .expect("manifest object id"),
    )
    .await;

    let manifest_object_id = GrepManifestObjectId::parse("gmf_22222222222222222222222222222222")
        .expect("manifest object id");
    store
        .put_overwrite(
            &grep_manifest_key(&corrupt_manifest, &manifest_object_id),
            Bytes::from_static(b"corrupt grep manifest"),
        )
        .await
        .expect("write corrupt grep manifest");
    write_grep_pointer(
        &*store,
        &corrupt_manifest,
        corrupt_manifest.clone(),
        manifest_object_id,
    )
    .await;

    write_grep_pointer(
        &*store,
        &identity_mismatch,
        NamespaceId::parse("different-grep-identity").expect("different namespace id"),
        GrepManifestObjectId::parse("gmf_33333333333333333333333333333333")
            .expect("manifest object id"),
    )
    .await;

    writer.shutdown().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "index-corrupt-server").await;
    let client = &harness.client;
    for namespace_id in [
        corrupt_pointer,
        missing_manifest,
        corrupt_manifest,
        identity_mismatch,
    ] {
        let result = client
            .grep(&namespace_id, &grep_error_request(), None)
            .await;
        assert_grep_api_error_and_core_read(
            client,
            &namespace_id,
            result,
            500,
            ErrorCode::IndexCorrupt,
            "disable and re-enable grep to rebuild it",
        )
        .await;
    }
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_publication_conflict_is_stale_head_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-error-conflict");
    let root_key = grep_root_key(&namespace_id);
    let fault_store = Arc::new(FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        move |context: &OperationContext<'_>| {
            context.key() == root_key
                && matches!(
                    context.kind(),
                    OperationKind::Put {
                        mode: PutMode::CreateIfAbsent | PutMode::CompareAndSwap { .. },
                        ..
                    }
                )
        },
        InjectedError::PreconditionFailed,
    ));
    let store = fault_store.clone() as SharedObjectStore;
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    writer.shutdown().await.expect("shutdown writer");

    let harness = start_grep_admin_error_server(store, temp_dir.path(), "conflict-server").await;
    fault_store.fail_next(1);
    let client = &harness.client;
    let result = client.enable_grep_index(&namespace_id);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        409,
        ErrorCode::StaleHead,
        "publication conflict",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_created_state_is_readable_through_http() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = test_runtime(store.clone(), "runtime-writer").await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace through runtime");
    fs.put_file_bytes(
        &namespace_id,
        "/notes/hello.txt",
        b"hello from runtime",
        PutFileOptions {
            behavior: DestinationBehavior::NoReplace,
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: Some(CommitId::parse("runtime-put").expect("valid commit id")),
                message: None,
            },
            expected_revision_no: None,
        },
    )
    .await
    .expect("write file through runtime");

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/hello.txt").expect("target");
    let stat = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat file");
    assert_eq!(stat.path, "/notes/hello.txt");
    assert_eq!(stat.size_bytes(), Some(18));
    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello from runtime");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_created_state_is_readable_through_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = test_runtime(store.clone(), "runtime-reader").await;
    let harness = start_server(store.clone(), temp_dir.path(), "server-writer").await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace through http");
    let target = NamespacePath::parse("demo", "/notes/from-http.txt").expect("target");
    harness
        .client
        .put_file_bytes(&target, b"hello from http", &replace_file_options())
        .await
        .expect("write file through http");

    let file = fs
        .reader()
        .get_file_bytes(
            &NamespaceId::parse("demo").expect("valid namespace id"),
            "/notes/from-http.txt",
        )
        .await
        .expect("read file through runtime");
    assert_eq!(file.bytes, b"hello from http");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_missing_namespace_mutations_return_namespace_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let target = NamespacePath::parse("missing", "/notes/hello.txt").expect("target");
    assert_api_error(
        harness
            .client
            .put_file_bytes(&target, b"hello", &replace_file_options())
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );
    assert_api_error(
        harness
            .client
            .delete_path(
                &target,
                &DeleteOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );
    let destination = NamespacePath::parse("missing", "/notes/renamed.txt").expect("target");
    assert_api_error(
        harness
            .client
            .move_path(
                &target,
                &destination,
                &MoveOptions {
                    behavior: DestinationBehavior::NoReplace,
                    commit: loonfs_api::options::CommitOptions {
                        actor: loonfs_test_support::test_actor(),
                        commit_id: None,
                        message: None,
                    },
                },
            )
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_missing_namespace_reads_return_namespace_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let target = NamespacePath::parse("missing", "/").expect("target");
    let mut pager =
        harness
            .client
            .list_path_entries_pager(&target, None, None, &Default::default());
    assert_api_error(
        pager.next().await.expect("a fresh pager has one page"),
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_missing_path_returns_path_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/missing.txt").expect("target");
    assert_api_error(
        harness
            .client
            .delete_path(
                &target,
                &DeleteOptions::new(loonfs_test_support::test_actor()),
            )
            .await,
        404,
        "path_not_found",
        None,
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_over_directory_and_move_into_existing_target_return_path_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seeder = bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/readme.txt",
        b"readme",
        "seed-docs",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/tmp/a.txt",
        b"from tmp",
        "seed-tmp",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/a.txt",
        b"in docs",
        "seed-target",
    )
    .await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let dir_target = NamespacePath::parse("demo", "/docs").expect("dir target");
    assert_api_error(
        harness
            .client
            .put_file_bytes(&dir_target, b"not a file", &replace_file_options())
            .await,
        409,
        "path_conflict",
        None,
    );

    let from = NamespacePath::parse("demo", "/tmp/a.txt").expect("from");
    let to = NamespacePath::parse("demo", "/docs/a.txt").expect("to");
    assert_api_error(
        harness
            .client
            .move_path(
                &from,
                &to,
                &MoveOptions {
                    behavior: DestinationBehavior::NoReplace,
                    commit: loonfs_api::options::CommitOptions {
                        actor: loonfs_test_support::test_actor(),
                        commit_id: None,
                        message: None,
                    },
                },
            )
            .await,
        409,
        "path_conflict",
        None,
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_and_move_under_deleted_ancestor_create_fresh_subtrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seeder = bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/old.txt",
        b"old",
        "seed-docs",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/tmp/source.txt",
        b"source",
        "seed-source",
    )
    .await;
    delete_path_recursive(&seeder, &namespace_id("demo"), "/docs", "delete-docs").await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    // The deleted name is invisible and immediately reusable; the
    // dead subtree's children stay dead.
    let put_target = NamespacePath::parse("demo", "/docs/new.txt").expect("put target");
    harness
        .client
        .put_file_bytes(&put_target, b"new", &replace_file_options())
        .await
        .expect("put recreates the subtree");
    let old_child = NamespacePath::parse("demo", "/docs/old.txt").expect("old child");
    assert_api_error(
        harness
            .client
            .get_path_entry(&old_child, &Default::default())
            .await,
        404,
        "path_not_found",
        None,
    );

    let from = NamespacePath::parse("demo", "/tmp/source.txt").expect("from");
    let to = NamespacePath::parse("demo", "/docs/source.txt").expect("to");
    harness
        .client
        .move_path(
            &from,
            &to,
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
            },
        )
        .await
        .expect("move lands in the recreated subtree");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_path_mutation_retries_transient_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let store =
        Arc::new(StaleHeadOnceStore::new(temp_dir.path(), &namespace_id)) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/race.txt").expect("target");
    let result = harness
        .client
        .put_file_bytes(&target, b"race", &replace_file_options())
        .await
        .expect("path write retries stale head");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_first_write_takes_over_a_namespace_owned_by_another_writer() {
    // With no lease, the server's first semantic write acquires the
    // epoch immediately and fences the previous session.
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "other-writer", &namespace_id("demo")).await;
    let store_for_check = store.clone();

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/taken-over.txt").expect("target");
    let result = harness
        .client
        .put_file_bytes(&target, b"taken over", &replace_file_options())
        .await
        .expect("first write takes over the namespace");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    let head = loonfs::control::load_namespace_head_control(
        store_for_check.as_ref(),
        &namespace_id("demo"),
    )
    .await
    .expect("read head")
    .state;
    assert_eq!(
        head.writer.expect("writer block").writer_id,
        "server-writer"
    );

    harness.server.abort();
}

struct TestHarness {
    client: Client,
    server: tokio::task::JoinHandle<()>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_answers_401_in_envelope_for_missing_and_wrong_tokens() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    for auth_token in [None, Some("wrong-token".to_owned())] {
        let client = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: auth_token.map(Into::into),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config");
        assert_api_error(
            client.get_namespace(&namespace_id("demo")).await,
            401,
            "unauthorized",
            Some("missing or invalid bearer token"),
        );
        // The checkpoint inventory names this deployment's garbage-collection
        // roots, so it answers behind the same token as everything else.
        let mut pager = client.list_checkpoints_pager(&namespace_id("demo"), None, None);
        assert_api_error(
            pager.next().await.expect("a fresh pager has one page"),
            401,
            "unauthorized",
            Some("missing or invalid bearer token"),
        );
    }

    server.abort();
}

/// Sends a request and checks its JSON error response.
///
/// On macOS, ureq can hit EINVAL when the server rejects a request before
/// reading its body. The retry helper handles only that socket error.
fn expect_enveloped(
    send: impl Fn() -> Result<ureq::Response, ureq::Error>,
    expectation: &str,
    status: u16,
    code: &str,
) -> serde_json::Value {
    retry_on_macos_teardown_einval(|| {
        let error = send().expect_err(expectation);
        let ureq::Error::Status(actual_status, response) = error else {
            panic!("expected a status error, got {error:?}");
        };
        assert_eq!(actual_status, status);
        assert!(response.header("x-request-id").is_some());
        let body = response.into_string().expect("read error body");
        let body: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|_| panic!("json body, got: {body}"));
        assert_eq!(body["code"], code);
        body
    })
}

/// Checks that malformed requests return JSON API errors and that
/// authentication runs before request parsing.
// The request closures return ureq's large error type.
#[allow(clippy::result_large_err)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_malformed_request_pieces_answer_in_envelope_behind_auth() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    // An invalid numeric query value returns invalid_request.
    let changes_url = format!("http://{addr}/v0/namespaces/demo/changes?after_seq=abc");
    let body = expect_enveloped(
        || {
            raw_agent()
                .get(&changes_url)
                .set("authorization", "Bearer test-token")
                .call()
        },
        "malformed after_seq should answer 400",
        400,
        "invalid_request",
    );
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("invalid after_seq `abc`:")),
        "{body}"
    );
    assert_eq!(body["param"], "after_seq");

    // The largest allowed value reaches namespace lookup. One higher is
    // rejected while parsing the query.
    expect_enveloped(
        || {
            raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/changes?after_seq=9007199254740991"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "valid sequence reaches the missing namespace",
        404,
        "namespace_not_found",
    );

    let body = expect_enveloped(
        || {
            raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/changes?after_seq=9007199254740992"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "out-of-range after_seq should return 400",
        400,
        "invalid_request",
    );
    assert_eq!(
        body["message"],
        "invalid after_seq `9007199254740992`: must be an integer from 0 through 9007199254740991"
    );
    assert_eq!(body["param"], "after_seq");

    // Without credentials, authentication fails before the query is parsed.
    expect_enveloped(
        || raw_agent().get(&changes_url).call(),
        "unauthorized should answer 401",
        401,
        "unauthorized",
    );

    // Optional numeric fields return a message that identifies the field.
    let body = expect_enveloped(
        || {
            raw_agent()
                .delete(&format!(
                    "http://{addr}/v0/namespaces/demo?expected_head_seq=abc"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "malformed expected_head_seq should answer 400",
        400,
        "invalid_request",
    );
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("invalid expected_head_seq `abc`:")),
        "{body}"
    );
    assert_eq!(body["param"], "expected_head_seq");

    // A missing required query parameter returns invalid_request.
    let body = expect_enveloped(
        || {
            raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/entry"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "missing path parameter should answer 400",
        400,
        "invalid_request",
    );
    assert_eq!(body["param"], "path");

    // A malformed JSON body returns invalid_request after authentication.
    // Without credentials, the server returns 401 before reading the body.
    let create_url = format!("http://{addr}/v0/namespaces");
    let body = expect_enveloped(
        || {
            raw_agent()
                .post(&create_url)
                .set("authorization", "Bearer test-token")
                .set("content-type", "application/json")
                .send_string("{not json")
        },
        "malformed body should answer 400",
        400,
        "invalid_request",
    );
    assert!(body.get("param").is_none());
    expect_enveloped(
        || {
            raw_agent()
                .post(&create_url)
                .set("content-type", "application/json")
                .send_string("{not json")
        },
        "unauthorized malformed body should answer 401",
        401,
        "unauthorized",
    );

    // Invalid operation paths return invalid_request after authentication.
    let commits_url = format!("http://{addr}/v0/namespaces/demo/commits");
    let invalid_operation = r#"{
        "commit_id":"invalid-path",
        "actor":{"kind":"service","id":"test-service"},
        "operations":[{"kind":"create_directory","path":"relative"}]
    }"#;
    let body = expect_enveloped(
        || {
            raw_agent()
                .post(&commits_url)
                .set("authorization", "Bearer test-token")
                .set("content-type", "application/json")
                .send_string(invalid_operation)
        },
        "invalid operation path should answer 400",
        400,
        "invalid_request",
    );
    assert_eq!(body["param"], "/operations/0/path");
    expect_enveloped(
        || {
            raw_agent()
                .post(&commits_url)
                .set("content-type", "application/json")
                .send_string(invalid_operation)
        },
        "authorization should precede operation path decoding",
        401,
        "unauthorized",
    );

    for (body, description) in [
        (
            r#"{"commit_id":"missing-actor","operations":[{"kind":"create_directory","path":"/docs"}]}"#,
            "missing actor",
        ),
        (
            r#"{"commit_id":"malformed-actor","actor":{"kind":"robot","id":"x"},"operations":[{"kind":"create_directory","path":"/docs"}]}"#,
            "malformed actor",
        ),
    ] {
        expect_enveloped(
            || {
                raw_agent()
                    .post(&commits_url)
                    .set("authorization", "Bearer test-token")
                    .set("content-type", "application/json")
                    .send_string(body)
            },
            description,
            400,
            "invalid_request",
        );
        expect_enveloped(
            || {
                raw_agent()
                    .post(&commits_url)
                    .set("content-type", "application/json")
                    .send_string(body)
            },
            description,
            401,
            "unauthorized",
        );
    }

    // Invalid grep path prefixes return invalid_request after authentication.
    let grep_url =
        format!("http://{addr}/v0/namespaces/demo/grep?pattern=needle&path_prefix=relative");
    let body = expect_enveloped(
        || {
            raw_agent()
                .get(&grep_url)
                .set("authorization", "Bearer test-token")
                .call()
        },
        "invalid grep path should answer 400",
        400,
        "invalid_request",
    );
    assert_eq!(body["param"], "path_prefix");
    expect_enveloped(
        || raw_agent().get(&grep_url).call(),
        "authorization should precede grep path decoding",
        401,
        "unauthorized",
    );

    let grep_gc_url = format!("http://{addr}/v0/admin/namespaces/demo/grep/index/gc");
    expect_enveloped(
        || raw_agent().post(&grep_gc_url).call(),
        "grep index collection should require authorization",
        401,
        "unauthorized",
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_body_over_the_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_upload_bytes = 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    let target = NamespacePath::parse("demo", "/big.bin").expect("target");
    let namespace = namespace_id("demo");

    // The cap is the server's, and it holds against any client: this drives
    // the proxied content route directly, the way a client that never read
    // the capability document would.
    let session = client
        .create_upload(
            &namespace,
            &loonfs_api::v0::BeginUploadRequest::ServiceProxied {},
        )
        .await
        .expect("begin a proxied upload session");
    assert_api_error(
        client
            .put_upload_content(&namespace, session.upload_id(), &[0u8; 4096])
            .await,
        413,
        "content_too_large",
        None,
    );

    // A client that did read the document never sends it at all: with no
    // direct transport on offer here, the payload has nowhere to go and the
    // refusal comes before the bytes move.
    match client
        .put_file_bytes(&target, &[0u8; 4096], &replace_file_options())
        .await
    {
        Err(ClientError::UploadTooLarge { size_bytes, .. }) => assert_eq!(size_bytes, 4096),
        other => panic!("expected a client-side refusal, got {other:?}"),
    }

    // A body inside the limit still goes through on the same route.
    client
        .put_file_bytes(&target, &[0u8; 512], &replace_file_options())
        .await
        .expect("small upload fits under the limit");

    server.abort();
}

/// A payload with a distinct byte at every offset, so bytes landing in the
/// wrong order or twice cannot go unnoticed.
fn distinct_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// What the write path is allowed to hold at once, and what it is asked to
/// carry: three internal parts' worth, so a path that materializes its
/// payload is caught by more than a rounding error.
const MEMORY_BOUND_PART_BYTES: u64 = loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES;
const MEMORY_BOUND_PAYLOAD_BYTES: usize = 3 * MEMORY_BOUND_PART_BYTES as usize + 4_096;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxied_upload_route_never_holds_the_whole_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let watched = Arc::new(BufferWatchStore::watching_content(
        LocalFsStore::new(temp_dir.path()).expect("store"),
    ));
    let store = Arc::clone(&watched) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let payload = distinct_bytes(MEMORY_BOUND_PAYLOAD_BYTES);
    let target = NamespacePath::parse("demo", "/streamed.bin").expect("target");
    harness
        .client
        .put_file_bytes(&target, &payload, &replace_file_options())
        .await
        .expect("a multi-part payload uploads through the proxied route");

    let peaks = watched.peaks();
    assert_eq!(
        peaks.total_bytes, MEMORY_BOUND_PAYLOAD_BYTES as u64,
        "every payload byte crossed the store boundary exactly once"
    );
    assert!(
        peaks.largest_buffer_bytes <= MEMORY_BOUND_PART_BYTES,
        "no single buffer may exceed one part: largest was {}",
        peaks.largest_buffer_bytes
    );
    assert!(
        peaks.peak_live_bytes <= MEMORY_BOUND_PART_BYTES,
        "the write path held {} bytes at once, past its one-part window",
        peaks.peak_live_bytes
    );

    // And the bytes are the bytes.
    let read_back = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read the streamed object back");
    assert_eq!(read_back, payload);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_streamed_writes_a_multi_part_payload_one_part_at_a_time() {
    let temp_dir = tempdir().expect("tempdir");
    let watched =
        BufferWatchStore::watching_content(LocalFsStore::new(temp_dir.path()).expect("store"));

    let payload = distinct_bytes(MEMORY_BOUND_PAYLOAD_BYTES);
    let key = loonfs_objectstore::keys::content_blob(
        &loonfs_api::ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id"),
        &loonfs_api::ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
    );
    // Use HTTP-sized chunks so the store must regroup them.
    let chunks: Vec<Bytes> = payload
        .chunks(64 * 1024)
        .map(Bytes::copy_from_slice)
        .collect();
    let stored = watched
        .put_streamed(
            &key,
            futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("stream a multi-part payload into the store");

    assert_eq!(stored, MEMORY_BOUND_PAYLOAD_BYTES as u64);
    let peaks = watched.peaks();
    assert_eq!(peaks.total_bytes, MEMORY_BOUND_PAYLOAD_BYTES as u64);
    assert!(
        peaks.largest_buffer_bytes <= MEMORY_BOUND_PART_BYTES,
        "no single buffer may exceed one part: largest was {}",
        peaks.largest_buffer_bytes
    );
    assert!(
        peaks.peak_live_bytes <= MEMORY_BOUND_PART_BYTES,
        "the store held {} bytes at once, past its one-part window",
        peaks.peak_live_bytes
    );
    assert_eq!(
        watched
            .get(&key, None)
            .await
            .expect("read back")
            .expect("object exists"),
        Bytes::from(payload)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_unknown_routes_and_methods_answer_in_envelope() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    // Unknown path: in-envelope 404 instead of axum's empty body.
    let error = raw_agent()
        .get(&format!("http://{addr}/v0/nonexistent"))
        .call()
        .expect_err("unknown route should answer 404");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for an unknown route");
    };
    assert_eq!(status, 404);
    assert!(response.header("x-request-id").is_some());
    let body = response.into_string().expect("read 404 body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json 404 body");
    assert_eq!(body["code"], "route_not_found");

    // Served path, unserved method: in-envelope 405.
    let error = raw_agent()
        .delete(&format!("http://{addr}/v0/capabilities"))
        .call()
        .expect_err("wrong method should answer 405");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for a wrong method");
    };
    assert_eq!(status, 405);
    let body = response.into_string().expect("read 405 body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json 405 body");
    assert_eq!(body["code"], "method_not_allowed");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_revisions_cursor_resumes_after_head_drift_and_rejects_the_future() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/file.txt",
        b"one",
        "c-rev1",
    )
    .await;
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/file.txt",
        b"two",
        "c-rev2",
    )
    .await;

    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let cursor = tokio::task::spawn_blocking({
        move || {
            let response = raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/revisions"
                ))
                .set("authorization", "Bearer test-token")
                .query("path", "/notes/file.txt")
                .query("limit", "1")
                .call()
                .expect("first revisions page");
            let body = response.into_string().expect("read revisions body");
            let body: serde_json::Value = serde_json::from_str(&body).expect("json revisions");
            body["next_cursor"]
                .as_str()
                .expect("two revisions produce a next_cursor")
                .to_owned()
        }
    })
    .await
    .expect("join blocking task");

    // A commit landing mid-listing does not retire the cursor: the resume
    // continues after the last returned revision against the new head.
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/other.txt",
        b"x",
        "c-rev3",
    )
    .await;

    let resumed = tokio::task::spawn_blocking({
        let cursor = cursor.clone();
        move || {
            let response = raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/revisions"
                ))
                .set("authorization", "Bearer test-token")
                .query("path", "/notes/file.txt")
                .query("limit", "1")
                .query("cursor", &cursor)
                .call()
                .expect("cursor resumes after head drift");
            let body = response.into_string().expect("read resumed body");
            serde_json::from_str::<serde_json::Value>(&body).expect("json resumed body")
        }
    })
    .await
    .expect("join blocking task");
    assert_eq!(resumed["revisions"][0]["revision_no"], 1);
    assert!(resumed["next_cursor"].is_null());

    // A cursor from the future stays unanswerable.
    let mut future_cursor: loonfs_api::FileRevisionsPageCursor =
        loonfs_api::decode_cursor(&cursor).expect("decode revisions cursor");
    future_cursor.head_seq = loonfs_api::ChangeSeq(future_cursor.head_seq.0 + 1000);
    let future_cursor = loonfs_api::encode_cursor(&future_cursor).expect("encode future cursor");
    let error = raw_agent()
        .get(&format!(
            "http://{addr}/v0/namespaces/demo/filesystem/revisions"
        ))
        .set("authorization", "Bearer test-token")
        .query("path", "/notes/file.txt")
        .query("limit", "1")
        .query("cursor", &future_cursor)
        .call()
        .expect_err("future cursor should answer rebootstrap_required");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for a future cursor");
    };
    assert_eq!(status, 409);
    let body = response.into_string().expect("read future-cursor body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json future-cursor body");
    assert_eq!(body["code"], "rebootstrap_required");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_uploads_answer_server_busy_at_the_concurrency_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_uploads = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    // Hold the only buffering slot, standing in for a slow concurrent
    // upload; the next proxied body must be refused before buffering.
    let held = state
        .upload_permits
        .clone()
        .try_acquire_owned()
        .expect("hold the only upload slot");

    let client_config = ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        // These tests assert the raw concurrency-cap answer; the client's
        // transient retry would otherwise sleep through it.
        disable_transient_retry: true,
        ca_cert_path: None,
    };
    let config_for_busy = client_config.clone();
    let client = Client::new(config_for_busy).expect("valid client config");
    let target = NamespacePath::parse("demo", "/one.bin").expect("target");
    assert_api_error(
        client
            .put_file_bytes(&target, &[0u8; 64], &replace_file_options())
            .await,
        503,
        "server_busy",
        Some("the server is at its concurrency limit for proxied uploads; retry shortly"),
    );
    // The refusal is countable: an operator sizing `max_concurrent_uploads`
    // needs to know it is happening, not only that some clients saw 503.
    assert!(state
        .metrics
        .render(None, 0, 0)
        .contains("loonfs_server_busy_rejections_total{kind=\"upload\"} 1\n"));

    drop(held);
    let client = Client::new(client_config).expect("valid client config");
    let target = NamespacePath::parse("demo", "/one.bin").expect("target");
    client
        .put_file_bytes(&target, &[0u8; 64], &replace_file_options())
        .await
        .expect("a freed slot admits the upload");

    server.abort();
}

// The request closures return ureq's large error type.
#[allow(clippy::result_large_err)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_content_reads_answer_server_busy_at_the_concurrency_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seed_writer = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/note.txt",
        b"bounded",
        "download-busy-seed-01",
    )
    .await;
    let inode_id = seed_writer
        .reader()
        .get_path_entry(&namespace_id("demo"), "/note.txt", Default::default())
        .await
        .expect("stat seeded file")
        .inode_id;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_downloads = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let held = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .expect("hold the only download slot");

    let client_config = ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        // These tests assert the raw concurrency-cap answer; the client's
        // transient retry would otherwise sleep through it.
        disable_transient_retry: true,
        ca_cert_path: None,
    };
    let config_for_busy = client_config.clone();
    let client = Client::new(config_for_busy).expect("valid client config");
    let target = NamespacePath::parse("demo", "/note.txt").expect("target");
    assert_api_error(
        client.get_file_bytes(&target).await,
        503,
        "server_busy",
        Some("the server is at its concurrency limit for proxied content reads; retry shortly"),
    );
    assert_api_error(
        client
            .get_file_revision_bytes_by_inode(
                &namespace_id("demo"),
                inode_id,
                loonfs_api::RevisionNo(1),
            )
            .await,
        503,
        "server_busy",
        Some("the server is at its concurrency limit for proxied content reads; retry shortly"),
    );
    expect_enveloped(
        || {
            raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/content"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "a missing path should answer 400 at the concurrency cap",
        400,
        "invalid_request",
    );
    expect_enveloped(
        || {
            raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/inodes/not-an-inode/revisions/1/content"
                ))
                .set("authorization", "Bearer test-token")
                .call()
        },
        "a malformed inode id should answer 400 at the concurrency cap",
        400,
        "invalid_request",
    );
    assert!(state
        .metrics
        .render(None, 0, 0)
        .contains("loonfs_server_busy_rejections_total{kind=\"download\"} 2\n"));

    drop(held);
    let client = Client::new(client_config).expect("valid client config");
    let target = NamespacePath::parse("demo", "/note.txt").expect("target");
    let bytes = client
        .get_file_bytes(&target)
        .await
        .expect("a freed slot admits the read");
    assert_eq!(bytes, b"bounded");
    assert_eq!(
        client
            .get_file_revision_bytes_by_inode(
                &namespace_id("demo"),
                inode_id,
                loonfs_api::RevisionNo(1),
            )
            .await
            .expect("a freed slot admits the inode read"),
        b"bounded"
    );

    server.abort();
}

#[tokio::test]
async fn download_admission_is_held_until_the_response_body_is_consumed() {
    use tower::ServiceExt;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seed_writer = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/note.txt",
        b"bounded",
        "download-body-permit-seed-01",
    )
    .await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_downloads = 1;
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/v0/namespaces/demo/filesystem/content?path=%2Fnote.txt")
                .header(axum::http::header::AUTHORIZATION, "Bearer test-token")
                .body(axum::body::Body::empty())
                .expect("download request"),
        )
        .await
        .expect("download response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert_eq!(
        response.headers().get(axum::http::header::CONTENT_TYPE),
        Some(&axum::http::HeaderValue::from_static(
            "application/octet-stream"
        ))
    );
    assert_eq!(state.download_permits.available_permits(), 0);

    let next_permit = state.download_permits.clone().acquire_owned();
    tokio::pin!(next_permit);
    assert!(matches!(
        futures::poll!(next_permit.as_mut()),
        std::task::Poll::Pending
    ));

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("download body");
    assert_eq!(&body[..], b"bounded");
    let acquired = next_permit
        .await
        .expect("the next download is admitted after full consumption");
    drop(acquired);
    assert_eq!(state.download_permits.available_permits(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_content_read_over_the_download_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seed_writer = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/big.bin",
        &[0u8; 64],
        "download-limit-seed-01",
    )
    .await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/small.bin",
        &[0u8; 8],
        "download-limit-seed-02",
    )
    .await;
    let big_inode_id = seed_writer
        .reader()
        .get_path_entry(&namespace_id("demo"), "/big.bin", Default::default())
        .await
        .expect("stat big file")
        .inode_id;
    let small_inode_id = seed_writer
        .reader()
        .get_path_entry(&namespace_id("demo"), "/small.bin", Default::default())
        .await
        .expect("stat small file")
        .inode_id;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_download_bytes = 16;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    assert_api_error(
        client
            .get_file_bytes(&NamespacePath::parse("demo", "/big.bin").expect("target"))
            .await,
        413,
        "content_too_large",
        None,
    );
    assert_api_error(
        client
            .get_file_revision_bytes_by_inode(
                &namespace_id("demo"),
                big_inode_id,
                loonfs_api::RevisionNo(1),
            )
            .await,
        413,
        "content_too_large",
        None,
    );
    // Content inside the limit still reads through the same route.
    let bytes = client
        .get_file_bytes(&NamespacePath::parse("demo", "/small.bin").expect("target"))
        .await
        .expect("small content fits under the limit");
    assert_eq!(bytes.len(), 8);
    assert_eq!(
        client
            .get_file_revision_bytes_by_inode(
                &namespace_id("demo"),
                small_inode_id,
                loonfs_api::RevisionNo(1),
            )
            .await
            .expect("small inode content fits under the limit")
            .len(),
        8
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
// Monotonic time is used only to bound this shutdown check.
async fn shutdown_keeps_readiness_reachable_until_an_active_request_finishes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn get(addr: std::net::SocketAddr, path: &str) -> Vec<u8> {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to serving listener");
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("send request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        response
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let mut config = test_config(temp_dir.path(), "readiness-shutdown-writer");
    config.shutdown_deadline_ms = 1_000;
    let shutdown_deadline_ms = config.shutdown_deadline_ms;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let slow_started = Arc::new(tokio::sync::Notify::new());
    let slow_release = Arc::new(tokio::sync::Notify::new());
    let router = router.route(
        "/slow",
        axum::routing::get({
            let slow_started = Arc::clone(&slow_started);
            let slow_release = Arc::clone(&slow_release);
            move || {
                let slow_started = Arc::clone(&slow_started);
                let slow_release = Arc::clone(&slow_release);
                async move {
                    slow_started.notify_one();
                    slow_release.notified().await;
                    "slow request finished"
                }
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve::serve_and_settle(
        listener,
        router,
        state.writer.clone(),
        None,
        shutdown_deadline_ms,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let slow = tokio::spawn(get(addr, "/slow"));
    tokio::time::timeout(std::time::Duration::from_secs(1), slow_started.notified())
        .await
        .expect("slow request starts");

    let shutdown_started = tokio::time::Instant::now();
    shutdown_tx.send(()).expect("trigger shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !state.writer.is_shutting_down() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown closes admission");

    let readiness = get(addr, "/readiness").await;
    let readiness = String::from_utf8(readiness).expect("readiness is utf-8");
    assert!(
        readiness.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
        "readiness returns 503 during drain: {readiness}"
    );
    assert!(
        readiness.contains("\"code\":\"shutting_down\""),
        "readiness names the shutdown: {readiness}"
    );
    assert!(
        readiness.contains("retry-after: 1\r\n"),
        "readiness tells callers when to retry: {readiness}"
    );
    assert!(
        !slow.is_finished(),
        "the original request is still in flight"
    );
    assert!(
        !server.is_finished(),
        "the listener remains serving while the request drains"
    );

    slow_release.notify_one();
    let slow = tokio::time::timeout(std::time::Duration::from_secs(1), slow)
        .await
        .expect("slow request completes")
        .expect("join slow request");
    let slow = String::from_utf8(slow).expect("slow response is utf-8");
    assert!(slow.starts_with("HTTP/1.1 200 OK\r\n"), "{slow}");
    assert!(slow.contains("slow request finished"), "{slow}");

    tokio::time::timeout(
        std::time::Duration::from_millis(shutdown_deadline_ms),
        server,
    )
    .await
    .expect("serve_and_settle finishes within the configured deadline")
    .expect("join server task")
    .expect("shutdown settles the server");
    assert!(
        shutdown_started.elapsed() < std::time::Duration::from_millis(shutdown_deadline_ms),
        "shutdown finishes inside its configured budget"
    );
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the listener stops accepting after the slow request finishes"
    );
}

async fn seed_grep_error_namespace(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> FsWriter {
    let writer = test_runtime(store.clone(), "grep-error-seed").await;
    seed_grep_error_namespace_on(&writer, namespace_id).await;
    writer
}

async fn seed_grep_error_namespace_on(writer: &FsWriter, namespace_id: &NamespaceId) {
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create grep-error namespace");
    writer
        .put_file_bytes(
            namespace_id,
            "/core.txt",
            b"core remains readable",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write core isolation sentinel");
}

async fn grep_error_worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    grep_worker(store, "grep-error-worker").await
}

/// A worker composed the way the server composes its own: grep's keyspace
/// on the given store, its filesystem reads and checkpoints on handles over
/// the same store.
async fn grep_worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store.clone(), reader, admin)
}

fn grep_error_request() -> GrepRequest {
    GrepRequest {
        pattern: "needle".to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        allow_stale: false,
        allow_scan: false,
    }
}

async fn write_grep_pointer(
    store: &dyn ObjectStore,
    stored_namespace_id: &NamespaceId,
    pointer_namespace_id: NamespaceId,
    manifest_object_id: GrepManifestObjectId,
) {
    // Every caller here injects a fault the load hits before it compares
    // digests, so any well-formed digest stands in for the real one.
    let envelope = GrepRootEnvelope::from_pointer(GrepRootPointer::new(
        pointer_namespace_id,
        manifest_object_id,
        loonfs_api::sha256_digest(b"a manifest these tests never reach"),
    ))
    .expect("build grep pointer");
    store
        .put_overwrite(
            &grep_root_key(stored_namespace_id),
            Bytes::from(encode_grep_root(&envelope).expect("encode grep pointer")),
        )
        .await
        .expect("write grep pointer");
}

/// A deployment that answers searches over an index it does not maintain:
/// exactly the query error surface these tests are about, with no
/// maintenance step racing the fault they injected.
async fn start_grep_error_server(
    store: SharedObjectStore,
    root: &Path,
    writer_id: &str,
) -> TestHarness {
    let mut config = test_config(root, writer_id);
    config.grep.mode = crate::config::GrepMode::ServeOnly;
    start_server_with_config(store, config).await
}

/// Administering a grep root belongs to a deployment that maintains one.
async fn start_grep_admin_error_server(
    store: SharedObjectStore,
    root: &Path,
    writer_id: &str,
) -> TestHarness {
    let mut config = test_config(root, writer_id);
    config.grep.mode = crate::config::GrepMode::ServeAndMaintain;
    start_server_with_config(store, config).await
}

async fn assert_grep_api_error_and_core_read<T: std::fmt::Debug>(
    client: &Client,
    namespace_id: &NamespaceId,
    result: Result<T, ClientError>,
    status: u16,
    code: ErrorCode,
    message_fragment: &str,
) {
    match result {
        Err(ClientError::Api {
            status: actual_status,
            code: actual_code,
            feature,
            message,
            ..
        }) => {
            assert_eq!(actual_status, status);
            assert_eq!(actual_code, code.as_str());
            assert!(
                message.contains(message_fragment),
                "expected `{message_fragment}` in `{message}`"
            );
            if code == ErrorCode::NotSupported {
                // The reported feature is the capability key clients gate
                // on, not a private name for the index.
                assert_eq!(feature.as_deref(), Some(FEATURE_QUERY_GREP));
            } else {
                assert_eq!(feature, None);
            }
        }
        other => panic!(
            "expected grep api error {status} {}, got {other:?}",
            code.as_str()
        ),
    }

    let target = NamespacePath::parse(namespace_id.as_str(), "/core.txt").expect("core target");
    let bytes = client
        .get_file_bytes(&target)
        .await
        .expect("grep failure must not affect core reads");
    assert_eq!(bytes, b"core remains readable");
}

async fn start_server(store: SharedObjectStore, root: &Path, writer_id: &str) -> TestHarness {
    start_server_with_config(store, test_config(root, writer_id)).await
}

async fn start_server_with_config(store: SharedObjectStore, config: ServerConfig) -> TestHarness {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    TestHarness {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config"),
        server,
    }
}

async fn test_runtime(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

fn test_config(root: &Path, writer_id: &str) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: writer_id.to_owned(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        local_cache: None,
        grep: crate::config::GrepConfig::default(),
        maintenance: crate::config::MaintenanceMode::Automatic,
        min_publish_interval_ms: 0,
        request_deadline_ms: 60_000,
        shutdown_deadline_ms: 600_000,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: root.display().to_string(),
            key_prefix: Some("http-tests".to_owned()),
        },
    }
}

/// Bootstraps a namespace through a second embedded runtime — seeding
/// durable state as `writer_id` would from another process — and returns
/// that runtime for follow-up seed writes.
async fn bootstrap_namespace(
    store: &SharedObjectStore,
    writer_id: &str,
    namespace_id: &NamespaceId,
) -> FsWriter {
    let writer = test_runtime(store.clone(), writer_id).await;
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap namespace");
    writer
}

async fn write_file_bytes(
    fs: &FsWriter,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    commit_id: &str,
) {
    fs.put_file_bytes(
        namespace_id,
        absolute_path,
        bytes,
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: Some(CommitId::parse(commit_id).expect("valid test commit id")),
                message: None,
            },
            expected_revision_no: None,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("seed `{absolute_path}`: {error}"));
}

async fn delete_path_recursive(
    fs: &FsWriter,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    commit_id: &str,
) {
    fs.delete_path(
        namespace_id,
        absolute_path,
        DeleteOptions {
            behavior: DeleteDirectoryBehavior::Recursive,
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: Some(CommitId::parse(commit_id).expect("valid test commit id")),
                message: None,
            },
            expected_inode_id: None,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("delete `{absolute_path}`: {error}"));
}

fn assert_api_error<T: std::fmt::Debug>(
    result: Result<T, ClientError>,
    status: u16,
    code: &str,
    message: Option<&str>,
) {
    match result {
        Err(ClientError::Api {
            status: actual_status,
            code: actual_code,
            message: actual_message,
            ..
        }) => {
            assert_eq!(actual_status, status);
            assert_eq!(actual_code, code);
            if let Some(expected_message) = message {
                assert_eq!(actual_message, expected_message);
            }
        }
        other => panic!("expected api error {status} {code}, got {other:?}"),
    }
}

/// A deployment that authorizes direct uploads has to be able to hand back
/// what they wrote. These exercise that with a store double standing in for
/// the provider: a loopback issuer that signs nothing, and a loopback
/// object server reading the same store the deployment writes to — so the
/// whole grant path (route, issuer adapter, presigned fetch, client
/// verification) runs end to end without a real bucket.
mod direct_download {
    use super::*;
    use crate::http::app_with_store_and_direct_transfers;
    use loonfs_api::{
        v0::{CompleteMultipartUploadRequest, UploadContentClaim},
        Checksum, ChecksumAlgorithm, RevisionNo, FEATURE_DOWNLOADS_DIRECT_GET,
        FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
        LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    };
    use loonfs_objectstore::presign::{
        DirectGetIssuer, DirectPutIssuer, DirectTransferIssuers, PresignedGetRequest,
        PresignedPutRequest, PresignedUrl,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    /// The read cap these deployments are configured with.
    ///
    /// Small on purpose. The audit's case is a file the deployment refuses
    /// to buffer, and what makes a file that is the cap rather than the
    /// byte count — so the behavior under test is identical at a kilobyte
    /// and at 256 MiB, and this suite does not move a gigabyte per run to
    /// restate the same comparison. That the comparison itself picks the
    /// grant for a 300 MiB file at the real default is pinned in the
    /// client's own tests.
    const PROXY_CAP_BYTES: u64 = 1024;

    /// The whole-object ceiling the loopback put issuer reports.
    ///
    /// Above [`loonfs_client::STREAMING_PUT_MIN_BYTES`], because a payload
    /// below that never asks what transports are on offer at all — so a
    /// ceiling under it could never be the thing a put ran into.
    const LOOPBACK_DIRECT_PUT_MAX_BYTES: u64 = 16 * 1024 * 1024;

    /// An issuer that hands out unsigned loopback URLs.
    ///
    /// It stands in for the signing half only. What a real presigner adds —
    /// that the capability expires, that `Range` stays outside the
    /// signature, and that the provider enforces the digest — is pinned
    /// where it is decided: in the S3-compatible presigner's own tests, and
    /// against a live provider in the ignored suite.
    ///
    /// It implements the read and whole-object-write traits and not the
    /// multipart one, which is exactly the provider shape this suite exists
    /// to cover: each test composes the bundle it means to serve.
    #[derive(Debug)]
    struct LoopbackIssuer {
        object_base_url: String,
        /// The whole-object checksum this stand-in provider enforces.
        ///
        /// Providers do not agree on one -- the S3 family verifies SHA-256
        /// and GCS verifies CRC-32C -- so the shape is a parameter here for
        /// the same reason it is a trait method in the real issuers: the
        /// client folds whichever the deployment names.
        checksum_algorithm: ChecksumAlgorithm,
    }

    impl LoopbackIssuer {
        /// The S3-compatible shape: a whole-object SHA-256.
        fn at(object_base_url: impl Into<String>) -> Arc<Self> {
            Self::with_checksum(object_base_url, ChecksumAlgorithm::Sha256)
        }

        /// The GCS shape: a whole-object CRC-32C. Callers pair it with a
        /// bundle carrying no multipart signer, which is the rest of that
        /// shape.
        fn crc32c_at(object_base_url: impl Into<String>) -> Arc<Self> {
            Self::with_checksum(object_base_url, ChecksumAlgorithm::Crc32c)
        }

        fn with_checksum(
            object_base_url: impl Into<String>,
            checksum_algorithm: ChecksumAlgorithm,
        ) -> Arc<Self> {
            Arc::new(Self {
                object_base_url: object_base_url.into(),
                checksum_algorithm,
            })
        }
    }

    #[async_trait]
    impl DirectGetIssuer for LoopbackIssuer {
        async fn presign_get(
            &self,
            request: PresignedGetRequest<'_>,
            _now: SystemTime,
        ) -> Result<PresignedUrl, ObjectStoreError> {
            Ok(PresignedUrl {
                method: "GET".to_owned(),
                url: format!("{}/{}", self.object_base_url, request.object_key),
                headers: BTreeMap::new(),
                expires_at_ms: u64::MAX,
            })
        }
    }

    #[async_trait]
    impl DirectPutIssuer for LoopbackIssuer {
        fn stored_checksum_algorithm(&self) -> ChecksumAlgorithm {
            self.checksum_algorithm
        }

        fn max_content_bytes(&self) -> u64 {
            LOOPBACK_DIRECT_PUT_MAX_BYTES
        }

        async fn presign_put(
            &self,
            request: PresignedPutRequest<'_>,
            _now: SystemTime,
        ) -> Result<PresignedUrl, ObjectStoreError> {
            Ok(PresignedUrl {
                method: "PUT".to_owned(),
                url: format!("{}/{}", self.object_base_url, request.object_key),
                headers: BTreeMap::new(),
                expires_at_ms: u64::MAX,
            })
        }
    }

    /// Serves objects out of the deployment's own store, the way a provider
    /// answers a presigned transfer, and reports its base URL.
    async fn serve_objects(store: SharedObjectStore) -> String {
        async fn read_object(
            axum::extract::State(store): axum::extract::State<SharedObjectStore>,
            axum::extract::Path(key): axum::extract::Path<String>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse as _;
            match store.get(&key, None).await {
                Ok(Some(bytes)) => (axum::http::StatusCode::OK, bytes).into_response(),
                Ok(None) => axum::http::StatusCode::NOT_FOUND.into_response(),
                Err(error) => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                )
                    .into_response(),
            }
        }

        async fn write_object(
            axum::extract::State(store): axum::extract::State<SharedObjectStore>,
            axum::extract::Path(key): axum::extract::Path<String>,
            body: bytes::Bytes,
        ) -> axum::response::Response {
            use axum::response::IntoResponse as _;
            match store.put_if_absent(&key, body).await {
                Ok(_) => axum::http::StatusCode::OK.into_response(),
                Err(error) => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                )
                    .into_response(),
            }
        }

        let router = axum::Router::new()
            .route("/{*key}", axum::routing::get(read_object).put(write_object))
            // A provider takes whatever the presigned write carries; this
            // double must not impose a limit of its own on top.
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(store);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind object listener");
        let addr = listener.local_addr().expect("object listener addr");
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve objects");
        });
        format!("http://{addr}")
    }

    /// Starts a deployment whose read cap is [`PROXY_CAP_BYTES`], serving
    /// whichever direct transfers the caller composed, and returns a client
    /// pointed at it.
    async fn start(
        root: &Path,
        writer_id: &str,
        direct_transfers: Option<DirectTransferIssuers>,
    ) -> Client {
        start_with_upload_cap(root, writer_id, direct_transfers, None).await
    }

    /// The same deployment with its *write* cap narrowed too, for the cases
    /// about which transport a payload the proxy will not take ends up on.
    async fn start_with_upload_cap(
        root: &Path,
        writer_id: &str,
        direct_transfers: Option<DirectTransferIssuers>,
        max_upload_bytes: Option<u64>,
    ) -> Client {
        start_with_store(
            Arc::new(LocalFsStore::new(root).expect("construct local store")),
            root,
            writer_id,
            direct_transfers,
            max_upload_bytes,
        )
        .await
    }

    /// The same deployment over a caller-supplied store, for the cases where
    /// what the provider reports back about an object is the thing under
    /// test.
    async fn start_with_store(
        store: SharedObjectStore,
        root: &Path,
        writer_id: &str,
        direct_transfers: Option<DirectTransferIssuers>,
        max_upload_bytes: Option<u64>,
    ) -> Client {
        let mut config = test_config(root, writer_id);
        config.max_download_bytes = PROXY_CAP_BYTES;
        if let Some(max_upload_bytes) = max_upload_bytes {
            config.max_upload_bytes = max_upload_bytes;
        }
        let (router, _state) = app_with_store_and_direct_transfers(config, store, direct_transfers)
            .await
            .expect("build app");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve app");
        });
        Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: Some("test-token".into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config")
    }

    /// The store the deployment writes to, opened again for the object
    /// double. Both see the same objects because both are the same root.
    fn object_store_at(root: &Path) -> SharedObjectStore {
        Arc::new(LocalFsStore::new(root).expect("construct local store for the object double"))
    }

    /// A store that reports an object's stored checksum as a CRC-32C, the way
    /// a GCS provider answers.
    ///
    /// The reference local store reports SHA-256, which is the readback that
    /// pairs with an S3-compatible issuer. A provider's readback algorithm
    /// and its `direct_put` issuer's algorithm have to be the same one, or
    /// completion compares two digests of different kinds and refuses every
    /// upload — so a CRC-32C issuer needs a CRC-32C readback beneath it, and
    /// this double supplies one.
    #[derive(Debug)]
    struct Crc32cReadbackStore {
        inner: LocalFsStore,
    }

    #[async_trait::async_trait]
    impl loonfs_objectstore::ObjectStore for Crc32cReadbackStore {
        async fn head(
            &self,
            key: &str,
        ) -> Result<Option<loonfs_objectstore::ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn head_stored_checksum(
            &self,
            key: &str,
        ) -> Result<Option<loonfs_objectstore::StoredObjectChecksum>, ObjectStoreError> {
            let Some(bytes) = self.inner.get(key, None).await? else {
                return Ok(None);
            };
            Ok(Some(loonfs_objectstore::StoredObjectChecksum {
                size_bytes: bytes.len() as u64,
                checksum: Checksum::crc32c(&bytes),
            }))
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<loonfs_objectstore::ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<loonfs_objectstore::ByteRange>,
        ) -> Result<Option<bytes::Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: bytes::Bytes,
            mode: loonfs_objectstore::PutMode,
        ) -> Result<loonfs_objectstore::ObjectMetadata, ObjectStoreError> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_from_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> futures::stream::BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_from_stream(prefix, start_after)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_past_the_proxy_cap_round_trips_through_a_download_grant() {
        let temp_dir = tempdir().expect("tempdir");
        let object_base_url = serve_objects(object_store_at(temp_dir.path())).await;
        let transfers = DirectTransferIssuers::read_only(LoopbackIssuer::at(object_base_url));
        let client = start(temp_dir.path(), "direct-download", Some(transfers)).await;

        let namespace = namespace_id("direct-download");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/big.bin").expect("target");
        // Past the cap by enough that a truncation would show, and cheap.
        let payload: Vec<u8> = (0..PROXY_CAP_BYTES as usize * 3)
            .map(|index| (index % 251) as u8)
            .collect();
        client
            .put_file_bytes(&target, &payload, &replace_file_options())
            .await
            .expect("seed the oversized file");
        let entry = client
            .get_path_entry(&target, &Default::default())
            .await
            .expect("stat seeded file");

        // The wall the audit found: this deployment let the file exist and
        // will not proxy it back.
        assert_api_error(
            client.get_file_bytes(&target).await,
            413,
            ErrorCode::ContentTooLarge.as_str(),
            None,
        );

        let grant = client
            .create_download(&target, None)
            .await
            .expect("download grant");
        assert_eq!(grant.path.as_str(), "/big.bin");
        assert_eq!(grant.content_ref.size_bytes, payload.len() as u64);
        assert_eq!(Some(&grant.content_ref), entry.content_ref());

        let mut received = Vec::new();
        let written = client
            .download_via_presigned_url(&grant, &mut received)
            .await
            .expect("stream the granted object");
        assert_eq!(written, payload.len() as u64);
        assert_eq!(received, payload);

        client
            .delete_path(
                &target,
                &DeleteOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("delete current binding");
        let inode_grant = client
            .create_download_by_inode(&namespace, entry.inode_id, RevisionNo(1))
            .await
            .expect("grant retained inode revision");
        assert_eq!(inode_grant.inode_id, entry.inode_id);
        assert_eq!(inode_grant.content_ref, grant.content_ref);
        let mut stream = client
            .open_direct_download_by_inode(&inode_grant)
            .await
            .expect("open inode grant");
        let mut inode_received = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("read inode grant") {
            inode_received.extend_from_slice(&chunk);
        }
        assert_eq!(inode_received, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_grant_keeps_reading_the_revision_it_was_issued_for() {
        let temp_dir = tempdir().expect("tempdir");
        let object_base_url = serve_objects(object_store_at(temp_dir.path())).await;
        let transfers = DirectTransferIssuers::read_only(LoopbackIssuer::at(object_base_url));
        let client = start(temp_dir.path(), "grant-pins", Some(transfers)).await;

        let namespace = namespace_id("grant-pins");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/pinned.bin").expect("target");
        let first = vec![b'a'; PROXY_CAP_BYTES as usize * 2];
        let second = vec![b'b'; PROXY_CAP_BYTES as usize * 2];
        client
            .put_file_bytes(&target, &first, &replace_file_options())
            .await
            .expect("seed revision 1");

        let grant = client
            .create_download(&target, None)
            .await
            .expect("grant for revision 1");
        assert_eq!(grant.revision_no, RevisionNo(1));

        client
            .put_file_bytes(&target, &second, &replace_file_options())
            .await
            .expect("replace with revision 2");

        let mut received = Vec::new();
        client
            .download_via_presigned_url(&grant, &mut received)
            .await
            .expect("the already-issued grant still reads its own object");
        assert_eq!(received, first);

        // And asking for the old revision by number resolves to the same
        // object the earlier grant named.
        let pinned = client
            .create_download(&target, Some(RevisionNo(1)))
            .await
            .expect("grant for a prior revision");
        assert_eq!(pinned.revision_no, RevisionNo(1));
        assert_eq!(pinned.content_ref, grant.content_ref);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deployment_that_cannot_presign_refuses_the_grant_by_capability() {
        let temp_dir = tempdir().expect("tempdir");
        let client = start(temp_dir.path(), "no-issuer", None).await;

        let namespace = namespace_id("no-issuer");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/small.txt").expect("target");
        client
            .put_file_bytes(&target, b"small enough to proxy", &replace_file_options())
            .await
            .expect("seed a file");

        let error = client
            .create_download(&target, None)
            .await
            .expect_err("a deployment with no issuer cannot grant reads");
        match &error {
            ClientError::Api {
                status,
                code,
                feature,
                ..
            } => {
                assert_eq!(*status, 501);
                assert_eq!(code, ErrorCode::NotSupported.as_str());
                assert_eq!(feature.as_deref(), Some(FEATURE_DOWNLOADS_DIRECT_GET));
            }
            other => panic!("expected a typed not_supported, got {other:?}"),
        }
        let inode_id = client
            .get_path_entry(&target, &Default::default())
            .await
            .expect("stat proxied file")
            .inode_id;
        let inode_error = client
            .create_download_by_inode(&namespace, inode_id, RevisionNo(1))
            .await
            .expect_err("the inode route honors the same provider gate");
        match inode_error {
            ClientError::Api {
                status,
                code,
                feature,
                ..
            } => {
                assert_eq!(status, 501);
                assert_eq!(code, ErrorCode::NotSupported.as_str());
                assert_eq!(feature.as_deref(), Some(FEATURE_DOWNLOADS_DIRECT_GET));
            }
            other => panic!("expected a typed not_supported, got {other:?}"),
        }

        // The proxied read it does serve is untouched.
        assert_eq!(
            client
                .get_file_bytes(&target)
                .await
                .expect("proxied read of a file under the cap"),
            b"small enough to proxy"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_provider_without_multipart_advertises_put_and_get_and_denies_multipart() {
        let temp_dir = tempdir().expect("tempdir");
        let issuer = LoopbackIssuer::at("http://object.invalid");
        let transfers = DirectTransferIssuers::read_only(issuer.clone()).with_put(issuer);
        let advertised = start(temp_dir.path(), "put-without-multipart", Some(transfers))
            .await
            .get_capabilities()
            .await
            .expect("capabilities");

        assert!(advertised.supports(FEATURE_UPLOADS_DIRECT_PUT));
        assert!(advertised.supports(FEATURE_DOWNLOADS_DIRECT_GET));
        assert!(
            !advertised.supports(FEATURE_UPLOADS_DIRECT_MULTIPART),
            "a provider with no multipart API must not advertise one"
        );
        assert_eq!(
            advertised
                .limits
                .get(LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES),
            Some(&LOOPBACK_DIRECT_PUT_MAX_BYTES),
            "the provider's own single-request ceiling is advertised, not the proxy's"
        );
        assert_eq!(
            advertised.limits.get(LIMIT_DOWNLOAD_MAX_CONTENT_BYTES),
            Some(&PROXY_CAP_BYTES),
            "the proxy cap stays advertised: it is what tells a client which reads need a grant"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_direct_put_session_rejects_multipart_completion_fields_precisely() {
        let temp_dir = tempdir().expect("tempdir");
        let issuer = LoopbackIssuer::at("http://object.invalid");
        let transfers = DirectTransferIssuers::read_only(issuer.clone()).with_put(issuer);
        let client = start(
            temp_dir.path(),
            "direct-put-completion-shape",
            Some(transfers),
        )
        .await;
        let namespace_id =
            NamespaceId::parse("direct-put-completion-shape").expect("valid namespace id");
        client
            .create_namespace(&namespace_id)
            .await
            .expect("create namespace");
        let begin = client
            .create_direct_put_upload(&namespace_id, Some(5))
            .await
            .expect("begin direct put");

        let error = client
            .complete_multipart_upload(
                &namespace_id,
                begin.upload_id(),
                &CompleteMultipartUploadRequest {
                    content: UploadContentClaim {
                        size_bytes: 5,
                        checksum: Checksum::sha256(b"hello"),
                    },
                    parts: Vec::new(),
                },
            )
            .await
            .expect_err("multipart completion body does not belong to direct put");
        match error {
            ClientError::Api {
                status,
                code,
                message,
                ..
            } => {
                assert_eq!(status, 400);
                assert_eq!(code, ErrorCode::InvalidRequest.as_str());
                assert_eq!(
                    message,
                    "invalid upload content: completion request mode `direct_multipart` does not \
                     match stored upload mode `direct_put`"
                );
            }
            other => panic!("expected a typed invalid_request, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crc32c_provider_without_multipart_carries_a_file_end_to_end() {
        let temp_dir = tempdir().expect("tempdir");
        let object_base_url = serve_objects(object_store_at(temp_dir.path())).await;
        let issuer = LoopbackIssuer::crc32c_at(object_base_url);
        let transfers = DirectTransferIssuers::read_only(issuer.clone()).with_put(issuer);
        let store: SharedObjectStore = Arc::new(Crc32cReadbackStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        });
        let client = start_with_store(
            store,
            temp_dir.path(),
            "ladder-crc32c-put",
            Some(transfers),
            Some(PROXY_CAP_BYTES),
        )
        .await;

        let namespace = namespace_id("ladder-crc32c-put");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/large.bin").expect("target");
        // Large enough that the client looks for a direct transport at all,
        // and far past the proxy cap, so nothing else could carry it.
        let payload: Vec<u8> = (0..loonfs_client::STREAMING_PUT_MIN_BYTES as usize)
            .map(|index| (index % 251) as u8)
            .collect();

        client
            .put_file_bytes(&target, &payload, &replace_file_options())
            .await
            .expect("a large file goes straight to object storage under a crc32c claim");

        let grant = client
            .create_download(&target, None)
            .await
            .expect("download grant");
        assert_eq!(
            grant.content_ref.checksum,
            Checksum::crc32c(&payload),
            "the recorded ref carries the digest the provider was made to enforce"
        );

        // And it comes home byte for byte through the read half of the same
        // bundle, which is the symmetry the bundle type exists to guarantee.
        let mut received = Vec::new();
        client
            .download_via_presigned_url(&grant, &mut received)
            .await
            .expect("stream the granted object");
        assert_eq!(received, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_large_file_takes_direct_put_where_multipart_is_not_offered() {
        let temp_dir = tempdir().expect("tempdir");
        let object_base_url = serve_objects(object_store_at(temp_dir.path())).await;
        let issuer = LoopbackIssuer::at(object_base_url);
        let transfers = DirectTransferIssuers::read_only(issuer.clone()).with_put(issuer);
        let client = start_with_upload_cap(
            temp_dir.path(),
            "ladder-direct-put",
            Some(transfers),
            Some(PROXY_CAP_BYTES),
        )
        .await;

        let namespace = namespace_id("ladder-direct-put");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/large.bin").expect("target");
        // Large enough that the client looks for a direct transport at all,
        // and far past the proxy cap, so nothing else could carry it.
        let payload: Vec<u8> = (0..loonfs_client::STREAMING_PUT_MIN_BYTES as usize)
            .map(|index| (index % 251) as u8)
            .collect();

        client
            .put_file_bytes(&target, &payload, &replace_file_options())
            .await
            .expect("a large file goes straight to object storage");

        // It came home through the grant, byte for byte, which proves the
        // object the presigned write created is the one the commit named.
        let grant = client
            .create_download(&target, None)
            .await
            .expect("download grant");
        assert_eq!(grant.content_ref.size_bytes, payload.len() as u64);
        let mut received = Vec::new();
        client
            .download_via_presigned_url(&grant, &mut received)
            .await
            .expect("stream the granted object");
        assert_eq!(received, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payload_past_every_transport_is_refused_with_the_caps_named() {
        let temp_dir = tempdir().expect("tempdir");
        let object_base_url = serve_objects(object_store_at(temp_dir.path())).await;
        // Reads only: neither write direction is on offer, which is the
        // shape that used to push an oversized payload into the proxy.
        let transfers = DirectTransferIssuers::read_only(LoopbackIssuer::at(object_base_url));
        let client = start_with_upload_cap(
            temp_dir.path(),
            "ladder-too-large",
            Some(transfers),
            Some(PROXY_CAP_BYTES),
        )
        .await;

        let namespace = namespace_id("ladder-too-large");
        client
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        let target = NamespacePath::parse(namespace.as_str(), "/enormous.bin").expect("target");
        let payload = vec![7u8; loonfs_client::STREAMING_PUT_MIN_BYTES as usize];

        let error = client
            .put_file_bytes(&target, &payload, &replace_file_options())
            .await
            .expect_err("no transport can carry this payload");
        match &error {
            ClientError::UploadTooLarge { size_bytes, reason } => {
                assert_eq!(*size_bytes, payload.len() as u64);
                assert!(
                    reason.contains(FEATURE_UPLOADS_DIRECT_MULTIPART),
                    "the refusal names what was missing: {reason}"
                );
            }
            other => panic!("expected an upload-too-large refusal, got {other:?}"),
        }
    }
}
