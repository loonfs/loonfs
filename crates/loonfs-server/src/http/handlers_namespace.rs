//! Namespace lifecycle, status, capability discovery, and admin maintenance
//! handlers.

use super::error::ApiResponseError;
use super::{authorize, AppJson, AppPath, AppQuery, AppState, NamespaceIdPath, OptionalAppJson};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::{CreateNamespaceOptions, DeleteNamespaceOptions};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::ChangeSeq;
use loonfs_api::{
    AdvanceRetentionResponse, CheckpointId, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, ErrorCode, FlushWalResponse, ForkNamespaceRequest, GcRequest,
    GcResponse, MaintenanceTickRequest, MaintenanceTickResponse, ReleaseCheckpointResponse,
    FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_PUT, LIMIT_COMMIT_MAX_BODY_BYTES,
    LIMIT_DOWNLOAD_MAX_CONCURRENT, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_DEFAULT,
    LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    LIMIT_UPLOAD_MAX_CONCURRENT, LIMIT_UPLOAD_MAX_CONTENT_BYTES,
};

#[derive(Debug, serde::Deserialize)]
pub(super) struct DeleteNamespaceQuery {
    /// Delete only if the head is still at this sequence (`stale_head` on
    /// mismatch).
    expected_head_seq: Option<u64>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/config",
        tag = "config",
        summary = "Get config",
        description = "Returns a summary of supported features and limits.",
        responses(
            (status = 200, description = "Capability document", body = loonfs_api::CapabilityDocument),
            (status = 401, description = "Unauthorized", body = ApiError)
        )
    )
)]
pub(super) async fn config(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::CapabilityDocument>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let mut capabilities = state.reader.capabilities();
    // An issuer's existence is the capability: `ObjectTransferIssuer` is
    // only implemented where the provider enforces the digest and
    // create-only preconditions inside the signed request, which is what
    // lets completion verify a direct_put without reading content back.
    capabilities.features.insert(
        FEATURE_UPLOADS_DIRECT_PUT.to_owned(),
        state.transfer_issuer.is_some(),
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_MAX_CONTENT_BYTES.to_owned(),
        state.config.max_upload_bytes,
    );
    capabilities.limits.insert(
        LIMIT_DOWNLOAD_MAX_CONTENT_BYTES.to_owned(),
        state.config.max_download_bytes,
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_MAX_CONCURRENT.to_owned(),
        state.config.max_concurrent_uploads as u64,
    );
    capabilities.limits.insert(
        LIMIT_DOWNLOAD_MAX_CONCURRENT.to_owned(),
        state.config.max_concurrent_downloads as u64,
    );
    capabilities.limits.insert(
        LIMIT_COMMIT_MAX_BODY_BYTES.to_owned(),
        state.config.max_commit_body_bytes,
    );
    if !state.config.grep.mode.serves_grep() {
        capabilities.features.remove(FEATURE_QUERY_GREP);
        for limit in [
            LIMIT_QUERY_GREP_DEFAULT,
            LIMIT_QUERY_GREP_MAX,
            LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
            LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
        ] {
            capabilities.limits.remove(limit);
        }
    }
    Ok(Json(capabilities))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces",
        tag = "namespaces",
        summary = "Create namespace",
        description = "Creates a new empty namespace.",
        request_body = CreateNamespaceRequest,
        responses(
            (status = 200, description = "Namespace created", body = loonfs_api::NamespaceSummary),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 409, description = "Namespace already exists or is partial", body = ApiError),
            (status = 410, description = "Namespace id was deleted and retired", body = ApiError)
        )
    )
)]
pub(super) async fn create_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let summary = state
        .writer
        .create_namespace(&request.namespace_id, CreateNamespaceOptions::default())
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(summary))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}",
        tag = "namespaces",
        summary = "Get namespace status",
        description = "Returns the current head, manifest, checkpoint, WAL tail, and retention state for a namespace.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace status", body = loonfs_api::NamespaceStatusResponse),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn namespace_status(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::NamespaceStatusResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let status = state
        .admin
        .namespace_status(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(status))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        path = "/v0/namespaces/{namespace}",
        tag = "namespaces",
        summary = "Delete namespace",
        description = "Marks a namespace as deleted.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("expected_head_seq" = Option<u64>, Query, description = "Delete only if the namespace head is still at this sequence")
        ),
        responses(
            (status = 200, description = "Namespace deleted", body = loonfs_api::DeleteNamespaceResponse),
            (status = 400, description = "Invalid request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Delete conflict", body = ApiError),
            (status = 410, description = "Namespace already deleted", body = ApiError)
        )
    )
)]
pub(super) async fn delete_namespace(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    query: AppQuery<DeleteNamespaceQuery>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::DeleteNamespaceResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let options = DeleteNamespaceOptions {
        expected_head_seq: query.expected_head_seq.map(ChangeSeq),
    };
    let response = state
        .publisher
        .submit_delete(namespace_id.clone(), options)
        .await
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/forks",
        tag = "namespaces",
        summary = "Fork namespace",
        description = "Creates a new namespace as a fork from the source namespace's current durable view.",
        params(("namespace" = String, Path, description = "Source namespace id")),
        request_body = ForkNamespaceRequest,
        responses(
            (status = 200, description = "Namespace forked", body = loonfs_api::NamespaceSummary),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Source namespace not found", body = ApiError),
            (status = 409, description = "Fork conflict", body = ApiError),
            (status = 410, description = "Source namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn fork_namespace(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    AppJson(request): AppJson<ForkNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let source_namespace_id = namespace.into_id()?;
    let summary = state
        .writer
        .fork_namespace(&source_namespace_id, &request.new_namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&source_namespace_id, error))?;
    Ok(Json(summary))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/checkpoints",
        tag = "admin",
        summary = "Create checkpoint",
        description = "Creates or reuses a named, user-owned checkpoint record pinning the current namespace view. The record is a garbage-collection root until released or expired, so routine maintenance should flush the WAL instead. This is a maintenance/admin operation, not a file mutation.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body(content = CreateCheckpointRequest, description = "Checkpoint name and optional lifetime"),
        responses(
            (status = 200, description = "Checkpoint created or reused", body = CreateCheckpointResponse),
            (status = 400, description = "Invalid namespace id, name, or lifetime", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 503, description = "Checkpoint unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn create_checkpoint(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    AppJson(request): AppJson<CreateCheckpointRequest>,
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .create_checkpoint(
            &namespace_id,
            loonfs::CreateCheckpointOptions::from_request(request),
        )
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release",
        tag = "admin",
        summary = "Release checkpoint",
        description = "Releases a user-owned checkpoint pin by id. Idempotent: releasing an already-released or reaped record succeeds. The record is reaped by a later garbage-collection pass; its pinned data becomes collectable only on the pass after that.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("checkpoint_id" = String, Path, description = "Checkpoint id")
        ),
        responses(
            (status = 200, description = "Checkpoint released (or already gone)", body = ReleaseCheckpointResponse),
            (status = 400, description = "Invalid id, or the checkpoint is fork-owned", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError)
        )
    )
)]
pub(super) async fn release_checkpoint(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    path: AppPath<CheckpointPathParams>,
    headers: HeaderMap,
) -> Result<Json<ReleaseCheckpointResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let CheckpointPathParams { checkpoint_id } = path.into_params()?;
    let checkpoint_id = parse_checkpoint_id(&checkpoint_id)?;
    let response = state
        .admin
        .release_checkpoint(&namespace_id, &checkpoint_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct CheckpointPathParams {
    checkpoint_id: String,
}

fn parse_checkpoint_id(value: &str) -> Result<CheckpointId, ApiResponseError> {
    CheckpointId::parse(value).map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid checkpoint_id `{value}`: {error}"),
        )
    })
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/wal/flush",
        tag = "admin",
        summary = "Flush WAL",
        description = "Folds the visible WAL tail into metadata tables, publishes a manifest, and advances the metadata root to cover the current head. No checkpoint record is created; superseded manifests become garbage-collection candidates once nothing pins them. This is the routine latest-state maintenance operation.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Metadata root covers the current head", body = FlushWalResponse),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost the root race", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn flush_wal(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<FlushWalResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .flush_wal(&namespace_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/retention/advance",
        tag = "admin",
        summary = "Advance retention",
        description = "Advances the namespace retention floor after checkpoint state makes older WAL history unnecessary for normal replay.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Retention floor advanced", body = AdvanceRetentionResponse),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn advance_retention(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<AdvanceRetentionResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .advance_retention_floor(&namespace_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/gc",
        tag = "admin",
        summary = "Collect garbage",
        description = "Runs one mark-and-sweep garbage-collection pass under the format's safety rules (grace window, delete-time re-verification, retention wins). Nothing sweeps without this explicit call or a maintenance-tick opt-in.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body(content = GcRequest, description = "Optional window overrides"),
        responses(
            (status = 200, description = "Garbage collection pass completed", body = GcResponse),
            (status = 400, description = "Invalid namespace id or windows", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError)
        )
    )
)]
pub(super) async fn gc_namespace(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    OptionalAppJson(request): OptionalAppJson<GcRequest>,
) -> Result<Json<GcResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let config = loonfs::gc_config_from_request(request.unwrap_or_default());
    let report = state
        .admin
        .gc_namespace(&namespace_id, &config)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(loonfs::gc_response_from_report(namespace_id, report)))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/maintenance/tick",
        tag = "admin",
        summary = "Run maintenance tick",
        description = "Runs one bounded maintenance step: flushes the WAL tail once it reaches the threshold, and optionally runs a garbage-collection pass afterwards. Losing the root race is an outcome, not an error.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body(content = MaintenanceTickRequest, description = "Optional threshold and GC overrides"),
        responses(
            (status = 200, description = "Maintenance tick completed", body = MaintenanceTickResponse),
            (status = 400, description = "Invalid namespace id or options", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn maintenance_tick(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    OptionalAppJson(request): OptionalAppJson<MaintenanceTickRequest>,
) -> Result<Json<MaintenanceTickResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let options = loonfs::MaintenanceTickOptions::from_request(request.unwrap_or_default());
    let result = state
        .admin
        .maintenance_tick_namespace(&namespace_id, options)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(result.into_response()))
}
