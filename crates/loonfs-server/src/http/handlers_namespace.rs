//! Namespace lifecycle, status, capability discovery, and admin maintenance
//! handlers.

use super::error::ApiResponseError;
use super::{authorize, parse_namespace_id, AppJson, AppState, NamespaceIdPath, OptionalAppJson};
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use loonfs::{ChangeSeq, CreateNamespaceOptions, DeleteNamespaceOptions};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    AdvanceRetentionResponse, CreateCheckpointResponse, CreateNamespaceRequest,
    ForkNamespaceRequest, GcRequest, GcResponse, MaintenanceTickRequest, MaintenanceTickResponse,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_UPLOAD_MAX_CONTENT_BYTES,
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
    capabilities.features.insert(
        FEATURE_UPLOADS_DIRECT_PUT.to_owned(),
        state.transfer_issuer.is_some(),
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_MAX_CONTENT_BYTES.to_owned(),
        state.config.max_upload_bytes,
    );
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
    let namespace_id = parse_namespace_id(request.namespace_id)?;
    let summary = state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
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
    Query(query): Query<DeleteNamespaceQuery>,
    headers: HeaderMap,
) -> Result<Json<loonfs::DeleteNamespaceResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
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
    let new_namespace_id = parse_namespace_id(request.new_namespace_id)?;
    let summary = state
        .writer
        .fork_namespace(&source_namespace_id, &new_namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&source_namespace_id, error))?;
    Ok(Json(summary))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/checkpoint",
        tag = "admin",
        summary = "Create checkpoint",
        description = "Records a checkpoint of the current namespace view. Unnamed checkpoints are maintenance bookkeeping: a manifest retains only the four newest, so this is not a durable pin with a checkpoint. This is a maintenance/admin operation, not a file mutation.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Checkpoint created or reused", body = CreateCheckpointResponse),
            (status = 400, description = "Invalid namespace id", body = ApiError),
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
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .create_checkpoint(&namespace_id)
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
        description = "Runs one bounded maintenance step: publishes a checkpoint once the visible WAL tail reaches the threshold, and optionally runs a garbage-collection pass afterwards. Losing a checkpoint race is an outcome, not an error.",
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
