//! Namespace lifecycle, status, capability discovery, and admin maintenance
//! handlers.

use super::error::ApiResponseError;
use super::handlers_filesystem::{parse_public_ordinal, resolve_page_limit};
use super::{
    authorize, AppJson, AppPath, AppQuery, AppState, NamespaceIdPath, NoQuery, OptionalAppJson,
};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::{CheckpointPageCursor, CreateNamespaceOptions, DeleteNamespaceOptions};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::ChangeSeq;
use loonfs_api::{
    decode_namespace_cursor, CapabilityDocument, CheckpointId, CreateCheckpointRequest,
    CreateCheckpointResponse, CreateNamespaceRequest, ErrorCode, ForkNamespaceRequest,
    ListCheckpointsResponse, MaintenanceStepRequest, MaintenanceStepResponse, PageRequest,
    PaginationPolicy, ReleaseCheckpointResponse, FEATURE_ADMIN_GREP_INDEX,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_DOWNLOAD_MAX_CONCURRENT, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_MAX_CONCURRENT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_QUERY_V0,
};

/// Advertises a feature, or removes the key: an absent key and an
/// advertised-false key both mean unsupported, and the document stays small.
fn set_feature(capabilities: &mut CapabilityDocument, feature: &str, supported: bool) {
    if supported {
        capabilities.features.insert(feature.to_owned(), true);
    } else {
        capabilities.features.remove(feature);
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeleteNamespaceQuery {
    /// Delete only if the head is still at this sequence (`stale_head` on
    /// mismatch).
    expected_head_seq: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CheckpointPageQuery {
    limit: Option<String>,
    cursor: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_capabilities",
        path = "/v0/capabilities",
        tag = "system",
        summary = "Get capabilities",
        description = "Returns a summary of supported features and limits.",
        responses(
            (status = 200, description = "Capability document", body = loonfs_api::CapabilityDocument),
            (status = 400, description = "Unknown query parameter", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::CapabilityDocument>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    query.into_params()?;
    let mut capabilities = state.reader.capabilities();
    // Each direct transport is advertised from the issuer that performs it,
    // so a provider that signs whole-object writes but has no multipart API
    // says exactly that. The read is advertised for the bundle as a whole:
    // a deployment that lets a client create an object too large to proxy
    // back has to be able to hand it back.
    let direct_put = state
        .direct_transfers
        .as_ref()
        .and_then(|transfers| transfers.put.as_ref());
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_PUT,
        direct_put.is_some(),
    );
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_MULTIPART,
        state
            .direct_transfers
            .as_ref()
            .is_some_and(|transfers| transfers.multipart.is_some()),
    );
    set_feature(
        &mut capabilities,
        FEATURE_DOWNLOADS_DIRECT_GET,
        state.direct_transfers.is_some(),
    );
    if let Some(issuer) = direct_put {
        capabilities.limits.insert(
            LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES.to_owned(),
            issuer.max_content_bytes(),
        );
    }
    capabilities.limits.insert(
        LIMIT_UPLOAD_MAX_CONTENT_BYTES.to_owned(),
        state.config.max_upload_bytes,
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES.to_owned(),
        super::MAX_COMPLETION_BODY_BYTES as u64,
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
    // The runtime handles describe the core and admin planes. Grep is a
    // composed extension, so this deployment — not the runtime — says
    // whether the query plane exists and what it costs.
    //
    // Serving searches and administering the index are separate jobs and
    // separately deployable, so they are separately advertised. The
    // administration key sits in the admin plane, which the runtime always
    // advertises: a deployment that maintains an index it does not serve has
    // no query plane for a `query.` key to be parented by.
    set_feature(
        &mut capabilities,
        FEATURE_ADMIN_GREP_INDEX,
        state.config.grep.mode.maintains_index(),
    );
    if state.config.grep.mode.serves_grep() {
        let pagination = PaginationPolicy::default();
        capabilities.profiles.push(PROFILE_QUERY_V0.to_owned());
        capabilities
            .features
            .insert(FEATURE_QUERY_GREP.to_owned(), true);
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_DEFAULT.to_owned(),
            u64::from(pagination.default_limit().get()),
        );
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_MAX.to_owned(),
            u64::from(pagination.max_limit().get()),
        );
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_SCAN_BUDGET_FILES.to_owned(),
            loonfs_grep::MAX_GREP_SCAN_FILES as u64,
        );
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_TAIL_BUDGET_FILES.to_owned(),
            loonfs_grep::MAX_GREP_TAIL_FILES as u64,
        );
    }
    Ok(Json(capabilities))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_namespace",
        path = "/v0/namespaces",
        tag = "namespaces",
        summary = "Create namespace",
        description = "Creates a new empty namespace.",
        request_body = CreateNamespaceRequest,
        responses(
            (status = 200, description = "Namespace created", body = loonfs_api::Namespace),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 409, description = "Namespace already exists or is partial", body = ApiError),
            (status = 410, description = "Namespace id was deleted and retired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_namespace(
    State(state): State<AppState>,
    query: AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
    query.into_params()?;
    let namespace = state
        .writer
        .create_namespace(&request.namespace_id, CreateNamespaceOptions::default())
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace",
        path = "/v0/namespaces/{namespace_id}",
        tag = "namespaces",
        summary = "Get namespace",
        description = "Returns the current head and retention state for a namespace.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace", body = loonfs_api::Namespace),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_namespace(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let namespace = state
        .reader
        .get_namespace(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace_diagnostics",
        path = "/v0/admin/namespaces/{namespace_id}/diagnostics",
        tag = "admin",
        summary = "Get namespace diagnostics",
        description = "Returns namespace state together with the current manifest and visible WAL tail.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace diagnostics", body = loonfs_api::NamespaceDiagnostics),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_namespace_diagnostics(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::NamespaceDiagnostics>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let diagnostics = state
        .admin
        .get_namespace_diagnostics(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(diagnostics))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        operation_id = "delete_namespace",
        path = "/v0/namespaces/{namespace_id}",
        tag = "namespaces",
        summary = "Delete namespace",
        description = "Marks a namespace as deleted.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("expected_head_seq" = Option<ChangeSeq>, Query, description = "Delete only if the namespace head is still at this sequence")
        ),
        responses(
            (status = 200, description = "Namespace deleted", body = loonfs_api::DeleteNamespaceResponse),
            (status = 400, description = "Invalid request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Delete conflict", body = ApiError),
            (status = 410, description = "Namespace already deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn delete_namespace(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    query: AppQuery<DeleteNamespaceQuery>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::DeleteNamespaceResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let options = DeleteNamespaceOptions {
        expected_head_seq: query
            .expected_head_seq
            .as_deref()
            .map(parse_expected_head_seq)
            .transpose()?,
    };
    let response = state
        .writer
        .delete_namespace(&namespace_id, options)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

fn parse_expected_head_seq(value: &str) -> Result<ChangeSeq, ApiResponseError> {
    parse_public_ordinal("expected_head_seq", value, ChangeSeq::parse)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "fork_namespace",
        path = "/v0/namespaces/{namespace_id}/forks",
        tag = "namespaces",
        summary = "Fork namespace",
        description = "Creates a new namespace as a fork from the source namespace's current durable view.",
        params(("namespace_id" = String, Path, description = "Source namespace id")),
        request_body = ForkNamespaceRequest,
        responses(
            (status = 200, description = "Namespace forked", body = loonfs_api::Namespace),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Source namespace not found", body = ApiError),
            (status = 409, description = "Fork conflict", body = ApiError),
            (status = 410, description = "Source namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn fork_namespace(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    query: AppQuery<NoQuery>,
    AppJson(request): AppJson<ForkNamespaceRequest>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
    let source_namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let namespace = state
        .writer
        .fork_namespace(&source_namespace_id, &request.new_namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&source_namespace_id, error))?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_checkpoint",
        path = "/v0/admin/namespaces/{namespace_id}/checkpoints",
        tag = "admin",
        summary = "Create checkpoint",
        description = "Creates a named, user-owned checkpoint record pinning the current namespace view. Every call mints a new record under a new id; the name is a label, not a key. The record is a garbage-collection root until it is released, so routine maintenance should flush the WAL instead. This is a maintenance/admin operation, not a file mutation.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body(content = CreateCheckpointRequest, description = "Checkpoint name and optional lifetime"),
        responses(
            (status = 200, description = "Namespace envelope containing the created checkpoint", body = CreateCheckpointResponse),
            (status = 400, description = "Invalid namespace id, name, or lifetime", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_checkpoint(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    query: AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateCheckpointRequest>,
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let response = state
        .admin
        .create_checkpoint(
            &namespace_id,
            loonfs::CreateCheckpointOptions::from_request(request),
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("/name")
        })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_checkpoints",
        path = "/v0/admin/namespaces/{namespace_id}/checkpoints",
        tag = "admin",
        summary = "List checkpoints",
        description = "Lists one page of active checkpoints in checkpoint-id order. Expired checkpoints remain visible until collection releases them. Released checkpoints are omitted. The cursor resumes a live listing and does not create a snapshot.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("limit" = inline(Option<super::handlers_filesystem::OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque checkpoint-list page cursor")
        ),
        responses(
            (status = 200, description = "Active checkpoint objects", body = ListCheckpointsResponse),
            (status = 400, description = "Invalid namespace id, limit, or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_checkpoints(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<CheckpointPageQuery>,
) -> Result<Json<ListCheckpointsResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let cursor = query
        .cursor
        .as_deref()
        .map(|cursor| decode_namespace_cursor::<CheckpointPageCursor>(cursor, &namespace_id))
        .transpose()
        .map_err(|error| {
            ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &error.to_string(),
            )
            .with_param("cursor")
        })?;
    let response = state
        .admin
        .list_checkpoints_page(
            &namespace_id,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor,
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "release_checkpoint",
        path = "/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
        tag = "admin",
        summary = "Release checkpoint",
        description = "Releases a user-owned checkpoint pin by id. Idempotent: releasing an already-released or reaped record succeeds. The record is reaped by a later garbage-collection pass; its pinned data becomes collectable only on the pass after that.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("checkpoint_id" = String, Path, description = "Checkpoint id")
        ),
        responses(
            (status = 200, description = "Checkpoint release accepted (including an already released or reaped checkpoint)", body = ReleaseCheckpointResponse),
            (status = 400, description = "Invalid id, or the checkpoint is fork-owned", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn release_checkpoint(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<CheckpointPathParams>,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<ReleaseCheckpointResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let CheckpointPathParams { checkpoint_id } = path.into_params()?;
    query.into_params()?;
    let checkpoint_id = parse_checkpoint_id(&checkpoint_id)?;
    let response = state
        .admin
        .release_checkpoint(&namespace_id, &checkpoint_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
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
        .with_param("checkpoint_id")
    })
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_maintenance",
        path = "/v0/admin/namespaces/{namespace_id}/maintenance/run",
        tag = "admin",
        summary = "Run maintenance step",
        description = "Runs one bounded maintenance step. The body selects the actions by naming them: `metadata` folds the WAL tail once it reaches the threshold and merges one bounded reorganization unit, `advance_retention: true` advances the retention floor, and `gc` runs one bounded garbage-collection pass. Selected actions run in that order, each reports separately, and an absent report means the body did not select that action. A body that selects nothing is rejected. Nothing surrenders replay history or sweeps objects unless the body asked for it. A deleted namespace accepts a step that selects `gc` alone, which is how its reclaimable state is collected; any other selection is refused. Step-driven GC defaults to 1024 candidates and returns its cursor for a later step rather than looping internally. Losing the root race is an outcome, not an error.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body(content = MaintenanceStepRequest, description = "The actions this step selects"),
        responses(
            (status = 200, description = "Maintenance step completed", body = MaintenanceStepResponse),
            (status = 400, description = "Invalid namespace id or options", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn run_maintenance(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    query: AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<MaintenanceStepRequest>,
) -> Result<Json<MaintenanceStepResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let plan =
        loonfs::MaintenancePlan::from_request(request.unwrap_or_default()).map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("/metadata/max_wal_tail_segments")
        })?;
    let result = state
        .admin
        .maintenance_step_namespace(&namespace_id, plan)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(result))
}
