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
    CapabilityDocument, CheckpointId, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, ErrorCode, ForkNamespaceRequest, ListCheckpointsResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, PaginationPolicy, ReleaseCheckpointResponse,
    FEATURE_ADMIN_GREP_INDEX, FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT, LIMIT_DOWNLOAD_MAX_CONCURRENT,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX,
    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_MAX_CONCURRENT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_QUERY_V0, UPLOADS_DIRECT_PUT_CHECKSUM_FEATURES,
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
pub(super) struct DeleteNamespaceQuery {
    /// Delete only if the head is still at this sequence (`stale_head` on
    /// mismatch).
    expected_head_seq: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/capabilities",
        tag = "capabilities",
        summary = "Get capabilities",
        description = "Returns a summary of supported features and limits.",
        responses(
            (status = 200, description = "Capability document", body = loonfs_api::CapabilityDocument),
            (status = 401, description = "Unauthorized", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::CapabilityDocument>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
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
    // A direct-put client has to fold the provider's own whole-object
    // checksum while staging, so the deployment names which one it is —
    // read from the issuer, the same authority the begin handler validates
    // a claim against.
    let advertised_algorithm = direct_put.map(|issuer| issuer.checksum_algorithm());
    for (algorithm, feature) in UPLOADS_DIRECT_PUT_CHECKSUM_FEATURES {
        set_feature(
            &mut capabilities,
            feature,
            advertised_algorithm == Some(algorithm),
        );
    }
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
            (status = 410, description = "Namespace id was deleted and retired", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn create_namespace(
    State(state): State<AppState>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
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
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn namespace_status(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::NamespaceStatusResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
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
            ("expected_head_seq" = Option<String>, Query, description = "Delete only if the namespace head is still at this sequence")
        ),
        responses(
            (status = 200, description = "Namespace deleted", body = loonfs_api::DeleteNamespaceResponse),
            (status = 400, description = "Invalid request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Delete conflict", body = ApiError),
            (status = 410, description = "Namespace already deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn delete_namespace(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    query: AppQuery<DeleteNamespaceQuery>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::DeleteNamespaceResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace.into_id()?;
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
        .publisher()
        .submit_delete(namespace_id.clone(), options)
        .await
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

fn parse_expected_head_seq(value: &str) -> Result<ChangeSeq, ApiResponseError> {
    value.parse::<u64>().map(ChangeSeq).map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid expected_head_seq `{value}`: {error}"),
        )
    })
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
            (status = 410, description = "Source namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn fork_namespace(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AppJson(request): AppJson<ForkNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
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
        description = "Creates a named, user-owned checkpoint record pinning the current namespace view. Every call mints a new record under a new id; the name is a label, not a key. The record is a garbage-collection root until it is released, so routine maintenance should flush the WAL instead. This is a maintenance/admin operation, not a file mutation.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body(content = CreateCheckpointRequest, description = "Checkpoint name and optional lifetime"),
        responses(
            (status = 200, description = "Checkpoint created", body = CreateCheckpointResponse),
            (status = 400, description = "Invalid namespace id, name, or lifetime", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 503, description = "Checkpoint unavailable", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn create_checkpoint(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AppJson(request): AppJson<CreateCheckpointRequest>,
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .create_checkpoint(
            &namespace_id,
            loonfs::CreateCheckpointOptions::from_request(request),
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/admin/namespaces/{namespace}/checkpoints",
        tag = "admin",
        summary = "List checkpoints",
        description = "Lists every active checkpoint record on the namespace, oldest first. Each record is a garbage-collection root, and its id is what the release endpoint takes; because a checkpoint name is a label rather than a key, this is the only way to find a pin whose creation response is gone. An expired record that no collection pass has released yet is still active and is listed, with its expiry in the entry — the operator is looking for roots, and a record that still pins its basis is one. Released records are absent: a release is what stops a record pinning anything.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Active checkpoint records", body = ListCheckpointsResponse),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_checkpoints(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<ListCheckpointsResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .list_checkpoints(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
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
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn release_checkpoint(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    path: AppPath<CheckpointPathParams>,
    headers: HeaderMap,
) -> Result<Json<ReleaseCheckpointResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace.into_id()?;
    let CheckpointPathParams { checkpoint_id } = path.into_params()?;
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
    })
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/maintenance/step",
        tag = "admin",
        summary = "Run maintenance step",
        description = "Runs one bounded maintenance step. The body selects the actions by naming them: `metadata` folds the WAL tail once it reaches the threshold and merges one bounded reorganization unit, `advance_retention: true` advances the retention floor, and `gc` runs one bounded garbage-collection pass. Selected actions run in that order, each reports separately, and an absent report means the body did not select that action. A body that selects nothing is rejected. Nothing surrenders replay history or sweeps objects unless the body asked for it. A deleted namespace accepts a step that selects `gc` alone, which is how its reclaimable state is collected; any other selection is refused. Step-driven GC defaults to 1024 candidates and returns its cursor for a later step rather than looping internally. Losing the root race is an outcome, not an error.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body(content = MaintenanceStepRequest, description = "The actions this step selects"),
        responses(
            (status = 200, description = "Maintenance step completed", body = MaintenanceStepResponse),
            (status = 400, description = "Invalid namespace id or options", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn maintenance_step(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    OptionalAppJson(request): OptionalAppJson<MaintenanceStepRequest>,
) -> Result<Json<MaintenanceStepResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let plan = loonfs::MaintenancePlan::from_request(request.unwrap_or_default())
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let result = state
        .admin
        .maintenance_step_namespace(&namespace_id, plan)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(result))
}
