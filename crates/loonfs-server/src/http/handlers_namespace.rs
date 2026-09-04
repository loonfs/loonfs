//! Namespace lifecycle, status, capability discovery, and maintenance
//! handlers.

use super::error::ApiResponseError;
use super::query_params::{parse_path_id, parse_public_ordinal, resolve_page_limit};
use super::{AppJson, AppPath, AppQuery, AppState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{
    CheckpointPageCursor, CreateNamespaceOptions, CreateSnapshotOptions, DeleteNamespaceOptions,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::ChangeSeq;
use loonfs_api::{
    decode_namespace_cursor, CapabilityDocument, Checkpoint, CheckpointId, CreateCheckpointRequest,
    CreateNamespaceRequest, CreateSnapshotRequest, ErrorCode, ExtendSnapshotRequest,
    ForkNamespaceRequest, ListCheckpointsResponse, ListSnapshotsResponse, MaintenanceRunRequest,
    MaintenanceRunResponse, PageRequest, PaginationPolicy, ReleaseCheckpointResponse,
    ReleaseSnapshotResponse, SnapshotSummary, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_MAINTENANCE_GREP_INDEX, FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_DOWNLOAD_MAX_CONCURRENT, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_SNAPSHOT_MAX_LIFETIME_MS,
    LIMIT_SNAPSHOT_MAX_LIVE_PER_NAMESPACE, LIMIT_SNAPSHOT_MAX_TTL_MS,
    LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES, LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_MAX_CONCURRENT, LIMIT_UPLOAD_MAX_CONTENT_BYTES, PLANE_QUERY_V0,
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
        extensions(("x-loonfs-retry" = json!("idempotent"))),
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
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::CapabilityDocument>, ApiResponseError> {
    let mut capabilities = state.reader.get_capabilities();
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
    capabilities.limits.insert(
        LIMIT_SNAPSHOT_MAX_TTL_MS.to_owned(),
        state.config.snapshot_max_ttl_ms,
    );
    capabilities.limits.insert(
        LIMIT_SNAPSHOT_MAX_LIFETIME_MS.to_owned(),
        state.config.snapshot_max_lifetime_ms,
    );
    capabilities.limits.insert(
        LIMIT_SNAPSHOT_MAX_LIVE_PER_NAMESPACE.to_owned(),
        state.config.snapshot_max_live_per_namespace as u64,
    );
    // The runtime handles describe the filesystem and maintenance planes. Grep is a
    // composed extension, so this deployment — not the runtime — says
    // whether the query plane exists and what it costs.
    //
    // Serving searches and maintaining the index are separate jobs and
    // separately deployable, so they are separately advertised. The
    // maintenance key sits in the maintenance plane, which the runtime always
    // advertises: a deployment that maintains an index it does not serve has
    // no query plane for a `query.` key to be parented by.
    set_feature(
        &mut capabilities,
        FEATURE_MAINTENANCE_GREP_INDEX,
        state.config.grep.mode.maintains_index(),
    );
    if state.config.grep.mode.serves_grep() {
        let pagination = PaginationPolicy::default();
        capabilities.planes.push(PLANE_QUERY_V0.to_owned());
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
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
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
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
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
        extensions(("x-loonfs-retry" = json!("idempotent"))),
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
    let namespace = state
        .reader
        .get_namespace(&namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace_diagnostics",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
        tag = "maintenance",
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_api::NamespaceDiagnostics>, ApiResponseError> {
    let diagnostics = state
        .maintenance
        .get_namespace_diagnostics(&namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(diagnostics))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        operation_id = "delete_namespace",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<DeleteNamespaceQuery>,
) -> Result<Json<loonfs_api::DeleteNamespaceResponse>, ApiResponseError> {
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
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
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
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
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
    NamespaceIdPath(source_namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ForkNamespaceRequest>,
) -> Result<Json<loonfs_api::Namespace>, ApiResponseError> {
    let namespace = state
        .writer
        .fork_namespace(&source_namespace_id, &request.new_namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&source_namespace_id))?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_snapshot",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "Create snapshot",
        description = "Creates a snapshot of the current namespace state. Every call creates a new snapshot.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body = CreateSnapshotRequest,
        responses(
            (status = 200, description = "Snapshot created", body = SnapshotSummary),
            (status = 400, description = "Invalid namespace id, name, or ttl", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Snapshot quota exceeded", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_snapshot(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateSnapshotRequest>,
) -> Result<Json<SnapshotSummary>, ApiResponseError> {
    let now_ms = super::handlers_uploads::current_unix_ms()?;
    let expires_at_ms = snapshot_expiry_from_ttl(&state, now_ms, request.ttl_ms)?;
    let checkpoint = state
        .writer
        .create_snapshot_with_quota(
            &namespace_id,
            CreateSnapshotOptions {
                name: request.name,
                expires_at_ms,
            },
            now_ms,
            state.config.snapshot_max_live_per_namespace,
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("/name")
        })?;
    Ok(Json(SnapshotSummary::from_checkpoint(checkpoint).expect(
        "snapshot creation returns a snapshot-owned checkpoint",
    )))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_snapshots",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.snapshots",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "List snapshots",
        description = "Lists live snapshots in snapshot-id order. Released and expired snapshots are omitted.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("limit" = inline(Option<super::handlers_filesystem::OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque snapshot-list page cursor")
        ),
        responses(
            (status = 200, description = "Live snapshots", body = ListSnapshotsResponse),
            (status = 400, description = "Invalid namespace id, limit, or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_snapshots(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<CheckpointPageQuery>,
) -> Result<Json<ListSnapshotsResponse>, ApiResponseError> {
    let cursor = decode_checkpoint_cursor(query.cursor.as_deref(), &namespace_id)?;
    let response = state
        .reader
        .list_snapshots_page(
            &namespace_id,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor,
            },
        )
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "extend_snapshot",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
        tag = "namespaces",
        summary = "Extend snapshot",
        description = "Extends a live snapshot without passing its lifetime limit. Repeating the request has the same result.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("snapshot_id" = String, Path, description = "Snapshot id")
        ),
        request_body = ExtendSnapshotRequest,
        responses(
            (status = 200, description = "Snapshot extended", body = SnapshotSummary),
            (status = 400, description = "Invalid id or ttl", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Snapshot not found", body = ApiError),
            (status = 410, description = "Snapshot released or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn extend_snapshot(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ExtendSnapshotRequest>,
) -> Result<Json<SnapshotSummary>, ApiResponseError> {
    let snapshot_id = parse_path_id::<CheckpointId>("snapshot_id", &snapshot_id)?;
    let now_ms = super::handlers_uploads::current_unix_ms()?;
    let requested_expires_at_ms = snapshot_expiry_from_ttl(&state, now_ms, request.ttl_ms)?;
    let response = state
        .writer
        .extend_snapshot(
            &namespace_id,
            &snapshot_id,
            requested_expires_at_ms,
            state.config.snapshot_max_lifetime_ms,
        )
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "release_snapshot",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/release",
        tag = "namespaces",
        summary = "Release snapshot",
        description = "Releases a snapshot by id. Repeated releases succeed.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("snapshot_id" = String, Path, description = "Snapshot id")
        ),
        responses(
            (status = 200, description = "Snapshot release accepted", body = ReleaseSnapshotResponse),
            (status = 400, description = "Invalid id or non-snapshot record", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn release_snapshot(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<ReleaseSnapshotResponse>, ApiResponseError> {
    let snapshot_id = parse_path_id::<CheckpointId>("snapshot_id", &snapshot_id)?;
    let response = state
        .writer
        .release_snapshot(&namespace_id, &snapshot_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct SnapshotPathParams {
    snapshot_id: String,
}

fn snapshot_expiry_from_ttl(
    state: &AppState,
    now_ms: u64,
    ttl_ms: u64,
) -> Result<u64, ApiResponseError> {
    if ttl_ms == 0 || ttl_ms > state.config.snapshot_max_ttl_ms {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!(
                "ttl_ms must be greater than zero and may not exceed the \
                 `{LIMIT_SNAPSHOT_MAX_TTL_MS}` limit of {} milliseconds",
                state.config.snapshot_max_ttl_ms
            ),
        )
        .with_param("/ttl_ms"));
    }
    if ttl_ms > state.config.snapshot_max_lifetime_ms {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!(
                "ttl_ms may not exceed the `{LIMIT_SNAPSHOT_MAX_LIFETIME_MS}` limit of {} \
                 milliseconds",
                state.config.snapshot_max_lifetime_ms
            ),
        )
        .with_param("/ttl_ms"));
    }
    Ok(now_ms.saturating_add(ttl_ms))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_checkpoint",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
        path = "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
        tag = "maintenance",
        summary = "Create checkpoint",
        description = "Creates a named, user-owned checkpoint record pinning the current namespace view. Every call mints a new record under a new id; the name is a label, not a key. The record is a garbage-collection root until it is released, so routine maintenance should flush the WAL instead. This is a maintenance operation, not a file mutation.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body(content = CreateCheckpointRequest, description = "Checkpoint name and optional lifetime"),
        responses(
            (status = 200, description = "The created checkpoint", body = Checkpoint),
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateCheckpointRequest>,
) -> Result<Json<Checkpoint>, ApiResponseError> {
    let response = state
        .maintenance
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
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.checkpoints",
            })),
        ),
        path = "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
        tag = "maintenance",
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<CheckpointPageQuery>,
) -> Result<Json<ListCheckpointsResponse>, ApiResponseError> {
    let cursor = decode_checkpoint_cursor(query.cursor.as_deref(), &namespace_id)?;
    let response = state
        .maintenance
        .list_checkpoints_page(
            &namespace_id,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor,
            },
        )
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "release_checkpoint",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
        tag = "maintenance",
        summary = "Release checkpoint",
        description = "Releases a user-owned checkpoint pin by id. Idempotent: releasing an already-released or reaped record succeeds. The record is reaped by a later garbage-collection pass; its pinned data becomes collectable only on the pass after that.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("checkpoint_id" = String, Path, description = "Checkpoint id")
        ),
        responses(
            (status = 200, description = "Checkpoint release accepted (including an already released or reaped checkpoint)", body = ReleaseCheckpointResponse),
            (status = 400, description = "Invalid id, or the checkpoint is owned by another operation", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn release_checkpoint(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(CheckpointPathParams { checkpoint_id }): AppPath<CheckpointPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<ReleaseCheckpointResponse>, ApiResponseError> {
    let checkpoint_id = parse_path_id::<CheckpointId>("checkpoint_id", &checkpoint_id)?;
    let response = state
        .maintenance
        .release_checkpoint(&namespace_id, &checkpoint_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct CheckpointPathParams {
    checkpoint_id: String,
}

fn decode_checkpoint_cursor(
    cursor: Option<&str>,
    namespace_id: &loonfs_api::NamespaceId,
) -> Result<Option<CheckpointPageCursor>, ApiResponseError> {
    cursor
        .map(|cursor| decode_namespace_cursor::<CheckpointPageCursor>(cursor, namespace_id))
        .transpose()
        .map_err(|error| {
            ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string())
                .with_param("cursor")
        })
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_maintenance",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
        path = "/v0/maintenance/namespaces/{namespace_id}/runs",
        tag = "maintenance",
        summary = "Run one maintenance job",
        description = "Runs one maintenance job for the namespace. The body names the job with `kind`: `metadata`, `metadata_compaction`, `gc`, or `retention`. The response carries the same `kind` and that job's result. A deleted namespace accepts only `gc`. A `gc` run inspects up to 1024 objects unless `max_objects` says otherwise, and returns a cursor when more remain.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body(content = MaintenanceRunRequest, description = "The maintenance job to run"),
        responses(
            (status = 200, description = "Maintenance job completed", body = MaintenanceRunResponse),
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
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<MaintenanceRunRequest>,
) -> Result<Json<MaintenanceRunResponse>, ApiResponseError> {
    let result = state
        .maintenance
        .run_maintenance(&namespace_id, request)
        .await
        .map_err(|error| {
            let invalid_threshold = matches!(&error, loonfs::RuntimeError::Config(_));
            let response = ApiResponseError::runtime_for_namespace(&namespace_id, error);
            if invalid_threshold {
                response.with_invalid_request_param("/max_wal_tail_segments")
            } else {
                response
            }
        })?;
    Ok(Json(result))
}
