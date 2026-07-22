//! The `query/v0` plane: derived-index reads.

use super::{authorize, AppJson, AppState, NamespaceIdPath};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use loonfs_api::v0::{
    DisableGramsIndexResponse, EnableGramsIndexResponse, GrepRequest, GrepResponse,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::FEATURE_QUERY_GREP;

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/query/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, accelerated by the namespace's gram index (`index.grams`). Matches are verified against the real pattern and returned in ascending `(inode_id, byte_offset)` order; revisions committed after the index watermark are scanned exhaustively unless `allow_stale` skips them. Requires this deployment to serve grep and the index to be materialized on the namespace.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = GrepRequest,
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or an unindexable pattern without allow_scan", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Grep serving is disabled or the gram index is not materialized on this namespace", body = ApiError),
            (status = 503, description = "The index trails the head past the scan budget", body = ApiError)
        )
    )
)]
pub(super) async fn grep(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    AppJson(request): AppJson<GrepRequest>,
) -> Result<Json<GrepResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .reader
        .grep(&namespace_id, &request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

/// Uniform absent-capability response for every grep-owned HTTP operation.
pub(super) async fn grep_not_supported(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    Err(ApiResponseError::not_supported(
        FEATURE_QUERY_GREP,
        "grep is disabled for this deployment; set `[grep].mode` to `embedded` or `serve_only`",
    ))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/index/grams/enable",
        tag = "admin",
        summary = "Enable the gram index",
        description = "Publishes the namespace's index.grams feature entry. Backfill over existing revisions starts on the next grep worker sweep and the index stays current from then on. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Feature entry published or already present", body = EnableGramsIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
            (status = 503, description = "Lost a manifest publication race; retry", body = ApiError)
        )
    )
)]
pub(super) async fn enable_grams_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<EnableGramsIndexResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .enable_grams_index(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/index/grams/disable",
        tag = "admin",
        summary = "Disable the gram index",
        description = "Removes the namespace's index.grams feature entry and its segment references; garbage collection reclaims the segments. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Feature entry removed or already absent", body = DisableGramsIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
            (status = 503, description = "Lost a manifest publication race; retry", body = ApiError)
        )
    )
)]
pub(super) async fn disable_grams_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<DisableGramsIndexResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let response = state
        .admin
        .disable_grams_index(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}
