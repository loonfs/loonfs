//! The `query/v0` plane: derived-index reads.

use super::{authorize, AppJson, AppState, NamespaceIdPath};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
#[cfg(feature = "openapi")]
use loonfs_api::v0::ApiError;
use loonfs_api::v0::{GrepRequest, GrepResponse};

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/query/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, accelerated by the namespace's gram index (`index.grams`). Matches are verified against the real pattern and returned in ascending `(inode_id, byte_offset)` order; revisions committed after the index watermark are scanned exhaustively unless `allow_stale` skips them. Requires the index to be materialized on the namespace.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = GrepRequest,
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or an unindexable pattern without allow_scan", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "The gram index is not materialized on this namespace", body = ApiError),
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
