//! HTTP reads addressed by inode ID.

use super::error::ApiResponseError;
use super::handlers_filesystem::{
    buffered_download_response, decode_optional_cursor, parse_include_attributes,
    parse_revision_no, resolve_page_limit, PageQuery,
};
use super::{authorize, AppPath, AppQuery, AppState, NamespaceIdPath};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use loonfs::{ErrorCode, StatPathOptions};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    public_inode_id, FileRevisionsPageCursor, InodeId, ListFileRevisionsResponse, PageRequest,
};

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodePathParams {
    pub(super) inode_id: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodeRevisionPathParams {
    pub(super) inode_id: String,
    pub(super) revision_no: String,
}

pub(super) fn parse_inode_id(value: &str) -> Result<InodeId, ApiResponseError> {
    public_inode_id::decode(value).map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("path.inode_id {}", error.reason()),
        )
        .with_param("inode_id")
    })
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct StatInodeQuery {
    include_attributes: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
        tag = "inodes",
        summary = "Stat inode",
        description = "Returns the current path entry for a visible inode. Unknown or hidden inodes answer `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "Inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("include_attributes" = inline(Option<super::handlers_filesystem::OpenApiDefaultTrueBoolean>), Query, description = "Project the inode's attribute map and revision (`true` or `false`). Defaults to `true`: a stat answers for one path and a map is capped at 64 KiB.")
        ),
        responses(
            (status = 200, description = "Authoritative current inode entry", body = loonfs_api::AuthoritativePathEntry),
            (status = 400, description = "Invalid inode ID or include_attributes", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or visible inode not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn stat_inode(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<InodePathParams>,
    headers: HeaderMap,
    query: AppQuery<StatInodeQuery>,
) -> Result<Json<loonfs_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let inode_id = parse_inode_id(&path.into_params()?.inode_id)?;
    let query = query.into_params()?;
    let mut options = StatPathOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let entry = state
        .reader
        .stat_inode(&namespace_id, inode_id, options)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(entry))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
        tag = "inodes",
        summary = "List file revisions by inode",
        description = "Returns retained revisions for a file inode without requiring a current path.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::handlers_filesystem::OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque file-revisions page cursor")
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid inode ID, limit, or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or inode not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_file_revisions_by_inode(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<InodePathParams>,
    headers: HeaderMap,
    query: AppQuery<PageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let inode_id = parse_inode_id(&path.into_params()?.inode_id)?;
    let query = query.into_params()?;
    let response = state
        .reader
        .list_file_revisions_by_inode_page(
            &namespace_id,
            inode_id,
            PageRequest::<FileRevisionsPageCursor> {
                limit: resolve_page_limit(query.limit)?,
                cursor: decode_optional_cursor(query.cursor)?,
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        tag = "inodes",
        summary = "Read file revision by inode",
        description = "Reads and verifies one retained file revision by inode ID and revision number.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_api::RevisionNo, Path, description = "Revision number")
        ),
        responses(
            (status = 200, description = "Revision bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode ID or revision number", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.max_content_bytes` limit", body = ApiError),
            (status = 503, description = "The server is at its concurrent content-read limit; retry shortly", body = ApiError)
        )
    )
)]
pub(super) async fn get_file_revision_bytes_by_inode(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<InodeRevisionPathParams>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let path = path.into_params()?;
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let permit = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            state.metrics.download_rejected_as_busy();
            super::server_busy_error("proxied content reads")
        })?;
    let bytes = state
        .reader
        .get_file_revision_bytes_by_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(buffered_download_response(bytes, permit))
}
