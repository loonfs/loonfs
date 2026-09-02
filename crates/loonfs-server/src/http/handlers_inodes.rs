//! HTTP reads addressed by inode ID.

use super::download_body::buffered_download_response;
use super::error::ApiResponseError;
use super::handlers_filesystem::PageQuery;
use super::query_params::{
    decode_optional_cursor, invalid_path_id_error, parse_include_attributes, parse_revision_no,
    resolve_page_limit,
};
use super::{acquire_download_permit, AppPath, AppQuery, AppState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::{ListInodeChildrenOptions, StatPathOptions};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    public_inode_id, DirectoryPageCursor, FileRevisionsPageCursor, InodeId,
    ListFileRevisionsResponse, PageRequest,
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
    // Public inode ids use numeric encoding rather than generated string ids.
    public_inode_id::decode(value)
        .map_err(|error| invalid_path_id_error("inode_id", value, error.reason()))
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatInodeQuery {
    include_attributes: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_inode",
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
            (status = 200, description = "Authoritative current inode entry", body = loonfs_api::PathEntry),
            (status = 400, description = "Invalid inode ID or include_attributes", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or visible inode not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_inode(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<StatInodeQuery>,
) -> Result<Json<loonfs_api::PathEntry>, ApiResponseError> {
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = StatPathOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let entry = state
        .reader
        .get_inode(&namespace_id, inode_id, options)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(entry))
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListInodeChildrenQuery {
    limit: Option<String>,
    cursor: Option<String>,
    include_attributes: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_inode_children",
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        tag = "inodes",
        summary = "List directory children by inode",
        description = "Lists one page of a directory's children addressed by parent inode ID, in canonical name-key order. Inode addressing keeps a listing and its resumption on the same directory across concurrent renames or moves of the parent.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "Directory inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::handlers_filesystem::OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque directory page cursor"),
            ("include_attributes" = inline(Option<super::handlers_filesystem::OpenApiDefaultFalseBoolean>), Query, description = "Project each entry's attribute map and revision (`true` or `false`). Defaults to `false`: a page holds many entries and each map may be 64 KiB, so a listing does not carry them unless asked.")
        ),
        responses(
            (status = 200, description = "One page of directory children", body = loonfs_api::ListInodeChildrenResponse),
            (status = 400, description = "Invalid inode ID, limit, cursor, or include_attributes", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or visible inode not found", body = ApiError),
            (status = 409, description = "Inode is not a directory", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_inode_children(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<ListInodeChildrenQuery>,
) -> Result<Json<loonfs_api::ListInodeChildrenResponse>, ApiResponseError> {
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = ListInodeChildrenOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let listing = state
        .reader
        .list_inode_children_page(
            &namespace_id,
            inode_id,
            PageRequest::<DirectoryPageCursor> {
                limit: resolve_page_limit(query.limit)?,
                cursor: decode_optional_cursor(query.cursor)?,
            },
            options,
        )
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_file_revisions_by_inode",
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
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_file_revisions_by_inode(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let inode_id = parse_inode_id(&path.inode_id)?;
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
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_revision_bytes_by_inode",
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
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_revision_bytes_by_inode(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodeRevisionPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Response, ApiResponseError> {
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let permit = acquire_download_permit(&state)?;
    let bytes = state
        .reader
        .get_file_revision_bytes_by_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(buffered_download_response(bytes, permit))
}
