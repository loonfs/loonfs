//! HTTP reads addressed by inode ID.

use super::download_body::streamed_download_response;
use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::{
    parse_optional_snapshot_id, read_target, reject_snapshot_with_revision, PageQuery,
};
use super::query_params::{
    checked_cursor, invalid_path_id_error, parse_include_attributes, parse_revision_no,
    resolve_page_limit,
};
use super::{acquire_download_permit, AppPath, AppQuery, BindingState, NamespaceIdPath};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::{ListOptions, StatOptions};
#[cfg(feature = "openapi")]
use loonfs_types::ApiError;
use loonfs_types::{
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

/// The query of the inode content and download routes. A revision route
/// takes it only to refuse a snapshot with the error the path routes give.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InodeContentQuery {
    pub(super) snapshot_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatInodeQuery {
    include_attributes: Option<String>,
    snapshot_id: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
        tag = "inodes",
        summary = "Stat inode",
        description = "Returns the path entry for a visible inode from the current state or a live snapshot. Unknown or hidden inodes answer `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "Inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("include_attributes" = inline(Option<super::query_params::OpenApiDefaultTrueBoolean>), Query, description = "Project the inode's attribute map and revision (`true` or `false`). Defaults to `true`: a stat answers for one path and a map is capped at 64 KiB."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "Use the path state captured by this snapshot")
        ),
        responses(
            (status = 200, description = "Authoritative current inode entry", body = loonfs_types::PathEntry),
            (status = 400, description = "Invalid inode ID, include_attributes, snapshot id, or non-snapshot checkpoint", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, visible inode, or snapshot not found", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<StatInodeQuery>,
) -> Result<Json<loonfs_types::PathEntry>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = StatOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id).await?;
    let entry = target
        .stat_by_inode_with_options(inode_id, &options)
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
    snapshot_id: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_inode_children",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.entries",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        tag = "inodes",
        summary = "List directory children by inode",
        description = "Lists one page of a directory's children from the current state or a live snapshot, addressed by parent inode ID, in canonical name-key order. Inode addressing keeps a listing and its resumption on the same directory across concurrent renames or moves of the parent.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "Directory inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque directory page cursor"),
            ("include_attributes" = inline(Option<super::query_params::OpenApiDefaultFalseBoolean>), Query, description = "Project each entry's attribute map and revision (`true` or `false`). Defaults to `false`: a page holds many entries and each map may be 64 KiB, so a listing does not carry them unless asked."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "Use the directory state captured by this snapshot")
        ),
        responses(
            (status = 200, description = "One page of directory children", body = loonfs_types::ListInodeChildrenResponse),
            (status = 400, description = "Invalid inode ID, limit, cursor, include_attributes, snapshot id, or non-snapshot checkpoint", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, visible inode, or snapshot not found", body = ApiError),
            (status = 409, description = "Inode is not a directory", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_inode_children(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<ListInodeChildrenQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = ListOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id).await?;
    let listing = target
        .list_by_inode_with_options(inode_id, &options)
        .page(PageRequest {
            limit: resolve_page_limit(query.limit)?,
            cursor: checked_cursor::<DirectoryPageCursor>(query.cursor)?,
        })
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(super::page_response::page_response(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_file_revisions_by_inode",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.revisions",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
        tag = "inodes",
        summary = "List file revisions by inode",
        description = "Returns retained revisions for a file inode without requiring a current path.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "Maximum page size"),
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
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let namespace = runtime.namespace(&namespace_id);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let response = namespace
        .list_file_revisions_by_inode(inode_id)
        .page(PageRequest {
            limit: resolve_page_limit(query.limit)?,
            cursor: checked_cursor::<FileRevisionsPageCursor>(query.cursor)?,
        })
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_bytes_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        tag = "inodes",
        summary = "Read file by inode",
        description = "Reads and verifies the current revision of a visible file inode, wherever it is bound, or the revision a live snapshot captured. Unknown or hidden inodes answer `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "Use the file revision captured by this snapshot")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode ID, snapshot id, or non-snapshot checkpoint", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, visible inode, or snapshot not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_bytes_by_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<InodeContentQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id).await?;
    let permit = acquire_download_permit(&state)?;
    let stream = target
        .read_file_stream_by_inode(inode_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    streamed_download_response(
        stream,
        permit,
        state.options.max_download_bytes,
        &namespace_id,
    )
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_revision_bytes_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        tag = "inodes",
        summary = "Read file revision by inode",
        description = "Reads and verifies one retained file revision by inode ID and revision number.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_types::RevisionNo, Path, description = "Revision number")
        ),
        responses(
            (status = 200, description = "Revision bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode ID or revision number, or a snapshot_id, which cannot be combined with a revision", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_revision_bytes_by_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodeRevisionPathParams>,
    AppQuery(query): AppQuery<InodeContentQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let namespace = runtime.namespace(&namespace_id);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    reject_snapshot_with_revision(snapshot_id.as_ref(), Some(revision_no))?;
    let permit = acquire_download_permit(&state)?;
    let stream = namespace
        .read_file_revision_stream_by_inode(inode_id, revision_no)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    streamed_download_response(
        stream,
        permit,
        state.options.max_download_bytes,
        &namespace_id,
    )
}
