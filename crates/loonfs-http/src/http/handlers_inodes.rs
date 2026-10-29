//! HTTP reads addressed by inode ID.

use super::download_body::{streamed_download_response, RangeHeader};
use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::{reject_snapshot_with_revision, LivePageQuery};
use super::query_params::{
    checked_cursor, checked_list_cursor, effective_page_limit, invalid_path_id_error,
    live_consistency, read_at,
};
use super::{acquire_download_permit, AppPath, AppQuery, BindingState, NamespaceIdPath};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::{
    ListFileRevisionsOptions, ListOptions, ReadAt, ReadConsistency, ReadFileStreamOptions,
    StatOptions,
};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    public_inode_id, FileRevisionsPageCursor, InodeId, ListFileRevisionsResponse, PinId, RevisionNo,
};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodePathParams {
    pub(super) inode_id: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodeRevisionPathParams {
    pub(super) inode_id: String,
    pub(super) revision_no: RevisionNo,
}

pub(super) fn parse_inode_id(value: &str) -> Result<InodeId, ApiResponseError> {
    // Public inode ids use numeric encoding rather than generated string ids.
    public_inode_id::decode(value)
        .map_err(|error| invalid_path_id_error("inode_id", value, error.reason()))
}

/// The query of a content read by inode.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct InodeContentQuery {
    /// The snapshot whose captured file revision to read.
    snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read. It can't be combined with `snapshot_id`.
    consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    max_staleness_ms: Option<u64>,
}

/// The query of a revision content read by inode. The route takes `snapshot_id` only to refuse a
/// snapshot with the same error that the path routes return.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct InodeRevisionContentQuery {
    /// A snapshot, which a revision read refuses.
    #[cfg_attr(feature = "openapi", param(ignore))]
    snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head that it finds the revision in. `eventual`
    /// (the default, unless the server's `consistency` setting is `strong`) reads at the head that
    /// the server confirmed last while that confirmation is younger than the maximum staleness, and
    /// `strong` confirms the head before the read.
    consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    max_staleness_ms: Option<u64>,
}

/// The query of an inode stat.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct StatInodeQuery {
    /// Whether the response includes the inode's attribute map and attribute revision. The default
    /// is `true`, because a stat reads one path and a map is at most 64 KiB.
    #[cfg_attr(feature = "openapi", param(default = true))]
    include_attributes: Option<bool>,
    /// The snapshot whose captured path state to read.
    snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read. It can't be combined with `snapshot_id`.
    consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    max_staleness_ms: Option<u64>,
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
        description = "Returns the path entry for a visible inode, from the current state or an unexpired snapshot. An unknown or hidden inode returns `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            StatInodeQuery
        ),
        responses(
            (status = 200, description = "Path entry for the inode", body = loonfs_types::PathEntry),
            (status = 400, description = "Invalid inode ID, include_attributes, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, visible inode, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_inode(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<StatInodeQuery>,
) -> Result<Json<loonfs_types::PathEntry>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = StatOptions {
        at: read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?,
        ..StatOptions::default()
    };
    if let Some(include) = query.include_attributes {
        options.include_attributes = include.into();
    }
    let entry = runtime
        .namespace(&namespace_id)
        .stat_by_inode_with_options(inode_id, &options)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(entry))
}

/// The query of a listing of a directory's children by inode.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct ListInodeChildrenQuery {
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// The opaque cursor that the previous page returned.
    cursor: Option<String>,
    /// Whether the response includes each entry's attribute map and attribute revision. The default
    /// is `false`, because a page can have many entries and each map can be up to 64 KiB.
    #[cfg_attr(feature = "openapi", param(default = false))]
    include_attributes: Option<bool>,
    /// Whether the listing includes the directory's whole subtree, depth first, with each
    /// directory's entry before its descendants. The default is `false`, which lists the
    /// directory's children only.
    #[cfg_attr(feature = "openapi", param(default = false))]
    recursive: Option<bool>,
    /// The snapshot whose captured directory state to read.
    snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read. It can't be combined with `snapshot_id`.
    consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    max_staleness_ms: Option<u64>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_inode_children",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        tag = "inodes",
        summary = "List directory children by inode",
        description = "Lists one page of a directory's children, in canonical name key order, from the current state or an unexpired snapshot. The request identifies the directory by its inode ID, so a listing and its later pages stay on the same directory even if the directory is renamed or moved at the same time.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The directory's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ListInodeChildrenQuery
        ),
        responses(
            (status = 200, description = "One page of directory children", body = loonfs_types::ListInodeChildrenResponse),
            (status = 400, description = "Invalid inode ID, limit, cursor, include_attributes, recursive, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, visible inode, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Inode isn't a directory", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_inode_children(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<ListInodeChildrenQuery>,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let mut options = ListOptions::default();
    if let Some(include) = query.include_attributes {
        options.include_attributes = include.into();
    }
    if let Some(recursive) = query.recursive {
        options.recursive = recursive;
    }
    let limit = effective_page_limit(query.limit)?;
    let cursor = checked_list_cursor(query.cursor, &options)?;
    options.at = read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?;
    let listing = runtime
        .namespace(&namespace_id)
        .list_by_inode_with_options(inode_id, &options)
        .page_limit(limit)
        .cursor(cursor)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(super::page_response::page_response(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_file_revisions_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
        tag = "inodes",
        summary = "List file revisions by inode",
        description = "Returns the retained revisions of a file inode. The file doesn't need a current path.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            LivePageQuery
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid inode ID, limit, or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or inode not found", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_file_revisions_by_inode(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<LivePageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let options = ListFileRevisionsOptions {
        consistency: live_consistency(query.consistency, query.max_staleness_ms)?,
    };
    let response = namespace
        .list_file_revisions_by_inode_with_options(inode_id, &options)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(checked_cursor::<FileRevisionsPageCursor>(query.cursor)?)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_content_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        tag = "inodes",
        summary = "Read file by inode",
        description = "Reads and verifies the current revision of a visible file inode, wherever the inode is bound, or the revision that an unexpired snapshot captured. A `Range` header asks for one byte range of the file instead. An unknown or hidden inode returns `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            InodeContentQuery,
            ("Range" = Option<String>, Header, nullable = false, description = "One byte range of the file to read instead of the whole file, as `bytes=start-end`, where `end` is the offset of the last byte, or as `bytes=start-`. An `end` past the last byte reads to the end of the file. Any other value, including a suffix range or several ranges, returns 400 `invalid_request`.")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream", headers(("Accept-Ranges" = String, description = "`bytes`, because the route accepts a `Range` header."))),
            (status = 206, description = "The requested range of the file's bytes. The server doesn't verify a range against the file's checksum, which covers the whole file.", body = Vec<u8>, content_type = "application/octet-stream", headers(("Content-Range" = String, description = "The returned range and the file's size, as `bytes start-end/size`."), ("Accept-Ranges" = String, description = "`bytes`."))),
            (status = 400, description = "Invalid inode ID, snapshot ID, consistency, or `Range` header", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, visible inode, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "The file, or the range that the `Range` header asks for, exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            (status = 416, description = "The `Range` header starts at or past the end of the file. The error's `details.size_bytes` and the response's `Content-Range: bytes */size` header give the file's size.", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_content_by_inode(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<InodeContentQuery>,
    range: RangeHeader,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let options = ReadFileStreamOptions {
        range: range.stream_range(),
        at: read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?,
        ..ReadFileStreamOptions::default()
    };
    let permit = acquire_download_permit(&state)?;
    let stream = runtime
        .namespace(&namespace_id)
        .read_file_stream_by_inode_with_options(inode_id, &options)
        .await
        .map_err(ApiResponseError::runtime)?;
    streamed_download_response(stream, range.0, permit, state.options.max_download_bytes)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_revision_content_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        tag = "inodes",
        summary = "Read file revision by inode",
        description = "Reads and verifies one retained file revision by inode ID and revision number. A `Range` header asks for one byte range of the revision instead.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_types::RevisionNo, Path, description = "The revision number."),
            InodeRevisionContentQuery,
            ("Range" = Option<String>, Header, nullable = false, description = "One byte range of the file to read instead of the whole file, as `bytes=start-end`, where `end` is the offset of the last byte, or as `bytes=start-`. An `end` past the last byte reads to the end of the file. Any other value, including a suffix range or several ranges, returns 400 `invalid_request`.")
        ),
        responses(
            (status = 200, description = "Revision bytes", body = Vec<u8>, content_type = "application/octet-stream", headers(("Accept-Ranges" = String, description = "`bytes`, because the route accepts a `Range` header."))),
            (status = 206, description = "The requested range of the revision's bytes. The server doesn't verify a range against the file's checksum, which covers the whole file.", body = Vec<u8>, content_type = "application/octet-stream", headers(("Content-Range" = String, description = "The returned range and the file's size, as `bytes start-end/size`."), ("Accept-Ranges" = String, description = "`bytes`."))),
            (status = 400, description = "Invalid inode ID, revision number, consistency, or `Range` header, or a snapshot_id, which can't be combined with a revision", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, inode, or revision not found", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "The file, or the range that the `Range` header asks for, exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            (status = 416, description = "The `Range` header starts at or past the end of the file. The error's `details.size_bytes` and the response's `Content-Range: bytes */size` header give the file's size.", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_revision_content_by_inode(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodeRevisionPathParams>,
    AppQuery(query): AppQuery<InodeRevisionContentQuery>,
    range: RangeHeader,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = path.revision_no;
    reject_snapshot_with_revision(query.snapshot_id.as_ref(), Some(revision_no), "snapshot_id")?;
    let options = ReadFileStreamOptions {
        range: range.stream_range(),
        at: ReadAt::Live(live_consistency(query.consistency, query.max_staleness_ms)?),
        ..ReadFileStreamOptions::default()
    };
    let permit = acquire_download_permit(&state)?;
    let stream = namespace
        .read_file_revision_stream_by_inode_with_options(inode_id, revision_no, &options)
        .await
        .map_err(ApiResponseError::runtime)?;
    streamed_download_response(stream, range.0, permit, state.options.max_download_bytes)
}
