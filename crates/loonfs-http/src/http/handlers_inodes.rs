//! HTTP reads addressed by inode ID.

use super::download_body::streamed_download_response;
use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::{
    parse_optional_snapshot_id, read_target, reject_snapshot_with_revision, LivePageQuery,
};
use super::query_params::{
    checked_cursor, checked_list_cursor, effective_page_limit, invalid_path_id_error,
    parse_boolean_query_param, parse_consistency, parse_include_attributes, parse_read_at,
    parse_revision_no,
};
use super::{acquire_download_permit, AppPath, AppQuery, BindingState, NamespaceIdPath};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::{ListFileRevisionsOptions, ListOptions, ReadFileStreamOptions, StatOptions};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{public_inode_id, FileRevisionsPageCursor, InodeId, ListFileRevisionsResponse};

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

/// The query of the inode content routes. A revision route takes
/// `snapshot_id` only to refuse a snapshot with the same error that the path
/// routes return. The consistency parameters apply to both routes, because a
/// revision read also finds the revision at the live head.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InodeContentQuery {
    pub(super) snapshot_id: Option<String>,
    pub(super) consistency: Option<String>,
    pub(super) max_staleness_ms: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatInodeQuery {
    include_attributes: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
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
            ("include_attributes" = inline(Option<super::query_params::OpenApiDefaultTrueBoolean>), Query, description = "Whether the response includes the inode's attribute map and attribute revision. The default is `true`, because a stat reads one path and a map is at most 64 KiB."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured path state to read."),
            ("consistency" = inline(Option<super::query_params::OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<super::query_params::OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
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
    let at = parse_read_at(
        parse_optional_snapshot_id(query.snapshot_id)?,
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
    )?;
    let target = read_target(runtime.namespace(&namespace_id), at, "snapshot_id").await?;
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
    recursive: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
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
        description = "Lists one page of a directory's children, in canonical name key order, from the current state or an unexpired snapshot. The request identifies the directory by its inode ID, so a listing and its later pages stay on the same directory even if the directory is renamed or moved at the same time.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The directory's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned."),
            ("include_attributes" = inline(Option<super::query_params::OpenApiDefaultFalseBoolean>), Query, description = "Whether the response includes each entry's attribute map and attribute revision. The default is `false`, because a page can have many entries and each map can be up to 64 KiB."),
            ("recursive" = inline(Option<super::query_params::OpenApiDefaultFalseBoolean>), Query, description = "Whether the listing includes the directory's whole subtree, depth first, with each directory's entry before its descendants. The default is `false`, which lists the directory's children only."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured directory state to read."),
            ("consistency" = inline(Option<super::query_params::OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<super::query_params::OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
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
    if let Some(value) = query.recursive.as_deref() {
        options.recursive = parse_boolean_query_param(value, "recursive")?;
    }
    let at = parse_read_at(
        parse_optional_snapshot_id(query.snapshot_id)?,
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
    )?;
    let target = read_target(runtime.namespace(&namespace_id), at, "snapshot_id").await?;
    let listing = target
        .list_by_inode_with_options(inode_id, &options)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(checked_list_cursor(query.cursor, &options)?)
        .page()
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
        description = "Returns the retained revisions of a file inode. The file doesn't need a current path.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned."),
            ("consistency" = inline(Option<super::query_params::OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read."),
            ("max_staleness_ms" = inline(Option<super::query_params::OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
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
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<LivePageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let namespace = runtime.namespace(&namespace_id);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let options = ListFileRevisionsOptions {
        consistency: parse_consistency(
            query.consistency.as_deref(),
            query.max_staleness_ms.as_deref(),
        )?,
    };
    let response = namespace
        .list_file_revisions_by_inode_with_options(inode_id, &options)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(checked_cursor::<FileRevisionsPageCursor>(query.cursor)?)
        .page()
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
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
        description = "Reads and verifies the current revision of a visible file inode, wherever the inode is bound, or the revision that an unexpired snapshot captured. An unknown or hidden inode returns `inode_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured file revision to read."),
            ("consistency" = inline(Option<super::query_params::OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<super::query_params::OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode ID, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, visible inode, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "Content exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_content_by_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(query): AppQuery<InodeContentQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let at = parse_read_at(
        parse_optional_snapshot_id(query.snapshot_id)?,
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
    )?;
    let target = read_target(runtime.namespace(&namespace_id), at, "snapshot_id").await?;
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
        operation_id = "get_file_revision_content_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        tag = "inodes",
        summary = "Read file revision by inode",
        description = "Reads and verifies one retained file revision by inode ID and revision number.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_types::RevisionNo, Path, description = "The revision number."),
            ("consistency" = inline(Option<super::query_params::OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head that it finds the revision in. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read."),
            ("max_staleness_ms" = inline(Option<super::query_params::OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "Revision bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode ID, revision number, or consistency, or a snapshot_id, which can't be combined with a revision", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, inode, or revision not found", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "Content exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_revision_content_by_inode(
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
    reject_snapshot_with_revision(snapshot_id.as_ref(), Some(revision_no), "snapshot_id")?;
    let options = ReadFileStreamOptions {
        consistency: parse_consistency(
            query.consistency.as_deref(),
            query.max_staleness_ms.as_deref(),
        )?,
        ..ReadFileStreamOptions::default()
    };
    let permit = acquire_download_permit(&state)?;
    let stream = namespace
        .read_file_revision_stream_by_inode_with_options(inode_id, revision_no, &options)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    streamed_download_response(
        stream,
        permit,
        state.options.max_download_bytes,
        &namespace_id,
    )
}
