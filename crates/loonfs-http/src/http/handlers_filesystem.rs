//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, the committed-change
//! feed, and the diff.

use super::download_body::{streamed_download_response, RangeHeader};
use super::error::ApiResponseError;
use super::extractors::{ActorHeader, ContextHeaders};
use super::handlers_uploads::content_preparation_for_puts;
use super::query_params::{
    at_consistency, check_list_cursor, check_path, check_revisions_cursor, effective_page_limit,
    read_through, required_query_param,
};
use super::{acquire_download_permit, AppJson, AppQuery, HttpState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::publish::{CommitCandidate, CommitIntent, ContentPreparationError};
use loonfs::{
    AbsolutePath, CommitSeq, Consistency, DiffOptions, ErrorCode, ListChangesOptions, ListOptions,
    ReadFileStreamOptions, SnapshotId, StatOptions,
};
use loonfs_types::options::CommitSplitting;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    Commit, CommitRequest, FilesystemOperation, ListChangesResponse, ListDiffResponse,
    ListFileRevisionsResponse, ListTrashResponse, RevisionNo,
};
use std::sync::Arc;

/// The query of a path stat.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct PathQuery {
    /// The absolute path to read.
    #[cfg_attr(feature = "openapi", param(required = true))]
    path: Option<String>,
    /// Whether the response includes the inode's attribute map and attribute revision. The default
    /// is `false`, as on every route that takes this parameter.
    #[cfg_attr(feature = "openapi", param(default = false))]
    include_attributes: Option<bool>,
    /// The snapshot whose captured path state to read.
    snapshot_id: Option<SnapshotId>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read. It can't be combined
    /// with `snapshot_id`.
    consistency: Option<Consistency>,
}

/// The query of a listing of a file's revisions by path.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct PathPageQuery {
    /// The absolute path of the file.
    #[cfg_attr(feature = "openapi", param(required = true))]
    path: Option<String>,
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// The opaque cursor that the previous page returned.
    cursor: Option<String>,
    /// The snapshot whose state to list the file's revisions in. The listing starts at the
    /// revision that the snapshot captured.
    snapshot_id: Option<SnapshotId>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read. It can't be combined
    /// with `snapshot_id`.
    consistency: Option<Consistency>,
}

/// The query of a directory listing. The listing queries repeat their
/// shared fields because `serde_urlencoded` doesn't support
/// `#[serde(flatten)]`, and flattening would also prevent strict
/// unknown-field checks.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct ListPathPageQuery {
    /// The absolute path of the directory to list.
    #[cfg_attr(feature = "openapi", param(required = true))]
    path: Option<String>,
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
    snapshot_id: Option<SnapshotId>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read. It can't be combined
    /// with `snapshot_id`.
    consistency: Option<Consistency>,
}

/// The query of a listing that has no other parameters.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct PageQuery {
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    pub(super) limit: Option<u32>,
    /// The opaque cursor that the previous page returned.
    pub(super) cursor: Option<String>,
}

/// The query of a listing of the live head that has no other parameters.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct LivePageQuery {
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    pub(super) limit: Option<u32>,
    /// The opaque cursor that the previous page returned.
    pub(super) cursor: Option<String>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read.
    pub(super) consistency: Option<Consistency>,
}

/// The query of a content read by path.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct ContentQuery {
    /// The absolute path of the file to read.
    #[cfg_attr(feature = "openapi", param(required = true))]
    path: Option<String>,
    /// An earlier revision to read. It can't be combined with `snapshot_id`.
    revision_no: Option<RevisionNo>,
    /// The snapshot whose captured file revision to read.
    snapshot_id: Option<SnapshotId>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read. It can't be combined
    /// with `snapshot_id`.
    consistency: Option<Consistency>,
}

/// The query of the change feed.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct ChangesQuery {
    /// The sequence after which to return committed changes. With `event_index`, the commit at
    /// this sequence that the page resumes.
    #[cfg_attr(feature = "openapi", param(required = true))]
    after_seq: Option<CommitSeq>,
    /// Whether a page can end inside a commit that alone exceeds the page's budget. The default is
    /// `false`, which returns every commit whole.
    #[cfg_attr(feature = "openapi", param(default = false))]
    split_commits: Option<bool>,
    /// The `next_event_index` that the previous page returned, which resumes the commit at
    /// `after_seq` at that event. It requires `split_commits=true`. The default is 0, which starts
    /// after the commit at `after_seq`.
    #[cfg_attr(feature = "openapi", param(default = 0))]
    event_index: Option<u32>,
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// A snapshot to read the feed from. The feed ends at the sequence that the snapshot captured.
    snapshot_id: Option<SnapshotId>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read. It can't be combined
    /// with `snapshot_id`.
    consistency: Option<Consistency>,
}

/// The query of a diff page.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct DiffQuery {
    /// The sequence that the diff starts after. Each entry compares an inode's state at this
    /// sequence with its state at `to_seq`. It can't be below the namespace's retention floor.
    #[cfg_attr(feature = "openapi", param(required = true))]
    from_seq: Option<CommitSeq>,
    /// The sequence that the diff ends at, inclusive. The default is the head that the request
    /// reads. A later page passes the `to_seq` that the first page returned.
    to_seq: Option<CommitSeq>,
    /// The absolute path of the directory whose subtree the diff reports. The default is the root.
    #[cfg_attr(feature = "openapi", param(value_type = Option<String>))]
    path: Option<AbsolutePath>,
    /// The `next_after_seq` that the previous page returned. The default is `from_seq`.
    after_seq: Option<CommitSeq>,
    /// Whether a page can end inside a commit that alone touches more inodes than one page
    /// examines. The default is `false`, which reads every commit whole.
    #[cfg_attr(feature = "openapi", param(default = false))]
    split_commits: Option<bool>,
    /// The `next_event_index` that the previous page returned, which resumes the commit at
    /// `after_seq` at that event. It requires `split_commits=true`. The default is 0.
    #[cfg_attr(feature = "openapi", param(default = 0))]
    event_index: Option<u32>,
    /// The maximum number of commits that the page reads.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// How recently the read confirms the namespace head. `eventual` (the default) reads at the
    /// head that the server confirmed last while that confirmation is younger than the server's
    /// `max_staleness_ms`, and `strong` confirms the head before the read.
    consistency: Option<Consistency>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_path_entries",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["files"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListPathEntriesRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/entries",
        tag = "filesystem",
        summary = "List directory",
        description = "Lists a directory from the current state or an unexpired snapshot.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            ListPathPageQuery
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_types::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, cursor, include_attributes, recursive, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found. The ID of a deleted or expired snapshot returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "A path component is a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ListPathPageQuery>,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let path = required_query_param(query.path, "path")?;
    // An absent parameter leaves the option type's own default in place, so
    // the HTTP surface and the in-process one cannot return different results.
    let mut options = ListOptions::default();
    if let Some(include) = query.include_attributes {
        options.include_attributes = include.into();
    }
    if let Some(recursive) = query.recursive {
        options.recursive = recursive;
    }
    let limit = effective_page_limit(query.limit)?;
    let through = read_through(
        runtime.namespace(&namespace_id),
        query.snapshot_id,
        query.consistency,
    )?;
    check_list_cursor(query.cursor.as_deref(), &options)
        .and_then(|()| check_path(&path))
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    let (path, options, cursor) = (&path, &options, &query.cursor);
    let listing = through
        .read(
            |namespace| async move {
                namespace
                    .list_with_options(path, options)
                    .page_limit(limit)
                    .cursor(cursor.clone())
                    .page()
                    .await
            },
            |view| async move {
                view.list_with_options(path, options)
                    .page_limit(limit)
                    .cursor(cursor.clone())
                    .page()
                    .await
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    Ok(super::page_response::page_response(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_path_entry",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["files"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
            ("x-fern-request-name" = json!("GetPathEntryRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/entry",
        tag = "filesystem",
        summary = "Stat path",
        description = "Returns path metadata from the current state or an unexpired snapshot.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            PathQuery
        ),
        responses(
            (status = 200, description = "Path entry", body = loonfs_types::PathEntry),
            (status = 400, description = "Invalid path, include_attributes, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found. The ID of a deleted or expired snapshot returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "A path component is a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_path_entry(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathQuery>,
) -> Result<Json<loonfs_types::PathEntry>, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let path = required_query_param(query.path, "path")?;
    let through = read_through(
        runtime.namespace(&namespace_id),
        query.snapshot_id,
        query.consistency,
    )?;
    let mut options = StatOptions::default();
    if let Some(include) = query.include_attributes {
        options.include_attributes = include.into();
    }
    let (path, options) = (&path, &options);
    let entry = through
        .read(
            |namespace| async move { namespace.stat_with_options(path, options).await },
            |view| async move { view.stat_with_options(path, options).await },
        )
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    Ok(Json(entry))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_content",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["files"])),
            ("x-fern-sdk-method-name" = json!("content")),
            ("x-fern-request-name" = json!("GetFileContentRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/content",
        tag = "filesystem",
        summary = "Read file",
        description = "Returns the current file bytes, a retained revision, or the revision captured by an unexpired snapshot. A `Range` header asks for one byte range of the file instead.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            ContentQuery,
            ("Range" = Option<String>, Header, nullable = false, description = "One byte range of the file to read instead of the whole file, as `bytes=start-end`, where `end` is the offset of the last byte, or as `bytes=start-`. An `end` past the last byte reads to the end of the file. Any other value, including a suffix range or several ranges, returns 400 `invalid_request`.")
        ),
        responses(
            (status = 200, description = "File bytes", body = inline(crate::http::openapi::BinaryPayload), content_type = "application/octet-stream", headers(("Accept-Ranges" = String, description = "`bytes`, because the route accepts a `Range` header."))),
            (status = 206, description = "The requested range of the file's bytes. The server doesn't verify a range against the file's checksum, which covers the whole file.", body = inline(crate::http::openapi::BinaryPayload), content_type = "application/octet-stream", headers(("Content-Range" = String, description = "The returned range and the file's size, as `bytes start-end/size`."), ("Accept-Ranges" = String, description = "`bytes`."))),
            (status = 400, description = "Invalid path, revision, snapshot ID, consistency, or `Range` header, or revision_no combined with snapshot_id", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace, path, revision, or snapshot not found. The ID of a deleted or expired snapshot returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Path isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "The file, or the range that the `Range` header asks for, exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            (status = 416, description = "The `Range` header starts at or past the end of the file. The error's `details.size_bytes` and the response's `Content-Range: bytes */size` header give the file's size.", body = ErrorResponse, headers(("Content-Range" = String, description = "The file's size, as `bytes */size`."))),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_content(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ContentQuery>,
    range: RangeHeader,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let path = required_query_param(query.path, "path")?;
    reject_snapshot_with_revision(query.snapshot_id.as_ref(), query.revision_no, "revision_no")?;
    let options = ReadFileStreamOptions {
        range: range.stream_range(),
        ..ReadFileStreamOptions::default()
    };
    let through = read_through(
        runtime.namespace(&namespace_id),
        query.snapshot_id,
        query.consistency,
    )?;
    let permit = acquire_download_permit(&state)?;
    let (path, options, revision_no) = (&path, &options, query.revision_no);
    let stream = through
        .read(
            |namespace| async move {
                match revision_no {
                    Some(revision_no) => {
                        namespace
                            .read_file_revision_stream_with_options(path, revision_no, options)
                            .await
                    }
                    None => namespace.read_file_stream_with_options(path, options).await,
                }
            },
            |view| async move { view.read_file_stream_with_options(path, options).await },
        )
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    streamed_download_response(stream, range.0, permit, state.options.max_download_bytes)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_trash",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["trash"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListTrashRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/trash",
        tag = "filesystem",
        summary = "List the trash",
        description = "Returns the namespace's trash entries, oldest deletion first. An entry leaves the trash when it's restored, or when the namespace's retention floor reaches its `deletion_seq`. Each entry includes the inode ID and deletion sequence that a restore needs, and the directory binding that the delete removed.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            LivePageQuery
        ),
        responses(
            (status = 200, description = "Trash entries", body = ListTrashResponse),
            (status = 400, description = "Invalid limit or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_trash(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<LivePageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let namespace = at_consistency(runtime.namespace(&namespace_id), query.consistency);
    let response = namespace
        .list_trash()
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(query.cursor)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_file_revisions",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["files"])),
            ("x-fern-sdk-method-name" = json!("listRevisions")),
            ("x-fern-request-name" = json!("ListFileRevisionsRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/revisions",
        tag = "filesystem",
        summary = "List file revisions",
        description = "Resolves the current path to a file inode and returns that file's revisions. If the file might be renamed, use the inode revisions route, which identifies the file by a stable ID.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            PathPageQuery
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid path, limit, cursor, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found. The ID of a deleted or expired snapshot returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Path isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_file_revisions(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let path = required_query_param(query.path, "path")?;
    let through = read_through(
        runtime.namespace(&namespace_id),
        query.snapshot_id,
        query.consistency,
    )?;
    let limit = effective_page_limit(query.limit)?;
    check_revisions_cursor(query.cursor.as_deref())
        .and_then(|()| check_path(&path))
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    let (path, cursor) = (&path, &query.cursor);
    let response = through
        .read(
            |namespace| async move {
                namespace
                    .list_file_revisions(path)
                    .page_limit(limit)
                    .cursor(cursor.clone())
                    .page()
                    .await
            },
            |view| async move {
                view.list_file_revisions(path)
                    .page_limit(limit)
                    .cursor(cursor.clone())
                    .page()
                    .await
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    Ok(Json(response))
}

/// The server stores the `Loonfs-Actor` header; the shared token does not verify it.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_commit",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("replayable")),
            ("x-loonfs-scope" = json!("write")),
            ("x-fern-sdk-group-name" = json!(["commits"])),
            ("x-fern-sdk-method-name" = json!("create")),
        ),
        path = "/v0/namespaces/{namespace_id}/commits",
        tag = "filesystem",
        summary = "Apply a commit",
        description = "Applies one commit, which is an ordered, non-empty list of path operations that are committed together. The commit ID makes retries idempotent. The server checks the request's preconditions against the state before the commit, after it resolves any commit receipt and before it runs the operations. A failed precondition's position is reported in `details.precondition_index`. A request can contain a single operation. If an operation fails, the whole request fails, and for a request with more than one operation, `details.operation_index` reports the failed operation's position.",
        params(("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID.")),
        request_body = CommitRequest,
        responses(
            (status = 200, description = "Commit applied", body = Commit),
            (status = 400, description = "Invalid commit", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace or path not found", body = ErrorResponse),
            (status = 409, description = "Operation conflict", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit, or an append contains more than 256 KiB", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_commit(
    State(state): State<Arc<HttpState>>,
    ActorHeader(actor_id): ActorHeader,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CommitRequest>,
) -> Result<Json<Commit>, ApiResponseError> {
    let CommitRequest {
        preconditions,
        commit_id,
        message,
        content_tokens,
        mut operations,
    } = request;
    let inline_content = super::commit_content::prepare_inline_content(
        &mut operations,
        state
            .runtime
            .inline_content_policy()
            .inline_content_threshold_bytes,
        state.runtime.object_store().checksum_algorithm(),
    )
    .map_err(|error| error.with_commit_id(&commit_id))?;
    // Failed and uncertain outcomes echo the idempotency key the caller can
    // resubmit under (API spec, "Commit responses and safe retry").
    let commit_id_for_errors = commit_id.clone();
    let namespace = state
        .namespaces
        .open(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_commit_id(&commit_id))?;
    // Every put in the request shares one preparation pass: a proof belongs
    // to the content, not to the operation that names it.
    let put_content_refs = operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
        .collect::<Vec<_>>();
    let put_content_preparation = if put_content_refs.is_empty() {
        None
    } else {
        Some(
            content_preparation_for_puts(
                &namespace,
                state.options.content_token_secret.expose(),
                &put_content_refs,
                &content_tokens,
            )
            .await
            .map_err(|error| error.with_commit_id(&commit_id_for_errors))?,
        )
    };
    let request = CommitIntent {
        preconditions,
        commit_id,
        actor_id,
        subject: context.subject(),
        message,
        operations,
    };
    let response_result = if let Some(preparation) = put_content_preparation {
        let candidate = match preparation {
            Ok(prepared_content) => {
                CommitCandidate::with_inline_content(request, prepared_content, inline_content)
            }
            Err(rejections) => {
                let rejections = rejections
                    .into_iter()
                    .map(|rejected| (rejected.content_id, rejected.error))
                    .collect();
                CommitCandidate::with_inline_content(request, Vec::new(), inline_content)
                    .reject_content_preparation(ContentPreparationError::ContentToken(rejections))
            }
        };
        namespace.commit_candidate(candidate).await
    } else {
        namespace.commit(request).await
    };
    let response = response_result.map_err(|error| {
        ApiResponseError::runtime(error)
            .with_commit_field_param()
            .with_commit_id(&commit_id_for_errors)
    })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_changes",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["changes"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListChangesRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/changes",
        tag = "filesystem",
        summary = "List changes after a sequence",
        description = "Returns committed changes after a sequence. A snapshot limits the feed to its captured sequence.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            ChangesQuery
        ),
        responses(
            (status = 200, description = "Committed changes", body = ListChangesResponse),
            (status = 400, description = "Invalid change cursor, limit, snapshot ID, consistency, or after_seq above the head that the request reads", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace or snapshot not found. The ID of a deleted or expired snapshot returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "The cursor is older than the retained change history. Rebuild state from a fresh listing or snapshot, and follow the feed from that state's head.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ChangesQuery>,
) -> Result<Json<ListChangesResponse>, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let after_seq = required_query_param(query.after_seq, "after_seq")?;
    let limit = effective_page_limit(query.limit)?;
    let through = read_through(
        runtime.namespace(&namespace_id),
        query.snapshot_id,
        query.consistency,
    )?;
    let options = &ListChangesOptions {
        commit_splitting: commit_splitting(query.split_commits),
        event_index: query.event_index.unwrap_or(0),
    };
    let response = through
        .read(
            |namespace| async move {
                namespace
                    .list_changes_with_options(after_seq, options)
                    .page_limit(limit)
                    .page()
                    .await
            },
            |view| async move {
                view.list_changes_with_options(after_seq, options)
                    .page_limit(limit)
                    .page()
                    .await
            },
        )
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_diff",
        extensions(
            ("x-loonfs-authority" = json!("required")),
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-loonfs-scope" = json!("read")),
            ("x-fern-sdk-group-name" = json!(["diff"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListDiffRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/diff",
        tag = "filesystem",
        summary = "List the differences between two sequences",
        description = "Returns a page of the inodes whose visible state at `to_seq` differs from their state at `from_seq`, read from the commits between the two sequences.",
        params(
            ("namespace_id" = loonfs_types::NamespaceId, Path, description = "The namespace ID."),
            DiffQuery
        ),
        responses(
            (status = 200, description = "A page of the diff", body = ListDiffResponse),
            (status = 400, description = "Invalid sequence, path, limit, or consistency, a `to_seq` above the head that the request reads, or an `after_seq` outside `from_seq` through `to_seq`", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 409, description = "`from_seq` is below the retention floor, so the namespace no longer has the history after it. Rebuild state from a fresh listing or snapshot, and diff from that state's head.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_diff(
    State(state): State<Arc<HttpState>>,
    ContextHeaders(context): ContextHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<DiffQuery>,
) -> Result<Json<ListDiffResponse>, ApiResponseError> {
    let runtime = state.runtime_for(context);
    let from_seq = required_query_param(query.from_seq, "from_seq")?;
    let options = DiffOptions {
        path: query.path.unwrap_or_else(AbsolutePath::root),
        to_seq: query.to_seq,
        after_seq: query.after_seq,
        event_index: query.event_index.unwrap_or(0),
        limit: effective_page_limit(query.limit)?,
        commit_splitting: commit_splitting(query.split_commits),
    };
    let response = at_consistency(runtime.namespace(&namespace_id), query.consistency)
        .diff_with_options(from_seq, &options)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

/// Maps the `split_commits` query parameter of the change feed and the diff.
fn commit_splitting(split_commits: Option<bool>) -> CommitSplitting {
    if split_commits.unwrap_or(false) {
        CommitSplitting::AtPageBudget
    } else {
        CommitSplitting::Whole
    }
}

pub(super) fn reject_snapshot_with_revision(
    snapshot_id: Option<&SnapshotId>,
    revision_no: Option<RevisionNo>,
    param: &str,
) -> Result<(), ApiResponseError> {
    if snapshot_id.is_some() && revision_no.is_some() {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            "revision_no cannot be combined with snapshot_id",
        )
        .with_param(param));
    }
    Ok(())
}
