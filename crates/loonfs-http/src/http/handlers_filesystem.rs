//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, and the committed-change
//! feed.

use super::download_body::{streamed_download_response, RangeHeader};
use super::error::ApiResponseError;
use super::extractors::{ActorHeader, SubjectHeaders};
use super::handlers_uploads::{content_preparation_for_puts, PutContentPreparation};
use super::query_params::{
    checked_cursor, checked_list_cursor, effective_page_limit, live_consistency, read_at,
    required_query_param,
};
use super::{acquire_download_permit, AppJson, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::publish::{CommitCandidate, CommitIntent, ContentPreparationError};
use loonfs::{
    ChangeSeq, ErrorCode, ListChangesOptions, ListFileRevisionsOptions, ListOptions,
    ListTrashOptions, PinId, ReadConsistency, ReadFileStreamOptions, StatOptions,
};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    api::v0::{Commit, ListChangesResponse},
    CommitRequest, FileRevisionsPageCursor, FilesystemOperation, ListFileRevisionsResponse,
    ListTrashResponse, RevisionNo, TrashPageCursor,
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
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read.
    consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    max_staleness_ms: Option<u64>,
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
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read.
    pub(super) consistency: Option<ReadConsistency>,
    /// With `consistency=eventual`, how old the server's last confirmation of the head can be, in
    /// milliseconds. The default is the server's `max_staleness_ms`.
    #[cfg_attr(feature = "openapi", param(maximum = 1155000))]
    pub(super) max_staleness_ms: Option<u64>,
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

/// The query of the change feed.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct ChangesQuery {
    /// The sequence after which to return committed changes.
    #[cfg_attr(feature = "openapi", param(required = true))]
    after_seq: Option<ChangeSeq>,
    /// The maximum page size.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// A snapshot to read the feed from. The feed ends at the sequence that the snapshot captured.
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
        operation_id = "list_path_entries",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/entries",
        tag = "filesystem",
        summary = "List directory",
        description = "Lists a directory from the current state or an unexpired snapshot.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ListPathPageQuery
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_types::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, cursor, include_attributes, recursive, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ListPathPageQuery>,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let path = required_query_param(query.path, "path")?;
    // An absent parameter leaves the option type's own default in place, so
    // the HTTP surface and the in-process one cannot answer differently.
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
        .list_with_options(&path, &options)
        .page_limit(limit)
        .cursor(cursor)
        .page()
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    Ok(super::page_response::page_response(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_path_entry",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/entry",
        tag = "filesystem",
        summary = "Stat path",
        description = "Returns path metadata from the current state or an unexpired snapshot.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            PathQuery
        ),
        responses(
            (status = 200, description = "Path entry", body = loonfs_types::PathEntry),
            (status = 400, description = "Invalid path, include_attributes, snapshot ID, or consistency", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_path_entry(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathQuery>,
) -> Result<Json<loonfs_types::PathEntry>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let path = required_query_param(query.path, "path")?;
    let mut options = StatOptions {
        at: read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?,
        ..StatOptions::default()
    };
    if let Some(include) = query.include_attributes {
        options.include_attributes = include.into();
    }
    let entry = runtime
        .namespace(&namespace_id)
        .stat_with_options(&path, &options)
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    Ok(Json(entry))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_file_content",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/content",
        tag = "filesystem",
        summary = "Read file",
        description = "Returns the current file bytes, a retained revision, or the revision captured by an unexpired snapshot. A `Range` header asks for one byte range of the file instead.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ContentQuery,
            ("Range" = Option<String>, Header, nullable = false, description = "One byte range of the file to read instead of the whole file, as `bytes=start-end`, where `end` is the offset of the last byte, or as `bytes=start-`. An `end` past the last byte reads to the end of the file. Any other value, including a suffix range or several ranges, returns 400 `invalid_request`.")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream", headers(("Accept-Ranges" = String, description = "`bytes`, because the route accepts a `Range` header."))),
            (status = 206, description = "The requested range of the file's bytes. The server doesn't verify a range against the file's checksum, which covers the whole file.", body = Vec<u8>, content_type = "application/octet-stream", headers(("Content-Range" = String, description = "The returned range and the file's size, as `bytes start-end/size`."), ("Accept-Ranges" = String, description = "`bytes`."))),
            (status = 400, description = "Invalid path, revision, snapshot ID, consistency, or `Range` header, or revision_no combined with snapshot_id", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, revision, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "The file, or the range that the `Range` header asks for, exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            (status = 416, description = "The `Range` header starts at or past the end of the file. The error's `details.size_bytes` and the response's `Content-Range: bytes */size` header give the file's size.", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_content(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ContentQuery>,
    range: RangeHeader,
) -> Result<Response, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let path = required_query_param(query.path, "path")?;
    reject_snapshot_with_revision(query.snapshot_id.as_ref(), query.revision_no, "revision_no")?;
    let options = ReadFileStreamOptions {
        range: range.stream_range(),
        at: read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?,
        ..ReadFileStreamOptions::default()
    };
    let namespace = runtime.namespace(&namespace_id);
    let permit = acquire_download_permit(&state)?;
    let stream = match query.revision_no {
        Some(revision_no) => {
            namespace
                .read_file_revision_stream_with_options(&path, revision_no, &options)
                .await
        }
        None => {
            namespace
                .read_file_stream_with_options(&path, &options)
                .await
        }
    }
    .map_err(|error| ApiResponseError::runtime(error).with_invalid_request_param("path"))?;
    streamed_download_response(stream, range.0, permit, state.options.max_download_bytes)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_trash",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/trash",
        tag = "filesystem",
        summary = "List recoverable deletions",
        description = "Returns the namespace's recoverable deletions, oldest first. A deletion leaves the list when it's undeleted, or when the namespace's writer expires it after the retention floor reaches its `deletion_seq`. Each entry includes the inode ID and deletion sequence that an undelete needs, and the directory binding that the delete removed.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            LivePageQuery
        ),
        responses(
            (status = 200, description = "Recoverable deletions", body = ListTrashResponse),
            (status = 400, description = "Invalid limit or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_trash(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<LivePageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let options = ListTrashOptions {
        consistency: live_consistency(query.consistency, query.max_staleness_ms)?,
    };
    let response = namespace
        .list_trash_with_options(&options)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(checked_cursor::<TrashPageCursor>(query.cursor)?)
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
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/revisions",
        tag = "filesystem",
        summary = "List file revisions",
        description = "Resolves the current path to a file inode and returns that file's revisions. If the file might be renamed, use the inode revisions route, which identifies the file by a stable ID.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            PathPageQuery
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid path", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or path not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_file_revisions(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let path = required_query_param(query.path, "path")?;
    let options = ListFileRevisionsOptions {
        consistency: live_consistency(query.consistency, query.max_staleness_ms)?,
    };
    let response = namespace
        .list_file_revisions_with_options(&path, &options)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(checked_cursor::<FileRevisionsPageCursor>(query.cursor)?)
        .page()
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
        extensions(("x-loonfs-actor" = json!("required")), ("x-loonfs-retry" = json!("replayable"))),
        path = "/v0/namespaces/{namespace_id}/commits",
        tag = "filesystem",
        summary = "Apply a commit",
        description = "Applies one commit, which is an ordered, non-empty list of path operations that are committed together. The commit ID makes retries idempotent. The server checks the request's preconditions against the state before the commit, after it resolves any commit receipt and before it runs the operations. A failed precondition's position is reported in `details.precondition_index`. A request can contain a single operation. If an operation fails, the whole request fails, and for a request with more than one operation, `details.operation_index` reports the failed operation's position.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        request_body = CommitRequest,
        responses(
            (status = 200, description = "Commit applied", body = Commit),
            (status = 400, description = "Invalid commit", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or path not found", body = ErrorResponse),
            (status = 409, description = "Operation conflict", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit, or an append contains more than 256 KiB", body = ErrorResponse),
            (status = 501, description = "Inline content is disabled", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_commit(
    State(state): State<Arc<BindingState>>,
    ActorHeader(actor_id): ActorHeader,
    SubjectHeaders(subject): SubjectHeaders,
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
        &namespace_id,
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
                state.namespaces.as_ref(),
                &namespace,
                state.content_tokens(),
                &put_content_refs,
                &content_tokens,
            )
            .await?,
        )
    };
    let request = CommitIntent {
        preconditions,
        commit_id,
        actor_id,
        subject,
        message,
        operations,
    };
    let response_result = if let Some(preparation) = put_content_preparation {
        let candidate = match preparation {
            PutContentPreparation::Absent => {
                CommitCandidate::with_inline_content(request, Vec::new(), inline_content)
            }
            PutContentPreparation::Ready(prepared_content) => {
                CommitCandidate::with_inline_content(request, prepared_content, inline_content)
            }
            PutContentPreparation::Rejected(rejections) => {
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
        ApiResponseError::runtime_for_namespace_writer(
            state.namespaces.as_ref(),
            &namespace_id,
            error,
        )
        .with_commit_id(&commit_id_for_errors)
    })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_changes",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/changes",
        tag = "filesystem",
        summary = "List changes after a sequence",
        description = "Returns committed changes after a sequence. A snapshot limits the feed to its captured sequence.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ChangesQuery
        ),
        responses(
            (status = 200, description = "Committed changes", body = ListChangesResponse),
            (status = 400, description = "Invalid change cursor, limit, snapshot ID, consistency, or after_seq above the head that the request reads", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "The cursor is older than the retained change history. Rebuild state from a fresh listing or snapshot, and follow the feed from that state's head.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ChangesQuery>,
) -> Result<Json<ListChangesResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let after_seq = required_query_param(query.after_seq, "after_seq")?;
    let limit = effective_page_limit(query.limit)?;
    let options = ListChangesOptions {
        at: read_at(query.snapshot_id, query.consistency, query.max_staleness_ms)?,
    };
    let response = runtime
        .namespace(&namespace_id)
        .list_changes_with_options(after_seq, &options)
        .page_limit(limit)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

pub(super) fn reject_snapshot_with_revision(
    snapshot_id: Option<&PinId>,
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
