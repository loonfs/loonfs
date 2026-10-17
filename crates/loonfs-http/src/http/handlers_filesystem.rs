//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, and the committed-change
//! feed.

use super::download_body::streamed_download_response;
use super::error::ApiResponseError;
use super::extractors::{ActorHeader, SubjectHeaders};
use super::handlers_uploads::{
    content_preparation_for_puts, ContentTokenVerifier, PutContentPreparation,
};
use super::query_params::{
    checked_cursor, effective_page_limit, parse_consistency, parse_include_attributes,
    parse_public_ordinal, parse_revision_no, required_query_param,
};
#[cfg(feature = "openapi")]
use super::query_params::{
    OpenApiConsistency, OpenApiDefaultFalseBoolean, OpenApiDefaultTrueBoolean,
    OpenApiMaxStalenessMs, OpenApiPageLimit,
};
use super::{acquire_download_permit, AppJson, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::publish::{CommitCandidate, CommitIntent, ContentPreparationError};
use loonfs::{
    ChangeSeq, Consistency, DownloadOptions, ErrorCode, InodeId, ListChangesOptions, ListOptions,
    Namespace, PinId, ReadFileStreamOptions, ReadOnly, ReadView, StatOptions,
};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
// The wire commit request and the runtime's differ only by the content
// tokens, which this handler resolves and strips; the operations inside them
// are one type. The alias keeps the two request names readable side by side.
use loonfs_types::{
    api::v0::{Commit, ListChangesResponse},
    CommitRequest, DirectoryPageCursor, FileRevisionsPageCursor, FilesystemOperation,
    ListFileRevisionsResponse, ListTrashResponse, PageRequest, RevisionNo, TrashPageCursor,
};

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PathQuery {
    path: Option<String>,
    include_attributes: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PathPageQuery {
    path: Option<String>,
    limit: Option<String>,
    cursor: Option<String>,
}

/// The directory-listing query. These fields are repeated because
/// `serde_urlencoded` does not support `#[serde(flatten)]`, and flattening
/// would also prevent strict unknown-field checks.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListPathPageQuery {
    path: Option<String>,
    limit: Option<String>,
    cursor: Option<String>,
    include_attributes: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageQuery {
    pub(super) limit: Option<String>,
    pub(super) cursor: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentQuery {
    path: Option<String>,
    revision_no: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangesQuery {
    after_seq: Option<String>,
    limit: Option<String>,
    snapshot_id: Option<String>,
    consistency: Option<String>,
    max_staleness_ms: Option<String>,
}

pub(super) enum ReadTarget {
    Snapshot(Box<ReadView>),
    Live(Namespace<ReadOnly>),
}

impl ReadTarget {
    pub(super) fn list_with_options(
        &self,
        path: &str,
        options: &ListOptions,
    ) -> loonfs::PathEntriesPager {
        match self {
            Self::Snapshot(view) => view.list_with_options(path, options),
            Self::Live(namespace) => namespace.list_with_options(path, options),
        }
    }

    pub(super) async fn stat_with_options(
        &self,
        path: &str,
        options: &StatOptions,
    ) -> loonfs::Result<loonfs::PathEntry> {
        match self {
            Self::Snapshot(view) => view.stat_with_options(path, options).await,
            Self::Live(namespace) => namespace.stat_with_options(path, options).await,
        }
    }

    pub(super) fn list_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> loonfs::InodeChildrenPager {
        match self {
            Self::Snapshot(view) => view.list_by_inode_with_options(inode_id, options),
            Self::Live(namespace) => namespace.list_by_inode_with_options(inode_id, options),
        }
    }

    pub(super) async fn stat_by_inode_with_options(
        &self,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> loonfs::Result<loonfs::PathEntry> {
        match self {
            Self::Snapshot(view) => view.stat_by_inode_with_options(inode_id, options).await,
            Self::Live(namespace) => {
                namespace
                    .stat_by_inode_with_options(inode_id, options)
                    .await
            }
        }
    }

    pub(super) async fn read_file_stream(
        &self,
        path: &str,
        revision_no: Option<RevisionNo>,
        consistency: Consistency,
    ) -> loonfs::Result<loonfs::FileContentStream<loonfs::SharedObjectStore>> {
        let options = ReadFileStreamOptions {
            consistency,
            ..ReadFileStreamOptions::default()
        };
        match self {
            Self::Snapshot(view) => view.read_file_stream_with_options(path, &options).await,
            Self::Live(namespace) => match revision_no {
                Some(revision_no) => {
                    namespace
                        .read_file_revision_stream_with_options(path, revision_no, &options)
                        .await
                }
                None => {
                    namespace
                        .read_file_stream_with_options(path, &options)
                        .await
                }
            },
        }
    }

    pub(super) async fn read_file_stream_by_inode(
        &self,
        inode_id: InodeId,
        consistency: Consistency,
    ) -> loonfs::Result<loonfs::FileContentStream<loonfs::SharedObjectStore>> {
        let options = ReadFileStreamOptions {
            consistency,
            ..ReadFileStreamOptions::default()
        };
        match self {
            Self::Snapshot(view) => {
                view.read_file_stream_by_inode_with_options(inode_id, &options)
                    .await
            }
            Self::Live(namespace) => {
                namespace
                    .read_file_stream_by_inode_with_options(inode_id, &options)
                    .await
            }
        }
    }

    pub(super) async fn create_download(
        &self,
        path: &str,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> loonfs::Result<loonfs::downloads::DirectDownloadTarget> {
        match self {
            Self::Snapshot(view) => view.create_download_with_options(path, options).await,
            Self::Live(namespace) => match revision_no {
                Some(revision_no) => {
                    namespace
                        .create_revision_download_with_options(path, revision_no, options)
                        .await
                }
                None => namespace.create_download_with_options(path, options).await,
            },
        }
    }

    pub(super) async fn create_download_by_inode(
        &self,
        inode_id: InodeId,
        options: &DownloadOptions,
    ) -> loonfs::Result<loonfs::downloads::DirectDownloadByInodeTarget> {
        match self {
            Self::Snapshot(view) => {
                view.create_download_by_inode_with_options(inode_id, options)
                    .await
            }
            Self::Live(namespace) => {
                namespace
                    .create_download_by_inode_with_options(inode_id, options)
                    .await
            }
        }
    }

    /// A snapshot's feed has no consistency setting, and the handler rejects
    /// `consistency` with a `snapshot_id` before it reads.
    fn list_changes(&self, after_seq: ChangeSeq, consistency: Consistency) -> loonfs::ChangesPager {
        match self {
            Self::Snapshot(view) => view.list_changes(after_seq),
            Self::Live(namespace) => {
                namespace.list_changes_with_options(after_seq, &ListChangesOptions { consistency })
            }
        }
    }
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_path_entries",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.entries",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/entries",
        tag = "filesystem",
        summary = "List directory",
        description = "Lists a directory from the current state or a live snapshot.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("path" = String, Query, description = "The absolute path of the directory to list."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned."),
            ("include_attributes" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Whether the response includes each entry's attribute map and attribute revision. The default is `false`, because a page can have many entries and each map can be up to 64 KiB."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured directory state to read."),
            ("consistency" = inline(Option<OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_types::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, cursor, include_attributes, snapshot ID, consistency, or non-snapshot checkpoint", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ListPathPageQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let path = required_query_param(query.path, "path")?;
    // An absent parameter leaves the option type's own default in place, so
    // the HTTP surface and the in-process one cannot answer differently.
    let mut options = ListOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let request = PageRequest {
        limit: effective_page_limit(query.limit)?,
        cursor: checked_cursor::<DirectoryPageCursor>(query.cursor)?,
    };
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    options.consistency = parse_consistency(
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
        snapshot_id.is_some(),
        state.options.consistency,
    )?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id, "snapshot_id").await?;
    let listing = target
        .list_with_options(&path, &options)
        .page(request)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
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
        description = "Returns path metadata from the current state or a live snapshot.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("path" = String, Query, description = "The absolute path to read."),
            ("include_attributes" = inline(Option<OpenApiDefaultTrueBoolean>), Query, description = "Whether the response includes the inode's attribute map and attribute revision. The default is `true`, because a stat reads one path and a map is at most 64 KiB."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured path state to read."),
            ("consistency" = inline(Option<OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "Path entry", body = loonfs_types::PathEntry),
            (status = 400, description = "Invalid path, include_attributes, snapshot ID, consistency, or non-snapshot checkpoint", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, or snapshot not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_path_entry(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathQuery>,
) -> Result<Json<loonfs_types::PathEntry>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let path = required_query_param(query.path, "path")?;
    let mut options = StatOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    options.consistency = parse_consistency(
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
        snapshot_id.is_some(),
        state.options.consistency,
    )?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id, "snapshot_id").await?;
    let entry = target
        .stat_with_options(&path, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
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
        description = "Returns the current file bytes, a retained revision, or the revision captured by a live snapshot.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("path" = String, Query, description = "The absolute path of the file to read."),
            ("revision_no" = Option<RevisionNo>, Query, description = "An earlier revision to read. It can't be combined with `snapshot_id`."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "The snapshot whose captured file revision to read."),
            ("consistency" = inline(Option<OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid path, revision, snapshot ID, consistency, non-snapshot checkpoint, or revision_no combined with snapshot_id", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, revision, or snapshot not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ErrorResponse),
            (status = 413, description = "Content exceeds the advertised `download.service_proxied.max_content_bytes` limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_content(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let path = required_query_param(query.path, "path")?;
    let revision_no = query
        .revision_no
        .as_deref()
        .map(parse_revision_no)
        .transpose()?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    reject_snapshot_with_revision(snapshot_id.as_ref(), revision_no, "revision_no")?;
    let consistency = parse_consistency(
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
        snapshot_id.is_some(),
        state.options.consistency,
    )?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id, "snapshot_id").await?;
    let permit = acquire_download_permit(&state)?;
    let stream = target
        .read_file_stream(&path, revision_no, consistency)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
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
        operation_id = "list_trash",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.entries",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/trash",
        tag = "filesystem",
        summary = "List recoverable deletions",
        description = "Returns the namespace's recoverable deletions, oldest first. Advancing the retention floor doesn't remove entries. Each entry includes the inode ID and deletion sequence that an undelete needs, and the deleted name when the delete recorded one.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned.")
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
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let namespace = runtime.namespace(&namespace_id);
    let response = namespace
        .list_trash()
        .page(PageRequest {
            limit: effective_page_limit(query.limit)?,
            cursor: checked_cursor::<TrashPageCursor>(query.cursor)?,
        })
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_file_revisions",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.revisions",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/filesystem/revisions",
        tag = "filesystem",
        summary = "List file revisions",
        description = "Resolves the current path to a file inode and returns that file's revisions. If the file might be renamed, use the inode revisions route, which identifies the file by a stable ID.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("path" = String, Query, description = "The absolute path of the file."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned.")
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
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let namespace = runtime.namespace(&namespace_id);
    let path = required_query_param(query.path, "path")?;
    let response = namespace
        .list_file_revisions(&path)
        .page(PageRequest {
            limit: effective_page_limit(query.limit)?,
            cursor: checked_cursor::<FileRevisionsPageCursor>(query.cursor)?,
        })
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
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
    State(state): State<BindingState>,
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
        state.options.inline_content.inline_content_threshold_bytes,
        state.probe_store.checksum_algorithm(),
    )
    .map_err(|error| error.with_commit_id(&commit_id))?;
    // Failed and uncertain outcomes echo the idempotency key the caller can
    // resubmit under (API spec, "Commit responses and safe retry").
    let commit_id_for_errors = commit_id.clone();
    let namespace = state
        .namespaces
        .open(&namespace_id)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error).with_commit_id(&commit_id)
        })?;
    // Every put in the request shares one preparation pass: a proof belongs
    // to the content, not to the operation that names it.
    let put_content_refs = operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
        .collect::<Vec<_>>();
    // A request that puts nothing skips the preparation pass.
    let put_content_preparation = if put_content_refs.is_empty() {
        None
    } else {
        Some(
            content_preparation_for_puts(
                &state.namespaces,
                &namespace,
                ContentTokenVerifier::new(state.options.content_token_secret.expose()),
                &namespace_id,
                &put_content_refs,
                &content_tokens,
                state.runtime.now_ms(),
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
                CommitCandidate::with_inline_content(request, Vec::new(), inline_content)
                    .reject_content_preparation(ContentPreparationError::ContentToken(rejections))
            }
        };
        namespace.commit_candidate(candidate).await
    } else {
        namespace.commit(request).await
    };
    let response = response_result.map_err(|error| {
        ApiResponseError::runtime_for_namespace_writer(&state.namespaces, &namespace_id, error)
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
            ("after_seq" = loonfs_types::ChangeSeq, Query, description = "The sequence after which to return committed changes."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("snapshot_id" = Option<loonfs_types::PinId>, Query, description = "A snapshot to read the feed from. The feed ends at the sequence that the snapshot captured."),
            ("consistency" = inline(Option<OpenApiConsistency>), Query, description = "How recently the read confirms the namespace head. `eventual` (the default, unless the server's `consistency` setting is `strong`) reads at the head that the server confirmed last while that confirmation is younger than the maximum staleness, and `strong` confirms the head before the read. It can't be combined with `snapshot_id`."),
            ("max_staleness_ms" = inline(Option<OpenApiMaxStalenessMs>), Query, description = "With `consistency=eventual`, how old the server's last confirmation of the head can be, in milliseconds. The default is the server's `max_staleness_ms`.")
        ),
        responses(
            (status = 200, description = "Committed changes", body = ListChangesResponse),
            (status = 400, description = "Invalid change cursor, limit, snapshot ID, consistency, non-snapshot checkpoint, or after_seq above the head that the request reads", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or snapshot not found", body = ErrorResponse),
            (status = 409, description = "The cursor is older than the retained change history. Restart from a fresh listing or checkpoint.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ChangesQuery>,
) -> Result<Json<ListChangesResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let after_seq = parse_after_seq(&required_query_param(query.after_seq, "after_seq")?)?;
    let limit = effective_page_limit(query.limit)?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let consistency = parse_consistency(
        query.consistency.as_deref(),
        query.max_staleness_ms.as_deref(),
        snapshot_id.is_some(),
        state.options.consistency,
    )?;
    let target = read_target(runtime.namespace(&namespace_id), snapshot_id, "snapshot_id").await?;
    let response = target
        .list_changes(after_seq, consistency)
        .page(PageRequest {
            limit,
            cursor: None,
        })
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

pub(super) async fn read_target(
    namespace: Namespace<ReadOnly>,
    snapshot_id: Option<PinId>,
    snapshot_param: &str,
) -> Result<ReadTarget, ApiResponseError> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(ReadTarget::Live(namespace));
    };
    namespace
        .read_view_at_snapshot(&snapshot_id)
        .await
        .map(|view| ReadTarget::Snapshot(Box::new(view)))
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(namespace.id(), error)
                .with_invalid_request_param(snapshot_param)
        })
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

pub(super) fn parse_optional_snapshot_id(
    snapshot_id: Option<String>,
) -> Result<Option<PinId>, ApiResponseError> {
    snapshot_id
        .as_deref()
        .map(super::query_params::parse_snapshot_id)
        .transpose()
}

fn parse_after_seq(value: &str) -> Result<loonfs_types::ChangeSeq, ApiResponseError> {
    parse_public_ordinal("after_seq", value, loonfs_types::ChangeSeq::parse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn after_seq_parser_accepts_the_public_maximum_and_rejects_the_next_value() {
        assert!(matches!(
            parse_after_seq("9007199254740991"),
            Ok(loonfs_types::ChangeSeq(loonfs_types::MAX_PUBLIC_INTEGER))
        ));
        parse_after_seq("9007199254740992")
            .expect_err("the value after the public maximum should be rejected");
    }
}
