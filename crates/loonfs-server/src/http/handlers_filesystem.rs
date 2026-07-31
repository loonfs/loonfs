//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, and the committed-change
//! feed.

use super::error::ApiResponseError;
use super::handlers_uploads::{
    content_preparation_for_puts, current_unix_ms, PutContentPreparation,
};
use super::{authorize, AppJson, AppQuery, AppState, NamespaceIdPath};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use loonfs::publish::{
    ensure_mutation_path, CommitCandidate, CommitRequest, ContentPreparationError,
    FilesystemOperation as CommitOperation,
};
use loonfs::{payload_class, ErrorCode, ListChangesOptions, TraceStoreKind};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
// The wire request and the core planner's request share one name across two
// crates because they are the same language; the alias keeps both readable
// in the handler that maps one onto the other.
use loonfs_api::{
    decode_cursor,
    v0::{ChangesResponse, CommitResponse as ApiCommitResponse},
    AbsolutePath, CommitRequest as ApiCommitRequest, DirectoryPageCursor, FileRevisionsPageCursor,
    FilesystemOperation, LimitError, ListFileRevisionsResponse, ListTrashResponse, PageCursorError,
    PageRequest, PaginationPolicy, RevisionNo,
};
use tracing::Instrument;

#[derive(Debug, serde::Deserialize)]
pub(super) struct PathQuery {
    path: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PathPageQuery {
    path: String,
    limit: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PageQuery {
    limit: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ContentQuery {
    path: String,
    revision_no: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ChangesQuery {
    after_seq: u64,
    limit: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/filesystem/list",
        tag = "filesystem",
        summary = "List directory",
        description = "Lists a directory at the current namespace head.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path"),
            ("limit" = Option<String>, Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque directory-list page cursor")
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_api::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PathPageQuery>,
) -> Result<Json<loonfs_api::ListPathEntriesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    let listing = state
        .reader
        .list_path_entries_page(
            &namespace_id,
            &path,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor: decode_directory_page_cursor(query.cursor)?,
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/filesystem/stat",
        tag = "filesystem",
        summary = "Stat path",
        description = "Returns the current metadata for a path, including inode identity, kind, display name, and file content metadata.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path")
        ),
        responses(
            (status = 200, description = "Authoritative path entry", body = loonfs_api::AuthoritativePathEntry),
            (status = 400, description = "Invalid path", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn stat_path(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PathQuery>,
) -> Result<Json<loonfs_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    let entry = state
        .reader
        .stat_path(&namespace_id, &path)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(entry))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/filesystem/content",
        tag = "filesystem",
        summary = "Read file",
        description = "Returns file bytes for the current revision at a path, or for a specific retained revision when `revision_no` is provided.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute file path"),
            ("revision_no" = Option<String>, Query, description = "Optional prior revision number")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid path or revision", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, or revision not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.max_content_bytes` limit", body = ApiError),
            (status = 503, description = "The server is at its concurrent content-read limit; retry shortly", body = ApiError)
        )
    )
)]
pub(super) async fn get_file_bytes(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    // Held for the read below: content reads buffer the whole file, so the
    // permit is what bounds how many such buffers exist at once.
    let _permit = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| super::server_busy_error("proxied content reads"))?;
    let path = query.path;
    let revision_no = query
        .revision_no
        .as_deref()
        .map(parse_revision_no)
        .transpose()?;
    let file = match revision_no {
        Some(revision_no) => {
            state
                .reader
                .get_file_revision_bytes(&namespace_id, &path, revision_no)
                .await
        }
        None => state.reader.get_file_bytes(&namespace_id, &path).await,
    }
    .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok((StatusCode::OK, file.bytes).into_response())
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/filesystem/trash",
        tag = "filesystem",
        summary = "List recoverable deletions",
        description = "Returns the namespace's recoverable deletions, oldest deletion first. Entries never age out at the retention floor; each carries the inode id and deletion sequence undelete needs, plus the deleted name when the delete recorded one.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("limit" = Option<String>, Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque trash page cursor")
        ),
        responses(
            (status = 200, description = "Recoverable deletions", body = ListTrashResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn list_trash(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let response = state
        .reader
        .list_trash_page(
            &namespace_id,
            PageRequest {
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
        path = "/v0/namespaces/{namespace}/filesystem/revisions",
        tag = "filesystem",
        summary = "List file revisions",
        description = "Resolves the current path to a file inode and returns revisions for that file. If the file could be renamed, use the inode revision API for stable identity.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute file path"),
            ("limit" = Option<String>, Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque file-revisions page cursor")
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid path", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn list_file_revisions(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    let response = state
        .reader
        .list_file_revisions_page(
            &namespace_id,
            &path,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor: decode_file_revisions_page_cursor(query.cursor)?,
            },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/commits",
        tag = "filesystem",
        summary = "Apply a commit",
        description = "Applies one commit: an ordered, non-empty list of path operations that commit together as one logical commit, under one commit id that makes retries idempotent. A single-operation call is the one-element case. The first operation that fails aborts the whole request and names its position in `details.operation_index`.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = ApiCommitRequest,
        responses(
            (status = 200, description = "Commit applied", body = ApiCommitResponse),
            (status = 400, description = "Invalid commit", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 409, description = "Operation conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 503, description = "Commit unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn apply_commit(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AppJson(request): AppJson<ApiCommitRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let ApiCommitRequest {
        commit_id,
        message,
        content_tokens,
        operations,
    } = request;
    // Failed and uncertain outcomes echo the idempotency key the caller can
    // resubmit under (API spec, "Commit responses and safe retry").
    let commit_id_for_errors = commit_id.clone();
    // Every put in the request shares one preparation pass: a proof belongs
    // to the content, not to the operation that names it.
    let put_content_refs = operations
        .iter()
        .filter_map(|operation| match operation {
            FilesystemOperation::PutFile { content_ref, .. } => Some(content_ref),
            _ => None,
        })
        .collect::<Vec<_>>();
    // A request that puts nothing skips both the preparation pass and the
    // put span; one that puts is classified by the bytes it publishes.
    let put_content_preparation = if put_content_refs.is_empty() {
        None
    } else {
        let put_bytes = put_content_refs
            .iter()
            .map(|content_ref| content_ref.size_bytes)
            .fold(0, u64::saturating_add);
        Some((
            payload_class(usize::try_from(put_bytes).unwrap_or(usize::MAX)),
            content_preparation_for_puts(
                &state.writer,
                &state.config,
                &namespace_id,
                &put_content_refs,
                &content_tokens,
                current_unix_ms()?,
            )
            .await?,
        ))
    };
    // Absolute-path grammar was validated while decoding the wire body. The
    // root remains a valid read path but is not a legal mutation target.
    let validate_path = |path: AbsolutePath| {
        ensure_mutation_path(&path).map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error)
                .with_commit_id(&commit_id_for_errors)
        })?;
        Ok(path)
    };
    let operations = operations
        .into_iter()
        .map(|operation| {
            Ok(match operation {
                FilesystemOperation::CreateDirectory { path, parents } => {
                    CommitOperation::CreateDir {
                        absolute_path: validate_path(path)?,
                        parents,
                    }
                }
                FilesystemOperation::PutFile {
                    path,
                    content_ref,
                    behavior,
                    expected_revision_no,
                } => CommitOperation::PutFile {
                    absolute_path: validate_path(path)?,
                    content_ref,
                    behavior,
                    expected_revision_no,
                },
                FilesystemOperation::DeletePath {
                    path,
                    behavior,
                    expected_inode_id,
                } => CommitOperation::DeletePath {
                    absolute_path: validate_path(path)?,
                    behavior,
                    expected_inode_id,
                },
                FilesystemOperation::MovePath {
                    from_path,
                    to_path,
                    behavior,
                } => CommitOperation::MovePath {
                    from_path: validate_path(from_path)?,
                    to_path: validate_path(to_path)?,
                    behavior,
                },
                FilesystemOperation::CopyPath {
                    from_path,
                    to_path,
                    behavior,
                } => CommitOperation::CopyFilePath {
                    from_path: validate_path(from_path)?,
                    to_path: validate_path(to_path)?,
                    behavior,
                },
                FilesystemOperation::RestoreRevision {
                    path,
                    source_revision_no,
                } => CommitOperation::RestoreRevision {
                    absolute_path: validate_path(path)?,
                    source_revision_no,
                },
                FilesystemOperation::Undelete {
                    inode_id,
                    deleted_at_seq,
                    path,
                } => CommitOperation::Undelete {
                    inode_id,
                    deleted_at_seq,
                    absolute_path: validate_path(path)?,
                },
            })
        })
        .collect::<Result<Vec<_>, ApiResponseError>>()?;
    let request = CommitRequest {
        commit_id,
        message,
        operations,
    };
    let response_result = if let Some((payload_class, preparation)) = put_content_preparation {
        let span = tracing::info_span!(
            "loonfs.put",
            operation = "put",
            mode = "remote",
            store_kind = TraceStoreKind::from(state.config.store.kind()).as_str(),
            payload_class,
        );
        async {
            let candidate = match preparation {
                PutContentPreparation::Absent => CommitCandidate::new(request),
                PutContentPreparation::Ready(prepared_content) => {
                    CommitCandidate::prepared(request, prepared_content)
                }
                PutContentPreparation::Rejected(error) => {
                    CommitCandidate::rejected(request, ContentPreparationError::ContentToken(error))
                }
            };
            state
                .writer
                .publisher()
                .submit_candidate(namespace_id.clone(), candidate)
                .await
        }
        .instrument(span)
        .await
    } else {
        state
            .writer
            .publisher()
            .submit_commit(namespace_id.clone(), request)
            .await
    };
    let response = response_result.map_err(|error| {
        ApiResponseError::core_for_namespace(&namespace_id, error)
            .with_commit_id(&commit_id_for_errors)
    })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/changes",
        tag = "filesystem",
        summary = "List changes after a sequence",
        description = "Returns committed changes from the write-ahead log. Callers can use this feed to keep another projection synchronized with WAL history.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("after_seq" = u64, Query, description = "Return committed changes after this sequence"),
            ("limit" = Option<String>, Query, description = "Maximum page size")
        ),
        responses(
            (status = 200, description = "Committed changes", body = ChangesResponse),
            (status = 400, description = "Invalid change cursor or limit", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
    let after_seq = loonfs_api::ChangeSeq(query.after_seq);
    let limit = resolve_page_limit(query.limit)?;
    let response = state
        .reader
        .list_changes(
            &namespace_id,
            after_seq,
            ListChangesOptions { limit: Some(limit) },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

fn parse_revision_no(value: &str) -> Result<RevisionNo, ApiResponseError> {
    value.parse::<u64>().map(RevisionNo).map_err(|err| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid revision_no `{value}`: {err}"),
        )
    })
}

fn resolve_page_limit(
    limit: Option<String>,
) -> Result<loonfs_api::EffectiveLimit, ApiResponseError> {
    let requested = limit.as_deref().map(parse_page_limit).transpose()?;
    PaginationPolicy::default()
        .resolve_limit(requested)
        .map_err(limit_response_error)
}

fn parse_page_limit(value: &str) -> Result<u32, ApiResponseError> {
    value.parse::<u32>().map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid limit `{value}`: {error}"),
        )
    })
}

fn decode_directory_page_cursor(
    cursor: Option<String>,
) -> Result<Option<DirectoryPageCursor>, ApiResponseError> {
    cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(page_cursor_response_error)
}

fn decode_optional_cursor<C: loonfs_api::PageCursor>(
    cursor: Option<String>,
) -> Result<Option<C>, ApiResponseError> {
    cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(page_cursor_response_error)
}

fn decode_file_revisions_page_cursor(
    cursor: Option<String>,
) -> Result<Option<FileRevisionsPageCursor>, ApiResponseError> {
    cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(page_cursor_response_error)
}

fn limit_response_error(error: LimitError) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidRequest,
        &error.to_string(),
    )
}

fn page_cursor_response_error(error: PageCursorError) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidRequest,
        &error.to_string(),
    )
}
