//! Path- and inode-oriented filesystem handlers: directory listing, stat,
//! content reads, revision listings and restore, filesystem mutations,
//! semantic commits, and the committed-change feed.

use super::error::ApiResponseError;
use super::handlers_uploads::{content_admissions_for_put, current_unix_ms};
use super::{authorize, AppJson, AppState, CommitAppJson, NamespaceIdPath};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use loonfs::publish::{parse_mutation_path, PathMutationIntent};
use loonfs::{payload_class, ErrorCode, ListChangesOptions, TraceStoreKind};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    decode_directory_cursor, decode_file_revisions_cursor,
    v0::{ChangesResponse, CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse},
    DirectoryPageCursor, FileRevisionsPageCursor, FilesystemOperation, FilesystemOperationRequest,
    InodeId, LimitError, ListFileRevisionsResponse, PageCursorError, PageRequest, PaginationPolicy,
    RestoreFileRevisionRequest, RevisionNo,
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

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodePathParams {
    inode_id: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodeRevisionPathParams {
    inode_id: String,
    revision_no: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct InodeRestorePathParams {
    inode_id: String,
    source_revision_no: String,
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
pub(super) async fn list_entries(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    Query(query): Query<PathPageQuery>,
) -> Result<Json<loonfs_api::ListPathEntriesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
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
pub(super) async fn stat_entry(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<loonfs_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
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
pub(super) async fn get_content(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    Query(query): Query<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
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
                .read_file_revision_bytes(&namespace_id, &path, revision_no)
                .await
        }
        None => state.reader.read_file_bytes(&namespace_id, &path).await,
    }
    .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok((StatusCode::OK, file.bytes).into_response())
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
pub(super) async fn list_path_revisions(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    Query(query): Query<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
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
        get,
        path = "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions",
        tag = "inodes",
        summary = "List inode revisions",
        description = "Returns revisions for a file inode, independent of the file's current path or later renames.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode id"),
            ("limit" = Option<String>, Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque file-revisions page cursor")
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid inode id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or inode not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn list_inode_revisions(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AxumPath(InodePathParams { inode_id }): AxumPath<InodePathParams>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let inode_id = parse_inode_id(&inode_id)?;
    let response = state
        .reader
        .list_file_revisions_for_inode_page(
            &namespace_id,
            inode_id,
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
        get,
        path = "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions/{revision_no}/content",
        tag = "inodes",
        summary = "Read inode revision",
        description = "Returns bytes for one retained file revision by inode id and revision number.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode id"),
            ("revision_no" = String, Path, description = "File revision number")
        ),
        responses(
            (status = 200, description = "Revision bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid inode id or revision", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.max_content_bytes` limit", body = ApiError),
            (status = 503, description = "The server is at its concurrent content-read limit; retry shortly", body = ApiError)
        )
    )
)]
pub(super) async fn get_inode_revision_content(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AxumPath(InodeRevisionPathParams {
        inode_id,
        revision_no,
    }): AxumPath<InodeRevisionPathParams>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let _permit = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| super::server_busy_error("proxied content reads"))?;
    let inode_id = parse_inode_id(&inode_id)?;
    let revision_no = parse_revision_no(&revision_no)?;
    let bytes = state
        .reader
        .read_file_revision_bytes_for_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok((StatusCode::OK, bytes).into_response())
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions/{source_revision_no}/restore",
        tag = "inodes",
        summary = "Restore inode revision",
        description = "Appends a new current revision to a file inode using bytes from an older retained revision. The request includes the caller's expected current revision for race safety.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode id"),
            ("source_revision_no" = String, Path, description = "Revision number to restore")
        ),
        request_body = RestoreFileRevisionRequest,
        responses(
            (status = 200, description = "Restore commit response", body = ApiCommitResponse),
            (status = 400, description = "Invalid inode id, revision, or request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 409, description = "Commit conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn restore_inode_revision(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AxumPath(InodeRestorePathParams {
        inode_id,
        source_revision_no,
    }): AxumPath<InodeRestorePathParams>,
    headers: HeaderMap,
    AppJson(request): AppJson<RestoreFileRevisionRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let inode_id = parse_inode_id(&inode_id)?;
    let source_revision_no = parse_revision_no(&source_revision_no)?;
    let commit = ApiCommitRequest {
        commit_id: request.commit_id,
        preconditions: vec![loonfs_api::v0::CommitPrecondition::InodeRevisionIs {
            inode_id,
            revision_no: request.base_revision_no,
        }],
        ops: vec![loonfs_api::v0::CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no: request.base_revision_no,
        }],
        message: None,
    };
    let commit_id = commit.commit_id.clone();
    let response = state
        .publisher
        .submit_commit(namespace_id.clone(), commit)
        .await
        .map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error).with_commit_id(&commit_id)
        })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/filesystem/operations",
        tag = "filesystem",
        summary = "Run filesystem operation",
        description = "Runs one path-oriented filesystem mutation, such as create directory, put file, move, copy, delete, or restore revision. The commit id makes retries idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = FilesystemOperationRequest,
        responses(
            (status = 200, description = "Filesystem operation committed", body = ApiCommitResponse),
            (status = 400, description = "Invalid operation", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 409, description = "Operation conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 503, description = "Commit unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn filesystem_operation(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    AppJson(request): AppJson<FilesystemOperationRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let FilesystemOperationRequest {
        commit_id,
        content_tokens,
        operation,
    } = request;
    // Failed and uncertain outcomes echo the idempotency key the caller can
    // resubmit under (API spec, "Mutation responses and safe retry").
    let commit_id_for_errors = commit_id.clone();
    let put_payload_class = match &operation {
        FilesystemOperation::PutFile { content_ref, .. } => Some(payload_class(
            usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
        )),
        _ => None,
    };
    let content_admissions = match &operation {
        FilesystemOperation::PutFile { content_ref, .. } => content_admissions_for_put(
            &state.config,
            &namespace_id,
            content_ref,
            &content_tokens,
            current_unix_ms()?,
        ),
        _ => Vec::new(),
    };
    // Wire paths are raw strings; the intent carries validated paths, so
    // this is the convert-once point for the whole remote mutation path.
    let parse_path = |path: &str| {
        parse_mutation_path(path).map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error)
                .with_commit_id(&commit_id_for_errors)
        })
    };
    let intent = match operation {
        FilesystemOperation::CreateDirectory { path } => PathMutationIntent::CreateDir {
            commit_id,
            absolute_path: parse_path(&path)?,
        },
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
        } => PathMutationIntent::PutFile {
            commit_id,
            absolute_path: parse_path(&path)?,
            content_ref,
            behavior,
        },
        FilesystemOperation::DeletePath { path, behavior } => PathMutationIntent::DeletePath {
            commit_id,
            absolute_path: parse_path(&path)?,
            behavior,
        },
        FilesystemOperation::MovePath {
            from_path,
            to_path,
            behavior,
        } => PathMutationIntent::MovePath {
            commit_id,
            from_path: parse_path(&from_path)?,
            to_path: parse_path(&to_path)?,
            behavior,
        },
        FilesystemOperation::CopyPath {
            from_path,
            to_path,
            behavior,
        } => PathMutationIntent::CopyFilePath {
            commit_id,
            from_path: parse_path(&from_path)?,
            to_path: parse_path(&to_path)?,
            behavior,
        },
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => PathMutationIntent::RestoreRevision {
            commit_id,
            absolute_path: parse_path(&path)?,
            source_revision_no,
        },
    };
    let response_result = if let Some(payload_class) = put_payload_class {
        let span = tracing::info_span!(
            "loon.put",
            operation = "put",
            mode = "remote",
            store_kind = TraceStoreKind::from(state.config.store.kind()).as_str(),
            payload_class,
        );
        async {
            if content_admissions.is_empty() {
                state
                    .publisher
                    .submit_path_intent(namespace_id.clone(), intent)
                    .await
            } else {
                state
                    .publisher
                    .submit_path_intent_with_content_admission(
                        namespace_id.clone(),
                        intent,
                        content_admissions,
                    )
                    .await
            }
        }
        .instrument(span)
        .await
    } else {
        state
            .publisher
            .submit_path_intent(namespace_id.clone(), intent)
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
        post,
        path = "/v0/namespaces/{namespace}/commits",
        tag = "commits",
        summary = "Commit operations",
        description = "Applies an explicit semantic commit containing ordered inode-level operations and optional preconditions. Use this for advanced cases that require inode-specificity, preconditions, or batch transactions.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = ApiCommitRequest,
        responses(
            (status = 200, description = "Commit accepted", body = ApiCommitResponse),
            (status = 400, description = "Invalid commit request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Commit conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Commit body exceeds the advertised `commit.max_body_bytes` limit", body = ApiError),
            (status = 503, description = "Commit unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn commit_operations(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    CommitAppJson(request): CommitAppJson<ApiCommitRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let commit_id = request.commit_id.clone();
    let response = state
        .publisher
        .submit_commit(namespace_id.clone(), request)
        .await
        .map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error).with_commit_id(&commit_id)
        })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/changes",
        tag = "commits",
        summary = "List changes",
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
    Query(query): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let after_seq = loonfs_api::ChangeSeq(query.after_seq);
    let limit = resolve_page_limit(query.limit)?;
    let response = state
        .reader
        .list_changes_after(
            &namespace_id,
            after_seq,
            ListChangesOptions { limit: Some(limit) },
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

fn parse_inode_id(value: &str) -> Result<InodeId, ApiResponseError> {
    value.parse::<u64>().map(InodeId).map_err(|err| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid inode_id `{value}`: {err}"),
        )
    })
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
        .map(decode_directory_cursor)
        .transpose()
        .map_err(page_cursor_response_error)
}

fn decode_file_revisions_page_cursor(
    cursor: Option<String>,
) -> Result<Option<FileRevisionsPageCursor>, ApiResponseError> {
    cursor
        .as_deref()
        .map(decode_file_revisions_cursor)
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
