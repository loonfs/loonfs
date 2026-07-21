//! Path- and inode-oriented filesystem handlers: directory listing, stat,
//! content reads, revision listings and restore, filesystem mutations,
//! semantic commits, and the committed-change feed.

use super::error::ApiResponseError;
use super::handlers_uploads::{
    content_preparation_for_put, current_unix_ms, PutContentPreparation,
};
use super::{authorize, AppJson, AppPath, AppQuery, AppState, CommitAppJson, NamespaceIdPath};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use loonfs::publish::{
    parse_mutation_path, ContentPreparationError, NamespaceMutation, NamespaceMutationCandidate,
    PathMutationIntent, PreparedContent, MAX_COMMIT_CONTENT_TOKENS,
    MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_OPERATIONS,
};
use loonfs::{
    content_tokens::{verify_content_token, ContentTokenError},
    payload_class, CoreError, ErrorCode, ListChangesOptions, TraceStoreKind,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    decode_directory_cursor, decode_file_revisions_cursor,
    v0::{
        ChangesResponse, CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CommitSubmissionRequest, ValidatedContentToken,
    },
    ContentRef, DirectoryPageCursor, FileRevisionsPageCursor, FilesystemOperation,
    FilesystemOperationRequest, InodeId, LimitError, ListFileRevisionsResponse, PageCursorError,
    PageRequest, PaginationPolicy, RestoreFileRevisionRequest, RevisionNo,
};
use std::collections::HashSet;
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
pub(super) async fn stat_entry(
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
pub(super) async fn get_content(
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
    path: AppPath<InodePathParams>,
    headers: HeaderMap,
    query: AppQuery<PageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let InodePathParams { inode_id } = path.into_params()?;
    let query = query.into_params()?;
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
    path: AppPath<InodeRevisionPathParams>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let InodeRevisionPathParams {
        inode_id,
        revision_no,
    } = path.into_params()?;
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
    path: AppPath<InodeRestorePathParams>,
    headers: HeaderMap,
    AppJson(request): AppJson<RestoreFileRevisionRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let InodeRestorePathParams {
        inode_id,
        source_revision_no,
    } = path.into_params()?;
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
    let put_content_preparation = match &operation {
        FilesystemOperation::PutFile { content_ref, .. } => Some(content_preparation_for_put(
            &state.config,
            &namespace_id,
            content_ref,
            &content_tokens,
            current_unix_ms()?,
        )),
        _ => None,
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
        FilesystemOperation::CreateDirectory { path, parents } => PathMutationIntent::CreateDir {
            commit_id,
            absolute_path: parse_path(&path)?,
            parents,
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
        FilesystemOperation::DeletePath {
            path,
            behavior,
            expected_inode_id,
        } => PathMutationIntent::DeletePath {
            commit_id,
            absolute_path: parse_path(&path)?,
            behavior,
            expected_inode_id,
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
        FilesystemOperation::Undelete {
            inode_id,
            deleted_at_seq,
            path,
        } => PathMutationIntent::Undelete {
            commit_id,
            inode_id,
            deleted_at_seq,
            absolute_path: parse_path(&path)?,
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
            let candidate = match put_content_preparation
                .expect("put payload class should carry content preparation")
            {
                PutContentPreparation::Absent => NamespaceMutationCandidate::path(intent),
                PutContentPreparation::Ready(prepared_content) => {
                    NamespaceMutationCandidate::path_prepared(intent, prepared_content)
                }
                PutContentPreparation::Rejected(error) => NamespaceMutationCandidate::rejected(
                    NamespaceMutation::Path(intent),
                    ContentPreparationError::ContentToken(error),
                ),
            };
            state
                .publisher
                .submit_candidate(namespace_id.clone(), candidate)
                .await
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
        request_body = CommitSubmissionRequest,
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
    CommitAppJson(submission): CommitAppJson<CommitSubmissionRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    let CommitSubmissionRequest {
        commit: request,
        content_tokens,
    } = submission;
    let namespace_id = namespace.into_id()?;
    let commit_id = request.commit_id.clone();
    let external_content_refs = distinct_external_content_refs(&request);
    validate_commit_submission_limits(&request, content_tokens.len(), external_content_refs.len())
        .map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error).with_commit_id(&commit_id)
        })?;
    let content_preparation = prepare_commit_content(
        &state.config,
        &namespace_id,
        &external_content_refs,
        &content_tokens,
    )?;
    let candidate = match content_preparation {
        CommitContentPreparation::Ready(content) => {
            NamespaceMutationCandidate::commit_prepared(request, content)
        }
        CommitContentPreparation::Rejected(error) => NamespaceMutationCandidate::rejected(
            NamespaceMutation::Commit(request),
            ContentPreparationError::ContentToken(error),
        ),
    };
    let response = state
        .publisher
        .submit_candidate(namespace_id.clone(), candidate)
        .await
        .map_err(|error| {
            ApiResponseError::core_for_namespace(&namespace_id, error).with_commit_id(&commit_id)
        })?;
    Ok(Json(response))
}

enum CommitContentPreparation {
    Ready(Vec<PreparedContent>),
    Rejected(ContentTokenError),
}

fn distinct_external_content_refs(request: &ApiCommitRequest) -> Vec<ContentRef> {
    let mut seen = HashSet::new();
    let mut refs = Vec::new();
    for content_ref in request.ops.iter().filter_map(|op| match op {
        loonfs_api::v0::CommitOp::CreateFile { content_ref, .. }
        | loonfs_api::v0::CommitOp::ReplaceFile { content_ref, .. } => Some(content_ref),
        _ => None,
    }) {
        if seen.insert(content_ref.clone()) {
            refs.push(content_ref.clone());
        }
    }
    refs
}

fn validate_commit_submission_limits(
    request: &ApiCommitRequest,
    content_token_count: usize,
    external_content_ref_count: usize,
) -> Result<(), CoreError> {
    if request.ops.len() > MAX_COMMIT_OPERATIONS {
        return Err(CoreError::InvalidCommitRequest(format!(
            "commit has {} operations; maximum is {MAX_COMMIT_OPERATIONS}",
            request.ops.len()
        )));
    }
    if content_token_count > MAX_COMMIT_CONTENT_TOKENS {
        return Err(CoreError::InvalidCommitRequest(format!(
            "commit has {content_token_count} content token entries; maximum is {MAX_COMMIT_CONTENT_TOKENS}"
        )));
    }
    if external_content_ref_count > MAX_COMMIT_EXTERNAL_CONTENT_REFS {
        return Err(CoreError::InvalidCommitRequest(format!(
            "commit references {external_content_ref_count} distinct external content refs; maximum is {MAX_COMMIT_EXTERNAL_CONTENT_REFS}"
        )));
    }
    Ok(())
}

fn prepare_commit_content(
    config: &crate::config::ServerConfig,
    namespace_id: &loonfs_api::NamespaceId,
    external_content_refs: &[ContentRef],
    content_tokens: &[ValidatedContentToken],
) -> Result<CommitContentPreparation, ApiResponseError> {
    let relevant_refs = external_content_refs
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let relevant_tokens = content_tokens
        .iter()
        .filter(|token| relevant_refs.contains(&token.content_ref));
    let mut relevant_tokens = relevant_tokens.peekable();
    if relevant_tokens.peek().is_none() {
        return Ok(CommitContentPreparation::Ready(Vec::new()));
    }

    let now_ms = current_unix_ms()?;
    let mut prepared = Vec::new();
    let mut prepared_refs = HashSet::new();
    let mut first_error = None;
    for token in relevant_tokens {
        match verify_content_token(config.content_token_secret(), namespace_id, token, now_ms) {
            Ok(content) => {
                prepared_refs.insert(content.content_ref().clone());
                prepared.push(content);
            }
            Err(error) => {
                tracing::debug!(
                    namespace_id = %namespace_id,
                    content_ref_digest = %token.content_ref.digest,
                    error = %error,
                    "content token rejected during commit preparation"
                );
                first_error.get_or_insert(error);
            }
        }
    }

    if external_content_refs
        .iter()
        .all(|content_ref| prepared_refs.contains(content_ref))
    {
        Ok(CommitContentPreparation::Ready(prepared))
    } else if let Some(error) = first_error {
        Ok(CommitContentPreparation::Rejected(error))
    } else {
        Ok(CommitContentPreparation::Ready(prepared))
    }
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
    query: AppQuery<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let query = query.into_params()?;
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
