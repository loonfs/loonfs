//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, and the committed-change
//! feed.

use super::error::ApiResponseError;
use super::handlers_uploads::{
    content_preparation_for_puts, current_unix_ms, ContentTokenVerifier, PutContentPreparation,
};
use super::{authorize, AppJson, AppQuery, AppState, NamespaceIdPath};
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::Stream;
use loonfs::publish::{CommitCandidate, CommitRequest, ContentPreparationError};
use loonfs::{
    payload_class, ErrorCode, ListChangesOptions, ListPathEntriesOptions, StatPathOptions,
    TraceStoreKind,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
// The wire commit request and the runtime's differ only by the content
// tokens, which this handler resolves and strips; the operations inside them
// are one type. The alias keeps the two request names readable side by side.
use loonfs_api::{
    decode_cursor,
    v0::{ChangesResponse, CommitResponse as ApiCommitResponse},
    CommitRequest as ApiCommitRequest, FilesystemOperation, LimitError, ListFileRevisionsResponse,
    ListTrashResponse, PageCursorError, PageRequest, PaginationPolicy, PublicOrdinalRangeError,
    RevisionNo,
};
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument;

#[derive(Debug, serde::Deserialize)]
pub(super) struct PathQuery {
    path: String,
    include_attributes: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PathPageQuery {
    path: String,
    limit: Option<String>,
    cursor: Option<String>,
}

/// The directory listing's query. It repeats [`PathPageQuery`]'s fields
/// rather than composing them because the query extractor deserializes with
/// `serde_urlencoded`, which does not support `#[serde(flatten)]`.
#[derive(Debug, serde::Deserialize)]
pub(super) struct ListPathPageQuery {
    path: String,
    limit: Option<String>,
    cursor: Option<String>,
    include_attributes: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PageQuery {
    pub(super) limit: Option<String>,
    pub(super) cursor: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ContentQuery {
    path: String,
    revision_no: Option<String>,
}

/// One materialized download plus the permit accounting for its memory.
///
/// The stream owns the permit even after yielding its only chunk, so the
/// response body releases admission only when it is fully consumed or
/// abandoned and dropped.
struct DownloadBodyStream {
    bytes: Option<bytes::Bytes>,
    _permit: OwnedSemaphorePermit,
}

pub(super) fn buffered_download_response(bytes: Vec<u8>, permit: OwnedSemaphorePermit) -> Response {
    let body = Body::from_stream(DownloadBodyStream {
        bytes: Some(bytes.into()),
        _permit: permit,
    });
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

impl Stream for DownloadBodyStream {
    type Item = Result<bytes::Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.bytes.take().map(Ok))
    }
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ChangesQuery {
    after_seq: String,
    limit: Option<String>,
}

/// Schema-only override for the shared page-limit query contract.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiPageLimit;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiPageLimit {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Integer)
            .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                utoipa::openapi::KnownFormat::Int32,
            )))
            .minimum(Some(1u32))
            .maximum(Some(loonfs_api::DEFAULT_MAX_PAGE_LIMIT))
            .default(Some(serde_json::json!(loonfs_api::DEFAULT_PAGE_LIMIT)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiPageLimit {}

/// Schema-only override for an optional boolean that defaults to true.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiDefaultTrueBoolean;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiDefaultTrueBoolean {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Boolean)
            .default(Some(serde_json::json!(true)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiDefaultTrueBoolean {}

/// Schema-only override for an optional boolean that defaults to false.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiDefaultFalseBoolean;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiDefaultFalseBoolean {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Boolean)
            .default(Some(serde_json::json!(false)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiDefaultFalseBoolean {}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/filesystem/list",
        tag = "filesystem",
        summary = "List directory",
        description = "Lists a directory at the current namespace head.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque directory-list page cursor"),
            ("include_attributes" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Project each entry's attribute map and revision (`true` or `false`). Defaults to `false`: a page holds many entries and each map may be 64 KiB, so a listing does not carry them unless asked.")
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_api::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, cursor, or include_attributes", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<ListPathPageQuery>,
) -> Result<Json<loonfs_api::ListPathEntriesResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    // An absent parameter leaves the option type's own default in place, so
    // the HTTP surface and the in-process one cannot answer differently.
    let mut options = ListPathEntriesOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let listing = state
        .reader
        .list_path_entries_page(
            &namespace_id,
            &path,
            PageRequest {
                limit: resolve_page_limit(query.limit)?,
                cursor: decode_optional_cursor(query.cursor)?,
            },
            options,
        )
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(listing))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/filesystem/stat",
        tag = "filesystem",
        summary = "Stat path",
        description = "Returns the current metadata for a path, including inode identity, kind, display name, file content metadata, and the inode's attributes.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path"),
            ("include_attributes" = inline(Option<OpenApiDefaultTrueBoolean>), Query, description = "Project the inode's attribute map and revision (`true` or `false`). Defaults to `true`: a stat answers for one path and a map is capped at 64 KiB.")
        ),
        responses(
            (status = 200, description = "Authoritative path entry", body = loonfs_api::AuthoritativePathEntry),
            (status = 400, description = "Invalid path or include_attributes", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn stat_path(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PathQuery>,
) -> Result<Json<loonfs_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    let mut options = StatPathOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let entry = state
        .reader
        .stat_path(&namespace_id, &path, options)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(entry))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/filesystem/content",
        tag = "filesystem",
        summary = "Read file",
        description = "Returns file bytes for the current revision at a path, or for a specific retained revision when `revision_no` is provided.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute file path"),
            ("revision_no" = Option<RevisionNo>, Query, description = "Optional prior revision number")
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
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    // Content reads buffer the whole file, so the permit must follow those
    // bytes through the response body rather than ending with this handler.
    let permit = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            state.metrics.download_rejected_as_busy();
            super::server_busy_error("proxied content reads")
        })?;
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
    Ok(buffered_download_response(file.bytes, permit))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/filesystem/trash",
        tag = "filesystem",
        summary = "List recoverable deletions",
        description = "Returns the namespace's recoverable deletions, oldest deletion first. Entries never age out at the retention floor; each carries the inode id and deletion sequence undelete needs, plus the deleted name when the delete recorded one.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque trash page cursor")
        ),
        responses(
            (status = 200, description = "Recoverable deletions", body = ListTrashResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_trash(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
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
        path = "/v0/namespaces/{namespace_id}/filesystem/revisions",
        tag = "filesystem",
        summary = "List file revisions",
        description = "Resolves the current path to a file inode and returns revisions for that file. If the file could be renamed, use the inode revision API for stable identity.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute file path"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque file-revisions page cursor")
        ),
        responses(
            (status = 200, description = "File revisions", body = ListFileRevisionsResponse),
            (status = 400, description = "Invalid path", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_file_revisions(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let path = query.path;
    let response = state
        .reader
        .list_file_revisions_page(
            &namespace_id,
            &path,
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
        post,
        path = "/v0/namespaces/{namespace_id}/commits",
        tag = "filesystem",
        summary = "Apply a commit",
        description = "Applies one commit: an ordered, non-empty list of path operations that commit together as one logical commit, under one commit id that makes retries idempotent. A single-operation call is the one-element case. The first operation that fails aborts the whole request, and a request carrying more than one operation names that operation's position in `details.operation_index`.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body = ApiCommitRequest,
        responses(
            (status = 200, description = "Commit applied", body = ApiCommitResponse),
            (status = 400, description = "Invalid commit", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or path not found", body = ApiError),
            (status = 409, description = "Operation conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 503, description = "Commit unavailable", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
/// The server stores the actor from the request; the shared token does not verify it.
pub(super) async fn apply_commit(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    AppJson(request): AppJson<ApiCommitRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    let ApiCommitRequest {
        commit_id,
        actor,
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
                ContentTokenVerifier::new(state.config.content_token_secret()),
                &namespace_id,
                &put_content_refs,
                &content_tokens,
                current_unix_ms()?,
            )
            .await?,
        ))
    };
    let request = CommitRequest {
        commit_id,
        actor,
        message,
        operations,
    };
    let response_result = if let Some((payload_class, preparation)) = put_content_preparation {
        let span = tracing::debug_span!(
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
                PutContentPreparation::Rejected(rejections) => CommitCandidate::rejected(
                    request,
                    ContentPreparationError::ContentToken(rejections),
                ),
            };
            state
                .writer
                .commit_candidate(&namespace_id, candidate)
                .await
        }
        .instrument(span)
        .await
    } else {
        state.writer.commit(&namespace_id, request).await
    };
    let response = response_result.map_err(|error| {
        ApiResponseError::runtime_for_namespace(&namespace_id, error)
            .with_commit_id(&commit_id_for_errors)
    })?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace_id}/changes",
        tag = "filesystem",
        summary = "List changes after a sequence",
        description = "Returns committed changes from the write-ahead log. Callers can use this feed to keep another projection synchronized with WAL history.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("after_seq" = loonfs_api::ChangeSeq, Query, description = "Return committed changes after this sequence"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size")
        ),
        responses(
            (status = 200, description = "Committed changes", body = ChangesResponse),
            (status = 400, description = "Invalid change cursor or limit", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let query = query.into_params()?;
    let after_seq = parse_after_seq(&query.after_seq)?;
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

fn parse_after_seq(value: &str) -> Result<loonfs_api::ChangeSeq, ApiResponseError> {
    parse_public_ordinal("after_seq", value, loonfs_api::ChangeSeq::parse)
}

pub(super) fn parse_include_attributes(value: &str) -> Result<bool, ApiResponseError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid include_attributes `{other}`: expected `true` or `false`"),
        )),
    }
}

pub(super) fn parse_revision_no(value: &str) -> Result<RevisionNo, ApiResponseError> {
    parse_public_ordinal("revision_no", value, RevisionNo::parse)
}

pub(super) fn parse_public_ordinal<T>(
    name: &str,
    value: &str,
    constructor: impl FnOnce(u64) -> Result<T, PublicOrdinalRangeError>,
) -> Result<T, ApiResponseError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| public_ordinal_response_error(name, value, PublicOrdinalRangeError))?;
    constructor(parsed).map_err(|error| public_ordinal_response_error(name, value, error))
}

fn public_ordinal_response_error(
    name: &str,
    value: &str,
    error: PublicOrdinalRangeError,
) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidRequest,
        &format!("invalid {name} `{value}`: {error}"),
    )
}

pub(super) fn resolve_page_limit(
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

pub(super) fn decode_optional_cursor<C: loonfs_api::PageCursor>(
    cursor: Option<String>,
) -> Result<Option<C>, ApiResponseError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn after_seq_parser_accepts_the_public_maximum_and_rejects_the_next_value() {
        assert!(matches!(
            parse_after_seq("9007199254740991"),
            Ok(loonfs_api::ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER))
        ));
        assert!(parse_after_seq("9007199254740992").is_err());
    }
}
