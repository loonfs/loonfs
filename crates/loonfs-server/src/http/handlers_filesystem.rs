//! Path-oriented filesystem handlers: directory listing, stat, content
//! reads, revision listings, filesystem mutations, and the committed-change
//! feed.

use super::download_body::buffered_download_response;
use super::error::ApiResponseError;
use super::handlers_uploads::{
    content_preparation_for_puts, current_unix_ms, ContentTokenVerifier, PutContentPreparation,
};
use super::query_params::{
    decode_optional_cursor, parse_include_attributes, parse_path_id, parse_public_ordinal,
    parse_revision_no, required_query_param, resolve_page_limit,
};
#[cfg(feature = "openapi")]
pub(super) use super::query_params::{
    OpenApiDefaultFalseBoolean, OpenApiDefaultTrueBoolean, OpenApiPageLimit,
};
use super::{acquire_download_permit, AppJson, AppQuery, AppState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::response::Response;
use axum::Json;
use loonfs::publish::{CommitCandidate, CommitRequest, ContentPreparationError};
use loonfs::{
    payload_class, CheckpointId, ErrorCode, FsReadSnapshot, FsReader, ListChangesOptions,
    ListPathEntriesOptions, NamespaceId, StatPathOptions, TraceMode, TraceStoreKind,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
// The wire commit request and the runtime's differ only by the content
// tokens, which this handler resolves and strips; the operations inside them
// are one type. The alias keeps the two request names readable side by side.
use loonfs_api::{
    v0::{CommitResponse as ApiCommitResponse, ListChangesResponse},
    CommitRequest as ApiCommitRequest, FilesystemOperation, ListFileRevisionsResponse,
    ListTrashResponse, PageRequest, RevisionNo,
};
use tracing::Instrument;

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PathQuery {
    path: Option<String>,
    include_attributes: Option<String>,
    snapshot_id: Option<String>,
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
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangesQuery {
    after_seq: Option<String>,
    limit: Option<String>,
    snapshot_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotQuery {
    pub(super) snapshot_id: Option<String>,
}

pub(super) enum ReadTarget {
    Snapshot(Box<FsReadSnapshot>),
    Live {
        reader: FsReader,
        namespace_id: NamespaceId,
    },
}

impl ReadTarget {
    pub(super) async fn list_path_entries_page(
        &self,
        path: &str,
        request: PageRequest<loonfs::DirectoryPageCursor>,
        options: ListPathEntriesOptions,
    ) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
        match self {
            Self::Snapshot(snapshot) => {
                snapshot
                    .list_path_entries_page(path, request, options)
                    .await
            }
            Self::Live {
                reader,
                namespace_id,
            } => {
                reader
                    .list_path_entries_page(namespace_id, path, request, options)
                    .await
            }
        }
    }

    pub(super) async fn get_path_entry(
        &self,
        path: &str,
        options: StatPathOptions,
    ) -> loonfs::Result<loonfs::PathEntry> {
        match self {
            Self::Snapshot(snapshot) => snapshot.get_path_entry(path, options).await,
            Self::Live {
                reader,
                namespace_id,
            } => reader.get_path_entry(namespace_id, path, options).await,
        }
    }

    pub(super) async fn get_file_bytes(
        &self,
        path: &str,
        revision_no: Option<RevisionNo>,
    ) -> loonfs::Result<loonfs::FileBytes> {
        match self {
            Self::Snapshot(snapshot) => snapshot.get_file_bytes(path).await,
            Self::Live {
                reader,
                namespace_id,
            } => match revision_no {
                Some(revision_no) => {
                    reader
                        .get_file_revision_bytes(namespace_id, path, revision_no)
                        .await
                }
                None => reader.get_file_bytes(namespace_id, path).await,
            },
        }
    }

    pub(super) async fn create_download(
        &self,
        path: &str,
        revision_no: Option<RevisionNo>,
    ) -> loonfs::Result<loonfs::downloads::DirectDownloadTarget> {
        match self {
            Self::Snapshot(snapshot) => snapshot.create_download(path).await,
            Self::Live {
                reader,
                namespace_id,
            } => {
                reader
                    .create_download(namespace_id, path, revision_no)
                    .await
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
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque directory-list page cursor"),
            ("include_attributes" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Project each entry's attribute map and revision (`true` or `false`). Defaults to `false`: a page holds many entries and each map may be 64 KiB, so a listing does not carry them unless asked."),
            ("snapshot_id" = Option<loonfs_api::CheckpointId>, Query, description = "Use the directory state captured by this snapshot")
        ),
        responses(
            (status = 200, description = "Directory listing page", body = loonfs_api::ListPathEntriesResponse),
            (status = 400, description = "Invalid path, limit, cursor, include_attributes, snapshot id, or non-snapshot checkpoint", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, or snapshot not found", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot released or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_path_entries(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ListPathPageQuery>,
) -> Result<Json<loonfs_api::ListPathEntriesResponse>, ApiResponseError> {
    let path = required_query_param(query.path, "path")?;
    // An absent parameter leaves the option type's own default in place, so
    // the HTTP surface and the in-process one cannot answer differently.
    let mut options = ListPathEntriesOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let request = PageRequest {
        limit: resolve_page_limit(query.limit)?,
        cursor: decode_optional_cursor(query.cursor)?,
    };
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = pin_requested_snapshot(&state, &namespace_id, snapshot_id).await?;
    let listing = target
        .list_path_entries_page(&path, request, options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
    Ok(Json(listing))
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
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute filesystem path"),
            ("include_attributes" = inline(Option<OpenApiDefaultTrueBoolean>), Query, description = "Project the inode's attribute map and revision (`true` or `false`). Defaults to `true`: a stat answers for one path and a map is capped at 64 KiB."),
            ("snapshot_id" = Option<loonfs_api::CheckpointId>, Query, description = "Use the path state captured by this snapshot")
        ),
        responses(
            (status = 200, description = "Authoritative path entry", body = loonfs_api::PathEntry),
            (status = 400, description = "Invalid path, include_attributes, snapshot id, or non-snapshot checkpoint", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, or snapshot not found", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot released or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_path_entry(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathQuery>,
) -> Result<Json<loonfs_api::PathEntry>, ApiResponseError> {
    let path = required_query_param(query.path, "path")?;
    let mut options = StatPathOptions::default();
    if let Some(value) = query.include_attributes.as_deref() {
        options.include_attributes = parse_include_attributes(value)?;
    }
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = pin_requested_snapshot(&state, &namespace_id, snapshot_id).await?;
    let entry = target
        .get_path_entry(&path, options)
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
        operation_id = "get_file_bytes",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/content",
        tag = "filesystem",
        summary = "Read file",
        description = "Returns the current file bytes, a retained revision, or the revision captured by a live snapshot.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("path" = String, Query, description = "Absolute file path"),
            ("revision_no" = Option<RevisionNo>, Query, description = "Optional prior revision number; cannot be combined with snapshot_id"),
            ("snapshot_id" = Option<loonfs_api::CheckpointId>, Query, description = "Use the file revision captured by this snapshot")
        ),
        responses(
            (status = 200, description = "File bytes", body = Vec<u8>, content_type = "application/octet-stream"),
            (status = 400, description = "Invalid path, revision, snapshot id, non-snapshot checkpoint, or revision_no combined with snapshot_id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, revision, or snapshot not found", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot released or expired", body = ApiError),
            (status = 413, description = "Content exceeds the advertised `download.max_content_bytes` limit", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_file_bytes(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    let path = required_query_param(query.path, "path")?;
    let revision_no = query
        .revision_no
        .as_deref()
        .map(parse_revision_no)
        .transpose()?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    reject_snapshot_with_revision(snapshot_id.as_ref(), revision_no)?;
    let target = pin_requested_snapshot(&state, &namespace_id, snapshot_id).await?;
    let permit = acquire_download_permit(&state)?;
    let file = target
        .get_file_bytes(&path, revision_no)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
    Ok(buffered_download_response(file.bytes, permit))
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
        description = "Returns the namespace's recoverable deletions, oldest deletion first. Entries never age out at the retention floor; each carries the inode id and deletion sequence undelete needs, plus the deleted name when the delete recorded one.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("cursor" = Option<String>, Query, description = "Opaque trash page cursor")
        ),
        responses(
            (status = 200, description = "Recoverable deletions", body = ListTrashResponse),
            (status = 400, description = "Invalid limit or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_trash(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListTrashResponse>, ApiResponseError> {
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
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_file_revisions(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PathPageQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    let path = required_query_param(query.path, "path")?;
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
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("path")
        })?;
    Ok(Json(response))
}

/// The server stores the actor from the request; the shared token does not verify it.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_commit",
        extensions(("x-loonfs-retry" = json!("replayable"))),
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
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_commit(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ApiCommitRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
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
        .filter_map(FilesystemOperation::content_ref)
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
            mode = TraceMode::Remote.as_str(),
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
        state.writer.create_commit(&namespace_id, request).await
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
        operation_id = "list_changes",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/changes",
        tag = "filesystem",
        summary = "List changes after a sequence",
        description = "Returns committed changes after a sequence. A snapshot limits the feed to its captured sequence.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("after_seq" = loonfs_api::ChangeSeq, Query, description = "Return committed changes after this sequence"),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum page size"),
            ("snapshot_id" = Option<loonfs_api::CheckpointId>, Query, description = "End the feed at this snapshot's captured sequence")
        ),
        responses(
            (status = 200, description = "Committed changes", body = ListChangesResponse),
            (status = 400, description = "Invalid change cursor, limit, snapshot id, non-snapshot checkpoint, or after_seq above the snapshot sequence", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or snapshot not found", body = ApiError),
            (status = 409, description = "The cursor is older than the retained change history and requires a fresh snapshot", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot released or expired", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_changes(
    State(state): State<AppState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<ChangesQuery>,
) -> Result<Json<ListChangesResponse>, ApiResponseError> {
    let after_seq = parse_after_seq(&required_query_param(query.after_seq, "after_seq")?)?;
    let limit = resolve_page_limit(query.limit)?;
    let snapshot_id = parse_optional_snapshot_id(query.snapshot_id)?;
    let target = pin_requested_snapshot(&state, &namespace_id, snapshot_id).await?;
    let response = match target {
        ReadTarget::Snapshot(snapshot) => {
            let captured_seq = snapshot.head_seq();
            if after_seq > captured_seq {
                return Err(ApiResponseError::new(
                    ErrorCode::InvalidRequest,
                    &format!("after_seq `{after_seq}` is above snapshot sequence `{captured_seq}`"),
                )
                .with_param("after_seq"));
            }
            if after_seq == captured_seq {
                ListChangesResponse {
                    namespace_id: namespace_id.clone(),
                    after_seq,
                    through_seq: captured_seq,
                    next_after_seq: None,
                    changes: Vec::new(),
                }
            } else {
                let mut page = state
                    .reader
                    .list_changes(
                        &namespace_id,
                        after_seq,
                        ListChangesOptions { limit: Some(limit) },
                    )
                    .await
                    .map_err(ApiResponseError::for_namespace(&namespace_id))?;
                page.changes
                    .retain(|change| change.committed_seq <= captured_seq);
                page.through_seq = captured_seq;
                page.next_after_seq = page
                    .changes
                    .last()
                    .map(|change| change.committed_seq)
                    .filter(|last_seq| *last_seq < captured_seq);
                page
            }
        }
        ReadTarget::Live {
            reader,
            namespace_id,
        } => reader
            .list_changes(
                &namespace_id,
                after_seq,
                ListChangesOptions { limit: Some(limit) },
            )
            .await
            .map_err(ApiResponseError::for_namespace(&namespace_id))?,
    };
    Ok(Json(response))
}

pub(super) async fn pin_requested_snapshot(
    state: &AppState,
    namespace_id: &loonfs_api::NamespaceId,
    snapshot_id: Option<CheckpointId>,
) -> Result<ReadTarget, ApiResponseError> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(ReadTarget::Live {
            reader: state.reader.clone(),
            namespace_id: namespace_id.clone(),
        });
    };
    state
        .reader
        .pin_namespace_at_snapshot(namespace_id, &snapshot_id)
        .await
        .map(|snapshot| ReadTarget::Snapshot(Box::new(snapshot)))
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(namespace_id, error)
                .with_invalid_request_param("snapshot_id")
        })
}

pub(super) fn reject_snapshot_with_revision(
    snapshot_id: Option<&CheckpointId>,
    revision_no: Option<RevisionNo>,
) -> Result<(), ApiResponseError> {
    if snapshot_id.is_some() && revision_no.is_some() {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            "revision_no cannot be combined with snapshot_id",
        )
        .with_param("revision_no"));
    }
    Ok(())
}

pub(super) fn parse_optional_snapshot_id(
    snapshot_id: Option<String>,
) -> Result<Option<CheckpointId>, ApiResponseError> {
    snapshot_id
        .as_deref()
        .map(|value| parse_path_id("snapshot_id", value))
        .transpose()
}

fn parse_after_seq(value: &str) -> Result<loonfs_api::ChangeSeq, ApiResponseError> {
    parse_public_ordinal("after_seq", value, loonfs_api::ChangeSeq::parse)
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
