//! The `query/v0` API group: derived-index reads.

use super::extractors::SubjectHeaders;
use super::handlers_inodes::parse_inode_id;
use super::query_params::{effective_page_limit, required_query_param};
use super::{AppQuery, BindingState, NamespaceIdPath, NoBody, NoQuery};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::{Extension, Json};
use loonfs::SharedObjectStore;
use loonfs_grep::{
    GrepDisableOutcome, GrepEnableOutcome, GrepError, GrepService, GrepWorker, NamespaceReads,
};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::GrepIndex;
use loonfs_types::{
    GrepRequest, GrepResponse, GrepScope, NamespaceId, PageRequest, FEATURE_MAINTENANCE_GREP_INDEX,
    FEATURE_QUERY_GREP,
};
use std::sync::Arc;

/// The query of a content search.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct GrepQuery {
    /// The search pattern, in the Rust `regex` crate's syntax. Its UTF-8 encoding must be at most
    /// 1024 bytes.
    #[cfg_attr(feature = "openapi", param(required = true))]
    pattern: Option<String>,
    /// Whether matching ignores case. The default is `false`.
    #[cfg_attr(feature = "openapi", param(default = false))]
    case_insensitive: Option<bool>,
    /// A complete absolute path that limits matches to its descendants. It can't be combined with
    /// `inode_id`.
    path_prefix: Option<String>,
    /// An inode that limits matches to its descendants. It can't be combined with `path_prefix`.
    #[cfg_attr(
        feature = "openapi",
        param(pattern = r"^ino_[1-9][0-9]*$", example = "ino_123")
    )]
    inode_id: Option<String>,
    /// Whether a pattern with no required literal bytes can use a capped exhaustive scan. The
    /// default is `false`.
    #[cfg_attr(feature = "openapi", param(default = false))]
    allow_scan: Option<bool>,
    /// Whether the server returns results from the index alone when the unindexed tail exceeds the
    /// scan budget. The default is `false`.
    #[cfg_attr(feature = "openapi", param(default = false))]
    allow_stale: Option<bool>,
    /// The maximum number of matches per page.
    #[cfg_attr(
        feature = "openapi",
        param(minimum = 1, maximum = 1000, default = 1000)
    )]
    limit: Option<u32>,
    /// The opaque cursor that the previous page returned.
    cursor: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "grep",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, using the namespace's grep index to narrow the candidates. The server checks each candidate against the full pattern and returns matches in ascending `(inode_id, byte_offset)` order. It scans every revision committed after the index watermark, unless `allow_stale` skips them. The deployment must serve grep, and the namespace must have a materialized, active grep index.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            GrepQuery
        ),
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or scope, both scopes, or an unindexable pattern without allow_scan", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, scope path, or scope inode not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 501, description = "This deployment doesn't serve grep queries, the grep index isn't enabled, or its backfill hasn't completed on this namespace", body = ErrorResponse),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn grep(
    State(state): State<Arc<BindingState>>,
    Extension(service): Extension<Arc<GrepService>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(mut query): AppQuery<GrepQuery>,
) -> Result<Json<GrepResponse>, ApiResponseError> {
    let page = PageRequest {
        limit: effective_page_limit(query.limit.take())?,
        cursor: query.cursor.take(),
    };
    let request = grep_request(query)?;
    // Grep's own segments come off the instrumented store every LoonFS
    // request in this process is measured on; its filesystem reads go
    // through the same runtime the core API groups serve from.
    let store = state.runtime.object_store();
    let reads = NamespaceReads::new(state.runtime.namespace(&namespace_id));
    let reads = match subject {
        Some(subject) => reads.with_subject(subject),
        None => reads,
    };
    let response = service
        .query(&request, page, &reads, &store)
        .await
        .map_err(map_grep_error)?;
    Ok(Json(response))
}

fn grep_request(query: GrepQuery) -> Result<GrepRequest, ApiResponseError> {
    let pattern = required_query_param(query.pattern, "pattern")?;
    let path_prefix = query
        .path_prefix
        .map(|value| {
            loonfs_types::AbsolutePath::parse(&value).map_err(|error| {
                ApiResponseError::new(loonfs_types::ErrorCode::InvalidRequest, &error.to_string())
                    .with_param("path_prefix")
            })
        })
        .transpose()?;
    let inode_id = query.inode_id.as_deref().map(parse_inode_id).transpose()?;
    let scope = match (path_prefix, inode_id) {
        (None, None) => GrepScope::Namespace,
        (Some(path_prefix), None) => GrepScope::PathPrefix(path_prefix),
        (None, Some(inode_id)) => GrepScope::Inode(inode_id),
        (Some(_), Some(_)) => {
            return Err(ApiResponseError::new(
                loonfs_types::ErrorCode::InvalidRequest,
                "`path_prefix` cannot be combined with `inode_id`",
            )
            .with_param("inode_id"))
        }
    };
    Ok(GrepRequest {
        pattern,
        case_insensitive: query.case_insensitive.unwrap_or(false),
        scope,
        allow_stale: query.allow_stale.unwrap_or(false),
        allow_scan: query.allow_scan.unwrap_or(false),
    })
}

/// Absent-capability response where this deployment serves no searches.
pub(super) async fn grep_queries_not_served() -> ApiResponseError {
    ApiResponseError::not_supported(
        FEATURE_QUERY_GREP,
        "this deployment does not serve grep queries; set `[grep].mode` to `serve_only` \
         or `serve_and_maintain`",
    )
}

/// Absent-capability response where this deployment maintains no index, so
/// nothing here may enable, disable, or collect one.
pub(super) async fn grep_index_not_maintained() -> ApiResponseError {
    ApiResponseError::not_supported(
        FEATURE_MAINTENANCE_GREP_INDEX,
        "this deployment does not maintain the grep index; set `[grep].mode` to \
         `maintain_only` or `serve_and_maintain`, or maintain the index on another deployment",
    )
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "enable_grep_index",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
        tag = "maintenance",
        summary = "Enable the grep index",
        description = "Enables the namespace's grep index. A deployment that runs maintenance starts the backfill on its next session tick and continues until the index is active. The response reports the index state read after the change. A new enable reports `backfilling`, with the sequence that its checkpoint captured, and an index that was already enabled reports its current status. Repeating the request has the same result. A deployment that doesn't maintain the grep index returns `not_supported`.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        responses(
            (status = 200, description = "Grep index enabled or already enabled", body = GrepIndex),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 409, description = "Another publisher changed the grep manifest first. Retry the request.", body = ErrorResponse),
            (status = 501, description = "This deployment doesn't maintain the grep index", body = ErrorResponse),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn enable_grep_index(
    State(state): State<Arc<BindingState>>,
    Extension(worker): Extension<GrepWorker<SharedObjectStore>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    NoBody: NoBody,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    let _handle = state.open_writer(&namespace_id, None).await?;
    let outcome = worker.enable(&namespace_id).await.map_err(map_grep_error)?;
    state.namespaces.mark_index_dirty(&namespace_id);
    match outcome {
        GrepEnableOutcome::Enabled { .. } | GrepEnableOutcome::AlreadyEnabled { .. } => {}
        GrepEnableOutcome::Superseded => {
            return Err(grep_publication_conflict());
        }
    }
    // Read after the transition so every index endpoint reports bookkeeping
    // from the same durable root source as the status handler.
    let response = read_grep_index_status(&worker, &namespace_id).await?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_grep_index",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/grep/index",
        tag = "maintenance",
        summary = "Get grep index status",
        description = "Reports whether the namespace's grep index is `disabled`, `backfilling`, or `active`, with build progress when it's available. A namespace that has never enabled the index is `disabled`. A deployment that doesn't maintain grep indexes returns `not_supported`, and this operation doesn't change the index.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        responses(
            (status = 200, description = "Grep index status and build progress", body = GrepIndex),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 501, description = "This deployment doesn't maintain the grep index", body = ErrorResponse),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_grep_index(
    Extension(worker): Extension<GrepWorker<SharedObjectStore>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    Ok(Json(read_grep_index_status(&worker, &namespace_id).await?))
}

async fn read_grep_index_status(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
) -> Result<GrepIndex, ApiResponseError> {
    worker
        .get_grep_index(namespace_id)
        .await
        .map_err(map_grep_error)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "disable_grep_index",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
        tag = "maintenance",
        summary = "Disable the grep index",
        description = "Disables the namespace's grep index by publishing a manifest with the next number and no segment references. Index maintenance stops when a step reads the disabled manifest, and an explicit grep garbage collection run reclaims the segments later. Repeating the request has the same result. A deployment that doesn't maintain the grep index returns `not_supported`.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        responses(
            (status = 200, description = "Grep index disabled or already disabled", body = GrepIndex),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 409, description = "Another publisher changed the grep manifest first. Retry the request.", body = ErrorResponse),
            (status = 501, description = "This deployment doesn't maintain the grep index", body = ErrorResponse),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn disable_grep_index(
    State(state): State<Arc<BindingState>>,
    Extension(worker): Extension<GrepWorker<SharedObjectStore>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    NoBody: NoBody,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    let _handle = state.open_writer(&namespace_id, None).await?;
    let outcome = worker
        .disable(&namespace_id)
        .await
        .map_err(map_grep_error)?;
    state.namespaces.mark_index_dirty(&namespace_id);
    match outcome {
        GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {}
        GrepDisableOutcome::Superseded => {
            return Err(grep_publication_conflict());
        }
    }
    Ok(Json(read_grep_index_status(&worker, &namespace_id).await?))
}

pub(super) fn map_grep_error(error: GrepError) -> ApiResponseError {
    ApiResponseError::from_error_response(error.code(), error.to_error_response())
}

fn grep_publication_conflict() -> ApiResponseError {
    ApiResponseError::new(
        loonfs_types::ErrorCode::StaleHead,
        "grep index publication conflict; retry",
    )
}
