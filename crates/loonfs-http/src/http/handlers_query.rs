//! The `query/v0` API group: derived-index reads.

use super::extractors::SubjectHeaders;
use super::query_params::{effective_page_limit, parse_boolean_query_param, required_query_param};
#[cfg(feature = "openapi")]
use super::query_params::{OpenApiDefaultFalseBoolean, OpenApiPageLimit};
use super::{AppQuery, BindingState, NamespaceIdPath, NoBody, NoQuery};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::{Extension, Json};
use loonfs::SharedObjectStore;
use loonfs_grep::{
    GrepDisableOutcome, GrepEnableOutcome, GrepError, GrepService, GrepWorker, NamespaceReads,
};
use loonfs_types::api::v0::GrepIndex;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    GrepRequest, GrepResponse, NamespaceId, PageRequest, FEATURE_MAINTENANCE_GREP_INDEX,
    FEATURE_QUERY_GREP,
};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GrepQuery {
    pattern: Option<String>,
    case_insensitive: Option<String>,
    path_prefix: Option<String>,
    inode_id: Option<String>,
    allow_scan: Option<String>,
    allow_stale: Option<String>,
    limit: Option<String>,
    cursor: Option<String>,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "grep",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-pagination" = json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": "$response.matches",
            })),
        ),
        path = "/v0/namespaces/{namespace_id}/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, using the namespace's grep index to narrow the candidates. The server checks each candidate against the full pattern and returns matches in ascending `(inode_id, byte_offset)` order. It scans every revision committed after the index watermark, unless `allow_stale` skips them. The deployment must serve grep, and the namespace must have a materialized, active grep index.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("pattern" = String, Query, description = "The search pattern, in the Rust `regex` crate's syntax. Its UTF-8 encoding must be at most 1024 bytes."),
            ("case_insensitive" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Whether matching ignores case. The default is `false`."),
            ("path_prefix" = Option<String>, Query, description = "A complete absolute path that limits matches to its descendants. It can't be combined with `inode_id`."),
            ("inode_id" = Option<String>, Query, description = "An inode that limits matches to its descendants. It can't be combined with `path_prefix`.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("allow_scan" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Whether a pattern with no required literal bytes can use a capped exhaustive scan. The default is `false`."),
            ("allow_stale" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Whether the server returns results from the index alone when the unindexed tail exceeds the scan budget. The default is `false`."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "The maximum number of matches per page."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned.")
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
    State(state): State<BindingState>,
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
        .map_err(|error| map_grep_error(&namespace_id, error))?;
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
    let inode_id = query
        .inode_id
        .map(|value| {
            loonfs_types::public_inode_id::decode(&value).map_err(|error| {
                ApiResponseError::new(loonfs_types::ErrorCode::InvalidRequest, &error.to_string())
                    .with_param("inode_id")
            })
        })
        .transpose()?;
    Ok(GrepRequest {
        pattern,
        case_insensitive: parse_optional_boolean(query.case_insensitive, "case_insensitive")?,
        path_prefix,
        inode_id,
        allow_stale: parse_optional_boolean(query.allow_stale, "allow_stale")?,
        allow_scan: parse_optional_boolean(query.allow_scan, "allow_scan")?,
    })
}

fn parse_optional_boolean(value: Option<String>, name: &str) -> Result<bool, ApiResponseError> {
    value
        .as_deref()
        .map(|value| parse_boolean_query_param(value, name))
        .unwrap_or(Ok(false))
}

/// Absent-capability response where this deployment answers no searches.
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
    State(state): State<BindingState>,
    Extension(worker): Extension<GrepWorker<SharedObjectStore>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    NoBody: NoBody,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    let _handle = state
        .namespaces
        .open(&namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    let outcome = worker
        .enable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
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
        .map_err(|error| map_grep_error(namespace_id, error))
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
    State(state): State<BindingState>,
    Extension(worker): Extension<GrepWorker<SharedObjectStore>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    NoBody: NoBody,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    let _handle = state
        .namespaces
        .open(&namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    let outcome = worker
        .disable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    state.namespaces.mark_index_dirty(&namespace_id);
    match outcome {
        GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {}
        GrepDisableOutcome::Superseded => {
            return Err(grep_publication_conflict());
        }
    }
    Ok(Json(read_grep_index_status(&worker, &namespace_id).await?))
}

pub(super) fn map_grep_error(
    namespace_id: &loonfs_types::NamespaceId,
    error: GrepError,
) -> ApiResponseError {
    if let GrepError::Runtime(error) = error {
        return ApiResponseError::runtime_for_namespace(namespace_id, error);
    }
    ApiResponseError::from_error_response(error.code(), error.to_error_response())
}

fn grep_publication_conflict() -> ApiResponseError {
    ApiResponseError::new(
        loonfs_types::ErrorCode::StaleHead,
        "grep index publication conflict; retry",
    )
}
