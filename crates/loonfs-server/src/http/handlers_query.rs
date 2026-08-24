//! The `query/v0` plane: derived-index reads.

#[cfg(feature = "openapi")]
use super::handlers_filesystem::{OpenApiDefaultFalseBoolean, OpenApiPageLimit};
use super::handlers_uploads::current_unix_ms;
use super::{
    authorize,
    handlers_filesystem::{parse_boolean_query_param, required_query_param, resolve_page_limit},
    AppQuery, AppState, NamespaceIdPath, NoQuery, OptionalAppJson,
};
use crate::http::error::{status_for_core_error_code, ApiResponseError};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs_api::v0::{GrepGcRequest, GrepGcResponse, GrepIndex};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    GrepRequest, GrepResponse, NamespaceId, FEATURE_ADMIN_GREP_INDEX, FEATURE_QUERY_GREP,
};
use loonfs_grep::{GrepDisableOutcome, GrepEnableOutcome, GrepError, NamespaceReads};

/// Maximum grep pattern length in UTF-8 bytes.
const MAX_GREP_PATTERN_BYTES: usize = 1024;

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GrepQuery {
    pattern: Option<String>,
    case_insensitive: Option<String>,
    path_prefix: Option<String>,
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
        path = "/v0/namespaces/{namespace_id}/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, accelerated by the namespace's grep index. Matches are verified against the real pattern and returned in ascending `(inode_id, byte_offset)` order; revisions committed after the index watermark are scanned exhaustively unless `allow_stale` skips them. Requires this deployment to serve grep and the namespace to carry a materialized active grep root.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("pattern" = String, Query, description = "Pattern in the Rust `regex` crate's dialect. Its UTF-8 encoding must be at most 1024 bytes."),
            ("case_insensitive" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Match case-insensitively (`true` or `false`). Defaults to `false`."),
            ("path_prefix" = Option<String>, Query, description = "Complete absolute path used to restrict matches."),
            ("allow_scan" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Permit a capped exhaustive scan when the pattern has no required grams (`true` or `false`). Defaults to `false`."),
            ("allow_stale" = inline(Option<OpenApiDefaultFalseBoolean>), Query, description = "Return indexed-only results when the unindexed tail exceeds the scan budget (`true` or `false`). Defaults to `false`."),
            ("limit" = inline(Option<OpenApiPageLimit>), Query, description = "Maximum matches per page"),
            ("cursor" = Option<String>, Query, description = "Opaque grep page cursor")
        ),
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or an unindexable pattern without allow_scan", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "This deployment does not serve grep queries, the grep index is not enabled, or its backfill has not completed on this namespace", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn grep(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<GrepQuery>,
) -> Result<Json<GrepResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let request = grep_request(query.into_params()?)?;
    // First touch: on a deployment that maintains this index, a search is
    // also the hint that someone cares about this namespace again — after a
    // restart, nothing else has said so.
    if let Some(maintenance) = &state.grep_maintenance {
        maintenance.nudge_if_behind(&namespace_id).await;
    }
    let service = state.grep_service();
    // Grep's own segments come off the instrumented store every LoonFS
    // request in this process is measured on; its filesystem reads go
    // through the same reader handle the core planes serve from.
    let store = state.writer.object_store();
    let reads = NamespaceReads::new(&state.reader, &namespace_id);
    let response = service
        .query(&request, &reads, &store)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    Ok(Json(response))
}

fn grep_request(query: GrepQuery) -> Result<GrepRequest, ApiResponseError> {
    let pattern = required_query_param(query.pattern, "pattern")?;
    if pattern.len() > MAX_GREP_PATTERN_BYTES {
        return Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            loonfs_api::ErrorCode::InvalidRequest,
            &format!(
                "grep pattern is {} bytes; the maximum is {MAX_GREP_PATTERN_BYTES} bytes",
                pattern.len()
            ),
        )
        .with_param("pattern"));
    }
    let path_prefix = query
        .path_prefix
        .map(|value| {
            loonfs_api::AbsolutePath::parse(&value).map_err(|error| {
                ApiResponseError::new(
                    StatusCode::BAD_REQUEST,
                    loonfs_api::ErrorCode::InvalidRequest,
                    &error.to_string(),
                )
                .with_param("path_prefix")
            })
        })
        .transpose()?;
    let limit = query
        .limit
        .map(|value| resolve_page_limit(Some(value)).map(|limit| limit.get()))
        .transpose()?;
    Ok(GrepRequest {
        pattern,
        case_insensitive: parse_optional_boolean(query.case_insensitive, "case_insensitive")?,
        path_prefix,
        cursor: query.cursor,
        limit,
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
pub(super) async fn grep_queries_not_served(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    Err(ApiResponseError::not_supported(
        FEATURE_QUERY_GREP,
        "this deployment does not serve grep queries; set `[grep].mode` to `serve_only` \
         or `serve_and_maintain`",
    ))
}

/// Absent-capability response where this deployment maintains no index, so
/// nothing here may enable, disable, or collect one.
pub(super) async fn grep_index_not_maintained(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    Err(ApiResponseError::not_supported(
        FEATURE_ADMIN_GREP_INDEX,
        "this deployment does not maintain the grep index; set `[grep].mode` to \
         `maintain_only` or `serve_and_maintain`, or administer the index where it is maintained",
    ))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "enable_grep_index",
        path = "/v0/admin/namespaces/{namespace_id}/grep/index/enable",
        tag = "admin",
        summary = "Enable the grep index",
        description = "Enables the namespace's grep root and asks this deployment's maintenance runner for the backfill's first step. The response reports the lifecycle and bookkeeping read after the transition: a fresh enable is `backfilling` with the sequence its checkpoint captured, while an already-enabled namespace answers with its current status. Idempotent. Requires this deployment to maintain the grep index.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root enabled or already enabled", body = GrepIndex),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn enable_grep_index(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let outcome = state
        .grep_worker()
        .enable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    match outcome {
        GrepEnableOutcome::Enabled { .. } | GrepEnableOutcome::AlreadyEnabled { .. } => {}
        GrepEnableOutcome::Superseded => {
            return Err(grep_root_conflict(&namespace_id));
        }
    }
    // Read after the transition so every index endpoint reports bookkeeping
    // from the same durable root source as the status handler.
    let response = read_grep_index_status(&state, &namespace_id).await?;
    // The root is durable now; the backfill is one nudge away from starting.
    if let Some(maintenance) = &state.grep_maintenance {
        maintenance.nudge(&namespace_id);
    }
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_grep_index",
        path = "/v0/admin/namespaces/{namespace_id}/grep/index",
        tag = "admin",
        summary = "Get grep index status",
        description = "Returns whether the namespace's grep index is `disabled`, `backfilling`, or `active`, including build progress when available. A namespace that has never enabled the index is `disabled`. This operation requires a deployment that maintains grep indexes and does not change the index.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep index status and build progress", body = GrepIndex),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_grep_index(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    Ok(Json(read_grep_index_status(&state, &namespace_id).await?))
}

async fn read_grep_index_status(
    state: &AppState,
    namespace_id: &NamespaceId,
) -> Result<GrepIndex, ApiResponseError> {
    state
        .grep_worker()
        .get_grep_index_status(namespace_id)
        .await
        .map_err(|error| map_grep_error(namespace_id, error))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "disable_grep_index",
        path = "/v0/admin/namespaces/{namespace_id}/grep/index/disable",
        tag = "admin",
        summary = "Disable the grep index",
        description = "Disables the namespace's grep root and clears its segment references with one durable compare-and-swap; index maintenance stops on its own once a step reads the disabled root. Explicit grep garbage collection later reclaims the segments. Idempotent. Requires this deployment to maintain the grep index.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root disabled or already disabled", body = GrepIndex),
            (status = 400, description = "Invalid namespace id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn disable_grep_index(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
) -> Result<Json<GrepIndex>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    // Disabling is one durable compare-and-swap and nothing else. A step
    // already running loses its own publication race to this one and
    // retries; the retry reads a disabled root, concludes there is nothing
    // to maintain, and the runner forgets the namespace. Nothing here waits
    // on a background task to notice.
    let outcome = state
        .grep_worker()
        .disable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    match outcome {
        GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {}
        GrepDisableOutcome::Superseded => {
            return Err(grep_root_conflict(&namespace_id));
        }
    }
    // The disabled root retains genuine index bookkeeping, so read it after
    // the transition instead of synthesizing counters here.
    Ok(Json(read_grep_index_status(&state, &namespace_id).await?))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "gc_grep_index",
        path = "/v0/admin/namespaces/{namespace_id}/grep/index/gc",
        tag = "admin",
        summary = "Collect grep-index garbage",
        description = "Runs one explicit garbage-collection pass over only this namespace's grep-owned extension keyspace. A tombstoned or absent namespace has aged extension state reaped; no grep garbage collection runs implicitly. `max_objects` bounds the reads the pass spends and returns a `next_cursor` when keys remain; resuming re-reads liveness and the grep root, so a cursor only skips enumeration. Requires this deployment to maintain the grep index.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body = Option<GrepGcRequest>,
        responses(
            (status = 200, description = "Namespace grep garbage collection completed", body = GrepGcResponse),
            (status = 400, description = "Invalid budget or cursor", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn gc_grep_index(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    headers: HeaderMap,
    query: AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<GrepGcRequest>,
) -> Result<Json<GrepGcResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    let request = request.unwrap_or_default();
    let report = state
        .grep_worker()
        .garbage_collect_namespace(
            &namespace_id,
            current_unix_ms()?,
            &loonfs_grep::GrepGcOptions {
                max_objects: request.max_objects,
                cursor: request.cursor,
            },
        )
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    Ok(Json(GrepGcResponse {
        namespace_id,
        deleted_segments: report.deleted_segments,
        deleted_other_objects: report.deleted_other_objects,
        namespace_reaped: report.namespace_reaped,
        retained_candidates: report.retained_candidates,
        namespace_degraded: report.namespace_degraded,
        next_cursor: report.next_cursor,
    }))
}

fn map_grep_error(namespace_id: &loonfs_api::NamespaceId, error: GrepError) -> ApiResponseError {
    let code = error.code();
    match error {
        // Both cases mean the advertised query.grep capability is unavailable.
        error @ (GrepError::NotEnabled | GrepError::Backfilling) => {
            ApiResponseError::not_supported(FEATURE_QUERY_GREP, &error.public_message())
        }
        GrepError::Runtime(error) => {
            let cursor_is_invalid = matches!(
                &error,
                loonfs::RuntimeError::Core(loonfs::CoreError::InvalidCursor(_))
            );
            let response = ApiResponseError::runtime_for_namespace(namespace_id, error);
            if cursor_is_invalid {
                response.with_param("cursor")
            } else {
                response
            }
        }
        error => ApiResponseError::new(
            status_for_core_error_code(code),
            code,
            &error.public_message(),
        ),
    }
}

fn grep_root_conflict(namespace_id: &NamespaceId) -> ApiResponseError {
    map_grep_error(
        namespace_id,
        GrepError::PublicationConflict {
            object_key: loonfs_grep::keyspace::root_key(namespace_id),
        },
    )
}
