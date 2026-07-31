//! The `query/v0` plane: derived-index reads.

use super::handlers_uploads::current_unix_ms;
use super::{authorize, AppJson, AppState, NamespaceIdPath};
use crate::http::error::{status_for_core_error_code, ApiResponseError};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use loonfs_api::v0::{
    DisableGrepIndexResponse, EnableGrepIndexResponse, GrepGcResponse, GrepRequest, GrepResponse,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::FEATURE_QUERY_GREP;
use loonfs_grep::{
    GrepDisableOutcome, GrepEnableOutcome, GrepError, GrepIndexSnapshot, NamespaceReads,
};

const GREP_INDEX_FEATURE: &str = "grep.index";

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/query/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, accelerated by the namespace's grep index. Matches are verified against the real pattern and returned in ascending `(inode_id, byte_offset)` order; revisions committed after the index watermark are scanned exhaustively unless `allow_stale` skips them. Requires this deployment to serve grep and the namespace to carry a materialized steady-state grep root.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = GrepRequest,
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or an unindexable pattern without allow_scan", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "This deployment does not serve grep queries, the grep index is not enabled, or its backfill has not completed on this namespace", body = ApiError),
            (status = 503, description = "The index trails the head past the scan budget", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn grep(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AppJson(request): AppJson<GrepRequest>,
) -> Result<Json<GrepResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    // First touch: on a deployment that maintains this index, a search is
    // also the hint that someone cares about this namespace again — after a
    // restart, nothing else has said so.
    if let Some(maintenance) = &state.grep_maintenance {
        maintenance.nudge_if_behind(&namespace_id).await;
    }
    let service = state
        .grep_service
        .as_ref()
        .expect("grep routes should carry a grep service");
    // Grep's own segments come off the instrumented store every LoonFS
    // request in this process is measured on; its filesystem reads go
    // through the same reader handle the core planes serve from.
    let store = state.writer.object_store();
    let reads = NamespaceReads::new(&state.reader, &namespace_id);
    let snapshot = GrepIndexSnapshot::from_grep_root(&*store, &namespace_id, service).await;
    let response = service
        .query(&request, &snapshot, &reads, &store)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    Ok(Json(response))
}

/// Absent-capability response where this deployment answers no searches.
pub(super) async fn grep_queries_not_served(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiResponseError> {
    authorize(&state.config, &headers)?;
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
    authorize(&state.config, &headers)?;
    Err(ApiResponseError::not_supported(
        FEATURE_QUERY_GREP,
        "this deployment does not maintain the grep index; set `[grep].mode` to \
         `maintain_only` or `serve_and_maintain`, or administer the index where it is maintained",
    ))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/grep/index/enable",
        tag = "admin",
        summary = "Enable the grep index",
        description = "Enables the namespace's grep root and asks this deployment's maintenance runner for the backfill's first step. Idempotent. Requires this deployment to maintain the grep index.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root enabled or already enabled", body = EnableGrepIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn enable_grep_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<EnableGrepIndexResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let outcome = state
        .grep_worker
        .as_ref()
        .expect("grep routes should carry a grep worker")
        .enable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    let response = match outcome {
        GrepEnableOutcome::Enabled { target_seq } => EnableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            built_through_seq: target_seq,
            already_enabled: false,
        },
        GrepEnableOutcome::AlreadyEnabled { built_through_seq } => EnableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            built_through_seq,
            already_enabled: true,
        },
        GrepEnableOutcome::Superseded => {
            return Err(map_grep_error(
                &namespace_id,
                GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(&namespace_id),
                },
            ));
        }
    };
    // The root is durable now; the backfill is one nudge away from starting.
    if let Some(maintenance) = &state.grep_maintenance {
        maintenance.nudge(&namespace_id);
    }
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/grep/index/disable",
        tag = "admin",
        summary = "Disable the grep index",
        description = "Disables the namespace's grep root and clears its segment references with one durable compare-and-swap; index maintenance stops on its own once a step reads the disabled root. Explicit grep garbage collection later reclaims the segments. Idempotent. Requires this deployment to maintain the grep index.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root disabled or already disabled", body = DisableGrepIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn disable_grep_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<DisableGrepIndexResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    // Disabling is one durable compare-and-swap and nothing else. A step
    // already running loses its own publication race to this one and
    // retries; the retry reads a disabled root, concludes there is nothing
    // to maintain, and the runner forgets the namespace. Nothing here waits
    // on a background task to notice.
    let outcome = state
        .grep_worker
        .as_ref()
        .expect("grep routes should carry a grep worker")
        .disable(&namespace_id)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    let response = match outcome {
        GrepDisableOutcome::Disabled => DisableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            was_enabled: true,
        },
        GrepDisableOutcome::NotEnabled => DisableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            was_enabled: false,
        },
        GrepDisableOutcome::Superseded => {
            return Err(map_grep_error(
                &namespace_id,
                GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(&namespace_id),
                },
            ));
        }
    };
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/grep/index/gc",
        tag = "admin",
        summary = "Collect grep-index garbage",
        description = "Runs one explicit garbage-collection pass over only this namespace's grep-owned extension keyspace. A tombstoned or absent namespace has aged extension state reaped; no grep garbage collection runs implicitly. Requires this deployment to maintain the grep index.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace grep garbage collection completed", body = GrepGcResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 501, description = "This deployment does not maintain the grep index", body = ApiError),
            (status = 500, description = "The grep index is corrupt or its backing store is unavailable", body = ApiError)
        )
    )
)]
pub(super) async fn gc_grep_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<GrepGcResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let report = state
        .grep_worker
        .as_ref()
        .expect("grep routes should carry a grep worker")
        .garbage_collect_namespace(&namespace_id, current_unix_ms()?)
        .await
        .map_err(|error| map_grep_error(&namespace_id, error))?;
    Ok(Json(GrepGcResponse {
        namespace_id,
        deleted_segments: report.deleted_segments,
        deleted_other_objects: report.deleted_other_objects,
        namespace_reaped: report.namespace_reaped,
        retained_candidates: report.retained_candidates,
        namespace_degraded: report.namespace_degraded,
    }))
}

fn map_grep_error(namespace_id: &loonfs_api::NamespaceId, error: GrepError) -> ApiResponseError {
    let code = error.code();
    match error {
        error @ (GrepError::NotEnabled | GrepError::Backfilling) => {
            ApiResponseError::not_supported(GREP_INDEX_FEATURE, &error.to_string())
        }
        GrepError::Runtime(error) => ApiResponseError::runtime_for_namespace(namespace_id, error),
        error => ApiResponseError::new(status_for_core_error_code(code), code, &error.to_string()),
    }
}
