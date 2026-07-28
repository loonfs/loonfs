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
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{GrepDisableOutcome, GrepEnableOutcome, GrepError};

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
            (status = 501, description = "Grep serving is disabled, the grep index is not enabled, or its backfill has not completed on this namespace", body = ApiError),
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
    start_driver_for_query_if_needed(&state, &namespace_id).await;
    let response = state
        .reader
        .grep(&namespace_id, &request)
        .await
        .map_err(|error| map_runtime_grep_error(&namespace_id, error))?;
    Ok(Json(response))
}

/// Uniform absent-capability response for every grep-owned HTTP operation.
pub(super) async fn grep_not_supported(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    Err(ApiResponseError::not_supported(
        FEATURE_QUERY_GREP,
        "grep is disabled for this deployment; set `[grep].mode` to `embedded` or `serve_only`",
    ))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/grep/index/enable",
        tag = "admin",
        summary = "Enable the grep index",
        description = "Enables the namespace's grep root. Embedded mode immediately starts that namespace's event-driven backfill driver; serve-only deployments rely on their explicitly assigned external driver. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root enabled or already enabled", body = EnableGrepIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
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
    if let Some(drivers) = &state.grep_drivers {
        drivers.start(&namespace_id);
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
        description = "Disables the namespace's grep root, clears its segment references, and stops its embedded driver. Explicit grep garbage collection later reclaims the segments. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root disabled or already disabled", body = DisableGrepIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 409, description = "Lost a grep root-pointer publication race; retry", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
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
    if let Some(drivers) = &state.grep_drivers {
        drivers
            .stop(&namespace_id)
            .await
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    }
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
        description = "Runs one explicit garbage-collection pass over only this namespace's grep-owned extension keyspace. A tombstoned or absent namespace has aged extension state reaped; no grep garbage collection runs implicitly.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace grep garbage collection completed", body = GrepGcResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 403, description = "The backing store rejected its configured credentials", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
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

fn map_runtime_grep_error(
    namespace_id: &loonfs_api::NamespaceId,
    error: loonfs::RuntimeError,
) -> ApiResponseError {
    match error {
        loonfs::RuntimeError::Grep(error) => map_grep_error(namespace_id, error),
        error => ApiResponseError::runtime_for_namespace(namespace_id, error),
    }
}

fn map_grep_error(namespace_id: &loonfs_api::NamespaceId, error: GrepError) -> ApiResponseError {
    let code = error.code();
    match error {
        error @ (GrepError::NotEnabled | GrepError::Backfilling) => {
            ApiResponseError::not_supported(GREP_INDEX_FEATURE, &error.to_string())
        }
        GrepError::Core(error) => {
            ApiResponseError::runtime_for_namespace(namespace_id, loonfs::RuntimeError::Core(error))
        }
        error => ApiResponseError::new(status_for_core_error_code(code), code, &error.to_string()),
    }
}

async fn start_driver_for_query_if_needed(
    state: &AppState,
    namespace_id: &loonfs_api::NamespaceId,
) {
    let Some(drivers) = &state.grep_drivers else {
        return;
    };
    let Ok(Some(root)) = load_grep_root(&*state.writer.object_store(), namespace_id).await else {
        return;
    };
    let needs_catch_up = match root.state().lifecycle() {
        GrepLifecycle::Backfilling { .. } => true,
        GrepLifecycle::Steady => {
            state
                .admin
                .namespace_status(namespace_id)
                .await
                .is_ok_and(|status| {
                    root.state().index().next_delta_index != 0
                        || root.state().index().built_through_seq < status.head_seq
                })
        }
        GrepLifecycle::Disabled => false,
    };
    if needs_catch_up {
        drivers.start(namespace_id);
    }
}
