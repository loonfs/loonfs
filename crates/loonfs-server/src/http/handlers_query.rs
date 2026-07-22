//! The `query/v0` plane: derived-index reads.

use super::{authorize, AppJson, AppState, NamespaceIdPath};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use loonfs_api::v0::{
    DisableGramsIndexResponse, EnableGramsIndexResponse, GrepGcResponse, GrepRequest, GrepResponse,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::FEATURE_QUERY_GREP;
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{GrepDisableOutcome, GrepEnableOutcome};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/query/grep",
        tag = "query",
        summary = "Content search",
        description = "Searches file content with a regular expression, accelerated by the namespace's gram index. Matches are verified against the real pattern and returned in ascending `(inode_id, byte_offset)` order; revisions committed after the index watermark are scanned exhaustively unless `allow_stale` skips them. Requires this deployment to serve grep and the namespace to carry a materialized steady-state grep root.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = GrepRequest,
        responses(
            (status = 200, description = "One page of matches", body = GrepResponse),
            (status = 400, description = "Invalid pattern, cursor, or an unindexable pattern without allow_scan", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Grep serving is disabled or the gram index is not materialized on this namespace", body = ApiError),
            (status = 503, description = "The index trails the head past the scan budget", body = ApiError)
        )
    )
)]
pub(super) async fn grep(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
    AppJson(request): AppJson<GrepRequest>,
) -> Result<Json<GrepResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    start_driver_for_query_if_needed(&state, &namespace_id).await;
    let response = state
        .reader
        .grep(&namespace_id, &request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
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
        path = "/v0/admin/namespaces/{namespace}/index/grams/enable",
        tag = "admin",
        summary = "Enable the gram index",
        description = "Enables the namespace's grep root. Embedded mode immediately starts that namespace's event-driven backfill driver; serve-only deployments rely on their explicitly assigned external driver. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root enabled or already enabled", body = EnableGramsIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
            (status = 503, description = "Lost a grep root-pointer publication race; retry", body = ApiError)
        )
    )
)]
pub(super) async fn enable_grams_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<EnableGramsIndexResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let outcome = state
        .grep_worker
        .as_ref()
        .expect("grep routes should carry a grep worker")
        .enable(&namespace_id)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(
                &namespace_id,
                loonfs::RuntimeError::Core(error),
            )
        })?;
    let response = match outcome {
        GrepEnableOutcome::Enabled { target_seq } => EnableGramsIndexResponse {
            namespace_id: namespace_id.clone(),
            built_through_seq: target_seq,
            already_enabled: false,
        },
        GrepEnableOutcome::AlreadyEnabled { built_through_seq } => EnableGramsIndexResponse {
            namespace_id: namespace_id.clone(),
            built_through_seq,
            already_enabled: true,
        },
        GrepEnableOutcome::Superseded => {
            return Err(ApiResponseError::runtime_for_namespace(
                &namespace_id,
                loonfs::RuntimeError::Core(loonfs::CoreError::CheckpointUnavailable(
                    "enabling grep lost a root publication race; retry".to_owned(),
                )),
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
        path = "/v0/admin/namespaces/{namespace}/index/grams/disable",
        tag = "admin",
        summary = "Disable the gram index",
        description = "Disables the namespace's grep root, clears its segment references, and stops its embedded driver. Explicit grep garbage collection later reclaims the segments. Idempotent.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Grep root disabled or already disabled", body = DisableGramsIndexResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError),
            (status = 503, description = "Lost a grep root-pointer publication race; retry", body = ApiError)
        )
    )
)]
pub(super) async fn disable_grams_index(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    headers: HeaderMap,
) -> Result<Json<DisableGramsIndexResponse>, ApiResponseError> {
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
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(
                &namespace_id,
                loonfs::RuntimeError::Core(error),
            )
        })?;
    let response = match outcome {
        GrepDisableOutcome::Disabled => DisableGramsIndexResponse {
            namespace_id: namespace_id.clone(),
            was_enabled: true,
        },
        GrepDisableOutcome::NotEnabled => DisableGramsIndexResponse {
            namespace_id: namespace_id.clone(),
            was_enabled: false,
        },
        GrepDisableOutcome::Superseded => {
            return Err(ApiResponseError::runtime_for_namespace(
                &namespace_id,
                loonfs::RuntimeError::Core(loonfs::CoreError::CheckpointUnavailable(
                    "disabling grep lost a root publication race; retry".to_owned(),
                )),
            ));
        }
    };
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/admin/namespaces/{namespace}/index/grams/gc",
        tag = "admin",
        summary = "Collect gram-index garbage",
        description = "Runs one explicit garbage-collection pass over only this namespace's grep-owned extension keyspace. A tombstoned or absent namespace has aged extension state reaped; no grep garbage collection runs implicitly.",
        params(("namespace" = String, Path, description = "Namespace id")),
        responses(
            (status = 200, description = "Namespace grep garbage collection completed", body = GrepGcResponse),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 501, description = "Grep serving is disabled for this deployment", body = ApiError)
        )
    )
)]
pub(super) async fn gc_grams_index(
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
        .garbage_collect_namespace(
            &namespace_id,
            current_time_ms().map_err(|error| {
                ApiResponseError::runtime_for_namespace(
                    &namespace_id,
                    loonfs::RuntimeError::Core(error),
                )
            })?,
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(
                &namespace_id,
                loonfs::RuntimeError::Core(error),
            )
        })?;
    Ok(Json(GrepGcResponse {
        namespace_id,
        deleted_segments: report.deleted_segments,
        deleted_other_objects: report.deleted_other_objects,
        namespace_reaped: report.namespace_reaped,
        retained_candidates: report.retained_candidates,
        namespace_degraded: report.namespace_degraded,
    }))
}

async fn start_driver_for_query_if_needed(
    state: &AppState,
    namespace_id: &loonfs_api::NamespaceId,
) {
    let Some(drivers) = &state.grep_drivers else {
        return;
    };
    let Ok(Some(root)) = load_grep_root(&*state.store, namespace_id).await else {
        return;
    };
    let needs_catch_up = match root.state().lifecycle() {
        GrepLifecycle::Backfilling { .. } => true,
        GrepLifecycle::Steady => state
            .admin
            .namespace_status(namespace_id)
            .await
            .is_ok_and(|status| root.state().index().built_through_seq < status.head_seq),
        GrepLifecycle::Disabled => false,
    };
    if needs_catch_up {
        drivers.start(namespace_id);
    }
}

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> Result<u64, loonfs::CoreError> {
    // Explicit server grep GC enters wall time at the admin boundary; durable replay stays deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|error| {
            loonfs::CoreError::Internal(format!("system clock before unix epoch: {error}"))
        })
}
