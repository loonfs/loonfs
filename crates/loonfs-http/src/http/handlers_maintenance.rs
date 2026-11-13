//! The maintenance job handlers of a namespace and of a pool.

use super::error::ApiResponseError;
use super::extractors::{missing_actor, OptionalActorHeader};

use super::{AppJson, AppPath, AppQuery, HttpState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{AdvanceRetentionFloorOptions, GcOptions, MaintainMetadataOptions};

#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    PoolId, RunMaintenanceJobRequest, RunMaintenanceJobResponse, RunPoolMaintenanceJobRequest,
    RunPoolMaintenanceJobResponse,
};
use std::sync::Arc;

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_maintenance_job",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/jobs",
        tag = "maintenance",
        summary = "Run one maintenance job",
        description = "Runs one maintenance job for the namespace. The body's `kind` selects the job, which is one of `metadata`, `gc`, `retention`, `retention_window`, or `recover_administrator`. The response includes the same `kind` and the job's result. A deleted namespace accepts only `gc`. A `metadata` job folds the WAL tail and compacts metadata until nothing is due, or until it publishes 16 compaction units. A compaction unit can take minutes, so the call can too. When the result's `outcome` is `due`, another `metadata` run continues the work. A `gc` job reads the current manifest, lists the snapshots, and then sweeps every object family under the namespace's prefix to the end, including the segments of extensions such as the grep index. The collection of the namespace's pool, which deletes content objects, upload records, and temporary objects, is a job of the pool. Each listing starts at the beginning, and the job doesn't keep a position to continue from. A `retention` job advances the retention floor to the folded manifest head, or to the one target that `to_seq` or `cutoff_at_ms` sets. A `retention_window` job sets the namespace's retention window to `retention_window_ms`, or clears it when the body doesn't include that field.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        request_body(content = RunMaintenanceJobRequest, description = "The maintenance job to run"),
        responses(
            (status = 200, description = "Maintenance job completed", body = RunMaintenanceJobResponse),
            (status = 400, description = "Invalid namespace ID or options", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn run_maintenance_job(
    State(state): State<Arc<HttpState>>,
    OptionalActorHeader(actor_id): OptionalActorHeader,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<RunMaintenanceJobRequest>,
) -> Result<Json<RunMaintenanceJobResponse>, ApiResponseError> {
    let maintenance = &state.maintenance;
    let response = match request {
        RunMaintenanceJobRequest::Metadata(request) => {
            let options = MaintainMetadataOptions::from_request(request).map_err(|error| {
                ApiResponseError::runtime(error).with_body_field_params(&["/max_wal_tail_objects"])
            })?;
            maintenance
                .maintain_metadata_while_due_with_options(
                    &namespace_id,
                    &loonfs::MaintenanceCancellation::new(),
                    &options,
                )
                .await
                .map(|report| RunMaintenanceJobResponse::Metadata(report.response))
        }
        RunMaintenanceJobRequest::Gc(request) => maintenance
            .gc_with_options(&namespace_id, &GcOptions::from_request(request))
            .await
            .map(RunMaintenanceJobResponse::Gc),
        RunMaintenanceJobRequest::Retention(request) => {
            let options = AdvanceRetentionFloorOptions::from_request(request).map_err(|error| {
                ApiResponseError::runtime(error).with_body_field_params(&["/cutoff_at_ms"])
            })?;
            maintenance
                .advance_retention_floor_with_options(&namespace_id, &options)
                .await
                .map(RunMaintenanceJobResponse::Retention)
        }
        RunMaintenanceJobRequest::RetentionWindow(request) => maintenance
            .set_retention_window(&namespace_id, request.retention_window_ms)
            .await
            .map(RunMaintenanceJobResponse::RetentionWindow),
        RunMaintenanceJobRequest::RecoverAdministrator(request) => {
            let actor_id = actor_id.ok_or_else(missing_actor)?;
            let namespace = state.open_writer(&namespace_id, None).await?;
            return namespace
                .recover_administrator(&request.principal_id, &actor_id)
                .await
                .map(|recovered| Json(RunMaintenanceJobResponse::RecoverAdministrator(recovered)))
                .map_err(ApiResponseError::runtime);
        }
    };
    response.map(Json).map_err(ApiResponseError::runtime)
}

/// The `pool_id` path segment of the pool routes.
#[derive(Debug, serde::Deserialize)]
pub(super) struct PoolSegment {
    pool_id: PoolId,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_pool_maintenance_job",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/maintenance/pools/{pool_id}/jobs",
        tag = "maintenance",
        summary = "Run one maintenance job on a pool",
        description = "Runs one maintenance job for the pool. The body's `kind` selects the job, which is `gc`. The response includes the same `kind` and the job's result. A `gc` job reads the pool record, lists the pool's upload sessions, reads the roots of every member of the pool, and then sweeps the pool's content objects, upload records, and temporary objects to the end. Each listing starts at the beginning, and the job doesn't keep a position to continue from.",
        params(("pool_id" = String, Path, description = "The pool ID.")),
        request_body(content = RunPoolMaintenanceJobRequest, description = "The maintenance job to run"),
        responses(
            (status = 200, description = "Maintenance job completed", body = RunPoolMaintenanceJobResponse),
            (status = 400, description = "Invalid pool ID or options", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Pool not found", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn run_pool_maintenance_job(
    State(state): State<Arc<HttpState>>,
    OptionalActorHeader(_): OptionalActorHeader,
    AppPath(PoolSegment { pool_id }): AppPath<PoolSegment>,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<RunPoolMaintenanceJobRequest>,
) -> Result<Json<RunPoolMaintenanceJobResponse>, ApiResponseError> {
    let RunPoolMaintenanceJobRequest::Gc(request) = request;
    state
        .maintenance
        .gc_pool_with_options(&pool_id, &GcOptions::from_request(request))
        .await
        .map(|report| Json(RunPoolMaintenanceJobResponse::Gc(report)))
        .map_err(ApiResponseError::runtime)
}
