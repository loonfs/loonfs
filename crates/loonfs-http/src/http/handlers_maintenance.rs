//! The maintenance job handler.

use super::error::ApiResponseError;
use super::extractors::{missing_actor, OptionalActorHeader};

use super::handlers_query::map_grep_error;

use super::{AppJson, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{AdvanceRetentionFloorOptions, GcOptions, MaintainMetadataOptions};

#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{RunMaintenanceRequest, RunMaintenanceResponse};
use std::sync::Arc;

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_maintenance",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/runs",
        tag = "maintenance",
        summary = "Run one maintenance job",
        description = "Runs one maintenance job for the namespace. The body's `kind` selects the job, which is one of `metadata`, `gc`, `retention`, `retention_window`, or `recover_administrator`. The response includes the same `kind` and the job's result. A deleted namespace accepts only `gc`. A `metadata` job folds the WAL tail and compacts metadata until nothing is due, or until it publishes 16 compaction units. A compaction unit can take minutes, so the call can too. When the result's `outcome` is `due`, another `metadata` run continues the work. A `gc` job reads the current manifest, lists the pins, and then sweeps every object family to the end. Each listing starts at the beginning, and the job doesn't keep a position to continue from. When the deployment maintains a grep index, the job then deletes aged, unreferenced grep index objects, and the result includes their counts in `extensions.grep`. A `retention` job advances the retention floor to the folded manifest head, or to the one target that `to_seq` or `cutoff_at_ms` sets. A `retention_window` job sets the namespace's retention window to `retention_window_ms`, or clears it when the body doesn't include that field.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        request_body(content = RunMaintenanceRequest, description = "The maintenance job to run"),
        responses(
            (status = 200, description = "Maintenance job completed", body = RunMaintenanceResponse),
            (status = 400, description = "Invalid namespace ID or options", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn run_maintenance(
    State(state): State<Arc<BindingState>>,
    OptionalActorHeader(actor_id): OptionalActorHeader,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<RunMaintenanceRequest>,
) -> Result<Json<RunMaintenanceResponse>, ApiResponseError> {
    let maintenance = &state.maintenance;
    let response = match request {
        RunMaintenanceRequest::Metadata(request) => {
            let options = MaintainMetadataOptions::from_request(request)
                .map_err(ApiResponseError::runtime)?;
            maintenance
                .maintain_metadata_while_due_with_options(
                    &namespace_id,
                    &loonfs::MaintenanceCancellation::new(),
                    &options,
                )
                .await
                .map(RunMaintenanceResponse::Metadata)
        }
        RunMaintenanceRequest::Gc(request) => {
            return crate::gc_namespace(
                maintenance,
                state.grep.worker(),
                &namespace_id,
                &GcOptions::from_request(request),
            )
            .await
            .map(|collected| Json(RunMaintenanceResponse::Gc(collected)))
            .map_err(map_grep_error);
        }
        RunMaintenanceRequest::Retention(request) => {
            let options = AdvanceRetentionFloorOptions::from_request(request)
                .map_err(ApiResponseError::runtime)?;
            maintenance
                .advance_retention_floor_with_options(&namespace_id, &options)
                .await
                .map(RunMaintenanceResponse::Retention)
        }
        RunMaintenanceRequest::RetentionWindow(request) => maintenance
            .set_retention_window(&namespace_id, request.retention_window_ms)
            .await
            .map(RunMaintenanceResponse::RetentionWindow),
        RunMaintenanceRequest::RecoverAdministrator(request) => {
            let actor_id = actor_id.ok_or_else(missing_actor)?;
            let namespace = state.open_writer(&namespace_id, None).await?;
            return namespace
                .recover_administrator(&request.principal_id, &actor_id)
                .await
                .map(|recovered| Json(RunMaintenanceResponse::RecoverAdministrator(recovered)))
                .map_err(ApiResponseError::for_namespace_writer(
                    state.namespaces.as_ref(),
                    &namespace_id,
                ));
        }
    };
    response.map(Json).map_err(ApiResponseError::runtime)
}
