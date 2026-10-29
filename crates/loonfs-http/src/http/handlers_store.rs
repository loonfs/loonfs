//! The `maintenance/v0` routes whose subject is the backing store itself rather
//! than one namespace.

use super::{AppQuery, BindingState, NoQuery, OptionalAppJson};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::Json;
use loonfs_objectstore::probe::run_store_contract_probe;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{StoreProbeRequest, StoreProbeResponse};
use std::sync::Arc;

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "probe_store",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/maintenance/store/probe",
        tag = "maintenance",
        summary = "Probe the store contract",
        description = "Checks that the configured object store supports the create-if-absent, compare-and-swap, visibility, listing, and ranged-read behavior that LoonFS depends on, and reports the result of each check. The probe runs only when a client calls this route, because it writes and deletes objects. It writes them all under a scratch prefix that isn't a durable object family, and its last check deletes them and confirms that the prefix is empty. If the store fails a check, the response is still 200, and it reports that check as `failed`. If the store declares that it doesn't support an optional capability, the response reports that check as `unsupported`, which isn't a failure. The probe doesn't control whether the deployment can serve presigned direct uploads. The endpoint allowlist controls that, because the probe tests only the server's own requests and never a presigned capability issued to a client.",
        request_body(content = StoreProbeRequest, description = "The probe options. Send `{}` to use the defaults."),
        responses(
            (status = 200, description = "The probe completed, and the body reports each check's outcome", body = StoreProbeResponse),
            (status = 400, description = "Malformed request body", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn probe_store(
    State(state): State<Arc<BindingState>>,
    AppQuery(_): AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<StoreProbeRequest>,
) -> Result<Json<StoreProbeResponse>, ApiResponseError> {
    let StoreProbeRequest {} = request.unwrap_or_default();
    // The run id scopes the objects this run writes, so two probes against
    // one store never collide, and a provider's own log names the run.
    let run_id = loonfs_types::ProbeRunId::generate();
    let store = state.runtime.object_store();
    let report = run_store_contract_probe(store.as_ref(), &run_id).await;
    Ok(Json(report.into()))
}
