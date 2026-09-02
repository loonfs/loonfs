//! The `admin/v0` routes whose subject is the backing store itself rather
//! than one namespace.

use super::{AppQuery, AppState, NoQuery, OptionalAppJson};
use crate::http::error::ApiResponseError;
use axum::extract::State;
use axum::Json;
use loonfs_api::v0::{
    StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeRequest, StoreProbeResponse,
};
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome};

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "probe_store",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-retries" = json!({"disabled": true})),
        ),
        path = "/v0/admin/store/probe",
        tag = "admin",
        summary = "Probe the store contract",
        description = "Proves the configured object store honours the create-if-absent, compare-and-swap, visibility, listing, and ranged-read semantics LoonFS depends on, and reports what it found check by check. Nothing runs this implicitly: a probe writes and deletes objects, all of them under a scratch prefix that is not a durable object family, and its last check deletes them and proves the prefix empty. A store that fails a check answers 200 with that check reported `failed` — the probe ran, and the answer is that the store is wrong. Optional capabilities a store declares it lacks answer `unsupported`, which is an answer rather than a fault. This route does not decide whether the deployment may serve presigned direct uploads: that trust comes from the endpoint allowlist, because a probe exercises the server's own request path and never a presigned capability handed to a client.",
        request_body(content = StoreProbeRequest, description = "Probe options; send `{}` for defaults"),
        responses(
            (status = 200, description = "Probe completed; per-check outcomes are in the body", body = StoreProbeResponse),
            (status = 400, description = "Malformed request body", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn probe_store(
    State(state): State<AppState>,
    AppQuery(_): AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<StoreProbeRequest>,
) -> Result<Json<StoreProbeResponse>, ApiResponseError> {
    let StoreProbeRequest {} = request.unwrap_or_default();
    // The run id scopes the objects this run writes, so two probes against
    // one store never collide, and a provider's own log names the run.
    let run_id = loonfs_api::generated_id("probe");
    let report = run_store_contract_probe(state.probe_store.as_ref(), &run_id).await;
    Ok(Json(StoreProbeResponse {
        run_id: report.run_id,
        checks: report
            .checks
            .into_iter()
            .map(|check| {
                let (outcome, message) = match check.outcome {
                    StoreProbeOutcome::Passed => (StoreProbeCheckOutcome::Passed, None),
                    StoreProbeOutcome::Unsupported => (StoreProbeCheckOutcome::Unsupported, None),
                    StoreProbeOutcome::Failed { message } => {
                        (StoreProbeCheckOutcome::Failed, Some(message))
                    }
                };
                StoreProbeCheckResult {
                    name: check.name.to_owned(),
                    outcome,
                    message,
                }
            })
            .collect(),
    }))
}
