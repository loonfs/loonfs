//! OpenAPI descriptions for the representative host operational routes.

#[utoipa::path(
        get,
        operation_id = "get_health",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/health",
        tag = "system",
        summary = "Check health",
        description = "Returns `ok` when the server is running and can accept requests.",
        security(()),
        responses(
            (status = 200, description = "Server health check", body = String),
            crate::http::openapi::UnavailableResponses
        )
)]
#[expect(
    dead_code,
    reason = "OpenAPI metadata for a route implemented by the host"
)]
fn get_health() {}

#[utoipa::path(
        get,
        operation_id = "get_readiness",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/readiness",
        tag = "system",
        summary = "Check readiness",
        description = "Returns `ready` while the server admits new work. Once shutdown \
                       begins and publisher admission closes, answers 503 `shutting_down` \
                       so load balancers can drain the instance. `/health` stays the \
                       liveness probe: it only reports that the process is up.",
        security(()),
        responses(
            (status = 200, description = "The server admits new work", body = String),
            crate::http::openapi::UnavailableResponses
        )
)]
#[expect(
    dead_code,
    reason = "OpenAPI metadata for a route implemented by the host"
)]
fn get_readiness() {}

#[utoipa::path(
        get,
        operation_id = "get_metrics",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/metrics",
        tag = "system",
        summary = "Scrape metrics",
        description = "Returns this process's metrics in Prometheus text exposition format \
                       0.0.4. Unlike `/health` and `/readiness`, the route requires the \
                       deployment's bearer token.",
        responses(
            (status = 200, description = "Prometheus text exposition", body = String),
            (status = 401, description = "Missing or invalid bearer token", body = loonfs_api::ApiError),
            crate::http::openapi::UnavailableResponses
        )
)]
#[expect(
    dead_code,
    reason = "OpenAPI metadata for a route implemented by the host"
)]
fn get_metrics() {}
