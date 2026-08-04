//! HTTP route assembly for the LoonFS server.
//!
//! Handlers are grouped by API area, request decoding lives in
//! [`extractors`], and handle construction and listener ownership live in
//! [`serve`].

mod error;
mod extractors;
mod handlers_downloads;
mod handlers_filesystem;
mod handlers_namespace;
mod handlers_query;
mod handlers_store;
mod handlers_uploads;
mod metrics;
#[cfg(feature = "openapi")]
mod openapi;
mod serve;
#[cfg(test)]
mod tests;
mod tls;

#[cfg(feature = "openapi")]
pub use self::openapi::{openapi_document, openapi_json_pretty};
pub use self::serve::{app, serve, serve_with_shutdown, ServeError};
pub use self::tls::TlsConfigError;

use self::error::ApiResponseError;
use self::extractors::{
    authorize, server_busy_error, AppJson, AppPath, AppQuery, NamespaceIdPath, OptionalAppJson,
    UploadBodyStream,
};
use self::handlers_downloads::begin_download;
use self::handlers_filesystem::{
    apply_commit, get_file_bytes, list_changes, list_file_revisions, list_path_entries, list_trash,
    stat_path,
};
use self::handlers_namespace::{
    create_checkpoint, create_namespace, delete_namespace, fork_namespace, list_checkpoints,
    maintenance_step, namespace_status, release_checkpoint,
};
use self::handlers_query::{
    disable_grep_index, enable_grep_index, gc_grep_index, grep, grep_index_not_maintained,
    grep_index_status, grep_queries_not_served,
};
use self::handlers_store::probe_store;
use self::handlers_uploads::{
    abort_upload, begin_upload, complete_upload, read_upload_status, sign_upload_parts,
    upload_content,
};
use self::serve::AppState;
#[cfg(test)]
use self::serve::{
    app_with_store, app_with_store_and_direct_transfers, app_with_store_and_state,
    build_handles_with_metrics_jsonl_path, serve_on,
};
use axum::extract::{MatchedPath, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use loonfs::ErrorCode;
#[cfg(test)]
use loonfs::SharedObjectStore;

/// Response header carrying the request's correlation id.
const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    /// Correlation id of the request being served. Scoped around every
    /// handler by [`with_request_id`]; [`error::ApiResponseError`] reads it
    /// when rendering an error body.
    pub(super) static REQUEST_ID: String;
}

/// Assigns each request a correlation id: every response carries it as the
/// `x-request-id` header, and error bodies repeat it as
/// `ApiError.request_id` so a caller's log line and the server's trace can
/// be joined without header plumbing.
async fn with_request_id(request: Request, next: Next) -> Response {
    let request_id = loonfs_api::generated_id("req");
    let mut response = REQUEST_ID
        .scope(request_id.clone(), next.run(request))
        .await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

/// Counts and times every request against the route axum matched it to.
///
/// The label is the route template, never the request's own path: a path
/// carries namespace, upload, and checkpoint ids, and one unbounded label
/// set is how a metrics backend dies. A request that matched no route — a
/// 404 — reports as `unmatched`, which is one label rather than one per
/// probe an internet-facing listener receives.
async fn with_request_metrics(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    let method = request.method().clone();
    let started = request_clock();
    let response = next.run(request).await;
    state.metrics.request_served(
        route.as_deref(),
        &method,
        response.status(),
        started.elapsed().as_secs_f64(),
    );
    response
}

#[allow(clippy::disallowed_methods)]
fn request_clock() -> std::time::Instant {
    // The measuring boundary the workspace lint points to: this reading
    // becomes a histogram observation and reaches no protocol state.
    std::time::Instant::now()
}

fn router(state: AppState) -> Router {
    // Searching an index and keeping one built are separate jobs, so they
    // are separately deployable: the query route exists where this server
    // serves grep, and the three routes that mutate a grep root exist where
    // it maintains one.
    let serves_grep = state.config.grep.mode.serves_grep();
    let maintains_index = state.config.grep.mode.maintains_index();
    let grep_route = if serves_grep {
        post(grep)
    } else {
        post(grep_queries_not_served)
    };
    let enable_grep_route = if maintains_index {
        post(enable_grep_index)
    } else {
        post(grep_index_not_maintained)
    };
    let disable_grep_route = if maintains_index {
        post(disable_grep_index)
    } else {
        post(grep_index_not_maintained)
    };
    let grep_gc_route = if maintains_index {
        post(gc_grep_index)
    } else {
        post(grep_index_not_maintained)
    };
    // Reading the index's lifecycle is administering it, not serving it: a
    // deployment that only answers searches has no authority over the state
    // this reports, so it gates with the mutating three.
    let grep_status_route = if maintains_index {
        get(grep_index_status)
    } else {
        get(grep_index_not_maintained)
    };
    Router::new()
        .route("/health", get(health))
        .route("/readiness", get(readiness))
        .route("/metrics", get(serve_metrics))
        .route("/v0/capabilities", get(handlers_namespace::capabilities))
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/{namespace}",
            get(namespace_status).delete(delete_namespace),
        )
        .route("/v0/namespaces/{namespace}/forks", post(fork_namespace))
        .route(
            "/v0/namespaces/{namespace}/filesystem/list",
            get(list_path_entries),
        )
        .route("/v0/namespaces/{namespace}/filesystem/stat", get(stat_path))
        .route(
            "/v0/namespaces/{namespace}/filesystem/content",
            get(get_file_bytes),
        )
        // The read this deployment authorizes rather than performs. It sits
        // beside the proxied read because it answers the same question
        // about the same path; the route exists everywhere and refuses with
        // `not_supported` where no issuer does, exactly as the direct
        // upload modes do on `POST .../uploads`.
        .route(
            "/v0/namespaces/{namespace}/filesystem/downloads",
            post(begin_download),
        )
        .route("/v0/namespaces/{namespace}/query/grep", grep_route)
        .route(
            "/v0/admin/namespaces/{namespace}/grep/index",
            grep_status_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace}/grep/index/enable",
            enable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace}/grep/index/disable",
            disable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace}/grep/index/gc",
            grep_gc_route,
        )
        .route(
            "/v0/namespaces/{namespace}/filesystem/revisions",
            get(list_file_revisions),
        )
        .route(
            "/v0/namespaces/{namespace}/filesystem/trash",
            get(list_trash),
        )
        .route("/v0/namespaces/{namespace}/commits", post(apply_commit))
        .route("/v0/namespaces/{namespace}/uploads", post(begin_upload))
        .route(
            "/v0/namespaces/{namespace}/uploads/{upload_id}/content",
            // No body-limit layer: the upload route never buffers its
            // body, so a framework limit measured against a buffered read
            // would never fire. `UploadBodyStream` counts the bytes as it
            // forwards them and enforces `upload.max_content_bytes` itself.
            put(upload_content),
        )
        .route(
            "/v0/namespaces/{namespace}/uploads/{upload_id}/parts",
            post(sign_upload_parts),
        )
        .route(
            "/v0/namespaces/{namespace}/uploads/{upload_id}/complete",
            post(complete_upload),
        )
        .route(
            "/v0/namespaces/{namespace}/uploads/{upload_id}/abort",
            post(abort_upload),
        )
        .route(
            "/v0/namespaces/{namespace}/uploads/{upload_id}",
            get(read_upload_status),
        )
        .route("/v0/namespaces/{namespace}/changes", get(list_changes))
        .route(
            "/v0/admin/namespaces/{namespace}/checkpoints",
            post(create_checkpoint).get(list_checkpoints),
        )
        .route(
            "/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release",
            post(release_checkpoint),
        )
        .route(
            "/v0/admin/namespaces/{namespace}/maintenance/step",
            post(maintenance_step),
        )
        // The one admin route whose subject is the store rather than a
        // namespace, so it sits beside them rather than under one.
        .route("/v0/admin/store/probe", post(probe_store))
        // Unmatched paths and wrong methods answer inside the error
        // contract instead of axum's empty default bodies.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        // Request timing runs inside the correlation id, so a slow request's
        // trace and its measurement name the same id.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            with_request_metrics,
        ))
        .layer(middleware::from_fn(with_request_id))
        .with_state(state)
}

/// 404 for paths outside the served surface. Deliberately unauthenticated:
/// the route set is public in the API spec, and `authorize` runs per
/// matched handler.
async fn route_not_found() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::NOT_FOUND,
        ErrorCode::RouteNotFound,
        "no v0 route matches this path; see the API spec for the served surface",
    )
}

/// 405 for matched paths hit with an unserved method.
async fn method_not_allowed() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::MethodNotAllowed,
        "this path exists but does not serve this HTTP method",
    )
}

/// Opens the server's runtime handles inside the serving runtime.
///
/// The long-lived server writer opts into background maintenance; the
/// reader shares its caches so read endpoints observe writes immediately;
/// the admin handle drives the explicit maintenance endpoints under its own
/// actor identity, sharing the writer's decoded-block cache under the
/// configured budget. All three deliberately share one provider client
/// inside this one runtime ownership domain.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/health",
        tag = "health",
        summary = "Check health",
        description = "Returns `ok` when the server is running and can accept requests.",
        security(()),
        responses((status = 200, description = "Server health check", body = String))
    )
)]
async fn health() -> &'static str {
    "ok"
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/readiness",
        tag = "health",
        summary = "Check readiness",
        description = "Returns `ready` while the server admits new work. Once shutdown \
                       begins and publisher admission closes, answers 503 `shutting_down` \
                       so load balancers can drain the instance. `/health` stays the \
                       liveness probe: it only reports that the process is up.",
        security(()),
        responses(
            (status = 200, description = "The server admits new work", body = String),
            (status = 503, description = "Shutdown has begun; admission is closed", body = loonfs_api::ApiError)
        )
    )
)]
async fn readiness(State(state): State<AppState>) -> Result<&'static str, ApiResponseError> {
    if state.writer.is_shutting_down() {
        return Err(ApiResponseError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ShuttingDown,
            "the server is shutting down and no longer admits new work",
        ));
    }
    Ok("ready")
}

/// Content type of the Prometheus text exposition format this server emits.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// Renders this process's metrics.
///
/// Authorized like every other route, unlike `/health` and `/readiness`:
/// those report whether the process is up, while this reports what it has
/// been doing, and a deployment's traffic shape is not public. Scrapers send
/// the same bearer token clients do.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/metrics",
        tag = "health",
        summary = "Scrape metrics",
        description = "Returns this process's metrics in Prometheus text exposition format \
                       0.0.4. Unlike `/health` and `/readiness`, the route requires the \
                       deployment's bearer token.",
        responses(
            (status = 200, description = "Prometheus text exposition", body = String),
            (status = 401, description = "Missing or invalid bearer token", body = loonfs_api::ApiError)
        )
    )
)]
async fn serve_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    // Read at scrape time rather than accumulated: these are levels, and a
    // level is only true when it is asked for.
    let rendered = state.metrics.render(
        &state.writer.runtime_cache_stats(),
        state.upload_permits.available_permits(),
        state.download_permits.available_permits(),
    );
    Ok(([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], rendered).into_response())
}
