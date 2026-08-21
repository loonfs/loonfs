//! HTTP route assembly for the LoonFS server.
//!
//! Handlers are grouped by API area, request decoding lives in
//! [`extractors`], and handle construction and listener ownership live in
//! [`serve`].

mod error;
mod extractors;
mod handlers_downloads;
mod handlers_filesystem;
mod handlers_inodes;
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
pub use self::openapi::openapi_document;
#[cfg(feature = "test-support")]
pub use self::serve::app_with_test_transfers;
pub use self::serve::{
    app, app_with_store, check_config, probe_store, serve, serve_with_shutdown, ServeError,
};
pub use self::tls::TlsConfigError;

use self::error::ApiResponseError;
use self::extractors::{
    authorize, server_busy_error, AppJson, AppPath, AppQuery, NamespaceIdPath, OptionalAppJson,
    UploadBodyBytes, UploadBodyStream, UploadControlJson, MAX_COMPLETION_BODY_BYTES,
    MAX_UPLOAD_CONTROL_BODY_BYTES,
};
use self::handlers_downloads::{begin_download, begin_download_by_inode};
use self::handlers_filesystem::{
    apply_commit, get_file_bytes, list_changes, list_file_revisions, list_path_entries, list_trash,
    stat_path,
};
use self::handlers_inodes::{
    get_file_revision_bytes_by_inode, list_file_revisions_by_inode, stat_inode,
};
use self::handlers_namespace::{
    create_checkpoint, create_namespace, delete_namespace, fork_namespace, get_namespace,
    get_namespace_diagnostics, list_checkpoints, maintenance_step, release_checkpoint,
};
use self::handlers_query::{
    disable_grep_index, enable_grep_index, gc_grep_index, get_grep_index_status, grep,
    grep_index_not_maintained, grep_queries_not_served,
};
use self::handlers_store::probe_store as probe_store_handler;
use self::handlers_uploads::{
    abort_upload, begin_upload, complete_upload, get_upload_status, sign_upload_parts,
    upload_content,
};
use self::serve::AppState;
#[cfg(test)]
use self::serve::{
    app_with_store_and_direct_transfers, app_with_store_and_state,
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

macro_rules! request_completion_event {
    ($level:ident, $method:expr, $route:expr, $status:expr, $started:expr, $request_id:expr) => {
        tracing::$level!(
            target: "loonfs_server::http::request",
            method = %$method,
            route = $route,
            status = $status.as_u16(),
            elapsed_ms = u64::try_from($started.elapsed().as_millis()).unwrap_or(u64::MAX),
            request_id = $request_id.as_str(),
            "request completed"
        )
    };
}

// Membership is limited to streamed content and operator work that is long by design.
const DEADLINE_EXEMPT_ROUTES: &[&str] = &[
    "/v0/namespaces/{namespace_id}/filesystem/content",
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
    "/v0/admin/namespaces/{namespace_id}/maintenance/step",
    "/v0/admin/store/probe",
    "/v0/admin/namespaces/{namespace_id}/grep/index/gc",
];

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

/// Cancels bounded request work once the deployment's deadline passes.
async fn with_request_deadline(request_deadline_ms: u64, request: Request, next: Next) -> Response {
    if request
        .extensions()
        .get::<MatchedPath>()
        .is_some_and(|matched| DEADLINE_EXEMPT_ROUTES.contains(&matched.as_str()))
    {
        return next.run(request).await;
    }
    match tokio::time::timeout(
        std::time::Duration::from_millis(request_deadline_ms),
        next.run(request),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => ApiResponseError::new(
            StatusCode::REQUEST_TIMEOUT,
            ErrorCode::DeadlineExceeded,
            &format!(
                "the server cancelled the request at the configured deadline; \
                 request_deadline_ms is {request_deadline_ms} milliseconds"
            ),
        )
        .into_response(),
    }
}

/// Counts, times, and logs every request once it reaches its HTTP outcome.
async fn with_request_observability(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let matched_route = request.extensions().get::<MatchedPath>().cloned();
    let started = request_clock();
    let response = next.run(request).await;
    let status = response.status();
    let route = state.metrics.request_served(
        matched_route.as_ref().map(MatchedPath::as_str),
        &method,
        status,
        started.elapsed().as_secs_f64(),
    );

    REQUEST_ID.with(|request_id| match status {
        status if status.is_server_error() => {
            request_completion_event!(error, method, route, status, started, request_id)
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            request_completion_event!(warn, method, route, status, started, request_id)
        }
        _ => request_completion_event!(debug, method, route, status, started, request_id),
    });
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
        get(grep)
    } else {
        get(grep_queries_not_served)
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
        get(get_grep_index_status)
    } else {
        get(grep_index_not_maintained)
    };
    let request_deadline_ms = state.config.request_deadline_ms;
    Router::new()
        .route("/health", get(health))
        .route("/readiness", get(readiness))
        .route("/metrics", get(get_metrics))
        .route("/v0/capabilities", get(handlers_namespace::capabilities))
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/{namespace_id}",
            get(get_namespace).delete(delete_namespace),
        )
        .route("/v0/namespaces/{namespace_id}/forks", post(fork_namespace))
        .route(
            "/v0/admin/namespaces/{namespace_id}/diagnostics",
            get(get_namespace_diagnostics),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/list",
            get(list_path_entries),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/stat",
            get(stat_path),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/content",
            get(get_file_bytes),
        )
        // The read this deployment authorizes rather than performs. It sits
        // beside the proxied read because it answers the same question
        // about the same path; the route exists everywhere and refuses with
        // `not_supported` where no issuer does, exactly as the direct
        // upload modes do on `POST .../uploads`.
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/downloads",
            post(begin_download),
        )
        .route("/v0/namespaces/{namespace_id}/query/grep", grep_route)
        .route(
            "/v0/admin/namespaces/{namespace_id}/grep/index",
            grep_status_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace_id}/grep/index/enable",
            enable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace_id}/grep/index/disable",
            disable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/{namespace_id}/grep/index/gc",
            grep_gc_route,
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/revisions",
            get(list_file_revisions),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            get(stat_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            get(list_file_revisions_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            get(get_file_revision_bytes_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            post(begin_download_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/trash",
            get(list_trash),
        )
        // A commit that reaches the deadline may still land. The publisher
        // owns an accepted candidate, as after a client disconnect, and the
        // commit receipt makes retry safe.
        .route("/v0/namespaces/{namespace_id}/commits", post(apply_commit))
        .route("/v0/namespaces/{namespace_id}/uploads", post(begin_upload))
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            // No body-limit layer: the upload route never buffers its
            // body, so a framework limit measured against a buffered read
            // would never fire. `UploadBodyStream` counts the bytes as it
            // forwards them and enforces `upload.max_content_bytes` itself.
            put(upload_content),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
            post(sign_upload_parts),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            post(complete_upload),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
            post(abort_upload),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}",
            get(get_upload_status),
        )
        .route("/v0/namespaces/{namespace_id}/changes", get(list_changes))
        .route(
            "/v0/admin/namespaces/{namespace_id}/checkpoints",
            post(create_checkpoint).get(list_checkpoints),
        )
        .route(
            "/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            post(release_checkpoint),
        )
        .route(
            "/v0/admin/namespaces/{namespace_id}/maintenance/step",
            post(maintenance_step),
        )
        // The one admin route whose subject is the store rather than a
        // namespace, so it sits beside them rather than under one.
        .route("/v0/admin/store/probe", post(probe_store_handler))
        .route_layer(middleware::from_fn(move |request: Request, next: Next| {
            with_request_deadline(request_deadline_ms, request, next)
        }))
        // Unmatched paths and wrong methods answer inside the error
        // contract instead of axum's empty default bodies.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        // Request observation runs inside the correlation id, so its trace
        // and measurements name the same request.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            with_request_observability,
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
        operation_id = "health",
        path = "/health",
        tag = "system",
        summary = "Check health",
        description = "Returns `ok` when the server is running and can accept requests.",
        security(()),
        responses(
            (status = 200, description = "Server health check", body = String),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
async fn health() -> &'static str {
    "ok"
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "readiness",
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
            (status = 503, description = "Shutdown has begun; admission is closed", body = loonfs_api::ApiError),
            crate::http::openapi::DeadlineExceededResponses
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

/// Returns this process's metrics in Prometheus text format.
///
/// Unlike `/health` and `/readiness`, this route requires authentication
/// because its output describes deployment activity.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_metrics",
        path = "/metrics",
        tag = "system",
        summary = "Scrape metrics",
        description = "Returns this process's metrics in Prometheus text exposition format \
                       0.0.4. Unlike `/health` and `/readiness`, the route requires the \
                       deployment's bearer token.",
        responses(
            (status = 200, description = "Prometheus text exposition", body = String),
            (status = 401, description = "Missing or invalid bearer token", body = loonfs_api::ApiError),
            crate::http::openapi::DeadlineExceededResponses
        )
    )
)]
async fn get_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    // Sample server state here because these values can change between
    // scrapes. Cache activity is already included in the recorder snapshot.
    let rendered = state.metrics.render(
        state
            .local_cache
            .as_ref()
            .map(|local_cache| local_cache.foyer_stats()),
        state.upload_permits.available_permits(),
        state.download_permits.available_permits(),
    );
    Ok(([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], rendered).into_response())
}
