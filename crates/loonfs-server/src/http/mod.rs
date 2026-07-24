//! HTTP route assembly for the LoonFS server.
//!
//! Handlers are grouped by API area, request decoding lives in
//! [`extractors`], and listener/lifecycle ownership lives in [`serve`].

#[cfg(test)]
mod direct_put_provider_gate_tests;
mod error;
mod extractors;
mod handlers_filesystem;
mod handlers_namespace;
mod handlers_query;
mod handlers_uploads;
#[cfg(feature = "openapi")]
mod openapi;
mod serve;
#[cfg(test)]
mod tests;

#[cfg(feature = "openapi")]
pub use self::openapi::{openapi_document, openapi_json_pretty};
pub use self::serve::{app, serve, serve_with_shutdown, ServeError, ServerLifecycle};

use self::error::ApiResponseError;
use self::extractors::{
    authorize, server_busy_error, AppJson, AppPath, AppQuery, CommitAppJson, NamespaceIdPath,
    OptionalAppJson, UploadBodyBytes,
};
use self::handlers_filesystem::{
    apply_filesystem_operation, commit_operations, get_file_bytes,
    get_file_revision_bytes_by_inode, list_changes, list_file_revisions,
    list_file_revisions_by_inode, list_path_entries, restore_file_revision_by_inode, stat_path,
};
use self::handlers_namespace::{
    advance_retention_floor, create_checkpoint, create_namespace, delete_namespace, flush_wal,
    fork_namespace, gc_namespace, maintenance_step, namespace_status, release_checkpoint,
    repair_namespace,
};
use self::handlers_query::{
    disable_grep_index, enable_grep_index, gc_grep_index, grep, grep_not_supported,
};
use self::handlers_uploads::{begin_upload, complete_upload, upload_content};
use self::serve::AppState;
#[cfg(test)]
use self::serve::{
    app_with_store, app_with_store_and_state, app_with_store_and_transfer_issuer,
    build_handles_with_metrics_jsonl_path, serve_on,
};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
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

fn router(state: AppState) -> Router {
    // Two routes carry legitimately large bodies and get configured
    // budgets: upload content (raw file bytes) and commits (bulk metadata
    // JSON). Every other route keeps axum's conservative default limit.
    let max_upload_bytes = usize::try_from(state.config.max_upload_bytes).unwrap_or(usize::MAX);
    let max_commit_body_bytes =
        usize::try_from(state.config.max_commit_body_bytes).unwrap_or(usize::MAX);
    let grep_route = if state.config.grep.mode.serves_grep() {
        post(grep)
    } else {
        post(grep_not_supported)
    };
    let enable_grep_route = if state.config.grep.mode.serves_grep() {
        post(enable_grep_index)
    } else {
        post(grep_not_supported)
    };
    let disable_grep_route = if state.config.grep.mode.serves_grep() {
        post(disable_grep_index)
    } else {
        post(grep_not_supported)
    };
    let grep_gc_route = if state.config.grep.mode.serves_grep() {
        post(gc_grep_index)
    } else {
        post(grep_not_supported)
    };
    Router::new()
        .route("/health", get(health))
        .route("/readiness", get(readiness))
        .route("/v0/capabilities", get(handlers_namespace::capabilities))
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/:namespace",
            get(namespace_status).delete(delete_namespace),
        )
        .route("/v0/namespaces/:namespace/forks", post(fork_namespace))
        .route(
            "/v0/namespaces/:namespace/filesystem/list",
            get(list_path_entries),
        )
        .route("/v0/namespaces/:namespace/filesystem/stat", get(stat_path))
        .route(
            "/v0/namespaces/:namespace/filesystem/content",
            get(get_file_bytes),
        )
        .route("/v0/namespaces/:namespace/query/grep", grep_route)
        .route(
            "/v0/admin/namespaces/:namespace/grep/index/enable",
            enable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/:namespace/grep/index/disable",
            disable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/:namespace/grep/index/gc",
            grep_gc_route,
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/revisions",
            get(list_file_revisions),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/operations",
            post(apply_filesystem_operation),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions",
            get(list_file_revisions_by_inode),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:revision_no/content",
            get(get_file_revision_bytes_by_inode),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:source_revision_no/restore",
            post(restore_file_revision_by_inode),
        )
        .route("/v0/namespaces/:namespace/uploads", post(begin_upload))
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/content",
            put(upload_content).layer(DefaultBodyLimit::max(max_upload_bytes)),
        )
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/complete",
            post(complete_upload),
        )
        .route(
            "/v0/namespaces/:namespace/commits",
            post(commit_operations).layer(DefaultBodyLimit::max(max_commit_body_bytes)),
        )
        .route("/v0/namespaces/:namespace/changes", get(list_changes))
        .route(
            "/v0/admin/namespaces/:namespace/checkpoints",
            post(create_checkpoint),
        )
        .route(
            "/v0/admin/namespaces/:namespace/checkpoints/:checkpoint_id/release",
            post(release_checkpoint),
        )
        .route("/v0/admin/namespaces/:namespace/wal/flush", post(flush_wal))
        .route(
            "/v0/admin/namespaces/:namespace/retention/advance",
            post(advance_retention_floor),
        )
        .route(
            "/v0/admin/namespaces/:namespace/maintenance/step",
            post(maintenance_step),
        )
        .route("/v0/admin/namespaces/:namespace/gc", post(gc_namespace))
        .route(
            "/v0/admin/namespaces/:namespace/repair",
            post(repair_namespace),
        )
        // Unmatched paths and wrong methods answer inside the error
        // contract instead of axum's empty default bodies.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
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
    if state.publisher.is_admission_closed() {
        return Err(ApiResponseError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ShuttingDown,
            "publisher admission is closed; the server is shutting down",
        ));
    }
    Ok("ready")
}
