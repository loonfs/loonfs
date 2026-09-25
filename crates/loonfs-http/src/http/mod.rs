//! HTTP contract routes and request middleware over host-supplied handles.

mod commit_content;
mod download_body;
mod error;
mod extractors;
mod handlers_downloads;
mod handlers_filesystem;
mod handlers_inodes;
mod handlers_namespace;
mod handlers_query;
mod handlers_store;
mod handlers_uploads;
pub(crate) mod metrics;
#[cfg(feature = "openapi")]
mod openapi;
mod page_response;
mod query_params;
#[cfg(test)]
mod tests;

pub use self::error::api_error_response;

#[cfg(feature = "openapi")]
pub use self::openapi::openapi_document;
use crate::BindingState;

use self::error::{ApiResponseError, ServedErrorCode};
use self::extractors::{
    acquire_download_permit, authorize, AppJson, AppPath, AppQuery, NamespaceIdPath, NoQuery,
    OptionalAppJson, UploadBodyBytes, UploadBodyStream, UploadControlJson,
    MAX_COMPLETION_BODY_BYTES, MAX_JSON_BODY_BYTES, MAX_UPLOAD_CONTROL_BODY_BYTES,
};
use self::handlers_downloads::{create_download, create_download_by_inode};
use self::handlers_filesystem::{
    create_commit, get_file_bytes, get_path_entry, list_changes, list_file_revisions,
    list_path_entries, list_trash,
};
use self::handlers_inodes::{
    get_file_revision_bytes_by_inode, get_inode, list_file_revisions_by_inode, list_inode_children,
};
use self::handlers_namespace::{
    create_checkpoint, create_namespace, create_snapshot, delete_checkpoint, delete_namespace,
    delete_snapshot, extend_snapshot, fork_namespace, get_namespace, get_namespace_diagnostics,
    list_checkpoints, list_snapshots, run_maintenance,
};
use self::handlers_query::{
    disable_grep_index, enable_grep_index, get_grep_index, grep, grep_index_not_maintained,
    grep_queries_not_served,
};
use self::handlers_uploads::{
    abort_upload, complete_upload, create_upload, get_upload, put_upload_content, sign_upload_parts,
};
use axum::extract::{MatchedPath, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post, put, MethodRouter};
use axum::Router;
use loonfs::ErrorCode;
use loonfs_api::ErrorKind;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestLogSeverity {
    Error,
    Warn,
    Debug,
}

// Membership is limited to streamed content and operator work that is long by design.
const DEADLINE_EXEMPT_ROUTES: &[&str] = &[
    "/v0/namespaces/{namespace_id}/filesystem/content",
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
    "/v0/maintenance/namespaces/{namespace_id}/runs",
    "/v0/maintenance/store/probe",
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
    State(state): State<BindingState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let matched_route = request.extensions().get::<MatchedPath>().cloned();
    let started = request_clock();
    let response = next.run(request).await;
    let status = response.status();
    let severity = request_log_severity(
        status,
        response.extensions().get::<ServedErrorCode>().copied(),
    );
    let route = state.metrics.request_served(
        matched_route.as_ref().map(MatchedPath::as_str),
        &method,
        status,
        started.elapsed().as_secs_f64(),
    );

    REQUEST_ID.with(|request_id| match severity {
        RequestLogSeverity::Error => {
            request_completion_event!(error, method, route, status, started, request_id)
        }
        RequestLogSeverity::Warn => {
            request_completion_event!(warn, method, route, status, started, request_id)
        }
        RequestLogSeverity::Debug => {
            request_completion_event!(debug, method, route, status, started, request_id)
        }
    });
    response
}

fn request_log_severity(
    status: StatusCode,
    served_error_code: Option<ServedErrorCode>,
) -> RequestLogSeverity {
    let Some(ServedErrorCode(code)) = served_error_code else {
        return fallback_request_log_severity(status);
    };
    match code.kind() {
        ErrorKind::Internal | ErrorKind::DataCorruption => RequestLogSeverity::Error,
        ErrorKind::Unauthorized
        | ErrorKind::Forbidden
        | ErrorKind::StoragePermissionDenied
        | ErrorKind::Unavailable
        | ErrorKind::DeadlineExceeded
        | ErrorKind::OutcomeUnknown => RequestLogSeverity::Warn,
        ErrorKind::InvalidRequest
        | ErrorKind::ContentTooLarge
        | ErrorKind::NotSupported
        | ErrorKind::NotFound
        | ErrorKind::MethodNotAllowed
        | ErrorKind::Gone
        | ErrorKind::AlreadyExists
        | ErrorKind::Conflict => RequestLogSeverity::Debug,
        _ => fallback_request_log_severity(status),
    }
}

fn fallback_request_log_severity(status: StatusCode) -> RequestLogSeverity {
    match status {
        status if status.is_server_error() => RequestLogSeverity::Error,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => RequestLogSeverity::Warn,
        _ => RequestLogSeverity::Debug,
    }
}

#[allow(clippy::disallowed_methods)]
fn request_clock() -> std::time::Instant {
    // The measuring boundary the workspace lint points to: this reading
    // becomes a histogram observation and reaches no protocol state.
    std::time::Instant::now()
}

async fn require_bearer_token(
    State(state): State<BindingState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiResponseError> {
    authorize(&state.options.auth_policy, request.headers())?;
    Ok(next.run(request).await)
}

fn gated(
    enabled: bool,
    served: MethodRouter<BindingState>,
    denied: MethodRouter<BindingState>,
) -> MethodRouter<BindingState> {
    if enabled {
        served
    } else {
        denied
    }
}

/// Builds contract routes. Hosts register their own health, readiness, and metrics routes.
pub fn router(state: BindingState) -> Router {
    // Searching an index and keeping one built are separate jobs, so they
    // are separately deployable: the query route exists where this server
    // serves grep, and the three routes that mutate a grep manifest exist where
    // it maintains one.
    let serves_grep = state.options.serves_grep;
    let maintains_index = state.options.maintains_grep_index;
    let mut authenticated = Router::new()
        .route(
            "/v0/capabilities",
            get(handlers_namespace::get_capabilities),
        )
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/{namespace_id}",
            get(get_namespace).delete(delete_namespace),
        )
        .route("/v0/namespaces/{namespace_id}/forks", post(fork_namespace))
        .route(
            "/v0/namespaces/{namespace_id}/snapshots",
            post(create_snapshot).get(list_snapshots),
        )
        .route(
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
            post(extend_snapshot),
        )
        .route(
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
            delete(delete_snapshot),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/entries",
            get(list_path_entries),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/entry",
            get(get_path_entry),
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
            post(create_download),
        )
        .route(
            "/v0/namespaces/{namespace_id}/grep",
            gated(serves_grep, get(grep), get(grep_queries_not_served)),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/revisions",
            get(list_file_revisions),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            get(get_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            get(list_inode_children),
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
            post(create_download_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/trash",
            get(list_trash),
        )
        // A commit that reaches the deadline may still land. The publisher
        // owns an accepted candidate, as after a client disconnect, and the
        // commit receipt makes retry safe.
        .route("/v0/namespaces/{namespace_id}/commits", post(create_commit))
        .route("/v0/namespaces/{namespace_id}/uploads", post(create_upload))
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            // No body-limit layer: the upload route never buffers its
            // body, so a framework limit measured against a buffered read
            // would never fire. `UploadBodyStream` counts the bytes as it
            // forwards them and enforces `upload.service_proxied.max_content_bytes` itself.
            put(put_upload_content),
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
            get(get_upload),
        )
        .route("/v0/namespaces/{namespace_id}/changes", get(list_changes));
    if state.options.serves_maintenance {
        authenticated = authenticated
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
                get(get_namespace_diagnostics),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index",
                gated(
                    maintains_index,
                    get(get_grep_index),
                    get(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
                gated(
                    maintains_index,
                    post(enable_grep_index),
                    post(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
                gated(
                    maintains_index,
                    post(disable_grep_index),
                    post(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
                post(create_checkpoint).get(list_checkpoints),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
                delete(delete_checkpoint),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/runs",
                post(run_maintenance),
            )
            // The one maintenance route whose subject is the store rather than a
            // namespace, so it sits beside them rather than under one.
            .route(
                "/v0/maintenance/store/probe",
                post(handlers_store::probe_store),
            );
    } else {
        authenticated = authenticated.route("/v0/maintenance/{*path}", any(maintenance_not_served));
    }
    let authenticated = authenticate_routes(authenticated.with_state(state.clone()), &state);
    observe_routes(authenticated.fallback(route_not_found), &state)
}

/// Applies the binding's authentication and request deadline to host routes.
pub fn authenticate_routes(router: Router, state: &BindingState) -> Router {
    let request_deadline_ms = state.options.request_deadline_ms;
    router
        .route_layer(middleware::from_fn(move |request: Request, next: Next| {
            with_request_deadline(request_deadline_ms, request, next)
        }))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ))
}

/// Applies error envelopes, correlation IDs, and request metrics to host routes.
pub fn observe_routes(router: Router, state: &BindingState) -> Router {
    router
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            with_request_observability,
        ))
        .layer(middleware::from_fn(with_request_id))
}

/// 404 for paths outside the served surface. Deliberately unauthenticated:
/// the route set is public in the API spec.
async fn route_not_found() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::RouteNotFound,
        "no v0 route matches this path; see the API spec for the served surface",
    )
}

/// 404 for the maintenance routes on a deployment that does not serve that
/// API group. The group is a capability-document `api_groups` entry, not a
/// feature key, so `not_supported` and its `feature` field do not apply.
async fn maintenance_not_served() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::RouteNotFound,
        "this deployment does not serve the maintenance API group; set `maintenance` to \
         `serve_only` or `serve_and_maintain`",
    )
}

/// 405 for matched paths hit with an unserved method.
async fn method_not_allowed() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::MethodNotAllowed,
        "this path exists but does not serve this HTTP method",
    )
}
