//! Request middleware: correlation IDs, deadlines, panic containment,
//! request logs and metrics, and bearer authentication.

use super::error::{ApiResponseError, ServedErrorCode};
use super::extractors::authorize;
use super::metrics::{route_label, UNMATCHED_ROUTE};
use super::router::{find_route, method_not_allowed, Deadline};
use crate::BindingState;
use axum::extract::{MatchedPath, RawPathParams, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{RequestExt as _, Router};
use futures::FutureExt as _;
use loonfs::ErrorCode;
use loonfs_types::ErrorKind;
use std::any::Any;
use std::sync::Arc;

/// Response header carrying the request's correlation id.
pub(super) const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    /// Correlation id of the request being served. Scoped around every
    /// handler by [`with_request_id`]; [`ApiResponseError`] reads it
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
            elapsed_ms = loonfs_types::saturating_duration_ms($started.elapsed()),
            request_id = $request_id.as_str(),
            "request completed"
        )
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RequestLogSeverity {
    Error,
    Warn,
    Debug,
}

/// Assigns each request a correlation id: every response carries it as the
/// `x-request-id` header, and error bodies repeat it as
/// `ErrorResponse.request_id` so a caller's log line and the server's trace can
/// be joined without header plumbing.
pub(super) async fn with_request_id(request: Request, next: Next) -> Response {
    let request_id = loonfs_types::generated_id("req");
    let mut response = REQUEST_ID
        .scope(request_id.clone(), next.run(request))
        .await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

/// Cancels bounded request work once the deployment's deadline passes.
#[expect(
    clippy::disallowed_methods,
    reason = "the configured HTTP request deadline, so time only cancels a request and decides no stored result"
)]
pub(super) async fn with_request_deadline(
    request_deadline_ms: u64,
    request: Request,
    next: Next,
) -> Response {
    if request
        .extensions()
        .get::<MatchedPath>()
        .and_then(|matched| find_route(request.method(), matched.as_str()))
        .is_some_and(|route| route.deadline == Deadline::Exempt)
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

/// Turns a panic in a handler into a `server_error` response.
///
/// Tokio contains a panic to its task, so without this layer a panicking
/// handler drops the connection with no body, no request ID, and no log, and
/// a client treats that as a transport failure it may retry. With it, the
/// caller gets the error envelope and the request ID, and the server logs the
/// panic at error level. The rest of the process keeps serving; a panic is a
/// bug, so the work the handler had started may be half done.
async fn catch_handler_panic(mut request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    let namespace_id = request
        .extract_parts::<RawPathParams>()
        .await
        .ok()
        .and_then(|parameters| {
            parameters
                .iter()
                .find(|(name, _)| *name == "namespace_id")
                .map(|(_, segment)| segment.to_owned())
        });
    match std::panic::AssertUnwindSafe(next.run(request))
        .catch_unwind()
        .await
    {
        Ok(response) => response,
        Err(panic) => {
            let panic = panic_message(panic.as_ref());
            REQUEST_ID.with(|request_id| {
                tracing::error!(
                    request_id = request_id.as_str(),
                    route = route.as_deref(),
                    namespace_id = namespace_id.as_deref(),
                    panic,
                    "request handler panicked"
                );
            });
            ApiResponseError::new(
                ErrorCode::ServerError,
                "the server hit a bug while handling the request",
            )
            .into_response()
        }
    }
}

/// Returns the message that a panic carried, or a fixed description when
/// the payload isn't a string.
pub fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("a panic payload that isn't a string")
}

/// Counts, times, and logs every request once it reaches its HTTP outcome.
async fn with_request_observability(
    State(state): State<Arc<BindingState>>,
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
    let route = matched_route.as_ref().map(MatchedPath::as_str);
    state.metrics.request_served(
        route_label(route),
        &method,
        status,
        started.elapsed().as_secs_f64(),
    );
    let route = route.unwrap_or(UNMATCHED_ROUTE);

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

pub(super) fn request_log_severity(
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
        | ErrorKind::RangeNotSatisfiable
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

#[expect(
    clippy::disallowed_methods,
    reason = "the measuring boundary the workspace lint points to: this reading becomes a histogram observation and reaches no protocol state"
)]
fn request_clock() -> std::time::Instant {
    std::time::Instant::now()
}

async fn require_bearer_token(
    State(state): State<Arc<BindingState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiResponseError> {
    authorize(&state.options.auth_policy, request.headers())?;
    Ok(next.run(request).await)
}

/// Applies the binding's authentication and request deadline to host routes.
pub fn authenticate_routes(router: Router, state: &BindingState) -> Router {
    authenticate(router, Arc::new(state.clone()))
}

/// Applies the binding's authentication and request deadline to `router`.
pub(super) fn authenticate(router: Router, state: Arc<BindingState>) -> Router {
    let request_deadline_ms = state.options.request_deadline_ms;
    router
        .route_layer(middleware::from_fn(move |request: Request, next: Next| {
            with_request_deadline(request_deadline_ms, request, next)
        }))
        .route_layer(middleware::from_fn_with_state(state, require_bearer_token))
}

/// Applies request admission, usage reports, error envelopes, correlation IDs, and metrics
/// to host routes.
pub fn observe_routes(router: Router, state: &BindingState) -> Router {
    observe(router, Arc::new(state.clone()))
}

/// Applies request admission, usage reports, error envelopes, correlation IDs, and metrics
/// to `router`.
pub(super) fn observe(router: Router, state: Arc<BindingState>) -> Router {
    router
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn(catch_handler_panic))
        .layer(tower::util::MapResponseLayer::new(
            super::response_frames::split_response,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            super::usage::report_usage,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            super::request_limit::admit_request,
        ))
        .layer(middleware::from_fn_with_state(
            state,
            with_request_observability,
        ))
        .layer(middleware::from_fn(with_request_id))
}
