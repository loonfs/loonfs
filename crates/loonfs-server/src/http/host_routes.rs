//! The host routes the server adds to the contract routes: health,
//! readiness, and metrics.

use super::metrics;
use super::serve::AppState;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use loonfs_http::{authenticate_routes, error_response, observe_routes};
use loonfs_types::{ErrorCode, ErrorResponse};

pub(super) fn router(state: AppState) -> Router {
    let binding = loonfs_http::router(state.binding.clone());
    let public = Router::new()
        .route("/health", get(get_health))
        .route("/readiness", get(get_readiness))
        .with_state(state.clone());
    let authenticated = authenticate_routes(
        Router::new()
            .route("/metrics", get(get_metrics))
            .with_state(state.clone()),
        &state.binding,
    );
    binding.merge(observe_routes(public.merge(authenticated), &state.binding))
}

async fn get_health() -> &'static str {
    "ok"
}

async fn get_readiness(State(state): State<AppState>) -> Response {
    if state.runtime.is_shutting_down() {
        return error_response(
            ErrorCode::ShuttingDown,
            ErrorResponse::new(
                ErrorCode::ShuttingDown,
                "the server is shutting down and no longer admits new work",
            ),
        );
    }
    "ready".into_response()
}

async fn get_metrics(State(state): State<AppState>) -> Response {
    let rendered = metrics::render(&state);
    ([(CONTENT_TYPE, "text/plain; version=0.0.4")], rendered).into_response()
}
