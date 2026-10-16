//! HTTP composition for the standalone server.

mod connection_limit;
mod metrics;
mod serve;
#[cfg(test)]
mod tests;
mod tls;

pub use serve::{
    app, check_config, probe_store, serve, serve_with_shutdown, AppConfig, AppState, ServeError,
};
pub use tls::TlsConfigError;

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use loonfs_http::{authenticate_routes, error_response, observe_routes};
use loonfs_types::{ErrorCode, ErrorResponse};

fn router(state: AppState) -> Router {
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
            ErrorResponse {
                code: ErrorCode::ShuttingDown.as_str().to_owned(),
                message: "the server is shutting down and no longer admits new work".to_owned(),
                feature: None,
                param: None,
                request_id: None,
                details: None,
            },
        );
    }
    "ready".into_response()
}

async fn get_metrics(State(state): State<AppState>) -> Response {
    let rendered = metrics::render(&state);
    ([(CONTENT_TYPE, "text/plain; version=0.0.4")], rendered).into_response()
}
