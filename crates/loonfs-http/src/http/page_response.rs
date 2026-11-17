//! Time the actual Axum JSON conversion, preserving its bytes and error behavior.
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

pub(super) fn page_response<T: Serialize>(page: T) -> Response {
    let span = loonfs::phase_span!(loonfs::Phase::Serialize, request_id = tracing::field::Empty);
    if let Ok(request_id) = super::middleware::REQUEST_ID.try_with(|id| id.clone()) {
        span.record("request_id", request_id.as_str());
    }
    span.in_scope(|| Json(page).into_response())
}
