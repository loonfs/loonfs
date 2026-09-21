//! Time the actual Axum JSON conversion, preserving its bytes and error behavior.
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

pub(super) fn page_response<T: Serialize>(page: T) -> Response {
    let span = tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "serialize", request_id = tracing::field::Empty);
    if let Ok(request_id) = super::REQUEST_ID.try_with(|id| id.clone()) {
        span.record("request_id", request_id.as_str());
    }
    span.in_scope(|| Json(page).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conversion_preserves_success_and_serialization_errors() {
        async fn compare<T: Serialize + Clone>(value: T) {
            let expected = Json(value.clone()).into_response();
            let actual = page_response(value);
            assert_eq!(actual.status(), expected.status());
            assert_eq!(actual.headers(), expected.headers());
            assert_eq!(
                axum::body::to_bytes(actual.into_body(), usize::MAX)
                    .await
                    .expect("timed body"),
                axum::body::to_bytes(expected.into_body(), usize::MAX)
                    .await
                    .expect("original body"),
            );
        }
        compare(serde_json::json!({"entries": [{"path": "/example"}], "next_cursor": null})).await;
        // JSON cannot encode non-string map keys: retain Axum's 500 response.
        compare(std::collections::BTreeMap::from([((1, 2), 3)])).await;
    }
}
