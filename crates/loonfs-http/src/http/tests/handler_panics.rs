//! A handler that panics answers with the error envelope.

use super::{test_app, test_options, TestAppOptions};
use crate::observe_routes;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use tower::ServiceExt;

#[allow(clippy::panic, reason = "the route stands in for a handler with a bug")]
async fn panicking_handler() -> StatusCode {
    panic!("injected handler panic")
}

#[tokio::test]
async fn a_panicking_handler_returns_the_server_error_envelope_with_its_request_id() {
    let directory = tempfile::tempdir().expect("temporary store");
    let (_, state) = test_app(
        test_options(directory.path(), "panicking-handler-writer"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let router = observe_routes(
        Router::new().route(
            "/v0/namespaces/{namespace_id}/panic",
            get(panicking_handler),
        ),
        &state,
    );

    let response = router
        .oneshot(
            Request::get("/v0/namespaces/demo/panic")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("a response, not a dropped connection");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("the request ID header")
        .to_str()
        .expect("an ASCII request ID")
        .to_owned();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read the body");
    let envelope: serde_json::Value = serde_json::from_slice(&body).expect("a JSON envelope");
    assert_eq!(envelope["code"], "server_error");
    assert_eq!(envelope["request_id"], request_id.as_str());
}
