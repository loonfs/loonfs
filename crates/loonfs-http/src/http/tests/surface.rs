//! Contract route composition without host operational routes.

use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::RouterSurface;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn binding_routes_exclude_host_routes_and_preserve_maintenance_admission() {
    let directory = tempfile::tempdir().expect("store");
    let (_, mut state) = test_app(
        test_options(directory.path(), "binding-surface"),
        TestAppOptions::default(),
    )
    .await
    .expect("binding state");
    for (surface, serves_maintenance) in [
        (RouterSurface::Standalone, true),
        (RouterSurface::Standalone, false),
        (RouterSurface::Filesystem, false),
    ] {
        Arc::make_mut(&mut state.options).serves_maintenance = serves_maintenance;
        let router = crate::router(state.clone(), surface);
        for path in ["/health", "/readiness", "/metrics"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            let request_id = response.headers()["x-request-id"]
                .to_str()
                .expect("request id")
                .to_owned();
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .expect("body");
            let error: loonfs_api::ApiError = serde_json::from_slice(&body).expect("error");
            assert_eq!(error.code, loonfs_api::ErrorCode::RouteNotFound.as_str());
            assert_eq!(error.request_id.as_deref(), Some(request_id.as_str()));
        }
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/maintenance/store/probe")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            if surface == RouterSurface::Standalone {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::NOT_FOUND
            }
        );
    }
    state.writer.shutdown().await.expect("shutdown writer");
}
