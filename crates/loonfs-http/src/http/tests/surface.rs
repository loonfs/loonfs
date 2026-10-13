//! Contract route composition without host operational routes.

use super::fixtures::{test_app, test_options, TestAppOptions};
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
    for serves_maintenance in [true, false] {
        Arc::make_mut(&mut state.options).serves_maintenance = serves_maintenance;
        let router = crate::router(state.clone());
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
            let error: loonfs_types::ErrorResponse = serde_json::from_slice(&body).expect("error");
            assert_eq!(error.code, loonfs_types::ErrorCode::RouteNotFound.as_str());
            assert_eq!(error.request_id.as_deref(), Some(request_id.as_str()));
        }
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/maintenance/store/probe")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v0/maintenance/namespaces/missing/diagnostics")
                    .header("authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("body");
        let error: loonfs_types::ErrorResponse = serde_json::from_slice(&body).expect("error");
        assert_eq!(
            error.code,
            if serves_maintenance {
                loonfs_types::ErrorCode::NamespaceNotFound
            } else {
                loonfs_types::ErrorCode::RouteNotFound
            }
            .as_str()
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v0/maintenance/unrecognized")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            if serves_maintenance {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::UNAUTHORIZED
            }
        );
    }
    state.runtime.shutdown().await.expect("shutdown writer");
}

async fn post(router: &axum::Router, uri: &str, body: Body) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .body(body)
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, serde_json::from_slice(&body).expect("JSON body"))
}

#[tokio::test]
async fn a_post_route_without_a_request_body_rejects_a_body_and_accepts_none() {
    let directory = tempfile::tempdir().expect("store");
    let (router, state) = test_app(
        test_options(directory.path(), "no-body-routes"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    for uri in [
        "/v0/namespaces/missing/uploads/upl_00000000000000000000000000000001/abort",
        "/v0/maintenance/namespaces/missing/grep/index/enable",
        "/v0/maintenance/namespaces/missing/grep/index/disable",
    ] {
        let (status, error) = post(&router, uri, Body::from("{}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(error["code"], "invalid_request", "{uri}");
        assert!(error.get("param").is_none(), "{uri}: {error}");

        let (status, error) = post(&router, uri, Body::empty()).await;
        assert_ne!(status, StatusCode::BAD_REQUEST, "{uri}: {error}");
    }
    state.runtime.shutdown().await.expect("shutdown writer");
}

#[tokio::test]
async fn a_store_probe_body_up_to_two_mib_is_accepted() {
    const TWO_MIB: usize = 2 * 1024 * 1024;
    let directory = tempfile::tempdir().expect("store");
    let (router, state) = test_app(
        test_options(directory.path(), "probe-body-limit"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let padded = |len: usize| Body::from(format!("{{}}{}", " ".repeat(len - 2)));

    let (status, report) = post(&router, "/v0/maintenance/store/probe", padded(TWO_MIB)).await;
    assert_eq!(status, StatusCode::OK, "{report}");

    let (status, error) = post(&router, "/v0/maintenance/store/probe", padded(TWO_MIB + 1)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error["code"], "content_too_large");
    state.runtime.shutdown().await.expect("shutdown writer");
}
