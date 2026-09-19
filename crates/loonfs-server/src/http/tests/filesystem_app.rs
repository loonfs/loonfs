use super::*;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

#[tokio::test]
async fn composition_omits_operational_routes_and_preserves_core_behavior() {
    let directory = tempdir().expect("temporary store");
    let config = test_config(directory.path(), "composed-writer");
    let (router, state) = crate::filesystem_app(config, AppOptions::default())
        .await
        .expect("filesystem app");
    assert!(state.runner.is_some(), "background maintenance survives");

    // A missing-auth response would reveal a route or disabled wildcard that
    // was still mounted. Neither operational handlers nor that wildcard belong
    // to a composed filesystem surface.
    for path in [
        "/health",
        "/readiness",
        "/metrics",
        "/v0/maintenance/store/probe",
        "/v0/maintenance/namespaces/example/diagnostics",
        "/v0/maintenance/unrecognized",
    ] {
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
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("body");
        let error: serde_json::Value = serde_json::from_slice(&body).expect("error envelope");
        assert_eq!(error["code"], "route_not_found", "{path}");
    }

    let denied = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v0/capabilities")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let capabilities = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v0/capabilities")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(capabilities.status(), StatusCode::OK);
    let body = axum::body::to_bytes(capabilities.into_body(), 65536)
        .await
        .expect("body");
    let capabilities: CapabilityDocument = serde_json::from_slice(&body).expect("capabilities");
    capabilities.validate().expect("valid capabilities");
    assert!(!capabilities
        .api_groups
        .iter()
        .any(|group| group.starts_with("maintenance")));

    let created = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v0/namespaces")
                .header("authorization", "Bearer test-token")
                .header("loonfs-actor", "composition-test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"namespace_id":"example"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert!(created.status().is_success(), "{}", created.status());
    let fetched = router
        .oneshot(
            Request::builder()
                .uri("/v0/namespaces/example")
                .header("authorization", "Bearer test-token")
                .header("loonfs-actor", "composition-test")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(fetched.status(), StatusCode::OK);
    if let Some(runner) = &state.runner {
        runner.shutdown().await.expect("maintenance shutdown");
    }
    state.writer.shutdown().await.expect("writer shutdown");
}
