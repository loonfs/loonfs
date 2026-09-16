//! Subject headers and their invalid-request parameter names.

use crate::common::http_split_support::test_config;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs_api::{ApiError, ErrorCode};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;
use tower::ServiceExt as _;

#[tokio::test]
async fn subject_headers_are_parsed_and_rejected_with_the_header_named() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = loonfs_server::app(
        test_config(
            temp_dir.path().join("store"),
            "subject-headers",
            "subject-headers",
        ),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("build app");
    state
        .writer
        .create_namespace(
            &namespace_id("demo"),
            loonfs::CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let too_many = (0..65)
        .map(|index| format!("principal_{index}"))
        .collect::<Vec<_>>()
        .join(",");
    for (principals, subject, expected_param) in [
        ("team", None, "Loonfs-Subject"),
        ("bad principal", Some("usr_ada"), "Loonfs-Principals"),
        (too_many.as_str(), Some("usr_ada"), "Loonfs-Principals"),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/v0/namespaces/demo/uploads")
            .header("authorization", "Bearer test-token")
            .header("content-type", "application/json")
            .header("Loonfs-Principals", principals);
        if let Some(subject) = subject {
            request = request.header("Loonfs-Subject", subject);
        }
        let response = router
            .clone()
            .oneshot(
                request
                    .body(Body::from(r#"{"mode":"service_proxied"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let error: ApiError = serde_json::from_slice(&body).expect("error");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
        assert_eq!(error.param.as_deref(), Some(expected_param));
    }
    let response = router.oneshot(Request::builder().method("POST").uri("/v0/namespaces/demo/commits")
        .header("authorization", "Bearer test-token")
        .header("content-type", "application/json")
        .header("Loonfs-Actor", "service")
        .header("Loonfs-Subject", "usr_ada")
        .header("Loonfs-Principals", "team")
        .body(Body::from(r#"{"commit_id":"subject-commit","operations":[{"kind":"create_directory","path":"/docs"}]}"#))
        .expect("request")).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}
