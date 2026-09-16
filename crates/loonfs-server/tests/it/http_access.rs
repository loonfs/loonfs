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

#[tokio::test]
async fn read_handlers_accept_the_subject_headers() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = loonfs_server::app(
        test_config(
            temp_dir.path().join("store"),
            "read-subject-headers",
            "read-subject-headers",
        ),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("app");
    state
        .writer
        .create_namespace(
            &namespace_id("demo"),
            loonfs::CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    for (principals, status) in [
        ("team", StatusCode::OK),
        ("bad principal", StatusCode::BAD_REQUEST),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v0/namespaces/demo/filesystem/entry?path=/")
                    .header("authorization", "Bearer test-token")
                    .header("Loonfs-Subject", "usr_ada")
                    .header("Loonfs-Principals", principals)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), status);
        if status == StatusCode::BAD_REQUEST {
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let error: ApiError = serde_json::from_slice(&body).expect("error");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
            assert_eq!(error.param.as_deref(), Some("Loonfs-Principals"));
        }
    }
}

fn request(method: &str, uri: &str, body: Option<&str>, headers: &[(&str, &str)]) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token")
        .header("Loonfs-Actor", "service");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_owned())))
        .expect("request")
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&body).expect("json")
}

#[tokio::test]
async fn an_acl_namespace_is_created_over_the_wire() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = loonfs_server::app(
        test_config(temp_dir.path().join("store"), "acl-wire", "acl-wire"),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("app");
    let acl = |grants: &str| {
        format!(
            r#"{{"namespace_id":"demo-acl","access":{{"kind":"acl","principal_scope":"org","root_grants":{grants}}}}}"#
        )
    };
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(&acl(r#"{"team":["read"]}"#)),
            &[],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = serde_json::from_value(json_body(response).await).expect("error");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    assert_eq!(error.param.as_deref(), Some("/access/root_grants"));

    let expected_access = serde_json::json!({"kind": "acl", "principal_scope": "org"});
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(&acl(r#"{"prn_root":["admin"]}"#)),
            &[],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["access"], expected_access);
    let response = router
        .clone()
        .oneshot(request("GET", "/v0/namespaces/demo-acl", None, &[]))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["access"], expected_access);

    let snapshot = Some(r#"{"name":"pin","ttl_ms":60000}"#);
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo-acl/snapshots",
            snapshot,
            &[("Loonfs-Subject", "usr_x"), ("Loonfs-Principals", "nobody")],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo-acl/snapshots",
            snapshot,
            &[],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(request(
            "POST",
            "/v0/maintenance/namespaces/demo-acl/runs",
            Some(r#"{"kind":"recover_administrator","principal_id":"prn_ops"}"#),
            &[],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["kind"], "recover_administrator");
    assert_eq!(body["access_revision_no"], 1);
}
