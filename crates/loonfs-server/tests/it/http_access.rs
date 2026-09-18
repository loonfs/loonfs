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
async fn subject_headers_distinguish_service_and_subject_authority() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = loonfs_server::app(
        test_config(
            temp_dir.path().join("store"),
            "subject-authority",
            "subject-authority",
        ),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("build app");
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(
                r#"{"namespace_id":"demo-acl","access":{"kind":"acl","principal_scope":"org","root_grants":{"team":["admin"]}}}"#,
            ),
            &[],
        ))
        .await
        .expect("create namespace response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo-acl/snapshots",
            Some(r#"{"name":"pin","ttl_ms":60000}"#),
            &[("Loonfs-Subject", "usr_ada")],
        ))
        .await
        .expect("partial subject response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = serde_json::from_value(json_body(response).await).expect("error");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    assert_eq!(error.message, "missing required header Loonfs-Principals");
    assert_eq!(error.param.as_deref(), Some("Loonfs-Principals"));

    for headers in [
        vec![],
        vec![("Loonfs-Subject", "usr_ada"), ("Loonfs-Principals", "team")],
        vec![("Loonfs-Principals", "team")],
    ] {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/v0/namespaces/demo-acl/snapshots",
                Some(r#"{"name":"pin","ttl_ms":60000}"#),
                &headers,
            ))
            .await
            .expect("snapshot response");
        assert_eq!(response.status(), StatusCode::OK);
    }
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

#[tokio::test]
async fn a_repeated_grant_principal_is_invalid_and_does_not_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = loonfs_server::app(
        test_config(
            temp_dir.path().join("store"),
            "repeated-grant-principal",
            "repeated-grant-principal",
        ),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("app");
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(r#"{"namespace_id":"demo","access":{"kind":"acl","principal_scope":"org_demo","root_grants":{"administrator":["admin"]}}}"#),
            &[],
        ))
        .await
        .expect("create namespace response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo/commits",
            Some(
                r#"{"commit_id":"repeated-grant-principal","operations":[{"kind":"update_access","path":"/","boundary":false,"grants":{"viewer":["read"],"viewer":["manage"]}}]}"#,
            ),
            &[
                ("Loonfs-Subject", "administrator"),
                ("Loonfs-Principals", "administrator"),
            ],
        ))
        .await
        .expect("commit response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = serde_json::from_value(json_body(response).await).expect("error");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());

    let namespace = state
        .writer
        .reader()
        .get_namespace(&namespace_id("demo"))
        .await
        .expect("namespace");
    assert_eq!(namespace.head_seq, loonfs_api::ChangeSeq(0));
}

#[tokio::test]
async fn a_former_server_observes_revocation_on_its_first_read_after_publication() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(temp_dir.path().join("store"), "old-writer", "revocation");
    config.maintenance = loonfs_server::MaintenanceMode::ServeOnly;
    let (old_router, old_state) =
        loonfs_server::app(config.clone(), loonfs_server::AppOptions::default())
            .await
            .expect("old server");
    let response = old_router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(r#"{"namespace_id":"demo","access":{"kind":"acl","principal_scope":"org_demo","root_grants":{"prn_root":["admin"]}}}"#),
            &[],
        ))
        .await
        .expect("create namespace response");
    assert_eq!(response.status(), StatusCode::OK);
    let administrator = [
        ("Loonfs-Subject", "root"),
        ("Loonfs-Principals", "prn_root"),
    ];
    let publication = serde_json::json!({
        "commit_id": loonfs_api::CommitId::generate(),
        "operations": [
            {"kind": "create_directory", "path": "/team"},
            {"kind": "update_access", "path": "/team", "boundary": true,
             "grants": {"viewer": ["read"]}},
            {"kind": "put_file", "path": "/team/file", "behavior": "replace",
             "inline_content": "cHJpdmF0ZSBwYXlsb2Fk"}
        ]
    });
    let response = old_router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo/commits",
            Some(&publication.to_string()),
            &administrator,
        ))
        .await
        .expect("publish response");
    assert_eq!(response.status(), StatusCode::OK);
    config.writer_id = "peer".to_owned();
    let (peer_router, peer_state) =
        loonfs_server::app(config, loonfs_server::AppOptions::default())
            .await
            .expect("peer server");
    let revocation = serde_json::json!({
        "commit_id": loonfs_api::CommitId::generate(),
        "operations": [
            {"kind": "update_access", "path": "/team", "boundary": true, "grants": {}}
        ]
    });
    let response = peer_router
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo/commits",
            Some(&revocation.to_string()),
            &administrator,
        ))
        .await
        .expect("revoke response");
    assert_eq!(response.status(), StatusCode::OK);
    let response = old_router
        .oneshot(request(
            "GET",
            "/v0/namespaces/demo/filesystem/content?path=/team/file",
            None,
            &[
                ("Loonfs-Subject", "viewer"),
                ("Loonfs-Principals", "viewer"),
            ],
        ))
        .await
        .expect("first read response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(response).await["code"],
        ErrorCode::PathNotFound.as_str()
    );
    peer_state.writer.shutdown().await.expect("peer shutdown");
    old_state
        .writer
        .shutdown()
        .await
        .expect("old server shutdown");
}
