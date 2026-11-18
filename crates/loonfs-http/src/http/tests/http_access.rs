//! Subject headers and their invalid-request parameter names.

use super::fixtures::{self, test_app, test_options, TestAppOptions};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{ErrorCode, ErrorResponse};
use tempfile::tempdir;
use tower::ServiceExt as _;

#[tokio::test]
async fn subject_headers_are_parsed_and_rejected_with_the_header_named() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(&temp_dir.path().join("store"), "subject-headers"),
        TestAppOptions::default(),
    )
    .await
    .expect("build app");
    state
        .runtime
        .create_namespace(&namespace_id("demo"), &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let too_many = (0..65)
        .map(|index| format!("principal_{index}"))
        .collect::<Vec<_>>()
        .join(",");
    for (principals, subject, expected_param) in [
        ("bad principal", Some("usr_ada"), "Loonfs-Principals"),
        (too_many.as_str(), Some("usr_ada"), "Loonfs-Principals"),
        ("team", Some("bad subject"), "Loonfs-Subject"),
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
        let error: ErrorResponse =
            serde_json::from_value(json_body(response).await).expect("error");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
        assert_eq!(error.param.as_deref(), Some(expected_param));
    }
    let response = router
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo/commits",
            Some(
                r#"{"commit_id":"subject-commit","operations":[{"kind":"create_directory","path":"/docs"}]}"#,
            ),
            &[("Loonfs-Context", "service")],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

/// Checks that a read, a commit, and an upload operation each return
/// `invalid_request` for a request with neither `Loonfs-Principals` nor
/// `Loonfs-Context`, with both, or with a context other than `service`, and
/// that the error identifies the header to fix.
#[tokio::test]
async fn a_request_sends_exactly_one_of_principals_and_the_service_context() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "request-context"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    create_namespace(&router, "demo", r#"{"team":["admin"]}"#).await;
    let commit = r#"{"commit_id":"context-commit","operations":[{"kind":"create_directory","path":"/docs"}]}"#;
    let upload = r#"{"mode":"service_proxied"}"#;
    let routes = [
        ("GET", "/v0/namespaces/demo/filesystem/entry?path=/", None),
        ("POST", "/v0/namespaces/demo/commits", Some(commit)),
        ("POST", "/v0/namespaces/demo/uploads", Some(upload)),
    ];
    for (method, uri, body) in routes {
        for (headers, expected_param, expected_message) in [
            (
                vec![],
                "Loonfs-Principals",
                Some("send Loonfs-Principals or Loonfs-Context: service"),
            ),
            (
                vec![("Loonfs-Principals", "team"), ("Loonfs-Context", "service")],
                "Loonfs-Context",
                None,
            ),
            (vec![("Loonfs-Context", "Service")], "Loonfs-Context", None),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .header("Loonfs-Actor", "service");
            for (name, value) in &headers {
                request = request.header(*name, *value);
            }
            let response = router
                .clone()
                .oneshot(
                    request
                        .body(body.map_or_else(Body::empty, Body::from))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{method} {uri} {headers:?}"
            );
            let error: ErrorResponse =
                serde_json::from_value(json_body(response).await).expect("error");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
            assert_eq!(error.param.as_deref(), Some(expected_param));
            if let Some(expected_message) = expected_message {
                assert_eq!(error.message, expected_message);
            }
        }
    }
    for (method, uri, body) in routes {
        for headers in [
            vec![("Loonfs-Principals", "team")],
            vec![("Loonfs-Context", "service")],
        ] {
            let response = router
                .clone()
                .oneshot(request(method, uri, body, &headers))
                .await
                .expect("response");
            let status = response.status();
            assert!(
                status == StatusCode::OK || status == StatusCode::CONFLICT,
                "{method} {uri} {headers:?}: {status} {}",
                json_body(response).await
            );
        }
    }
}

#[tokio::test]
async fn every_upload_route_acts_as_the_request_subject() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "upload-subject"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    create_namespace(&router, "demo", r#"{"team":["admin"]}"#).await;
    let subject = |id| [("Loonfs-Subject", id), ("Loonfs-Principals", "team")];
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo/uploads",
            Some(r#"{"mode":"service_proxied"}"#),
            &subject("usr_ada"),
        ))
        .await
        .expect("create upload response");
    assert_eq!(response.status(), StatusCode::OK);
    let uri = format!(
        "/v0/namespaces/demo/uploads/{}",
        json_body(response).await["upload_id"]
            .as_str()
            .expect("upload id")
    );

    for (method, path, body) in [
        ("GET", uri.clone(), None),
        ("PUT", format!("{uri}/content"), Some("bytes")),
        (
            "POST",
            format!("{uri}/complete"),
            Some(r#"{"mode":"service_proxied"}"#),
        ),
        ("POST", format!("{uri}/abort"), None),
    ] {
        let response = router
            .clone()
            .oneshot(request(method, &path, body, &subject("usr_bob")))
            .await
            .expect("other subject response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
        let error: ErrorResponse =
            serde_json::from_value(json_body(response).await).expect("error");
        assert_eq!(error.code, ErrorCode::UploadNotFound.as_str());
    }
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("{uri}/abort"),
            None,
            &subject("usr_ada"),
        ))
        .await
        .expect("owner abort response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn subject_headers_distinguish_service_and_subject_authority() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "subject-authority"),
        TestAppOptions::default(),
    )
    .await
    .expect("build app");
    create_namespace(&router, "demo-acl", r#"{"team":["admin"]}"#).await;

    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo-acl/snapshots",
            Some(r#"{"name":"snapshot","ttl_ms":60000}"#),
            &[("Loonfs-Subject", "usr_ada")],
        ))
        .await
        .expect("partial subject response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ErrorResponse = serde_json::from_value(json_body(response).await).expect("error");
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
                Some(r#"{"name":"snapshot","ttl_ms":60000}"#),
                &headers,
            ))
            .await
            .expect("snapshot response");
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn retrieving_a_snapshot_in_an_acl_namespace_requires_an_administrator_subject() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "snapshot-retrieve-acl"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    create_namespace(&router, "demo-acl", r#"{"team":["admin"]}"#).await;
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces/demo-acl/snapshots",
            Some(r#"{"name":"snapshot","ttl_ms":60000}"#),
            &[],
        ))
        .await
        .expect("snapshot response");
    assert_eq!(response.status(), StatusCode::OK);
    let snapshot_id = json_body(response).await["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_owned();
    let uri = format!("/v0/namespaces/demo-acl/snapshots/{snapshot_id}");

    for (principals, status) in [
        (Some("nobody"), StatusCode::FORBIDDEN),
        (Some("team"), StatusCode::OK),
        (None, StatusCode::OK),
    ] {
        let headers = principals.map_or_else(Vec::new, |principals| {
            vec![
                ("Loonfs-Subject", "usr_ada"),
                ("Loonfs-Principals", principals),
            ]
        });
        let response = router
            .clone()
            .oneshot(request("GET", &uri, None, &headers))
            .await
            .expect("retrieve response");
        assert_eq!(response.status(), status, "principals {principals:?}");
    }
}

fn request(method: &str, uri: &str, body: Option<&str>, headers: &[(&str, &str)]) -> Request<Body> {
    let headers = [&[("Loonfs-Actor", "service")][..], headers].concat();
    fixtures::request(method, uri, body, &headers)
}

/// Creates `namespace_id` and sets its root access row to `grants`, a JSON
/// grants map, with an `update_access` commit in the service context.
async fn create_namespace(router: &axum::Router, namespace_id: &str, grants: &str) {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(&format!(r#"{{"namespace_id":"{namespace_id}"}}"#)),
            &[],
        ))
        .await
        .expect("create namespace response");
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v0/namespaces/{namespace_id}/commits"),
            Some(&format!(
                r#"{{"commit_id":"root-access","operations":[{{"kind":"update_access","path":"/","boundary":false,"grants":{grants}}}]}}"#
            )),
            &[("Loonfs-Context", "service")],
        ))
        .await
        .expect("root access response");
    assert_eq!(response.status(), StatusCode::OK);
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&body).expect("json")
}

/// Checks that a namespace is created over the wire without access
/// configuration, that the service context sets its administrators, and
/// that a snapshot then needs an administrator or the service context.
#[tokio::test]
async fn the_service_context_sets_administrators_over_the_wire() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "access-wire"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v0/namespaces",
            Some(r#"{"namespace_id":"demo","access":{"kind":"unrestricted"}}"#),
            &[],
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    create_namespace(&router, "demo", r#"{"prn_root":["admin"]}"#).await;
    let response = router
        .clone()
        .oneshot(request("GET", "/v0/namespaces/demo", None, &[]))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(json_body(response).await.get("access").is_none());

    let snapshot = Some(r#"{"name":"snapshot","ttl_ms":60000}"#);
    for (headers, status) in [
        (
            vec![("Loonfs-Subject", "usr_x"), ("Loonfs-Principals", "nobody")],
            StatusCode::FORBIDDEN,
        ),
        (
            vec![
                ("Loonfs-Subject", "usr_x"),
                ("Loonfs-Principals", "prn_root"),
            ],
            StatusCode::OK,
        ),
        (vec![("Loonfs-Context", "service")], StatusCode::OK),
    ] {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/v0/namespaces/demo/snapshots",
                snapshot,
                &headers,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), status, "{headers:?}");
    }
}

#[tokio::test]
async fn a_repeated_grant_principal_is_invalid_and_does_not_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(&temp_dir.path().join("store"), "repeated-grant-principal"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let namespace_reader = state.runtime.namespace(&namespace_id("demo"));
    create_namespace(&router, "demo", r#"{"administrator":["admin"]}"#).await;

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
    let error: ErrorResponse = serde_json::from_value(json_body(response).await).expect("error");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());

    let namespace = namespace_reader.metadata().await.expect("namespace");
    assert_eq!(namespace.head_seq, loonfs_types::CommitSeq(1));
}

/// Checks that a former server's first `strong` read after another server
/// revokes access sees the revocation.
///
/// Reads on the server that has the namespace's writer session start from
/// the session's position, and the session's manifest check finds the new
/// writer's claim. A `strong` read waits for that check, so a client that
/// changes a permission through one server and reads through another passes
/// `consistency=strong` on that read.
#[tokio::test]
async fn a_former_server_observes_revocation_on_its_first_strong_read() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_options(&temp_dir.path().join("store"), "old-writer");
    config.http.serves_maintenance = true;
    let (old_router, old_state) = test_app(config.clone(), TestAppOptions::default())
        .await
        .expect("old server");
    create_namespace(&old_router, "demo", r#"{"prn_root":["admin"]}"#).await;
    let administrator = [
        ("Loonfs-Subject", "root"),
        ("Loonfs-Principals", "prn_root"),
    ];
    let publication = serde_json::json!({
        "commit_id": loonfs_types::CommitId::generate(),
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
    config.writer_id = loonfs_types::WriterId::parse("peer").expect("writer id");
    let (peer_router, peer_state) = test_app(config, TestAppOptions::default())
        .await
        .expect("peer server");
    let revocation = serde_json::json!({
        "commit_id": loonfs_types::CommitId::generate(),
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
            "/v0/namespaces/demo/filesystem/content?path=/team/file&consistency=strong",
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
    peer_state.runtime.shutdown().await.expect("peer shutdown");
    old_state
        .runtime
        .shutdown()
        .await
        .expect("old server shutdown");
}
