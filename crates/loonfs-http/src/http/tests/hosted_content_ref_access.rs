//! Hosted admission of content references from another namespace.

#![allow(clippy::panic)]

use super::fixtures::{test_app, test_options, TestAppOptions};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::{CreateNamespaceOptions, PutFileOptions};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, ApiError, ErrorCode, NamespaceAccess, PrincipalId,
    PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::tempdir;
use tower::ServiceExt as _;

fn subject(principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn acl(administrator: &str) -> NamespaceAccess {
    NamespaceAccess::Acl {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        root_grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse(administrator).expect("principal"),
            AccessRights::from_iter([AccessRight::Admin]),
        )]))
        .expect("grants"),
    }
}

fn request(method: &str, uri: &str, body: Option<String>) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token")
        .header("Loonfs-Actor", "hosted-test")
        .header("Loonfs-Subject", "stranger")
        .header("Loonfs-Principal-Scope", "org_demo")
        .header("Loonfs-Principals", "stranger");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    request
        .body(body.map_or_else(Body::empty, Body::from))
        .expect("request")
}

#[tokio::test]
async fn hosted_subject_cannot_import_a_foreign_bare_content_reference() {
    let temp_dir = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(&temp_dir.path().join("store"), "hosted-content-ref-access"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let source = namespace_id("source");
    let source_namespace = state
        .runtime
        .read_only()
        .with_subject(subject("administrator"))
        .namespace(&source);
    let destination = namespace_id("destination");
    let destination_namespace = state.runtime.namespace(&destination);
    state
        .runtime
        .create_namespace_with_options(
            &source,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: acl("administrator"),
                ..Default::default()
            },
        )
        .await
        .expect("source namespace");
    let namespace = state
        .runtime
        .open_namespace(&source)
        .expect("open namespace");
    state
        .runtime
        .create_namespace(&destination, &loonfs_test_support::test_actor())
        .await
        .expect("destination namespace");
    let options = PutFileOptions::default();
    namespace
        .with_subject(subject("administrator"))
        .put_file_with_options(
            "/private",
            b"private bytes",
            &loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("publish source");
    let content_ref = source_namespace
        .stat("/private")
        .await
        .expect("source entry")
        .content_ref()
        .expect("content reference")
        .clone();

    let response = router
        .clone()
        .oneshot(request(
            "GET",
            "/v0/namespaces/source/filesystem/content?path=/private",
            None,
        ))
        .await
        .expect("source read response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let error: ApiError = serde_json::from_slice(&bytes).expect("api error");
    assert_eq!(error.code, ErrorCode::PathNotFound.as_str());

    let body = json!({
        "commit_id": "foreign-bare-content-ref",
        "operations": [{
            "kind": "put_file",
            "path": "/imported",
            "content_ref": content_ref
        }]
    })
    .to_string();
    let response = router
        .oneshot(request(
            "POST",
            "/v0/namespaces/destination/commits",
            Some(body),
        ))
        .await
        .expect("import response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let error: ApiError = serde_json::from_slice(&bytes).expect("api error");
    assert_eq!(error.code, ErrorCode::ContentNotPrepared.as_str());
    assert_eq!(
        destination_namespace
            .read_file("/imported")
            .await
            .expect_err("foreign reference was not published")
            .code(),
        ErrorCode::PathNotFound
    );

    state.runtime.shutdown().await.expect("shutdown");
}
