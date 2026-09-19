//! Hosted admission of content references from another namespace.

#![allow(clippy::panic)]

use crate::common::http_split_support::test_config;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::{CreateNamespaceOptions, PutFileOptions};
use loonfs_api::{
    AccessGrants, AccessRight, AccessRights, ApiError, ErrorCode, NamespaceAccess, PrincipalId,
    PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use loonfs_test_support::ids::namespace_id;
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
    let (router, state) = loonfs_server::app(
        test_config(
            temp_dir.path().join("store"),
            "hosted-content-ref-access",
            "hosted-content-ref-access",
        ),
        loonfs_server::AppOptions::default(),
    )
    .await
    .expect("app");
    let source = namespace_id("source");
    let destination = namespace_id("destination");
    state
        .writer
        .create_namespace(
            &source,
            CreateNamespaceOptions {
                access: acl("administrator"),
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("source namespace");
    state
        .writer
        .create_namespace(
            &destination,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("destination namespace");
    let mut options = PutFileOptions::new(loonfs_test_support::test_actor());
    options.commit.subject = Some(subject("administrator"));
    state
        .writer
        .put_file_bytes(&source, "/private", b"private bytes", options)
        .await
        .expect("publish source");
    let content_ref = state
        .writer
        .reader()
        .as_subject(subject("administrator"))
        .get_path_entry(&source, "/private", Default::default())
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
        state
            .writer
            .reader()
            .get_file_bytes(&destination, "/imported")
            .await
            .expect_err("foreign reference was not published")
            .code(),
        ErrorCode::PathNotFound
    );

    state.writer.shutdown().await.expect("shutdown");
}
