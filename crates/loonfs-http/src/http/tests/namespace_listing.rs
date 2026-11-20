//! The namespace listing through the router.

use super::fixtures;
use super::*;
use axum::body::Body;
use axum::http::Request;
use serde_json::{json, Value};
use std::collections::BTreeMap;

async fn get(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let (status, _, bytes) = fixtures::send(router, fixtures::request("GET", uri, None, &[])).await;
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON response"),
    )
}

async fn app(directory: &Path) -> axum::Router {
    let (router, _) = test_app(
        test_options(directory, "namespace-listing-writer"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    router
}

/// Checks that the listing pages in namespace ID order, that a deleted
/// namespace keeps its place in a page without being returned, and that each
/// item is the namespace's own metadata.
#[tokio::test]
async fn the_listing_pages_through_live_namespaces_in_key_order() {
    let directory = tempdir().expect("tempdir");
    let router = app(directory.path()).await;
    for namespace_id in ["alpha", "bravo", "charlie"] {
        let body = json!({ "namespace_id": namespace_id }).to_string();
        let request = fixtures::request(
            "POST",
            "/v0/namespaces",
            Some(&body),
            &[("Loonfs-Actor", "namespace-lister")],
        );
        let (status, _, bytes) = fixtures::send(&router, request).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let request = fixtures::request("DELETE", "/v0/namespaces/bravo", None, &[]);
    assert_eq!(fixtures::send(&router, request).await.0, StatusCode::OK);
    let mut metadata = BTreeMap::new();
    for namespace_id in ["alpha", "charlie"] {
        let (status, body) = get(&router, &format!("/v0/namespaces/{namespace_id}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        metadata.insert(namespace_id, body);
    }

    let mut pages = Vec::new();
    let mut uri = "/v0/namespaces?limit=1".to_owned();
    loop {
        let (status, page) = get(&router, &uri).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        pages.push(page["namespaces"].clone());
        let Some(cursor) = page["next_cursor"].as_str() else {
            break;
        };
        uri = format!("/v0/namespaces?limit=1&cursor={cursor}");
    }
    assert_eq!(
        pages,
        [
            json!([metadata["alpha"]]),
            json!([]),
            json!([metadata["charlie"]]),
        ]
    );

    let (status, page) = get(&router, "/v0/namespaces").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(
        page,
        json!({ "namespaces": [metadata["alpha"], metadata["charlie"]] })
    );
}

#[tokio::test]
async fn the_listing_reads_no_identity_headers() {
    let directory = tempdir().expect("tempdir");
    let router = app(directory.path()).await;
    let request = Request::builder()
        .method("GET")
        .uri("/v0/namespaces")
        .header("authorization", "Bearer test-token")
        .body(Body::empty())
        .expect("request");

    let (status, _, bytes) = fixtures::send(&router, request).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body, json!({ "namespaces": [] }));
}

#[tokio::test]
async fn an_invalid_limit_or_cursor_is_rejected() {
    let directory = tempdir().expect("tempdir");
    let router = app(directory.path()).await;
    for (query, param) in [
        ("limit=0", Some("limit")),
        ("limit=1001", Some("limit")),
        ("cursor=not-a-cursor", None),
        ("limitt=1", None),
    ] {
        let (status, body) = get(&router, &format!("/v0/namespaces?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["code"], ErrorCode::InvalidRequest.as_str(), "{query}");
        if let Some(param) = param {
            assert_eq!(body["param"], param, "{query}");
        }
    }
}
