//! Direct-put provider-gate HTTP and capability matrix.

#![allow(clippy::panic)]
// HTTP response tests panic in unexpected branches for precise diagnostics.

use super::app_with_store_and_transfer_issuer;
use crate::config::{DirectPutConfig, GrepConfig, RuntimeCacheConfigOverrides};
use crate::ServerConfig;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::CreateNamespaceOptions;
use loonfs_api::v0::{BeginUploadRequest, BeginUploadResponse, UploadMode};
use loonfs_api::{
    ApiError, CapabilityDocument, ContentRef, ErrorCode, NamespaceId, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::{ObjectTransferIssuer, S3CompatiblePresigner, S3PresignerConfig};
use loonfs_objectstore::{SharedObjectStore, StoreConfig as ObjectStoreConfig};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn unproven_endpoint_without_opt_in_refuses_direct_put_and_omits_capability() {
    assert_direct_put_case(
        Some("https://gateway.example"),
        false,
        DirectPutOutcome::NotSupported,
    )
    .await;
}

#[tokio::test]
async fn unproven_endpoint_with_opt_in_begins_direct_put_and_advertises_capability() {
    assert_direct_put_case(
        Some("https://gateway.example"),
        true,
        DirectPutOutcome::Begun,
    )
    .await;
}

#[tokio::test]
async fn proven_provider_begins_direct_put_and_advertises_capability_by_default() {
    assert_direct_put_case(None, false, DirectPutOutcome::Begun).await;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectPutOutcome {
    NotSupported,
    Begun,
}

async fn assert_direct_put_case(
    endpoint_url: Option<&str>,
    allow_unproven: bool,
    expected: DirectPutOutcome,
) {
    let temp_dir = tempdir().expect("store tempdir");
    let store =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store")) as SharedObjectStore;
    let config = server_config(endpoint_url, allow_unproven);
    let issuer = direct_put_issuer(endpoint_url);
    let (router, lifecycle, state) =
        app_with_store_and_transfer_issuer(config, store, Some(issuer))
            .await
            .expect("build provider-gate app");
    let namespace_id = NamespaceId::parse("direct-put-gate").expect("namespace id");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let capabilities: CapabilityDocument =
        response_json(send(&router, Method::GET, "/v0/capabilities", None).await).await;
    let expected_enabled = expected == DirectPutOutcome::Begun;
    assert_eq!(
        capabilities
            .features
            .contains_key(FEATURE_UPLOADS_DIRECT_PUT),
        expected_enabled
    );
    assert_eq!(
        capabilities.supports(FEATURE_UPLOADS_DIRECT_PUT),
        expected_enabled
    );
    assert_eq!(
        capabilities
            .limits
            .get(LIMIT_UPLOAD_MAX_CONTENT_BYTES)
            .copied(),
        Some(1024 * 1024),
        "the proxied-upload limit remains advertised in every gate state"
    );

    let request = BeginUploadRequest {
        mode: Some(UploadMode::DirectPut),
        content_ref: Some(ContentRef::whole_file_v0(b"provider gate")),
    };
    let response = send(
        &router,
        Method::POST,
        &format!("/v0/namespaces/{namespace_id}/uploads"),
        Some(serde_json::to_vec(&request).expect("encode begin request")),
    )
    .await;
    match expected {
        DirectPutOutcome::NotSupported => {
            assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
            let error: ApiError = response_json(response).await;
            assert_eq!(error.code, ErrorCode::NotSupported.as_str());
            assert_eq!(error.feature.as_deref(), Some(FEATURE_UPLOADS_DIRECT_PUT));
        }
        DirectPutOutcome::Begun => {
            assert_eq!(response.status(), StatusCode::OK);
            let begin: BeginUploadResponse = response_json(response).await;
            assert_eq!(begin.mode, UploadMode::DirectPut);
            assert!(begin.direct_put.is_some());
        }
    }

    let response = send(
        &router,
        Method::POST,
        &format!("/v0/namespaces/{namespace_id}/uploads"),
        Some(serde_json::to_vec(&BeginUploadRequest::default()).expect("encode proxied request")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let proxied: BeginUploadResponse = response_json(response).await;
    assert_eq!(proxied.mode, UploadMode::ServiceProxied);
    assert!(proxied.direct_put.is_none());

    lifecycle.shutdown().await.expect("drain lifecycle");
}

fn server_config(endpoint_url: Option<&str>, allow_unproven: bool) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: None,
        content_token_secret: "direct-put-gate-secret".into(),
        writer_id: "direct-put-gate".to_owned(),
        writer_version: "direct-put-gate/0.1.0".to_owned(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: GrepConfig::default(),
        direct_put: DirectPutConfig { allow_unproven },
        background_maintenance: false,
        min_publish_interval_ms: 0,
        max_upload_bytes: 1024 * 1024,
        max_download_bytes: 1024 * 1024,
        max_commit_body_bytes: 1024 * 1024,
        max_concurrent_uploads: 2,
        max_concurrent_downloads: 2,
        max_concurrent_maintenance: 2,
        allow_unauthenticated_remote: false,
        store: ObjectStoreConfig::AwsS3 {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: endpoint_url.map(str::to_owned),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("gate".to_owned()),
            force_path_style: endpoint_url.is_some(),
        },
    }
}

fn direct_put_issuer(endpoint_url: Option<&str>) -> Arc<dyn ObjectTransferIssuer> {
    Arc::new(
        S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: endpoint_url.map(str::to_owned),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("gate".to_owned()),
            force_path_style: endpoint_url.is_some(),
        })
        .expect("build test presigner"),
    )
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<Vec<u8>>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(body) => {
            builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
            Body::from(body)
        }
        None => Body::empty(),
    };
    router
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("router response")
}

async fn response_json<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&bytes).expect("decode response json")
}
