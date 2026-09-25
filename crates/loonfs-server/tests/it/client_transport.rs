//! Client operations through the server router without a socket.

use crate::common::http_split_support::test_config;
use bytes::Bytes;
use futures::StreamExt as _;
use http_body_util::BodyExt as _;
use loonfs_client::{
    Body, Client, ClientConfig, NamespacePath, PayloadSource, PutFileOptions, ReadFileOptions,
    TransportError,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn client_streams_uploads_and_downloads_through_the_server_router() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut config = test_config(
        directory.path().join("store"),
        "client-router",
        "client-router",
    );
    config.maintenance = loonfs_server::MaintenanceMode::ServeOnly;
    config.grep.mode = loonfs_server::GrepMode::Disabled;
    let (router, state) = loonfs_server::app(config, loonfs_server::AppOptions::default())
        .await
        .expect("app");
    let service = tower::service_fn(move |request: http::Request<Body>| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(request)
                .await
                .map_err(|never| match never {})?;
            Ok(response.map(|body| Body::new(body.map_err(TransportError::body))))
        }
    });
    let client = Client::with_transport(
        ClientConfig {
            server_url: "http://127.0.0.1".to_owned(),
            auth_token: Some("test-token".into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        },
        service,
    )
    .expect("client");
    let target = NamespacePath::parse("demo", "/stream.bin").expect("path");
    let actor = loonfs_test_support::test_actor();
    client
        .create_namespace(
            target.namespace(),
            &actor,
            loonfs_api::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("create namespace");
    let chunks = vec![Bytes::from(vec![42; 32 * 1024]); 4];
    let source =
        PayloadSource::stream(futures::stream::iter(chunks.clone().into_iter().map(Ok)).boxed());
    client
        .put_file_stream(&target, source, &PutFileOptions::new(actor))
        .await
        .expect("upload");
    let mut stream = client
        .read_file_stream(&target, &ReadFileOptions::default())
        .await
        .expect("download");
    let mut received = Vec::new();
    while let Some(chunk) = stream.next().await {
        received.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(received, chunks.concat());
    state.writer.shutdown().await.expect("shutdown");
}
