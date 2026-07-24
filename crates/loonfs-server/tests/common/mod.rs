//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]

use loonfs_client::{Client, ClientConfig};
use loonfs_server::{
    app, DirectPutConfig, GrepConfig, RuntimeCacheConfigOverrides, ServerConfig, StoreConfig,
};
use std::path::PathBuf;

pub(crate) struct TestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    #[allow(dead_code)]
    pub(crate) store_root: Option<PathBuf>,
    #[allow(dead_code)]
    pub(crate) store_key_prefix: Option<String>,
    pub(crate) server: tokio::task::JoinHandle<()>,
}

pub(crate) async fn start_server(config: ServerConfig) -> TestServer {
    let (store_root, store_key_prefix) = match &config.store {
        StoreConfig::LocalFs { root, key_prefix } => {
            (Some(PathBuf::from(root)), key_prefix.clone())
        }
        _ => (None, None),
    };
    let auth_token = config
        .auth_token
        .as_ref()
        .map(|token| token.expose().to_owned());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    // The lifecycle handle is dropped: tests abort the server task instead
    // of shutting it down gracefully.
    let (router, _lifecycle) = app(config).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let server_url = format!("http://{addr}");

    TestServer {
        client: Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token,
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config"),
        server_url,
        store_root,
        store_key_prefix,
        server,
    }
}

pub(crate) fn test_config(
    store: StoreConfig,
    auth_token: &str,
    content_token_secret: &str,
    writer_id: &str,
) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some(auth_token.into()),
        content_token_secret: content_token_secret.into(),
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: GrepConfig::default(),
        direct_put: DirectPutConfig::default(),
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_commit_body_bytes: 8 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        store,
    }
}

pub(crate) mod http_split_support {
    #![allow(dead_code)]

    use loonfs_api::{
        v0::{
            BeginUploadRequest, CommitSubmissionRequest, CompleteUploadRequest,
            CompleteUploadResponse, ValidatedContentToken,
        },
        DestinationBehavior, NamespaceId,
    };
    use loonfs_client::{Client, PutFileOptions};

    use loonfs_server::{ServerConfig, StoreConfig};

    use loonfs_test_support::http::raw_agent;

    pub(crate) const TEST_CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";

    pub(crate) fn replace_file_options() -> PutFileOptions {
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::default()
        }
    }

    pub(crate) fn test_config(
        store_root: std::path::PathBuf,
        writer_id: &str,
        key_prefix: &str,
    ) -> ServerConfig {
        super::test_config(
            StoreConfig::LocalFs {
                root: store_root.display().to_string(),
                key_prefix: Some(key_prefix.to_owned()),
            },
            "test-token",
            TEST_CONTENT_TOKEN_SECRET,
            writer_id,
        )
    }

    pub(crate) fn send_commit_submission(
        server_url: &str,
        namespace_id: &NamespaceId,
        request: &CommitSubmissionRequest,
    ) -> Result<ureq::Response, Box<ureq::Error>> {
        send_commit_json(server_url, namespace_id, request)
    }

    pub(crate) fn send_commit_json(
        server_url: &str,
        namespace_id: &NamespaceId,
        request: &impl serde::Serialize,
    ) -> Result<ureq::Response, Box<ureq::Error>> {
        raw_agent()
            .post(&format!(
                "{server_url}/v0/namespaces/{namespace_id}/commits"
            ))
            .set("authorization", "Bearer test-token")
            .send_json(request)
            .map_err(Box::new)
    }

    pub(crate) fn stage_uploaded_content(
        client: &Client,
        namespace_id: &NamespaceId,
        file_bytes: &[u8],
    ) -> CompleteUploadResponse {
        let begin = client
            .begin_upload(namespace_id, &BeginUploadRequest::default())
            .expect("begin upload");
        let staged = client
            .upload_content(namespace_id, &begin.upload_id, file_bytes)
            .expect("upload content");
        let complete_request = CompleteUploadRequest {
            content_ref: staged.content_ref,
        };
        let complete = client
            .complete_upload(namespace_id, &begin.upload_id, &complete_request)
            .expect("complete upload");
        let repeated = client
            .complete_upload(namespace_id, &begin.upload_id, &complete_request)
            .expect("repeat complete upload");
        assert_eq!(repeated.namespace_id, complete.namespace_id);
        assert_eq!(repeated.upload_id, complete.upload_id);
        assert_eq!(repeated.content_ref, complete.content_ref);
        assert!(complete.validated_content_token.is_some());
        assert!(repeated.validated_content_token.is_some());
        complete
    }

    pub(crate) fn validated_content_token(
        completed: &CompleteUploadResponse,
    ) -> ValidatedContentToken {
        ValidatedContentToken {
            content_ref: completed.content_ref.clone(),
            token: completed
                .validated_content_token
                .clone()
                .expect("completed upload carries token"),
        }
    }
}
