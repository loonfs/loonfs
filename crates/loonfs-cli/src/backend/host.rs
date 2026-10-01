//! Hosts the HTTP binding over the CLI's runtime without a listener.

use super::MaintenanceHost;
use crate::error::CliError;
use crate::render::write_stderr_warning;
use http_body_util::BodyExt as _;
use loonfs::InlineContentPolicy;
use loonfs_client::{Body, Client, ClientConfig, TransportError};
use loonfs_grep::GrepService;
use loonfs_http::{
    AuthPolicy, BindingOptions, BindingState, HttpMetrics, Namespaces,
    DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::SecretString;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use tower::ServiceExt as _;

static CONTENT_TOKEN_SECRET: OnceLock<SecretString> = OnceLock::new();

pub(crate) fn client(
    host: &MaintenanceHost,
    grep_service: GrepService,
    store_kind: ConfiguredObjectStoreKind,
    inline_content: InlineContentPolicy,
    no_retry: bool,
) -> Result<Client, CliError> {
    let options = Arc::new(BindingOptions {
        serves_grep: true,
        maintains_grep_index: true,
        serves_maintenance: true,
        snapshot_policy: loonfs::SnapshotPolicy::default(),
        max_download_bytes: u64::MAX,
        max_upload_bytes: u64::MAX,
        max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS,
        max_concurrent_downloads: DEFAULT_MAX_CONCURRENT_DOWNLOADS,
        inline_content,
        content_token_secret: CONTENT_TOKEN_SECRET
            .get_or_init(|| loonfs_types::generated_id("content-token").into())
            .clone(),
        request_deadline_ms: DEFAULT_REQUEST_DEADLINE_MS,
        idle_fold_after_ms: loonfs::MetadataMaintenanceOptions::default().idle_fold_after_ms,
        store_kind,
        auth_policy: AuthPolicy::Unauthenticated,
    });
    let state = BindingState {
        upload_permits: Arc::new(Semaphore::new(options.max_concurrent_uploads)),
        download_permits: Arc::new(Semaphore::new(options.max_concurrent_downloads)),
        options,
        runtime: host.runtime.clone(),
        namespaces: Arc::new(Namespaces::new(host.runtime.clone())),
        maintenance: host.maintenance.clone(),
        probe_store: host.runtime.object_store(),
        direct_transfers: None,
        grep_worker: Some(host.grep_worker.clone()),
        grep_service: Some(Arc::new(grep_service)),
        metrics: HttpMetrics::new(),
    };
    let router = loonfs_http::router(state);
    let runner = host.runner.clone();
    let service = tower::service_fn(move |request: http::Request<Body>| {
        let router = router.clone();
        let runner = runner.clone();
        async move {
            let response = router
                .oneshot(request)
                .await
                .map_err(|never| match never {})?;
            if let Err(error) = runner.drain().await {
                write_stderr_warning(format_args!(
                    "background maintenance did not settle cleanly: {error}"
                ));
            }
            Ok(response.map(|body| Body::new(body.map_err(TransportError::body))))
        }
    });
    Ok(Client::with_transport(
        ClientConfig {
            server_url: "http://embedded.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: no_retry,
            ca_cert_path: None,
        },
        service,
    )?)
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
