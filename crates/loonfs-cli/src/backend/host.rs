//! Hosts the HTTP binding over the CLI's runtime handles without a listener.

use super::MaintenanceHost;
use crate::error::CliError;
use crate::render::write_stderr_warning;
use http_body_util::BodyExt as _;
use loonfs::InlineContentOptions;
use loonfs_api::SecretString;
use loonfs_client::{Body, Client, ClientConfig, TransportError};
use loonfs_grep::GrepService;
use loonfs_http::{AuthPolicy, BindingOptions, BindingState, HttpMetrics, RouterSurface};
use loonfs_objectstore::ConfiguredObjectStoreKind;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use tower::ServiceExt as _;

static CONTENT_TOKEN_SECRET: OnceLock<SecretString> = OnceLock::new();

pub(crate) fn client(
    host: &MaintenanceHost,
    grep_service: GrepService,
    store_kind: ConfiguredObjectStoreKind,
    inline_content: InlineContentOptions,
    no_retry: bool,
) -> Result<Client, CliError> {
    let options = Arc::new(BindingOptions {
        serves_grep: true,
        maintains_grep_index: true,
        serves_maintenance: true,
        snapshot_policy: loonfs::SnapshotPolicy::default(),
        max_download_bytes: u64::MAX,
        max_upload_bytes: u64::MAX,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        inline_content,
        content_token_secret: CONTENT_TOKEN_SECRET
            .get_or_init(|| loonfs_api::generated_id("content-token").into())
            .clone(),
        request_deadline_ms: 60_000,
        store_kind,
        auth_policy: AuthPolicy::Unauthenticated,
    });
    let state = BindingState {
        upload_permits: Arc::new(Semaphore::new(options.max_concurrent_uploads)),
        download_permits: Arc::new(Semaphore::new(options.max_concurrent_downloads)),
        options,
        writer: host.writer.clone(),
        reader: host.writer.reader(),
        maintenance: host.maintenance.clone(),
        probe_store: host.writer.object_store(),
        direct_transfers: None,
        grep_worker: Some(host.grep_worker.clone()),
        grep_service: Some(Arc::new(grep_service)),
        // The command drives index steps so --no-wait and step budgets stay exact.
        grep_maintenance: None,
        metrics: HttpMetrics::new(),
    };
    let router = loonfs_http::router(state, RouterSurface::Standalone);
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
