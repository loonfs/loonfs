//! Hosts the HTTP binding over the CLI's runtime without a listener.

use crate::error::CliError;
use crate::render::write_stderr_warning;
use http_body_util::BodyExt as _;
use loonfs::{LoonFs, Maintenance, SharedObjectStore, Writable};
use loonfs_client::{Body, Client, ClientConfig, TransportError};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_http::{
    AuthPolicy, BindingConfig, BindingState, GrepBinding, HttpMetrics, Namespaces,
    DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::SecretString;
use std::sync::{Arc, OnceLock};
use tower::ServiceExt as _;

static CONTENT_TOKEN_SECRET: OnceLock<SecretString> = OnceLock::new();

/// The runtime an embedded profile opens, and the maintenance and grep
/// worker its binding serves.
pub(crate) struct MaintenanceHost {
    pub(crate) runtime: LoonFs<Writable>,
    pub(crate) maintenance: Maintenance,
    pub(crate) grep_worker: GrepWorker<SharedObjectStore>,
}

pub(crate) fn client(
    host: &MaintenanceHost,
    grep_service: GrepService,
    store_kind: ConfiguredObjectStoreKind,
    no_retry: bool,
) -> Result<Client, CliError> {
    let options = Arc::new(BindingConfig {
        serves_maintenance: true,
        max_download_bytes: None,
        max_upload_bytes: None,
        max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS,
        max_concurrent_downloads: DEFAULT_MAX_CONCURRENT_DOWNLOADS,
        content_token_secret: CONTENT_TOKEN_SECRET
            .get_or_init(|| loonfs_types::generated_id("content-token").into())
            .clone(),
        request_deadline_ms: DEFAULT_REQUEST_DEADLINE_MS,
        store_kind,
        auth_policy: AuthPolicy::Unauthenticated,
    });
    let state = BindingState {
        request_limit: None,
        upload_permits: loonfs_http::transfer_permits(options.max_concurrent_uploads),
        download_permits: loonfs_http::transfer_permits(options.max_concurrent_downloads),
        options,
        runtime: host.runtime.clone(),
        namespaces: Arc::new(Namespaces::new(host.runtime.clone())),
        maintenance: host.maintenance.clone(),
        probe_store: host.runtime.object_store(),
        direct_transfers: None,
        grep: GrepBinding::ServeAndMaintain {
            service: Arc::new(grep_service),
            worker: host.grep_worker.clone(),
        },
        metrics: HttpMetrics::new(),
        usage_recorder: None,
    };
    let router = loonfs_http::router(state);
    let runtime = host.runtime.clone();
    let service = tower::service_fn(move |request: http::Request<Body>| {
        let router = router.clone();
        let runtime = runtime.clone();
        async move {
            let response = router
                .oneshot(request)
                .await
                .map_err(|never| match never {})?;
            // The process exits when the command does, which would abandon
            // a fold or compaction the request started.
            if let Err(error) = runtime.drain().await {
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
