//! Hosts the HTTP API over the CLI's runtime without a listener.

use crate::args::TransientRetry;
use crate::error::Result;
use crate::render::write_stderr_warning;
use http_body_util::BodyExt as _;
use loonfs::{LoonFs, Maintenance, SharedObjectStore, Writable};
use loonfs_client::{Body, Client, ClientConfig, TransportError};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_http::{
    AuthPolicy, GrepMode, HttpConfig, HttpHandles, HttpMetrics, HttpState, Namespaces,
    DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
use loonfs_types::SecretString;
use std::cell::RefCell;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use tower::ServiceExt as _;

static CONTENT_TOKEN_SECRET: OnceLock<SecretString> = OnceLock::new();

tokio::task_local! {
    /// The runtimes that embedded profiles opened during the command that
    /// [`run_then_drain`] runs.
    static OPENED_RUNTIMES: RefCell<Vec<LoonFs<Writable>>>;
}

/// Runs `command`, then drains each runtime that an embedded profile opened
/// while it ran, and returns the command's output.
///
/// The process exits when the command does, which would abandon a fold or
/// compaction that the command's requests started. A runtime opened outside
/// `command` isn't drained.
pub(crate) async fn run_then_drain<T>(command: impl Future<Output = T>) -> T {
    let (output, runtimes) = OPENED_RUNTIMES
        .scope(RefCell::new(Vec::new()), async {
            let output = command.await;
            (output, OPENED_RUNTIMES.with(RefCell::take))
        })
        .await;
    for runtime in runtimes {
        if let Err(error) = runtime.drain().await {
            write_stderr_warning(format_args!(
                "background maintenance did not settle cleanly: {error}"
            ));
        }
    }
    output
}

/// The runtime an embedded profile opens, and the maintenance and grep
/// worker its HTTP API serves.
pub(crate) struct MaintenanceHost {
    pub(crate) runtime: LoonFs<Writable>,
    pub(crate) maintenance: Maintenance,
    pub(crate) grep_worker: GrepWorker<SharedObjectStore>,
}

pub(crate) fn client(
    host: &MaintenanceHost,
    grep_service: GrepService,
    transient_retry: TransientRetry,
) -> Result<Client> {
    let state = HttpState::new(
        HttpConfig {
            serves_maintenance: true,
            max_download_bytes: None,
            max_upload_bytes: None,
            max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS.get(),
            max_concurrent_downloads: DEFAULT_MAX_CONCURRENT_DOWNLOADS.get(),
            max_in_flight_requests: None,
            content_token_secret: CONTENT_TOKEN_SECRET
                .get_or_init(|| loonfs_types::generated_id("content-token").into())
                .clone(),
            request_deadline_ms: DEFAULT_REQUEST_DEADLINE_MS,
            auth_policy: AuthPolicy::Open,
        },
        HttpHandles {
            runtime: host.runtime.clone(),
            namespaces: Arc::new(Namespaces::<()>::new(host.runtime.clone())),
            maintenance: host.maintenance.clone(),
            direct_transfers: None,
            grep: GrepMode::ServeAndMaintain {
                service: Arc::new(grep_service),
                worker: host.grep_worker.clone(),
            },
            metrics: HttpMetrics::new(),
            usage_recorder: None,
        },
    );
    let router = loonfs_http::router(state);
    // Only a client opened inside `run_then_drain` has its runtime drained.
    let _ = OPENED_RUNTIMES.try_with(|runtimes| runtimes.borrow_mut().push(host.runtime.clone()));
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
    Ok(Client::with_transport(
        ClientConfig {
            server_url: "http://embedded.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: transient_retry == TransientRetry::NoRetry,
            ca_cert_path: None,
        },
        service,
    )?)
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
