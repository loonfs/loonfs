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
use std::sync::{Arc, Mutex, OnceLock};
use tower::ServiceExt as _;

static CONTENT_TOKEN_SECRET: OnceLock<SecretString> = OnceLock::new();

/// The runtimes that this process's embedded profiles opened, which
/// [`shut_down_embedded_runtimes`] shuts down before the process exits.
static EMBEDDED_RUNTIMES: Mutex<Vec<LoonFs<Writable>>> = Mutex::new(Vec::new());

/// Shuts down every runtime that this process's embedded profiles opened.
///
/// The process exits when the command returns, so the command shuts its
/// runtimes down first. A shutdown finishes the folds and compactions that
/// the command's requests started, and it raises each namespace's hint to the
/// newest manifest that the command published. Without the raise, a
/// namespace that only one-shot commands write would keep an old hint, and
/// garbage collection keeps every manifest at or above the hint, so the
/// namespace's manifests would accumulate. The raise waits for at most one
/// compare-and-swap write interval.
pub(crate) async fn shut_down_embedded_runtimes() {
    let runtimes = std::mem::take(
        &mut *EMBEDDED_RUNTIMES
            .lock()
            .expect("the embedded runtime list lock should not be poisoned"),
    );
    for runtime in runtimes {
        if let Err(error) = runtime.shutdown().await {
            write_stderr_warning(format_args!(
                "background maintenance did not settle cleanly: {error}"
            ));
        }
    }
}

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
    EMBEDDED_RUNTIMES
        .lock()
        .expect("the embedded runtime list lock should not be poisoned")
        .push(host.runtime.clone());
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
            // a fold or compaction the request started, so the request
            // drains the runtime. A command can send several requests, so a
            // request can't shut the runtime down. The process shuts it down
            // before it exits (see `shut_down_embedded_runtimes`), which also
            // raises the hint to the manifests that the command published.
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
