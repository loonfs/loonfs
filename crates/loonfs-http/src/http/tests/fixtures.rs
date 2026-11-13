//! Runtime handles, options and requests for HTTP API tests.

use crate::{
    AuthPolicy, HttpConfig, HttpHandles, HttpMetrics, HttpState, JobMode, Namespaces, UsageRecorder,
};
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode};
use loonfs::{InlineContentPolicy, LoonFs, SharedObjectStore, TraceMode};
use loonfs_client::{Client, ClientConfig};
use loonfs_grep::{
    GrepBlockCache, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_objectstore::{
    local_fs_store::LocalFsStore, presign::DirectTransferIssuers, ConfiguredObjectStoreKind,
};
use loonfs_types::{MonotonicTimer, WriterId};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tower::ServiceExt as _;

#[derive(Clone)]
pub(super) struct TestOptions {
    pub(super) http: HttpConfig,
    /// The runtime's inline content policy, which the HTTP API reads from the
    /// runtime.
    pub(super) inline_content: InlineContentPolicy,
    pub(super) grep: JobMode,
    pub(super) writer_id: WriterId,
    pub(super) store: SharedObjectStore,
}

#[derive(Default)]
pub(super) struct TestAppOptions {
    pub(super) store: Option<SharedObjectStore>,
    pub(super) direct_transfers: Option<DirectTransferIssuers>,
    /// The runtime's default maximum staleness of an eventual read, or the default when
    /// `None`.
    pub(super) max_staleness_ms: Option<u64>,
    /// The runtime's default consistency, or the default when `None`.
    pub(super) consistency: Option<loonfs::ReadConsistency>,
    /// The runtime's monotonic timer, or the system timer when `None`.
    pub(super) monotonic_timer: Option<Arc<dyn MonotonicTimer>>,
    /// The runtime's execution budget, or the default when `None`.
    pub(super) execution_budget: Option<loonfs::ExecutionBudget>,
    /// The WAL objects that make a writer session's own fold due, or the
    /// default when `None`.
    pub(super) session_fold_at_wal_objects: Option<std::num::NonZeroU64>,
    /// The HTTP API's usage recorder, or no recorder when `None`.
    pub(super) usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

pub(super) fn test_options(root: &Path, writer_id: &str) -> TestOptions {
    TestOptions {
        http: HttpConfig {
            serves_maintenance: true,
            max_upload_bytes: Some(256 * 1024 * 1024),
            max_download_bytes: Some(256 * 1024 * 1024),
            max_concurrent_uploads: 8,
            max_concurrent_downloads: 16,
            max_in_flight_requests: None,
            content_token_secret: "test-content-token-secret".into(),
            request_deadline_ms: 60_000,
            auth_policy: AuthPolicy::BearerToken("test-token".into()),
        },
        inline_content: InlineContentPolicy::default(),
        grep: JobMode::ServeAndMaintain,
        writer_id: WriterId::parse(writer_id).expect("writer id"),
        store: Arc::new(LocalFsStore::with_key_prefix(root, Some("http-tests")).expect("store")),
    }
}

pub(super) async fn test_app(
    config: TestOptions,
    inputs: TestAppOptions,
) -> loonfs::Result<(axum::Router, HttpState)> {
    let (router, state, _) = test_app_with_sessions(config, inputs).await?;
    Ok((router, state))
}

/// Builds the app of [`test_app`], and returns the table of writer sessions
/// that its HTTP API holds.
pub(super) async fn test_app_with_sessions(
    config: TestOptions,
    inputs: TestAppOptions,
) -> loonfs::Result<(axum::Router, HttpState, Arc<Namespaces<()>>)> {
    let options = config.http;
    let metrics = HttpMetrics::new();
    let defaults = loonfs::RuntimeConfig::default();
    let builder = LoonFs::builder_with_store(inputs.store.unwrap_or(config.store)).config(
        loonfs::RuntimeConfig {
            max_staleness_ms: inputs.max_staleness_ms.unwrap_or(defaults.max_staleness_ms),
            consistency: inputs.consistency.unwrap_or(defaults.consistency),
            min_publish_interval_ms: 0,
            inline_content: config.inline_content,
            max_read_content_bytes: options
                .max_download_bytes
                .expect("test HTTP APIs should cap proxied reads"),
            trace_mode: TraceMode::Remote,
            ..defaults
        },
    );
    let builder = match inputs.monotonic_timer {
        Some(timer) => builder.monotonic_timer(timer),
        None => builder,
    };
    let builder = match inputs.session_fold_at_wal_objects {
        Some(objects) => builder.session_fold_at_wal_objects(objects),
        None => builder,
    };
    let builder = match inputs.execution_budget {
        Some(execution_budget) => builder.execution_budget(execution_budget),
        None => builder,
    };
    let runtime = builder
        .writer_id(loonfs_test_support::ids::writer_id(
            config.writer_id.as_str(),
        ))
        .store_kind(ConfiguredObjectStoreKind::LocalFs)
        .metrics_recorder(metrics.recorder())
        .build()
        .await?;
    let maintenance = runtime.maintenance(
        WriterId::parse(format!("{}-maintenance", config.writer_id))
            .expect("a suffixed writer id should stay valid"),
    );
    let grep_worker = || {
        GrepWorker::new(
            runtime.object_store(),
            runtime.read_only(),
            maintenance.clone(),
            loonfs_grep::GrepStepBudget::default(),
        )
    };
    let grep_service = || {
        let cache = Arc::new(GrepBlockCache::new(
            loonfs::engine::DecodedBlockCacheConfig::new(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES),
        ));
        Arc::new(GrepService::new(cache))
    };
    let grep_service = config.grep.serves().then(grep_service);
    let grep_worker = config.grep.maintains().then(grep_worker);
    let namespaces = Arc::new(Namespaces::<()>::new(runtime.clone()));
    let state = HttpState::new(
        options,
        HttpHandles {
            namespaces: namespaces.clone(),
            runtime,
            maintenance,
            direct_transfers: inputs.direct_transfers,
            grep_service,
            grep_worker,
            metrics,
            usage_recorder: inputs.usage_recorder,
        },
    );
    Ok((crate::router(state.clone()), state, namespaces))
}

/// Builds a request with the test token, `headers`, and a JSON content type
/// when it has a body.
pub(super) fn request(
    method: &str,
    uri: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_owned())))
        .expect("request")
}

/// Sends `request` to `router` and reads the whole response.
pub(super) async fn send(
    router: &axum::Router,
    request: Request<Body>,
) -> (StatusCode, HeaderMap, Bytes) {
    let response = router.clone().oneshot(request).await.expect("response");
    let (parts, body) = response.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("response body");
    (parts.status, parts.headers, body)
}

/// Serves `router` on a loopback port and returns a client pointed at it.
pub(super) async fn serve(router: axum::Router) -> (Client, SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    (client, addr, server)
}
