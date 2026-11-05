//! Runtime handles and options for HTTP API tests.

use crate::{
    AuthPolicy, GrepMode, HttpConfig, HttpHandles, HttpMetrics, HttpState, JobMode, Namespaces,
    UsageRecorder,
};
use loonfs::{InlineContentPolicy, LoonFs, SharedObjectStore, TraceMode};
use loonfs_grep::{
    GrepBlockCache, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_objectstore::{
    local_fs_store::LocalFsStore, presign::DirectTransferIssuers, ConfiguredObjectStoreKind,
};
use loonfs_types::{MonotonicTimer, WriterId};
use std::path::Path;
use std::sync::Arc;

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
            runtime.clone(),
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
    let grep = match config.grep {
        JobMode::Disabled => GrepMode::Disabled,
        JobMode::ServeOnly => GrepMode::ServeOnly {
            service: grep_service(),
        },
        JobMode::MaintainOnly => GrepMode::MaintainOnly {
            worker: grep_worker(),
        },
        JobMode::ServeAndMaintain => GrepMode::ServeAndMaintain {
            service: grep_service(),
            worker: grep_worker(),
        },
    };
    let namespaces = Arc::new(Namespaces::<()>::new(runtime.clone()));
    let state = HttpState::new(
        options,
        HttpHandles {
            namespaces: namespaces.clone(),
            runtime,
            maintenance,
            direct_transfers: inputs.direct_transfers,
            grep,
            metrics,
            usage_recorder: inputs.usage_recorder,
        },
    );
    Ok((crate::router(state.clone()), state, namespaces))
}
