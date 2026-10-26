//! Runtime handles and options for binding tests.

use crate::{
    AuthPolicy, BindingConfig, BindingState, GrepBinding, HttpMetrics, JobMode, Namespaces,
    UsageRecorder,
};
use loonfs::{InlineContentPolicy, LoonFs, SharedObjectStore, TraceMode};
use loonfs_grep::{
    new_grep_block_cache, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_objectstore::{
    local_fs_store::LocalFsStore, presign::DirectTransferIssuers, ConfiguredObjectStoreKind,
};
use loonfs_types::{MonotonicTimer, WriterId};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct TestOptions {
    pub(super) binding: BindingConfig,
    /// The runtime's inline content policy, which the binding reads from the
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
    /// The binding's usage recorder, or no recorder when `None`.
    pub(super) usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

pub(super) fn test_options(root: &Path, writer_id: &str) -> TestOptions {
    TestOptions {
        binding: BindingConfig {
            serves_maintenance: true,
            max_upload_bytes: Some(256 * 1024 * 1024),
            max_download_bytes: Some(256 * 1024 * 1024),
            max_concurrent_uploads: 8,
            max_concurrent_downloads: 16,
            content_token_secret: "test-content-token-secret".into(),
            request_deadline_ms: 60_000,
            store_kind: ConfiguredObjectStoreKind::LocalFs,
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
) -> loonfs::Result<(axum::Router, BindingState)> {
    let options = Arc::new(config.binding);
    let metrics = HttpMetrics::new();
    let builder = LoonFs::builder_with_store(inputs.store.unwrap_or(config.store));
    let builder = match inputs.max_staleness_ms {
        Some(interval_ms) => builder.max_staleness_ms(interval_ms),
        None => builder,
    };
    let builder = match inputs.consistency {
        Some(consistency) => builder.consistency(consistency),
        None => builder,
    };
    let builder = match inputs.monotonic_timer {
        Some(timer) => builder.monotonic_timer(timer),
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
        .min_publish_interval_ms(0)
        .inline_content(config.inline_content)
        .max_read_content_bytes(
            options
                .max_download_bytes
                .expect("test bindings should cap proxied reads"),
        )
        .trace_mode(TraceMode::Remote)
        .store_kind(options.store_kind)
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
            loonfs_grep::GrepStepBudget::default(),
        )
    };
    let grep_service = || {
        let cache = Arc::new(new_grep_block_cache(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES));
        Arc::new(GrepService::new(cache, runtime.monotonic_timer()))
    };
    let grep = match config.grep {
        JobMode::Disabled => GrepBinding::Disabled,
        JobMode::ServeOnly => GrepBinding::ServeOnly {
            service: grep_service(),
        },
        JobMode::MaintainOnly => GrepBinding::MaintainOnly {
            worker: grep_worker(),
        },
        JobMode::ServeAndMaintain => GrepBinding::ServeAndMaintain {
            service: grep_service(),
            worker: grep_worker(),
        },
    };
    let state = BindingState {
        request_limit: None,
        upload_permits: crate::transfer_permits(options.max_concurrent_uploads),
        download_permits: crate::transfer_permits(options.max_concurrent_downloads),
        options,
        probe_store: runtime.object_store(),
        namespaces: Arc::new(Namespaces::new(runtime.clone())),
        runtime,
        maintenance,
        direct_transfers: inputs.direct_transfers,
        grep,
        metrics,
        usage_recorder: inputs.usage_recorder,
    };
    Ok((crate::router(state.clone()), state))
}
