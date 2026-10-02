//! Runtime handles and options for binding tests.

use crate::{AuthPolicy, BindingOptions, BindingState, HttpMetrics, Namespaces};
use loonfs::{LoonFs, SharedObjectStore, SnapshotPolicy, TraceMode, TraceStoreKind};
use loonfs_grep::{
    new_grep_block_cache, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_objectstore::{
    local_fs_store::LocalFsStore, presign::DirectTransferIssuers, ConfiguredObjectStoreKind,
};
use loonfs_types::WriterId;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) struct TestOptions {
    pub(super) binding: BindingOptions,
    pub(super) writer_id: WriterId,
    pub(super) store: SharedObjectStore,
}

#[derive(Default)]
pub(super) struct TestAppOptions {
    pub(super) store: Option<SharedObjectStore>,
    pub(super) direct_transfers: Option<DirectTransferIssuers>,
}

pub(super) fn test_options(root: &Path, writer_id: &str) -> TestOptions {
    TestOptions {
        binding: BindingOptions {
            serves_grep: true,
            maintains_grep_index: true,
            serves_maintenance: true,
            snapshot_policy: SnapshotPolicy {
                max_ttl_ms: 86_400_000,
                max_lifetime_ms: 604_800_000,
                max_live_per_namespace: 16,
            },
            max_upload_bytes: 256 * 1024 * 1024,
            max_download_bytes: 256 * 1024 * 1024,
            max_concurrent_uploads: 8,
            max_concurrent_downloads: 16,
            inline_content: Default::default(),
            content_token_secret: "test-content-token-secret".into(),
            request_deadline_ms: 60_000,
            idle_fold_after_ms: loonfs::MetadataMaintenanceOptions::default().idle_fold_after_ms,
            store_kind: ConfiguredObjectStoreKind::LocalFs,
            auth_policy: AuthPolicy::BearerToken("test-token".into()),
        },
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
    let runtime = LoonFs::builder_with_store(inputs.store.unwrap_or(config.store))
        .writer_id(config.writer_id.as_str())
        .min_publish_interval_ms(0)
        .inline_content(options.inline_content.clone())
        .max_read_content_bytes(options.max_download_bytes)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::from(options.store_kind))
        .metrics_recorder(metrics.recorder())
        .build()
        .await?;
    let maintenance = runtime.maintenance(
        WriterId::parse(format!("{}-maintenance", config.writer_id))
            .expect("a suffixed writer id should stay valid"),
    );
    let grep_worker = (options.serves_grep || options.maintains_grep_index).then(|| {
        GrepWorker::new(
            runtime.object_store(),
            runtime.read_only(),
            maintenance.clone(),
            loonfs_grep::GrepStepBudget::default(),
        )
    });
    let grep_service = options.serves_grep.then(|| {
        let cache = Arc::new(new_grep_block_cache(
            DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
            metrics.recorder().as_ref(),
        ));
        Arc::new(GrepService::new(cache))
    });
    let state = BindingState {
        upload_permits: Arc::new(Semaphore::new(options.max_concurrent_uploads)),
        download_permits: Arc::new(Semaphore::new(options.max_concurrent_downloads)),
        options,
        probe_store: runtime.object_store(),
        namespaces: Arc::new(Namespaces::new(runtime.clone())),
        runtime,
        maintenance,
        direct_transfers: inputs.direct_transfers,
        grep_worker,
        grep_service,
        metrics,
    };
    Ok((crate::router(state.clone()), state))
}
