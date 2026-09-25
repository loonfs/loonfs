//! Runtime handles and options for binding tests.

use crate::{AuthPolicy, BindingOptions, BindingState, HttpMetrics, RouterSurface};
use loonfs::{FsWriter, SharedObjectStore, SnapshotPolicy, TraceMode, TraceStoreKind};
use loonfs_api::WriterId;
use loonfs_grep::{
    new_grep_block_cache, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_objectstore::{
    local_fs_store::LocalFsStore, presign::DirectTransferIssuers, ConfiguredObjectStoreKind,
};
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
    let writer = FsWriter::builder_with_store(inputs.store.unwrap_or(config.store))
        .writer_id(config.writer_id.as_str())
        .min_publish_interval_ms(0)
        .inline_content(options.inline_content.clone())
        .max_read_content_bytes(options.max_download_bytes)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::from(options.store_kind))
        .metrics_recorder(metrics.recorder())
        .build()
        .await?;
    let reader = writer.reader();
    let maintenance = writer.maintenance_handle(format!("{}-maintenance", config.writer_id))?;
    let cache = Arc::new(new_grep_block_cache(
        DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
        metrics.recorder().as_ref(),
    ));
    let grep_worker = (options.serves_grep || options.maintains_grep_index).then(|| {
        GrepWorker::with_block_cache(
            writer.object_store(),
            reader.clone(),
            maintenance.clone(),
            cache.clone(),
        )
    });
    let grep_service = options
        .serves_grep
        .then(|| Arc::new(GrepService::new(cache)));
    let state = BindingState {
        upload_permits: Arc::new(Semaphore::new(options.max_concurrent_uploads)),
        download_permits: Arc::new(Semaphore::new(options.max_concurrent_downloads)),
        options,
        probe_store: writer.object_store(),
        writer,
        reader,
        maintenance,
        direct_transfers: inputs.direct_transfers,
        grep_worker,
        grep_service,
        grep_maintenance: None,
        metrics,
    };
    Ok((
        crate::router(state.clone(), RouterSurface::Standalone),
        state,
    ))
}
