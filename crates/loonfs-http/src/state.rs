//! Host-supplied options and handles for HTTP requests.

use crate::HttpMetrics;
use loonfs::{
    FsMaintenance, FsReader, FsWriter, InlineContentOptions, MaintenanceHandle, MaintenanceJob,
    MaintenanceProbe, SharedObjectStore, SnapshotPolicy,
};
use loonfs_api::{NamespaceId, SecretString};
use loonfs_grep::{GrepMaintenanceJob, GrepService, GrepWorker, GREP_INDEX_JOB};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum AuthPolicy {
    Unauthenticated,
    BearerToken(SecretString),
}

#[derive(Clone, Debug)]
pub struct BindingOptions {
    pub serves_grep: bool,
    pub maintains_grep_index: bool,
    pub serves_maintenance: bool,
    pub snapshot_policy: SnapshotPolicy,
    pub max_download_bytes: u64,
    pub max_upload_bytes: u64,
    pub max_concurrent_uploads: usize,
    pub max_concurrent_downloads: usize,
    pub inline_content: InlineContentOptions,
    pub content_token_secret: SecretString,
    pub request_deadline_ms: u64,
    pub store_kind: ConfiguredObjectStoreKind,
    pub auth_policy: AuthPolicy,
}

/// Hosts supply handles over one store, with grep services and permit limits matching `options`.
#[derive(Clone)]
pub struct BindingState {
    pub options: Arc<BindingOptions>,
    pub writer: FsWriter,
    pub reader: FsReader,
    pub maintenance: FsMaintenance,
    pub probe_store: SharedObjectStore,
    pub direct_transfers: Option<DirectTransferIssuers>,
    pub grep_worker: Option<GrepWorker<SharedObjectStore>>,
    pub grep_service: Option<Arc<GrepService>>,
    pub grep_maintenance: Option<GrepMaintenance>,
    pub upload_permits: Arc<Semaphore>,
    pub download_permits: Arc<Semaphore>,
    pub metrics: Arc<HttpMetrics>,
}

impl BindingState {
    pub(crate) fn grep_worker(&self) -> &GrepWorker<SharedObjectStore> {
        self.grep_worker
            .as_ref()
            .expect("grep routes should carry a grep worker")
    }

    pub(crate) fn grep_service(&self) -> &GrepService {
        self.grep_service
            .as_deref()
            .expect("grep routes should carry a grep service")
    }
}

#[derive(Clone)]
pub struct GrepMaintenance {
    pub handle: MaintenanceHandle,
    pub job: Arc<GrepMaintenanceJob<SharedObjectStore>>,
}

impl GrepMaintenance {
    pub fn nudge(&self, namespace_id: &NamespaceId) {
        self.handle.nudge(GREP_INDEX_JOB, namespace_id);
    }

    pub(crate) async fn nudge_if_behind(&self, namespace_id: &NamespaceId) {
        if matches!(
            self.job.probe(namespace_id).await,
            Ok(MaintenanceProbe::Due)
        ) {
            self.nudge(namespace_id);
        }
    }
}
