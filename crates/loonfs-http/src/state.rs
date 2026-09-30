//! Host-supplied options and handles for HTTP requests.

use crate::HttpMetrics;
use loonfs::{
    CloseNamespaceReport, InlineContentOptions, LoonFs, Maintenance, MaintenanceHandle,
    MaintenanceJob, MaintenanceProbe, Namespace, SharedObjectStore, SnapshotPolicy, Writable,
};
use loonfs_api::{ErrorCode, NamespaceId, SecretString};
use loonfs_grep::{GrepMaintenanceJob, GrepService, GrepWorker, GREP_INDEX_JOB};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum AuthPolicy {
    Unauthenticated,
    BearerToken(SecretString),
}

pub const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 8;
pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 16;
pub const DEFAULT_REQUEST_DEADLINE_MS: u64 = 60_000;

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
    pub idle_fold_after_ms: u64,
    pub store_kind: ConfiguredObjectStoreKind,
    pub auth_policy: AuthPolicy,
}

/// Hosts supply one runtime over one store, with grep services and permit limits matching `options`.
#[derive(Clone)]
pub struct BindingState {
    pub options: Arc<BindingOptions>,
    pub runtime: LoonFs<Writable>,
    pub namespaces: Arc<Namespaces>,
    pub maintenance: Maintenance,
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

/// The writable handle this host holds for each namespace.
///
/// The host decides which writer sessions stay open and for how long: a
/// session lives while its handle is held. This reference host keeps every
/// namespace it has written open, with no cap and no eviction. It stops
/// holding a handle only when the namespace is deleted or turns out not to
/// exist.
pub struct Namespaces {
    runtime: LoonFs<Writable>,
    handles: Mutex<HashMap<NamespaceId, Namespace<Writable>>>,
}

impl Namespaces {
    pub fn new(runtime: LoonFs<Writable>) -> Self {
        Self {
            runtime,
            handles: Mutex::default(),
        }
    }

    /// Returns the handle held for `namespace_id`, opening one first if none is held.
    pub fn open(&self, namespace_id: &NamespaceId) -> loonfs::Result<Namespace<Writable>> {
        let mut handles = self.lock();
        if let Some(handle) = handles.get(namespace_id) {
            return Ok(handle.clone());
        }
        let handle = self.runtime.open_namespace(namespace_id)?;
        handles.insert(namespace_id.clone(), handle.clone());
        Ok(handle)
    }

    /// Closes the session of the handle held for `namespace_id` and stops
    /// holding it. Returns `None` when no handle is held.
    pub async fn close(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<Option<CloseNamespaceReport>> {
        let Some(handle) = self.lock().remove(namespace_id) else {
            return Ok(None);
        };
        handle.close().await.map(Some)
    }

    /// Stops holding the handle of a deleted namespace.
    pub(crate) fn forget(&self, namespace_id: &NamespaceId) {
        self.lock().remove(namespace_id);
    }

    /// Stops holding the handle after a request through it found the
    /// namespace missing or deleted. Without this, the map would keep a
    /// session for every namespace id a client ever named.
    pub(crate) fn forget_if_gone(&self, namespace_id: &NamespaceId, code: ErrorCode) {
        if matches!(
            code,
            ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
        ) {
            self.forget(namespace_id);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<NamespaceId, Namespace<Writable>>> {
        self.handles.lock().unwrap_or_else(PoisonError::into_inner)
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
