//! Host-supplied options and handles for HTTP requests.

use crate::HttpMetrics;
use loonfs::{
    CloseNamespaceReport, InlineContentPolicy, LoonFs, Maintenance, Namespace,
    NamespaceSessionState, SharedObjectStore, SnapshotPolicy, Writable,
};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::{ErrorCode, NamespaceId, SecretString};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    pub inline_content: InlineContentPolicy,
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
/// session lives while its handle is held. This reference host holds a
/// handle only for a namespace that existed when the handle was first
/// opened. It keeps each such handle, with no cap and no eviction, and stops
/// holding it when the namespace is deleted or a request finds it gone.
/// A request that finds its session fenced fails with `writer_fenced` and
/// drops the held handle. The session stays dead. A later request opens a
/// new session, whose first publish takes the namespace back. While the old
/// session's work ends, an open can fail with retryable `writer_session_closed`.
/// Sustained fencing of one namespace means two writers receive its traffic.
pub struct Namespaces {
    runtime: LoonFs<Writable>,
    handles: Mutex<HashMap<NamespaceId, Namespace<Writable>>>,
    fenced_sessions_dropped: AtomicU64,
}

impl Namespaces {
    pub fn new(runtime: LoonFs<Writable>) -> Self {
        Self {
            runtime,
            handles: Mutex::default(),
            fenced_sessions_dropped: AtomicU64::new(0),
        }
    }

    /// Returns the handle held for `namespace_id`. When none is held, first
    /// reads the namespace through a read-only handle, which holds no writer
    /// session. A missing or deleted namespace fails that read with
    /// `namespace_not_found` or `namespace_deleted`. Otherwise this opens
    /// the writable handle and holds it in one step, with no await between,
    /// so a `close` that runs during the read cannot leave a closed session
    /// in the table.
    pub async fn open(&self, namespace_id: &NamespaceId) -> loonfs::Result<Namespace<Writable>> {
        let held = self.lock().get(namespace_id).cloned();
        if let Some(handle) = held {
            return Ok(handle);
        }
        self.runtime.namespace(namespace_id).metadata().await?;
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

    /// Returns a clone of every handle this host holds.
    pub fn held(&self) -> Vec<Namespace<Writable>> {
        self.lock().values().cloned().collect()
    }

    /// Checks the held session under the table lock, so a late error cannot
    /// drop a healthy replacement. `error` supplies the winning writer for
    /// the warning. Returns whether a fenced handle was dropped.
    pub fn forget_if_fenced(&self, namespace_id: &NamespaceId, error: &loonfs::Error) -> bool {
        let mut handles = self.lock();
        if !handles
            .get(namespace_id)
            .is_some_and(|handle| handle.session_state() == NamespaceSessionState::Fenced)
        {
            return false;
        }
        handles.remove(namespace_id);
        self.fenced_sessions_dropped.fetch_add(1, Ordering::Relaxed);
        drop(handles);
        let active_writer_id = error
            .to_api_error()
            .details
            .and_then(|details| details.active_writer_id);
        tracing::warn!(
            %namespace_id,
            active_writer_id = active_writer_id.as_ref().map(tracing::field::display),
            "host dropped a fenced writer session"
        );
        true
    }

    /// Counts fenced handles dropped over this table's lifetime.
    pub fn fenced_sessions_dropped(&self) -> u64 {
        self.fenced_sessions_dropped.load(Ordering::Relaxed)
    }

    /// Stops holding the handle of a deleted namespace.
    pub(crate) fn forget(&self, namespace_id: &NamespaceId) {
        self.lock().remove(namespace_id);
    }

    /// Stops holding the handle after a request through it found the
    /// namespace missing or deleted. Another writer can delete a namespace
    /// after this host holds its handle.
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
