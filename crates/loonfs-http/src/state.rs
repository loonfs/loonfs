//! Host-supplied options and handles for HTTP requests.

use crate::{HttpMetrics, RequestLimit};
use futures::future::{BoxFuture, Shared};
use futures::FutureExt as _;
use loonfs::{
    ChangeSeq, CloseNamespaceReport, InlineContentPolicy, LoonFs, Maintenance, Namespace,
    NamespaceSessionState, SharedObjectStore, SnapshotPolicy, Writable,
};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::{ErrorCode, MonotonicTimer, NamespaceId, SecretString, StdMonotonicTimer};
use std::collections::BTreeMap;
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
    pub request_limit: Option<RequestLimit>,
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
/// opened. A sweep can close a caught-up session after its last open is old enough
/// and no caller holds a clone. A later write opens a new session and acquires
/// a new writer epoch. Deletion or a request that finds the namespace gone
/// also stops holding the handle.
/// A request that finds its session fenced fails with `writer_fenced` and
/// drops the held handle. The session stays dead. A later request opens a
/// new session, whose first publish takes the namespace back. While the old
/// session's work ends, an open can fail with retryable `writer_session_closed`.
/// Sustained fencing of one namespace means two writers receive its traffic.
pub struct Namespaces {
    runtime: LoonFs<Writable>,
    handles: Mutex<BTreeMap<NamespaceId, Arc<Mutex<HeldNamespace>>>>,
    timer: Arc<dyn MonotonicTimer>,
    fenced_sessions_dropped: AtomicU64,
}

pub struct HeldNamespace {
    pub handle: Namespace<Writable>,
    last_opened_ms: u64,
    closing: Option<NamespaceClose>,
    pub indexed_seq: Option<ChangeSeq>,
    pub index_dirty: bool,
    pub collected_seq: Option<ChangeSeq>,
    pub collected_ms: u64,
    pub metadata_retry_after_ms: u64,
}

type NamespaceClose = Shared<BoxFuture<'static, loonfs::Result<CloseNamespaceReport>>>;

impl HeldNamespace {
    fn begin_close(&mut self) -> NamespaceClose {
        self.closing
            .get_or_insert_with(|| self.handle.clone().close().boxed().shared())
            .clone()
    }
}

impl Namespaces {
    pub fn new(runtime: LoonFs<Writable>) -> Self {
        Self::new_with_timer(runtime, Arc::new(StdMonotonicTimer::default()))
    }

    /// Supplies monotonic time for idle checks and deterministic host tests.
    pub fn new_with_timer(runtime: LoonFs<Writable>, timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            runtime,
            handles: Mutex::default(),
            timer,
            fenced_sessions_dropped: AtomicU64::new(0),
        }
    }

    /// Opens an existing namespace. An open during a close waits for its drain.
    pub async fn open(&self, namespace_id: &NamespaceId) -> loonfs::Result<Namespace<Writable>> {
        let mut checked = false;
        loop {
            let closing = {
                let mut handles = self.lock();
                if let Some(entry) = handles.get(namespace_id) {
                    let mut held = entry.lock().unwrap_or_else(PoisonError::into_inner);
                    if held.closing.is_none()
                        && held.handle.session_state() != NamespaceSessionState::Closed
                    {
                        held.last_opened_ms = self.now_ms();
                        return Ok(held.handle.clone());
                    }
                    Some((entry.clone(), held.begin_close()))
                } else if checked {
                    let handle = self.runtime.open_namespace(namespace_id)?;
                    let now_ms = self.now_ms();
                    handles.insert(
                        namespace_id.clone(),
                        Arc::new(Mutex::new(HeldNamespace {
                            handle: handle.clone(),
                            last_opened_ms: now_ms,
                            closing: None,
                            indexed_seq: None,
                            index_dirty: false,
                            collected_seq: None,
                            collected_ms: now_ms,
                            metadata_retry_after_ms: 0,
                        })),
                    );
                    return Ok(handle);
                } else {
                    None
                }
            };
            if let Some((entry, closing)) = closing {
                self.finish_close(namespace_id, &entry, closing).await?;
            } else {
                self.runtime.namespace(namespace_id).metadata().await?;
                checked = true;
            }
        }
    }

    /// Rechecks the seq, idle age, and exclusive ownership under the table lock.
    pub async fn close_if_idle(
        &self,
        namespace_id: &NamespaceId,
        expected_seq: Option<ChangeSeq>,
        idle_after_ms: u64,
        is_quiet: impl FnOnce(&HeldNamespace) -> bool,
    ) -> loonfs::Result<Option<CloseNamespaceReport>> {
        let (entry, closing) = {
            let handles = self.lock();
            let Some(entry) = handles.get(namespace_id) else {
                return Ok(None);
            };
            let mut held = entry.lock().unwrap_or_else(PoisonError::into_inner);
            if !held.handle.is_exclusively_held()
                || held.handle.last_published_seq() != expected_seq
                || !is_quiet(&held)
                || self.now_ms().saturating_sub(held.last_opened_ms) <= idle_after_ms
            {
                return Ok(None);
            }
            (entry.clone(), held.begin_close())
        };
        self.finish_close(namespace_id, &entry, closing)
            .await
            .map(Some)
    }

    /// Ends the held session before a waiting open can create its replacement.
    pub async fn close(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<Option<CloseNamespaceReport>> {
        let (entry, closing) = {
            let handles = self.lock();
            let Some(entry) = handles.get(namespace_id) else {
                return Ok(None);
            };
            let mut held = entry.lock().unwrap_or_else(PoisonError::into_inner);
            (entry.clone(), held.begin_close())
        };
        self.finish_close(namespace_id, &entry, closing)
            .await
            .map(Some)
    }

    async fn finish_close(
        &self,
        namespace_id: &NamespaceId,
        entry: &Arc<Mutex<HeldNamespace>>,
        closing: NamespaceClose,
    ) -> loonfs::Result<CloseNamespaceReport> {
        let result = closing.await;
        let mut handles = self.lock();
        if handles
            .get(namespace_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            handles.remove(namespace_id);
        }
        result
    }

    /// Lists held ids without extending any writer handle's lifetime.
    pub fn held_ids(&self) -> Vec<NamespaceId> {
        self.lock().keys().cloned().collect()
    }

    /// Advances a host walk with one table lookup, without holding the lock between entries.
    pub fn next_held_id(&self, after: Option<&NamespaceId>) -> Option<NamespaceId> {
        use std::ops::Bound::{Excluded, Unbounded};
        let handles = self.lock();
        match after {
            Some(after) => handles
                .range::<NamespaceId, _>((Excluded(after), Unbounded))
                .next(),
            None => handles.first_key_value(),
        }
        .map(|(namespace_id, _)| namespace_id.clone())
    }

    /// Reads or updates one held entry without cloning its handle.
    pub fn with_held<T>(
        &self,
        namespace_id: &NamespaceId,
        visit: impl FnOnce(&mut HeldNamespace) -> T,
    ) -> Option<T> {
        self.lock()
            .get(namespace_id)
            .map(|entry| visit(&mut entry.lock().unwrap_or_else(PoisonError::into_inner)))
    }

    /// Keeps a visit's facts attached to its original entry through a concurrent close.
    pub fn entry(&self, namespace_id: &NamespaceId) -> Option<Arc<Mutex<HeldNamespace>>> {
        self.lock().get(namespace_id).cloned()
    }

    pub fn now_ms(&self) -> u64 {
        self.timer.monotonic_now_ms()
    }

    pub fn mark_index_dirty(&self, namespace_id: &NamespaceId) {
        self.with_held(namespace_id, |held| held.index_dirty = true);
    }

    /// Checks the held session under the table lock, so a late error cannot
    /// drop a healthy replacement. `error` supplies the winning writer for
    /// the warning. Returns whether a fenced handle was dropped.
    pub fn forget_if_fenced(&self, namespace_id: &NamespaceId, error: &loonfs::Error) -> bool {
        let mut handles = self.lock();
        if !handles.get(namespace_id).is_some_and(|entry| {
            entry
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .session_state()
                == NamespaceSessionState::Fenced
        }) {
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

    fn lock(&self) -> MutexGuard<'_, BTreeMap<NamespaceId, Arc<Mutex<HeldNamespace>>>> {
        self.handles.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
