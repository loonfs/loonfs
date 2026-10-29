//! Host-supplied options and handles for HTTP requests.

use crate::http::ApiResponseError;
use crate::{HttpMetrics, RequestLimit, UsageRecorder};
use futures::future::{BoxFuture, Shared};
use futures::FutureExt as _;
use loonfs::{
    ChangeSeq, CloseNamespaceReport, LoonFs, Maintenance, Namespace, NamespaceSessionStatus,
    SharedObjectStore, Writable,
};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::{ErrorCode, MonotonicTimer, NamespaceId, SecretString, Subject};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum AuthPolicy {
    Unauthenticated,
    BearerToken(SecretString),
}

pub const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 8;
pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 16;
pub const DEFAULT_REQUEST_DEADLINE_MS: u64 = 60_000;

/// Builds the semaphore that admits `count` concurrent transfers, clamped to
/// the most permits a semaphore holds.
pub fn transfer_permits(count: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(count.min(Semaphore::MAX_PERMITS)))
}

/// Which of two jobs a host does for one subsystem: serve its requests, do
/// its background work, both, or neither.
///
/// The two jobs are independent. For grep, a read replica can serve searches
/// over an index another process maintains, and a write node can maintain
/// the index for namespaces it never answers searches about. Every
/// combination is named here, so none has to be validated away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobMode {
    /// Neither serve requests nor do the background work.
    Disabled,
    /// Serve requests without doing the background work.
    ServeOnly,
    /// Do the background work without serving requests. Grep is the one
    /// exception: a grep maintainer also serves the routes that enable,
    /// disable, and report the index it maintains.
    MaintainOnly,
    /// Serve requests and do the background work.
    ServeAndMaintain,
}

impl JobMode {
    /// Whether the host serves the subsystem's requests.
    pub fn serves(self) -> bool {
        matches!(self, Self::ServeOnly | Self::ServeAndMaintain)
    }

    /// Whether the host does the subsystem's background work.
    pub fn maintains(self) -> bool {
        matches!(self, Self::MaintainOnly | Self::ServeAndMaintain)
    }
}

#[derive(Clone, Debug)]
pub struct BindingConfig {
    /// Whether this binding serves the maintenance API group. The host runs
    /// any background maintenance itself.
    pub serves_maintenance: bool,
    /// Largest content a service-proxied read returns, or `None` for no limit.
    pub max_download_bytes: Option<u64>,
    /// Largest body a service-proxied upload accepts, or `None` for no limit.
    pub max_upload_bytes: Option<u64>,
    pub max_concurrent_uploads: usize,
    pub max_concurrent_downloads: usize,
    pub content_token_secret: SecretString,
    pub request_deadline_ms: u64,
    pub store_kind: ConfiguredObjectStoreKind,
    pub auth_policy: AuthPolicy,
}

/// The grep handles this binding holds, by job: a binding serves
/// queries only with a service, and maintains the index only with a
/// worker, so a capability it advertises always has its handle.
#[derive(Clone)]
pub enum GrepBinding {
    Disabled,
    ServeOnly {
        service: Arc<GrepService>,
    },
    MaintainOnly {
        worker: GrepWorker<SharedObjectStore>,
    },
    ServeAndMaintain {
        service: Arc<GrepService>,
        worker: GrepWorker<SharedObjectStore>,
    },
}

impl GrepBinding {
    /// The service that answers grep queries, where this binding serves them.
    pub fn service(&self) -> Option<&Arc<GrepService>> {
        match self {
            Self::ServeOnly { service } | Self::ServeAndMaintain { service, .. } => Some(service),
            Self::Disabled | Self::MaintainOnly { .. } => None,
        }
    }

    /// The worker that maintains the grep index, where this binding maintains it.
    pub fn worker(&self) -> Option<&GrepWorker<SharedObjectStore>> {
        match self {
            Self::MaintainOnly { worker } | Self::ServeAndMaintain { worker, .. } => Some(worker),
            Self::Disabled | Self::ServeOnly { .. } => None,
        }
    }

    /// Whether this binding answers grep queries.
    pub fn serves(&self) -> bool {
        self.service().is_some()
    }

    /// Whether this binding maintains the grep index.
    pub fn maintains(&self) -> bool {
        self.worker().is_some()
    }
}

/// Hosts supply one runtime over one store, with permit limits matching
/// `options`. The grep handles live in `grep`.
#[derive(Clone)]
pub struct BindingState {
    pub options: Arc<BindingConfig>,
    pub runtime: LoonFs<Writable>,
    pub namespaces: Arc<Namespaces>,
    pub maintenance: Maintenance,
    pub probe_store: SharedObjectStore,
    pub direct_transfers: Option<DirectTransferIssuers>,
    pub grep: GrepBinding,
    pub request_limit: Option<RequestLimit>,
    pub upload_permits: Arc<Semaphore>,
    pub download_permits: Arc<Semaphore>,
    pub metrics: Arc<HttpMetrics>,
    /// The recorder of one usage report for each namespace request, or `None` to record none.
    pub usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

impl BindingState {
    /// Returns the runtime scoped to `subject`, or the service runtime when
    /// the request has no subject.
    pub(crate) fn runtime_for(&self, subject: Option<Subject>) -> LoonFs<Writable> {
        match subject {
            Some(subject) => self.runtime.with_subject(subject),
            None => self.runtime.clone(),
        }
    }

    /// Opens the host's writable handle for `namespace_id`, scoped to
    /// `subject` unless `subject` is `None`.
    pub(crate) async fn open_writer(
        &self,
        namespace_id: &NamespaceId,
        subject: Option<Subject>,
    ) -> Result<Namespace<Writable>, ApiResponseError> {
        let namespace = self
            .namespaces
            .open(namespace_id)
            .await
            .map_err(ApiResponseError::for_namespace(namespace_id))?;
        Ok(match subject {
            Some(subject) => namespace.with_subject(subject),
            None => namespace,
        })
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
    last_opened_at_ms: u64,
    closing: Option<NamespaceClose>,
    pub indexed_seq: Option<ChangeSeq>,
    pub index_dirty: bool,
    pub collected_seq: Option<ChangeSeq>,
    pub collected_at_ms: u64,
    pub metadata_retry: SweepRetry,
    pub index_retry: SweepRetry,
    pub gc_retry: SweepRetry,
}

/// When a host's maintenance sweep next makes one call on a held namespace,
/// and how many times in a row that call has failed there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepRetry {
    /// The earliest time the sweep's ticks make the call again, on the
    /// table's monotonic timer.
    pub retry_at_ms: u64,
    /// The call's failures in a row, zero after an attempt that didn't fail.
    pub attempts: u32,
}

type NamespaceClose = Shared<BoxFuture<'static, loonfs::Result<CloseNamespaceReport>>>;

impl HeldNamespace {
    /// Whether the grep index has work for a session at `seq`: the index is
    /// marked dirty, or the sweep last brought the index up to date at
    /// another seq.
    pub fn index_pending(&self, seq: Option<ChangeSeq>) -> bool {
        self.index_dirty || self.indexed_seq != seq
    }

    /// Whether garbage collection is due for a session at `seq`: the seq
    /// moved since the last collection, and at least `interval_ms` passed
    /// since that collection.
    pub fn collection_pending(
        &self,
        seq: Option<ChangeSeq>,
        now_ms: u64,
        interval_ms: u64,
    ) -> bool {
        seq != self.collected_seq && now_ms.saturating_sub(self.collected_at_ms) >= interval_ms
    }

    fn begin_close(&mut self) -> NamespaceClose {
        self.closing
            .get_or_insert_with(|| self.handle.clone().close().boxed().shared())
            .clone()
    }
}

impl Namespaces {
    /// Builds an empty table that measures idle ages on the runtime's monotonic timer.
    pub fn new(runtime: LoonFs<Writable>) -> Self {
        Self {
            timer: runtime.monotonic_timer(),
            runtime,
            handles: Mutex::default(),
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
                    let mut held = entry
                        .lock()
                        .expect("a held namespace lock should not be poisoned");
                    if held.closing.is_none()
                        && held.handle.session_status() != NamespaceSessionStatus::Closed
                    {
                        held.last_opened_at_ms = self.now_ms();
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
                            last_opened_at_ms: now_ms,
                            closing: None,
                            indexed_seq: None,
                            index_dirty: false,
                            collected_seq: None,
                            collected_at_ms: now_ms,
                            metadata_retry: SweepRetry::default(),
                            index_retry: SweepRetry::default(),
                            gc_retry: SweepRetry::default(),
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
            let mut held = entry
                .lock()
                .expect("a held namespace lock should not be poisoned");
            if !held.handle.is_exclusively_held()
                || held.handle.last_published_seq() != expected_seq
                || !is_quiet(&held)
                || self.now_ms().saturating_sub(held.last_opened_at_ms) <= idle_after_ms
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
            let mut held = entry
                .lock()
                .expect("a held namespace lock should not be poisoned");
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
    pub fn visit_held<T>(
        &self,
        namespace_id: &NamespaceId,
        visit: impl FnOnce(&mut HeldNamespace) -> T,
    ) -> Option<T> {
        self.lock().get(namespace_id).map(|entry| {
            visit(
                &mut entry
                    .lock()
                    .expect("a held namespace lock should not be poisoned"),
            )
        })
    }

    /// Keeps a visit's facts attached to its original entry through a concurrent close.
    pub fn entry(&self, namespace_id: &NamespaceId) -> Option<Arc<Mutex<HeldNamespace>>> {
        self.lock().get(namespace_id).cloned()
    }

    /// Reads the runtime's monotonic timer, which the host measures idle ages
    /// and retry times on.
    pub fn now_ms(&self) -> u64 {
        self.timer.monotonic_now_ms()
    }

    pub fn mark_index_dirty(&self, namespace_id: &NamespaceId) {
        self.visit_held(namespace_id, |held| held.index_dirty = true);
    }

    /// Checks the held session under the table lock, so a late error cannot
    /// drop a healthy replacement. `error` supplies the winning writer for
    /// the warning. Returns whether a fenced handle was dropped.
    pub fn forget_if_fenced(&self, namespace_id: &NamespaceId, error: &loonfs::Error) -> bool {
        let mut handles = self.lock();
        if !handles.get(namespace_id).is_some_and(|entry| {
            entry
                .lock()
                .expect("a held namespace lock should not be poisoned")
                .handle
                .session_status()
                == NamespaceSessionStatus::Fenced
        }) {
            return false;
        }
        handles.remove(namespace_id);
        self.fenced_sessions_dropped.fetch_add(1, Ordering::Relaxed);
        drop(handles);
        let active_writer_id = error
            .to_error_response()
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
        self.handles
            .lock()
            .expect("the held namespace table lock should not be poisoned")
    }
}
