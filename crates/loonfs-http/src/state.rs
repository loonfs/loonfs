//! Host-supplied options and handles for HTTP requests, and the table of
//! writer sessions that a host keeps open.

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
use loonfs_types::{ErrorCode, MonotonicTimer, NamespaceId, SecretString, Subject};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum AuthPolicy {
    Unauthenticated,
    BearerToken(SecretString),
}

pub const DEFAULT_MAX_CONCURRENT_UPLOADS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: NonZeroUsize = NonZeroUsize::new(16).unwrap();
pub const DEFAULT_REQUEST_DEADLINE_MS: u64 = 60_000;

/// Which of two jobs a host does for one subsystem: serve its requests, do
/// its background work, both, or neither.
///
/// The two jobs are independent. For grep, a read replica can serve searches
/// over an index another process maintains, and a write node can maintain
/// the index for namespaces it never serves searches for. Every
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

    /// Returns the mode's spelling in a config file.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::ServeOnly => "serve_only",
            Self::MaintainOnly => "maintain_only",
            Self::ServeAndMaintain => "serve_and_maintain",
        }
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
    /// Most requests the binding serves at once, or `None` for no limit.
    pub max_in_flight_requests: Option<NonZeroUsize>,
    pub content_token_secret: SecretString,
    pub request_deadline_ms: u64,
    pub auth_policy: AuthPolicy,
}

/// The grep handles this binding has, by job: a binding serves
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
    /// The service that serves grep queries, where this binding serves them.
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

    /// Whether this binding serves grep queries.
    pub fn serves(&self) -> bool {
        self.service().is_some()
    }

    /// Whether this binding maintains the grep index.
    pub fn maintains(&self) -> bool {
        self.worker().is_some()
    }
}

/// The handles that a host supplies to a binding: one runtime over one
/// store, and the grep handles in `grep`.
pub struct BindingHandles {
    pub runtime: LoonFs<Writable>,
    pub namespaces: Arc<dyn WriterSessions>,
    pub maintenance: Maintenance,
    pub direct_transfers: Option<DirectTransferIssuers>,
    pub grep: GrepBinding,
    pub metrics: Arc<HttpMetrics>,
    /// The recorder of one usage report for each namespace request, or `None` to record none.
    pub usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

/// A binding's options and handles, and the permits that enforce the
/// concurrency limits in its options.
#[derive(Clone)]
pub struct BindingState {
    pub options: Arc<BindingConfig>,
    pub runtime: LoonFs<Writable>,
    pub namespaces: Arc<dyn WriterSessions>,
    pub maintenance: Maintenance,
    pub direct_transfers: Option<DirectTransferIssuers>,
    pub grep: GrepBinding,
    pub metrics: Arc<HttpMetrics>,
    pub usage_recorder: Option<Arc<dyn UsageRecorder>>,
    request_limit: Option<RequestLimit>,
    upload_permits: Arc<Semaphore>,
    download_permits: Arc<Semaphore>,
}

impl BindingState {
    /// Builds a binding whose permits admit the concurrency that `config`
    /// advertises.
    pub fn new(config: BindingConfig, handles: BindingHandles) -> Self {
        let BindingHandles {
            runtime,
            namespaces,
            maintenance,
            direct_transfers,
            grep,
            metrics,
            usage_recorder,
        } = handles;
        let permits = |count: usize| Arc::new(Semaphore::new(count.min(Semaphore::MAX_PERMITS)));
        Self {
            request_limit: config.max_in_flight_requests.map(RequestLimit::new),
            upload_permits: permits(config.max_concurrent_uploads),
            download_permits: permits(config.max_concurrent_downloads),
            options: Arc::new(config),
            runtime,
            namespaces,
            maintenance,
            direct_transfers,
            grep,
            metrics,
            usage_recorder,
        }
    }

    /// Returns the permits of proxied uploads.
    pub fn upload_permits(&self) -> &Arc<Semaphore> {
        &self.upload_permits
    }

    /// Returns the permits of proxied content reads.
    pub fn download_permits(&self) -> &Arc<Semaphore> {
        &self.download_permits
    }

    /// Returns the cap on requests in flight, or `None` when the binding has
    /// no cap.
    pub fn request_limit(&self) -> Option<&RequestLimit> {
        self.request_limit.as_ref()
    }

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
            .map_err(ApiResponseError::runtime)?;
        Ok(match subject {
            Some(subject) => namespace.with_subject(subject),
            None => namespace,
        })
    }
}

/// The writer sessions that a host keeps open for the handlers.
///
/// The handlers open a namespace's writable handle through these methods,
/// and call them when a request finds the namespace gone or the session
/// fenced, so that the host drops the handle.
pub trait WriterSessions: Send + Sync {
    /// Opens the writable handle of an existing namespace.
    fn open<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
    ) -> BoxFuture<'a, loonfs::Result<Namespace<Writable>>>;

    /// Drops the handle of a deleted namespace.
    fn forget(&self, namespace_id: &NamespaceId);

    /// Drops the handle after a request through it found the
    /// namespace missing or deleted. Another writer can delete a namespace
    /// after this host opened its handle.
    fn forget_if_gone(&self, namespace_id: &NamespaceId, code: ErrorCode);

    /// Drops the handle when its session is fenced, and returns
    /// whether a fenced handle was dropped. `error` supplies the winning
    /// writer for the warning.
    fn forget_if_fenced(&self, namespace_id: &NamespaceId, error: &loonfs::Error) -> bool;

    /// Records that the namespace's grep index has work for the host's
    /// maintenance.
    fn mark_index_dirty(&self, namespace_id: &NamespaceId);
}

/// The state that a host keeps beside each writer session that
/// [`Namespaces`] keeps.
pub trait HostState: Send + 'static {
    /// Returns the state of a session that the table opened at
    /// `opened_at_ms`, on the runtime's monotonic timer.
    fn new(opened_at_ms: u64) -> Self;
}

impl HostState for () {
    fn new(_opened_at_ms: u64) -> Self {}
}

/// The writable handle this host keeps for each namespace.
///
/// The host sets which writer sessions stay open and for how long: a
/// session stays open while the host keeps its handle. This reference host keeps a
/// handle only for a namespace that existed when the handle was first
/// opened. A sweep can close a caught-up session after its last open is old enough
/// and no caller has a clone. A later write opens a new session and acquires
/// a new writer epoch. Deletion or a request that finds the namespace gone
/// also drops the handle.
/// A request that finds its session fenced fails with `writer_fenced` and
/// drops the handle. The session stays dead. A later request opens a
/// new session, whose first publish takes the namespace back. While the old
/// session's work ends, an open can fail with retryable `writer_session_closed`.
/// Sustained fencing of one namespace means two writers receive its traffic.
pub struct Namespaces<S> {
    runtime: LoonFs<Writable>,
    handles: Mutex<BTreeMap<NamespaceId, Arc<Mutex<HeldNamespace<S>>>>>,
    timer: Arc<dyn MonotonicTimer>,
}

/// One writer session that the table keeps, and the state that its host
/// keeps beside the session.
pub struct HeldNamespace<S> {
    pub handle: Namespace<Writable>,
    last_opened_at_ms: u64,
    closing: Option<NamespaceClose>,
    /// Whether the grep index has work that the host's maintenance hasn't
    /// done. The grep enable and disable routes set this flag.
    pub index_dirty: bool,
    pub host_state: S,
}

type NamespaceClose = Shared<BoxFuture<'static, loonfs::Result<CloseNamespaceReport>>>;

impl<S> HeldNamespace<S> {
    fn begin_close(&mut self) -> NamespaceClose {
        self.closing
            .get_or_insert_with(|| self.handle.clone().close().boxed().shared())
            .clone()
    }
}

impl<S: HostState> Namespaces<S> {
    /// Builds an empty table that measures idle ages on the runtime's monotonic timer.
    pub fn new(runtime: LoonFs<Writable>) -> Self {
        Self {
            timer: runtime.monotonic_timer(),
            runtime,
            handles: Mutex::default(),
        }
    }

    /// Opens an existing namespace. An open during a close waits for its drain.
    async fn open_held(&self, namespace_id: &NamespaceId) -> loonfs::Result<Namespace<Writable>> {
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
                        held.last_opened_at_ms = self.timer.monotonic_now_ms();
                        return Ok(held.handle.clone());
                    }
                    Some((entry.clone(), held.begin_close()))
                } else if checked {
                    let handle = self.runtime.open_namespace(namespace_id)?;
                    let now_ms = self.timer.monotonic_now_ms();
                    handles.insert(
                        namespace_id.clone(),
                        Arc::new(Mutex::new(HeldNamespace {
                            handle: handle.clone(),
                            last_opened_at_ms: now_ms,
                            closing: None,
                            index_dirty: false,
                            host_state: S::new(now_ms),
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
        is_quiet: impl FnOnce(&HeldNamespace<S>) -> bool,
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
                || self
                    .timer
                    .monotonic_now_ms()
                    .saturating_sub(held.last_opened_at_ms)
                    <= idle_after_ms
            {
                return Ok(None);
            }
            (entry.clone(), held.begin_close())
        };
        self.finish_close(namespace_id, &entry, closing)
            .await
            .map(Some)
    }

    async fn finish_close(
        &self,
        namespace_id: &NamespaceId,
        entry: &Arc<Mutex<HeldNamespace<S>>>,
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

    /// Advances a host walk with one table lookup, and unlocks the table between entries.
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

    /// Keeps a visit's facts attached to its original entry through a concurrent close.
    pub fn entry(&self, namespace_id: &NamespaceId) -> Option<Arc<Mutex<HeldNamespace<S>>>> {
        self.lock().get(namespace_id).cloned()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<NamespaceId, Arc<Mutex<HeldNamespace<S>>>>> {
        self.handles
            .lock()
            .expect("the held namespace table lock should not be poisoned")
    }
}

impl<S: HostState> WriterSessions for Namespaces<S> {
    fn open<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
    ) -> BoxFuture<'a, loonfs::Result<Namespace<Writable>>> {
        self.open_held(namespace_id).boxed()
    }

    fn forget(&self, namespace_id: &NamespaceId) {
        self.lock().remove(namespace_id);
    }

    fn forget_if_gone(&self, namespace_id: &NamespaceId, code: ErrorCode) {
        if matches!(
            code,
            ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
        ) {
            self.forget(namespace_id);
        }
    }

    fn forget_if_fenced(&self, namespace_id: &NamespaceId, error: &loonfs::Error) -> bool {
        // The status is checked under the table lock, so a late error can't
        // drop a healthy replacement.
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
        drop(handles);
        let active_writer_id = error
            .to_error_response()
            .details
            .and_then(|details| details.active_writer)
            .map(|writer| writer.active_writer_id);
        tracing::warn!(
            %namespace_id,
            active_writer_id = active_writer_id.as_ref().map(tracing::field::display),
            "host dropped a fenced writer session"
        );
        true
    }

    fn mark_index_dirty(&self, namespace_id: &NamespaceId) {
        if let Some(entry) = self.entry(namespace_id) {
            entry
                .lock()
                .expect("a held namespace lock should not be poisoned")
                .index_dirty = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<S: HostState> Namespaces<S> {
        /// Ends the held session before a waiting open can create its replacement.
        pub(crate) async fn close(
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

        /// Lists held ids without extending any writer handle's lifetime.
        pub(crate) fn held_ids(&self) -> Vec<NamespaceId> {
            self.lock().keys().cloned().collect()
        }
    }
}
