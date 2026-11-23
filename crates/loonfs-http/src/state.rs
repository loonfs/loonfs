//! Host-supplied options and handles for HTTP requests, and the table of
//! writer sessions that a host keeps open.

use crate::http::ApiResponseError;
use crate::{HttpMetrics, RequestLimit, UsageRecorder};
use futures::future::{BoxFuture, Shared};
use futures::FutureExt as _;
use loonfs::{
    CloseNamespaceReport, CommitSeq, LoonFs, Maintenance, Namespace, NamespaceSessionStatus,
    SharedObjectStore, Writable,
};
use loonfs_grep::{GrepService, GrepWorker};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_types::{Context, MonotonicTimer, NamespaceId, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Semaphore;

/// How the HTTP API authenticates a request to a contract route, or to a
/// host route wrapped with [`authenticate_routes`](crate::authenticate_routes)
/// ([Authentication and transport](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#authentication-and-transport)).
#[derive(Clone, Debug)]
pub enum AuthPolicy {
    /// Admits every request without checking credentials.
    Open,
    /// Requires the `Authorization: Bearer <token>` header with this token,
    /// and refuses any other request with 401 `unauthorized` before the
    /// request's headers, query, or body are parsed.
    BearerToken(SecretString),
}

/// The default of [`HttpConfig::max_concurrent_uploads`], which the
/// reference server's config and the CLI's embedded profile use.
pub const DEFAULT_MAX_CONCURRENT_UPLOADS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// The default of [`HttpConfig::max_concurrent_downloads`], which the
/// reference server's config and the CLI's embedded profile use.
pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: NonZeroUsize = NonZeroUsize::new(16).unwrap();
/// The default of [`HttpConfig::request_deadline_ms`], which the reference
/// server's config and the CLI's embedded profile use.
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

/// The options that a host sets for the HTTP API: the API groups it serves,
/// its transfer and request limits, its content token secret, and its
/// authentication.
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// Whether the HTTP API serves the maintenance API group. The host runs
    /// any background maintenance itself.
    pub serves_maintenance: bool,
    /// Largest content a service-proxied read returns, or `None` for no limit.
    pub max_download_bytes: Option<u64>,
    /// Largest body a service-proxied upload accepts, or `None` for no limit.
    pub max_upload_bytes: Option<u64>,
    /// Service-proxied uploads that the HTTP API streams at once, shared by
    /// every caller. An upload beyond the limit fails with 503 `server_busy`,
    /// and 0 refuses every proxied upload. The capability document reports
    /// the value as `upload.service_proxied.max_concurrent_requests`
    /// ([The capability document](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#21-the-capability-document)).
    pub max_concurrent_uploads: usize,
    /// Service-proxied content reads that the HTTP API streams at once,
    /// shared by every caller. A read holds its slot until its response body
    /// finishes or is dropped, a read beyond the limit fails with 503
    /// `server_busy`, and 0 refuses every proxied read. The capability
    /// document reports the value as
    /// `download.service_proxied.max_concurrent_requests`
    /// ([The capability document](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#21-the-capability-document)).
    pub max_concurrent_downloads: usize,
    /// Most requests the HTTP API serves at once, or `None` for no limit.
    pub max_in_flight_requests: Option<NonZeroUsize>,
    /// The secret that the HTTP API signs a completed upload's content token
    /// with, and that a commit checks each content token against
    /// ([Content references and tokens](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#content-references-and-tokens)).
    /// A server that accepts commits with tokens that another server issued
    /// needs that server's secret, and changing the secret makes the tokens
    /// already issued fail.
    pub content_token_secret: SecretString,
    /// Milliseconds that a request runs before the HTTP API cancels it and
    /// returns 503 `outcome_unknown` with cause `deadline`. Content reads,
    /// proxied uploads, maintenance jobs, and the store probe aren't
    /// cancelled.
    pub request_deadline_ms: u64,
    /// How the HTTP API authenticates requests.
    pub auth_policy: AuthPolicy,
}

/// The handles that a host supplies to the HTTP API: one runtime over one
/// store, and the grep handles.
pub struct HttpHandles {
    /// The runtime that the HTTP API reads with, creates and forks
    /// namespaces with, and builds the capability document from. Each
    /// request uses a copy of the runtime that acts in the request's context.
    pub runtime: LoonFs<Writable>,
    /// The writer sessions that every write request opens its namespace's
    /// writable handle through. Deleting a namespace drops its session, and
    /// enabling or disabling its grep index marks the session for the host's
    /// maintenance. A host usually passes a [`Namespaces`] table.
    pub namespaces: Arc<dyn WriterSessions>,
    /// The handle that the namespace and pool maintenance job routes run
    /// their jobs with. The HTTP API mounts those routes only when
    /// [`HttpConfig::serves_maintenance`] is set.
    pub maintenance: Maintenance,
    /// The issuers that presign direct uploads and downloads, or `None` when
    /// the store can't presign them. Without an issuer, the capability
    /// document turns the matching direct transfer feature off, and a request
    /// for that transfer fails with `not_supported`.
    pub direct_transfers: Option<DirectTransferIssuers>,
    /// The service that serves grep queries, or `None` when the HTTP API doesn't serve grep.
    pub grep_service: Option<Arc<GrepService>>,
    /// The worker that maintains the grep index, or `None` when the HTTP API doesn't maintain grep.
    pub grep_worker: Option<GrepWorker<SharedObjectStore>>,
    /// The metrics that the HTTP API records each served request and each
    /// `server_busy` refusal in. A host that passes [`HttpMetrics::recorder`]
    /// to its runtime records the runtime's metrics in the same place.
    pub metrics: Arc<HttpMetrics>,
    /// The recorder of one usage report for each namespace request, or `None` to record none.
    pub usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

/// The HTTP API's options and handles, and the permits that enforce the
/// concurrency limits in its options.
#[derive(Clone)]
pub struct HttpState {
    /// The options that the host passed to [`HttpState::new`].
    pub options: Arc<HttpConfig>,
    /// The [`HttpHandles::runtime`] that the host passed.
    pub runtime: LoonFs<Writable>,
    /// The [`HttpHandles::namespaces`] that the host passed.
    pub namespaces: Arc<dyn WriterSessions>,
    /// The [`HttpHandles::maintenance`] that the host passed.
    pub maintenance: Maintenance,
    /// The [`HttpHandles::direct_transfers`] that the host passed.
    pub direct_transfers: Option<DirectTransferIssuers>,
    /// The [`HttpHandles::grep_service`] that the host passed.
    pub grep_service: Option<Arc<GrepService>>,
    /// The [`HttpHandles::grep_worker`] that the host passed.
    pub grep_worker: Option<GrepWorker<SharedObjectStore>>,
    /// The [`HttpHandles::metrics`] that the host passed.
    pub metrics: Arc<HttpMetrics>,
    /// The [`HttpHandles::usage_recorder`] that the host passed.
    pub usage_recorder: Option<Arc<dyn UsageRecorder>>,
    request_limit: Option<RequestLimit>,
    upload_permits: Arc<Semaphore>,
    download_permits: Arc<Semaphore>,
}

impl HttpState {
    /// Builds the HTTP API state, with permits that admit the concurrency that `config`
    /// advertises.
    pub fn new(config: HttpConfig, handles: HttpHandles) -> Self {
        let HttpHandles {
            runtime,
            namespaces,
            maintenance,
            direct_transfers,
            grep_service,
            grep_worker,
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
            grep_service,
            grep_worker,
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

    /// Returns the cap on requests in flight, or `None` when the HTTP API has
    /// no cap.
    pub fn request_limit(&self) -> Option<&RequestLimit> {
        self.request_limit.as_ref()
    }

    /// Returns the runtime that acts in the request's `context`.
    pub(crate) fn runtime_for(&self, context: Context) -> LoonFs<Writable> {
        self.runtime.with_context(context)
    }

    /// Opens the host's writable handle for `namespace_id`, acting in the
    /// request's `context`.
    pub(crate) async fn open_writer(
        &self,
        namespace_id: &NamespaceId,
        context: Context,
    ) -> Result<Namespace<Writable>, ApiResponseError> {
        Ok(self
            .namespaces
            .open(namespace_id)
            .await
            .map_err(ApiResponseError::runtime)?
            .with_context(context))
    }
}

/// The writer sessions that a host keeps open for the handlers.
///
/// The handlers open a namespace's writable handle through these methods. An
/// open returns the held handle only while its session is open. When the
/// held session is fenced, or its namespace was deleted or is missing, the
/// open drops the handle and opens the namespace again.
pub trait WriterSessions: Send + Sync {
    /// Opens the writable handle of an existing namespace.
    fn open<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
    ) -> BoxFuture<'a, loonfs::Result<Namespace<Writable>>>;

    /// Drops the handle of a deleted namespace.
    fn forget(&self, namespace_id: &NamespaceId);

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
/// opened. The host's maintenance scheduler can close a caught-up session after its last open is old enough
/// and no caller has a clone. A later write opens a new session and acquires
/// a new writer epoch. Deletion also drops the handle.
/// An open returns the held handle only while its session is open. When the
/// session found its namespace deleted or missing, the open closes the
/// handle. When another writer fenced the session, the request that found
/// the fence failed with `writer_fenced`, and the open drops the handle
/// without a close, because the session can't be used again. The open then
/// starts a new session, whose first publish takes the namespace back. While
/// the old session's work ends, an open can fail with retryable
/// `writer_session_closed`. Sustained fencing of one namespace means two
/// writers receive its traffic.
pub struct Namespaces<S> {
    runtime: LoonFs<Writable>,
    handles: Mutex<BTreeMap<NamespaceId, Arc<Mutex<HeldNamespace<S>>>>>,
    timer: Arc<dyn MonotonicTimer>,
}

/// One writer session that the table keeps, and the state that its host
/// keeps beside the session.
pub struct HeldNamespace<S> {
    /// The session's writable handle, which an open returns while the
    /// session is open.
    pub handle: Namespace<Writable>,
    last_opened_at_ms: u64,
    closing: Option<NamespaceClose>,
    /// Whether the grep index has work that the host's maintenance hasn't
    /// done. The grep enable and disable routes set this flag.
    pub index_dirty: bool,
    /// The host's state for the session, which [`HostState::new`] creates
    /// when the table opens the session.
    pub host_state: S,
}

type NamespaceClose = Shared<BoxFuture<'static, loonfs::Result<CloseNamespaceReport>>>;

impl<S> HeldNamespace<S> {
    /// Starts the session's close, or joins the close that's running. A panic
    /// inside the close becomes an error, so every waiter gets the error and
    /// the close's cleanup removes the entry. A shared future that panicked
    /// would panic again for every later open of the namespace.
    fn begin_close(&mut self) -> NamespaceClose {
        self.closing
            .get_or_insert_with(|| {
                let handle = self.handle.clone();
                AssertUnwindSafe(handle.close())
                    .catch_unwind()
                    .map(|closed| match closed {
                        Ok(result) => result,
                        Err(payload) => Err(loonfs::Error::RuntimeTask(format!(
                            "closing the session panicked: {}",
                            loonfs_types::panic_message(payload.as_ref())
                        ))),
                    })
                    .boxed()
                    .shared()
            })
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
                    match (held.closing.is_some(), held.handle.session_status()) {
                        (false, NamespaceSessionStatus::Open) => {
                            held.last_opened_at_ms = self.timer.monotonic_now_ms();
                            return Ok(held.handle.clone());
                        }
                        (false, NamespaceSessionStatus::Fenced) => {
                            drop(held);
                            handles.remove(namespace_id);
                            tracing::warn!(%namespace_id, "host dropped a fenced writer session");
                            None
                        }
                        _ => Some((entry.clone(), held.begin_close())),
                    }
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
        expected_seq: Option<CommitSeq>,
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

    /// Keeps the facts of one run of a host's maintenance steps attached to
    /// its original entry through a concurrent close.
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
