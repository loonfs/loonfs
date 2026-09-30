//! The handle for one namespace, in a read-only or a writable mode.

use super::{LoonFs, ReadOnly, Writable};
use crate::fs::ReadCore;
use crate::publisher::{CloseNamespaceReport, NamespaceSession, NamespaceSessionState};
use crate::{NamespaceId, Result};
use loonfs_api::Subject;
use std::fmt;
use std::sync::Arc;

/// One namespace. Every mode reads it, and only [`Writable`] writes it.
///
/// Methods on this handle take no namespace id. The mode `M` is the only
/// difference between the two kinds of handle.
///
/// A `Namespace<ReadOnly>` comes from [`LoonFs::namespace`] or from
/// [`Self::read_only`]. It owns no writer session and no writer epoch, so
/// creating one does no IO and clones are cheap. Each read checks the cached
/// namespace head against the store, so read-only handles on many nodes need
/// no coordination with the node that writes.
///
/// A `Namespace<Writable>` comes from [`LoonFs::open_namespace`] and is the
/// namespace's writer session: one publication queue, one WAL-tail fold, and
/// the writer epoch that the session's first publish acquires. It shares the
/// runtime's store client, caches, admission budgets, and shutdown.
///
/// The host that holds a writable handle owns the session. Clones share it,
/// and the session lives while any clone is held. [`Namespace::close`] ends
/// it and waits for the work it admitted. Dropping the last clone also ends
/// it: work already admitted still publishes, and opening the namespace
/// again before that work finishes returns the same session. Once a session
/// has ended, the next [`LoonFs::open_namespace`] starts a new one, and its
/// first publish acquires a new writer epoch. The runtime never opens a
/// session by itself and never closes one to make room for another.
#[derive(Clone)]
pub struct Namespace<M> {
    pub(crate) core: ReadCore,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) mode: M,
    /// Held exactly when the mode is [`Writable`]. The mode type is shared
    /// with [`LoonFs`], which has no session, so the session lives here.
    session: Option<Arc<NamespaceSession>>,
}

impl<M: fmt::Debug> fmt::Debug for Namespace<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Namespace")
            .field("namespace_id", &self.namespace_id)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl<M> Namespace<M> {
    /// Returns the namespace this handle acts on.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the subject this handle acts for, or `None` for an unscoped
    /// service handle.
    pub fn subject(&self) -> Option<&Subject> {
        self.core.subject.as_ref()
    }

    /// Clones this handle with the subject used for its reads and, on a
    /// writable handle, its commits and uploads.
    pub fn as_subject(&self, subject: Subject) -> Self
    where
        M: Clone,
    {
        Self {
            core: self.core.as_subject(subject),
            ..self.clone()
        }
    }

    /// Returns a handle on the same namespace, for the same subject, that
    /// holds no writer session. Holding it does not keep a session open.
    pub fn read_only(&self) -> Namespace<ReadOnly> {
        Namespace::new(self.core.clone(), self.namespace_id.clone())
    }

    // Reads live in `fs/reads.rs`, `fs/snapshots.rs`, and
    // `fs/speculative_read.rs`.
}

impl Namespace<ReadOnly> {
    pub(crate) fn new(core: ReadCore, namespace_id: NamespaceId) -> Self {
        Self {
            core,
            namespace_id,
            mode: ReadOnly,
            session: None,
        }
    }
}

impl Namespace<Writable> {
    pub(crate) fn open(runtime: &LoonFs<Writable>, session: Arc<NamespaceSession>) -> Self {
        Self {
            core: runtime.core.clone(),
            namespace_id: session.namespace_id().clone(),
            mode: runtime.mode.clone(),
            session: Some(session),
        }
    }

    pub(crate) fn session(&self) -> &NamespaceSession {
        self.session
            .as_deref()
            .expect("a writable namespace handle should hold its writer session")
    }

    /// Returns the state of this handle's writer session.
    pub fn session_state(&self) -> NamespaceSessionState {
        self.session().state()
    }

    /// Refuses new work, drains admitted work, and ends the session.
    ///
    /// From the moment this is called, commits and deletes through every
    /// clone of this handle fail with `writer_session_closed`, and so does
    /// [`LoonFs::open_namespace`] for this namespace until the drain
    /// finishes. After this returns, the next open starts a new session,
    /// which acquires a new writer epoch. Closing a session that no longer
    /// admits work only waits for its drain, and the report says it was not
    /// open. Fails with `shutting_down` after shutdown begins.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.close_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "close_namespace",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn close(self) -> Result<CloseNamespaceReport> {
        self.core.record_trace_context(&tracing::Span::current());
        Ok(self.session().close().await?)
    }

    /// Waits for the WAL-tail fold this session is running, if any.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.wait_for_fold",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "wait_for_fold",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn wait_for_fold(&self) -> Result<()> {
        self.core.record_trace_context(&tracing::Span::current());
        self.session().wait_for_fold().await
    }

    // Namespace deletion lives in `fs/namespaces.rs`; mutation, commit, and
    // upload operations in `fs/writes.rs` and `fs/uploads.rs`; snapshots in
    // `fs/snapshots.rs`; and administrator recovery in `fs/maintenance.rs`.
}
