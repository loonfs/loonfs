//! The handle for one namespace, in a read-only or a writable mode.

use super::{LoonFs, ReadOnly, Writable};
use crate::fs::RuntimeCore;
use crate::publisher::{CloseNamespaceReport, NamespaceSession, NamespaceSessionState};
use crate::{ChangeSeq, NamespaceId, Result};
use loonfs_types::Subject;
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
/// namespace's writer session: one publication queue, one WAL-tail fold, one
/// metadata compaction that each published fold starts, and the writer epoch
/// that the session's first publish acquires. It shares the runtime's store
/// client, caches, admission limits, execution budget, compactor claim, and
/// shutdown.
///
/// The host that holds a writable handle owns the session. Clones share it,
/// and the session lives while any clone is held. [`Namespace::close`] ends
/// it and waits for the work it admitted. Dropping the last clone also ends
/// it: work already admitted still publishes, and opening the namespace
/// again before that work finishes returns the same session. Once a session
/// has ended, the next [`LoonFs::open_namespace`] starts a new one, and its
/// first publish acquires a new writer epoch. The runtime never opens a
/// session by itself and never closes one to make room for another.
/// A fenced session stays dead through every clone. Opening again returns
/// `writer_session_closed` while its work ends, then starts a new session
/// even if clones of the dead session are still held.
#[derive(Clone)]
pub struct Namespace<M> {
    pub(crate) core: RuntimeCore,
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
    /// Returns the id of the namespace this handle acts on.
    pub fn id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the subject this handle acts for, or `None` for an unscoped
    /// service handle.
    pub fn subject(&self) -> Option<&Subject> {
        self.core.subject.as_ref()
    }

    /// Clones this handle with the subject used for its reads and, on a
    /// writable handle, its commits and uploads.
    pub fn with_subject(&self, subject: Subject) -> Self
    where
        M: Clone,
    {
        Self {
            core: self.core.with_subject(subject),
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
    pub(crate) fn new(core: RuntimeCore, namespace_id: NamespaceId) -> Self {
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

    /// Returns the highest seq this session has committed, or `None` before
    /// its first commit. Reads memory only. A commit from another session or
    /// another process does not change it.
    pub fn last_published_seq(&self) -> Option<ChangeSeq> {
        self.session().last_published_seq()
    }

    /// Reads the last publication time on the runtime's monotonic clock.
    pub fn last_published_ms(&self) -> Option<u64> {
        self.session().last_published_ms()
    }

    /// Reads whether metadata maintenance found nothing left due for this session.
    pub fn metadata_caught_up(&self) -> bool {
        self.session().metadata_caught_up()
    }

    /// Records a host's metadata result only if no newer commit has published.
    pub fn record_metadata_maintenance(&self, expected_seq: Option<ChangeSeq>, caught_up: bool) {
        self.session()
            .record_metadata_maintenance(expected_seq, caught_up);
    }

    /// Returns whether this is the only handle holding its writer session.
    /// Hosts must prevent concurrent opens while using this check to close it.
    pub fn is_exclusively_held(&self) -> bool {
        Arc::strong_count(
            self.session
                .as_ref()
                .expect("a writable namespace handle should hold its writer session"),
        ) == 1
    }

    /// Refuses new work, cancels the session's metadata compaction, drains
    /// admitted work, waits for the fold and the compaction, and ends the
    /// session.
    ///
    /// The compaction stops at once while it waits for a compaction permit
    /// or runs a bounded step, and at its next block while it runs a
    /// streaming compaction, so close never waits for another session's
    /// merge. From the moment this is called, commits and deletes through
    /// every clone of this handle fail with `writer_session_closed`, and so
    /// does [`LoonFs::open_namespace`] for this namespace until the drain
    /// finishes. After this returns, the next open starts a new session,
    /// which acquires a new writer epoch. Closing a session that no longer
    /// admits work only waits for its drain, and the report says it was not
    /// open. Fails with `shutting_down` after shutdown begins.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.close",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "close",
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
    // upload operations in `fs/writes.rs`, `fs/writes_by_inode.rs`, and
    // `fs/uploads.rs`; snapshots in `fs/snapshots.rs`; and administrator
    // recovery in `fs/maintenance.rs`.
}
