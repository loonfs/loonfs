//! The writer handle for one namespace.

use crate::fs::{ReadCore, WriterBits};
use crate::publisher::{CloseNamespaceReport, NamespaceSessionState, PublisherRegistry};
use crate::{FsWriter, NamespaceId, Result};
use loonfs_api::Subject;
use std::fmt;
use std::sync::Arc;

/// Mutates one namespace through that namespace's writer session.
///
/// Open it with [`FsWriter::open_namespace`]. The [`FsWriter`] is the
/// runtime: it owns the store client, the caches, the admission budgets, and
/// shutdown, and every `NamespaceWriter` it opens shares them. The session
/// belongs to the namespace: one publication queue, one WAL-tail fold, and
/// the writer epoch that the session's first publish acquires. Methods on
/// this handle take no namespace id.
///
/// The writer's [`NamespaceSessionPolicy`](crate::NamespaceSessionPolicy)
/// decides how long a session stays open. By default, a publish opens a
/// closed session, and when the session table is full, opening another
/// session closes the least recently used idle one. Under
/// [`NamespaceSessionPolicy::ExplicitOpen`](crate::NamespaceSessionPolicy::ExplicitOpen),
/// the host opens sessions with [`FsWriter::open_namespace`] and ends them
/// with [`Self::close`].
///
/// Clones share the session.
#[derive(Clone)]
pub struct NamespaceWriter {
    pub(crate) core: ReadCore,
    pub(crate) bits: Arc<WriterBits>,
    pub(crate) publisher: PublisherRegistry,
    pub(crate) namespace_id: NamespaceId,
}

impl fmt::Debug for NamespaceWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamespaceWriter")
            .field("namespace_id", &self.namespace_id)
            .finish_non_exhaustive()
    }
}

impl NamespaceWriter {
    /// Builds a handle for `namespace_id` without opening its session. If the
    /// session is closed, the writer's
    /// [`NamespaceSessionPolicy`](crate::NamespaceSessionPolicy) decides at
    /// the first publish whether to open it or to refuse the work.
    pub(crate) fn new(writer: &FsWriter, namespace_id: NamespaceId) -> Self {
        Self {
            core: writer.core.clone(),
            bits: Arc::clone(&writer.bits),
            publisher: writer.publisher.clone(),
            namespace_id,
        }
    }

    /// Returns the namespace this handle writes.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the subject this handle acts for, or `None` for an unscoped
    /// service handle.
    pub fn subject(&self) -> Option<&Subject> {
        self.core.subject.as_ref()
    }

    /// Clones this handle with the subject used for its reads, commits, and uploads.
    pub fn as_subject(&self, subject: Subject) -> Self {
        Self {
            core: self.core.as_subject(subject),
            ..self.clone()
        }
    }

    /// Returns the state of this namespace's writer session.
    pub fn session_state(&self) -> NamespaceSessionState {
        self.publisher.namespace_session_state(&self.namespace_id)
    }

    /// Refuses new work, drains admitted work, and ends the session.
    ///
    /// From the moment this is called until the drain finishes, commits and
    /// deletes through any handle for this namespace fail with
    /// `writer_session_closed`. After that, the writer's
    /// [`NamespaceSessionPolicy`](crate::NamespaceSessionPolicy) decides
    /// whether the next publish opens a new session, and a new session
    /// acquires a new writer epoch. Closing a session that is not open
    /// changes nothing, and the report says so. Fails with `shutting_down`
    /// after shutdown begins.
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
        Ok(self.publisher.close_namespace(&self.namespace_id).await?)
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
        self.publisher.wait_for_fold(&self.namespace_id).await
    }

    // Namespace deletion lives in `fs/namespaces.rs`; mutation, commit, and
    // upload operations in `fs/writes.rs` and `fs/uploads.rs`; snapshots in
    // `fs/snapshots.rs`; and administrator recovery in `fs/maintenance.rs`.
}
