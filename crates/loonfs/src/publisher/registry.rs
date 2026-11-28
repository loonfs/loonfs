//! The table of live writer sessions, and the cached heads that mutations
//! outside the sessions leave out of date.

use super::admission::{AdmissionPermit, PublicationAdmission};
use super::{lock_state, NamespacePublisher, NamespaceTurns, PublisherContext};
use crate::publish::CommitCandidate;
use crate::runtime_core::{HeadPositions, ReadPositionSlot, RuntimeCore};
use crate::trace::phase_event;
use crate::{CoreError, Error, Result, WriterState};
use futures::future::BoxFuture;
use loonfs_core::publish::WriterPosition;
use loonfs_core::Phase;
use loonfs_types::Commit;
use loonfs_types::{CommitSeq, ManifestNo, NamespaceId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::runtime::Handle;

/// Result of closing one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseNamespaceReport {
    /// Commits admitted before the close and published during the drain, or
    /// `None` when the session had already stopped admitting work.
    pub drained_commits: Option<usize>,
    /// Whether the closed session had been fenced.
    pub fenced: bool,
}

/// Current state of one namespace writer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSessionStatus {
    /// The session admits work.
    Open,
    /// Another writer acquired a later epoch. Mutations fail with
    /// `writer_fenced` for the rest of the session.
    Fenced,
    /// The session is finished. It was closed, its namespace was deleted
    /// through it, its last publish found its namespace deleted or missing,
    /// or the runtime is shutting down.
    Closed,
}

/// The live writer sessions of one writer, and the publication admission
/// they share.
///
/// Clones share the same sessions and worker tasks. The table keeps a
/// session while a writable [`Namespace`](crate::Namespace) refers to it or
/// while work it admitted is still running. A fenced session leaves once
/// its work ends, even while handles still refer to it. Nothing here sets how many
/// sessions exist or how long they live.
#[derive(Clone)]
pub(crate) struct PublisherRegistry {
    pub(super) shared: Arc<RegistryShared>,
}

/// State shared by all publishers in this registry: their context, the
/// session table, and the count of contained panics.
pub(super) struct RegistryShared {
    pub(super) context: Arc<PublisherContext>,
    state: Mutex<RegistryState>,
    /// Publication and deletion units whose panic a task survived. Tasks
    /// contain panics to keep the registry usable, so this (not a task
    /// join error) is what a drain reports.
    pub(super) panicked_units: AtomicUsize,
    /// A narrowed per-segment row budget for the sessions' compaction units,
    /// for the tests that need many compacted segments (see
    /// [`PublisherRegistry::narrow_segment_row_budget`]).
    #[cfg(test)]
    segment_row_budget: Mutex<Option<std::num::NonZeroUsize>>,
}

pub(super) struct RegistryState {
    pub(super) closed: bool,
    pub(super) sessions: HashMap<NamespaceId, LiveSession>,
    /// The fold and compaction locks of the namespaces that a session, a
    /// fold, or a compaction unit in this runtime has. Strong: the table
    /// owns the locks. Adding an entry removes every entry that no caller
    /// has, so the table lists at most one entry more than the most callers
    /// that had entries at once since the last addition, and a session keeps
    /// its namespace's entry for the session's life.
    turns: HashMap<NamespaceId, Arc<NamespaceTurns>>,
}

impl RegistryState {
    /// Returns the fold and compaction locks of `namespace_id`, adding an
    /// entry when the table has none.
    pub(super) fn turns(&mut self, namespace_id: &NamespaceId) -> Arc<NamespaceTurns> {
        if let Some(turns) = self.turns.get(namespace_id) {
            return Arc::clone(turns);
        }
        self.turns.retain(|_, turns| Arc::strong_count(turns) > 1);
        Arc::clone(self.turns.entry(namespace_id.clone()).or_default())
    }
}

/// One entry in the table of live sessions.
pub(super) struct LiveSession {
    pub(super) publisher: NamespacePublisher,
    /// Weak: the handles own the session, so the table must not keep it
    /// alive.
    pub(super) session: Weak<NamespaceSession>,
}

/// One namespace's writer session, shared by every clone of its writable
/// [`Namespace`](crate::Namespace).
///
/// Dropping the last one ends the session. Work it already admitted still
/// publishes, and the table removes the session's entry once that work finishes.
pub(crate) struct NamespaceSession {
    pub(super) publisher: NamespacePublisher,
}

/// A merge's reservation of the merge allowance, which the merge keeps from
/// before its plan until its commit settles, so the allowance covers the
/// merge's planning and its publication. The reservation grows while the
/// merge plans and doesn't shrink.
pub(crate) struct MergeReservation {
    pub(super) permit: Arc<AdmissionPermit>,
}

impl MergeReservation {
    /// Returns the merge allowance with this reservation, which the merge
    /// planner checks and grows.
    pub(crate) fn allowance(&self) -> loonfs_core::MergeAllowance {
        self.permit.merge_allowance()
    }
}

impl Drop for NamespaceSession {
    fn drop(&mut self) {
        self.publisher.forget_if_ended();
    }
}

impl NamespaceSession {
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        &self.publisher.namespace_id
    }

    /// Submits one already-classified candidate. Admitted work continues if
    /// the caller is cancelled.
    pub(crate) async fn submit_candidate(&self, candidate: CommitCandidate) -> Result<Commit> {
        self.publisher.submit_candidate(candidate).await
    }

    /// Reserves a merge's share of the merge allowance, before the merge is
    /// planned. When the reservations of other merges, into
    /// any namespace that shares the execution budget, leave no room, the
    /// merge gets `server_busy`.
    pub(crate) fn reserve_merge(&self) -> Result<MergeReservation> {
        Ok(self.publisher.reserve_merge()?)
    }

    /// Submits a merge under `reservation`. Admitted work continues if the
    /// caller is cancelled.
    pub(crate) async fn submit_merge(
        &self,
        candidate: CommitCandidate,
        reservation: &MergeReservation,
    ) -> Result<Commit> {
        self.publisher.submit_merge(candidate, reservation).await
    }

    pub(crate) fn state(&self) -> NamespaceSessionStatus {
        self.publisher.session_status()
    }

    pub(crate) fn last_published_seq(&self) -> Option<CommitSeq> {
        self.publisher.lock_state().last_published_seq
    }

    /// Whether the session has a writer position.
    pub(crate) fn has_current_writer_position(&self) -> bool {
        self.publisher.current_position().is_some()
    }

    pub(crate) async fn wait_for_fold(&self) -> Result<()> {
        self.publisher.wait_for_fold().await
    }

    /// Refuses new work from every handle that shares this session, cancels
    /// its compaction, waits for admitted work, the running fold, and the
    /// compaction, and removes the session's entry.
    ///
    /// The worker, the fold task, and the compaction task remove the session's entry
    /// too when they exit, so a cancelled close still leaves the table clean.
    pub(crate) async fn close(&self) -> std::result::Result<CloseNamespaceReport, CoreError> {
        let publisher = &self.publisher;
        let drained_commits = publisher.close_session()?;
        publisher.stop.cancel();
        publisher.end_session_work().await;
        Ok(CloseNamespaceReport {
            drained_commits,
            fenced: publisher.session_is_fenced(),
        })
    }
}

impl NamespacePublisher {
    /// Waits for the work of a session that admits no more work (its
    /// admitted work, the running fold, and the cancelled compaction), and
    /// removes the session's entry once that work has ended.
    async fn end_session_work(&self) {
        self.forget_if_ended();
        self.wait_for_worker().await;
        self.wait_for_maintenance().await;
        if let Err(error) = self.wait_for_fold().await {
            phase_event!(
                self.context.runtime_core,
                Phase::WalFold,
                self.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "wal fold failed while the namespace session ended"
            );
        }
        if let Err(error) = self.wait_for_compaction().await {
            phase_event!(
                self.context.runtime_core,
                Phase::MetadataCompaction,
                self.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "metadata compaction failed while the namespace session ended"
            );
        }
        self.forget_if_ended();
    }
}

impl RegistryShared {
    pub(super) fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        lock_state(&self.state)
    }

    /// Returns the narrowed row budget of the sessions' compaction units, if
    /// a test set one.
    #[cfg(test)]
    pub(super) fn segment_row_budget(&self) -> Option<std::num::NonZeroUsize> {
        *lock_state(&self.segment_row_budget)
    }

    fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| live.publisher.clone())
    }
}

impl HeadPositions for RegistryShared {
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| Arc::clone(&live.publisher.read_position))
    }

    fn settle_replaced_position<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
        replaced: ManifestNo,
    ) -> BoxFuture<'a, std::result::Result<(), CoreError>> {
        Box::pin(async move {
            match self.live_publisher(namespace_id) {
                Some(publisher) => publisher.settle_replaced_position(replaced).await,
                None => Ok(()),
            }
        })
    }
}

impl PublisherRegistry {
    /// Creates the registry a writer owns. Batches publish through each
    /// publisher's own commit engine and writer session.
    pub(crate) fn new(
        runtime_core: RuntimeCore,
        writer: &Arc<WriterState>,
        runtime: Handle,
    ) -> Self {
        let admission = Arc::new(PublicationAdmission::new(
            runtime_core.config().publication.clone(),
            runtime_core.execution_budget().clone(),
        ));
        Self {
            shared: Arc::new(RegistryShared {
                context: Arc::new(PublisherContext {
                    runtime_core,
                    writer: Arc::downgrade(writer),
                    writer_id: writer.writer_id.clone(),
                    runtime,
                    admission,
                }),
                state: Mutex::new(RegistryState {
                    closed: false,
                    sessions: HashMap::new(),
                    turns: HashMap::new(),
                }),
                panicked_units: AtomicUsize::new(0),
                #[cfg(test)]
                segment_row_budget: Mutex::new(None),
            }),
        }
    }

    /// Returns the session for `namespace_id`, starting one when the table
    /// has none.
    ///
    /// A session whose last handle dropped stays in the table until its
    /// admitted work finishes, and an open before then continues it: a
    /// second session would acquire a new epoch and fence that work. A
    /// session that is fenced, or whose namespace is gone, can't continue, so
    /// an open fails with the session's `writer_fenced` or
    /// `namespace_deleted` while its work ends, then starts a new session
    /// even while old handles still exist.
    pub(crate) fn open_session(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<Arc<NamespaceSession>, CoreError> {
        let mut state = self.shared.lock_state();
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        if let Some(live) = state.sessions.get(namespace_id) {
            if let Some(fenced) = live.publisher.live_session().err() {
                if !live.publisher.lock_state().has_ended(false) {
                    return Err(fenced);
                }
                live.publisher.read_position.clear();
                state.sessions.remove(namespace_id);
            }
        }
        if let Some(live) = state.sessions.get_mut(namespace_id) {
            if live.publisher.session_closed() {
                return Err(CoreError::WriterSessionClosed {
                    namespace_id: namespace_id.clone(),
                });
            }
            if let Some(session) = live.session.upgrade() {
                return Ok(session);
            }
            let session = Arc::new(NamespaceSession {
                publisher: live.publisher.clone(),
            });
            live.session = Arc::downgrade(&session);
            return Ok(session);
        }
        let turns = state.turns(namespace_id);
        let publisher = NamespacePublisher::new(namespace_id.clone(), &self.shared, turns);
        let session = Arc::new(NamespaceSession {
            publisher: publisher.clone(),
        });
        state.sessions.insert(
            namespace_id.clone(),
            LiveSession {
                publisher,
                session: Arc::downgrade(&session),
            },
        );
        let sessions = state.sessions.len();
        drop(state);
        self.shared
            .context
            .runtime_core
            .instruments()
            .publisher
            .sessions(sessions);
        Ok(session)
    }

    /// Waits until the namespace's session in this runtime, if it admits no
    /// more work because it's fenced, closed or its namespace is gone, has
    /// finished its work and left the table, so that the next
    /// [`Self::open_session`] starts a new session instead of returning the
    /// session's refusal. A session that admits work is left alone.
    ///
    /// Each of these cancels the session's compaction (see
    /// [`NamespacePublisher::release_engine`]), and such a session admits and
    /// starts nothing more, so the wait ends once the commits and the
    /// maintenance that it admitted and a running fold end.
    pub(crate) async fn wait_for_ended_session(&self, namespace_id: &NamespaceId) {
        let Some(publisher) = self.live_publisher(namespace_id) else {
            return;
        };
        if publisher.live_session().is_err() || publisher.session_closed() {
            publisher.end_session_work().await;
        }
    }

    /// Opens the namespace's session as [`Self::open_session`] does, for an
    /// operation on another namespace than the caller's, such as a fork's
    /// fold of its source. When this runtime's session for the namespace is
    /// fenced or closed and still ending, the call waits for it to end (see
    /// [`Self::wait_for_ended_session`]) and opens a new session, as the
    /// server's retry does for a request's own namespace. An open that still
    /// fails returns its error, which identifies the namespace.
    pub(crate) async fn open_session_once_ended(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<Arc<NamespaceSession>, CoreError> {
        match self.open_session(namespace_id) {
            Err(CoreError::WriterFenced(_) | CoreError::WriterSessionClosed { .. }) => {
                self.wait_for_ended_session(namespace_id).await;
                self.open_session(namespace_id)
            }
            opened => opened,
        }
    }

    /// Returns `result`, the result of a mutation outside the namespace's
    /// writer session, such as a create, a fork or a change to a snapshot,
    /// after it drops the namespace's cached head when the result shows that
    /// the cached head may be out of date: a stale basis (see
    /// [`CoreError::is_stale_basis`]) or a fence that another writer's change
    /// caused. The position of the namespace's writer session stays, because
    /// only the session's own queues change it.
    pub(crate) fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if let Err(Error::Core(error)) = &result {
            if error.is_stale_basis() || matches!(error.failure(), CoreError::WriterFenced(_)) {
                self.shared
                    .context
                    .runtime_core
                    .invalidate_namespace_read_cache(namespace_id);
            }
        }
        result
    }

    /// Drops the position of the namespace's writer session and the
    /// namespace's cached head, so the session's next unit and the runtime's
    /// next read load the namespace from the store, for a test of what a
    /// session does without a position. Only the session's own queues drop
    /// its position in a runtime.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn drop_session_view(&self, namespace_id: &NamespaceId) {
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher.lock_engine().await.invalidate_projection();
        }
        self.shared
            .context
            .runtime_core
            .invalidate_namespace_read_cache(namespace_id);
    }

    pub(super) fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.shared.live_publisher(namespace_id)
    }

    /// Narrows the rows one segment that the sessions' compaction units
    /// write may contain, so a namespace a test can build in seconds
    /// compacts into many segments.
    ///
    /// Test-only: folds keep the shipped segment shape, and planning and
    /// publishing are the shipped path either way.
    #[cfg(test)]
    pub(crate) fn narrow_segment_row_budget(&self, max_rows_per_segment: std::num::NonZeroUsize) {
        *lock_state(&self.shared.segment_row_budget) = Some(max_rows_per_segment);
    }

    /// Returns the writer sessions that this runtime's reads start from.
    pub(crate) fn head_positions(&self) -> Weak<dyn HeadPositions> {
        Arc::downgrade(&self.shared) as Weak<dyn HeadPositions>
    }

    #[cfg(test)]
    pub(crate) async fn wal_tail_inline_bytes(&self, namespace_id: &NamespaceId) -> Option<usize> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.wal_tail_inline_bytes()
    }

    #[cfg(test)]
    pub(crate) async fn wal_tail_bytes(&self, namespace_id: &NamespaceId) -> Option<usize> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.wal_tail_bytes()
    }

    /// Returns the namespace's writer position (see
    /// [`NamespacePublisher::current_position`]), or `None` when this runtime
    /// has no position for the namespace.
    pub(crate) fn current_position(&self, namespace_id: &NamespaceId) -> Option<WriterPosition> {
        self.live_publisher(namespace_id)?.current_position()
    }

    /// Whether [`Self::close_admission`] has run: later submissions fail
    /// with `shutting_down`. Readiness probes report this state.
    pub(crate) fn is_admission_closed(&self) -> bool {
        self.shared.lock_state().closed
    }

    /// Stops accepting new submissions while allowing admitted work to finish.
    ///
    /// Later submissions fail with `shutting_down`. Calling this more than once
    /// has no additional effect.
    pub(crate) fn close_admission(&self) {
        let publishers: Vec<NamespacePublisher> = {
            let mut state = self.shared.lock_state();
            state.closed = true;
            state
                .sessions
                .values()
                .map(|live| live.publisher.clone())
                .collect()
        };
        for publisher in publishers {
            publisher.close_admission();
        }
    }

    /// Waits for all current publisher workers, folds, and compactions to
    /// finish.
    ///
    /// Folds are waited for before compactions, because a fold starts its
    /// session's compaction.
    ///
    /// Returns an error if any publication, deletion, fold, or compaction
    /// panicked and its task contained the panic.
    pub(crate) async fn drain(&self) -> Result<()> {
        let publishers: Vec<NamespacePublisher> = self
            .shared
            .lock_state()
            .sessions
            .values()
            .map(|live| live.publisher.clone())
            .collect();
        for publisher in &publishers {
            publisher.wait_for_worker().await;
            publisher.wait_for_maintenance().await;
        }
        let mut task_error = None;
        for publisher in &publishers {
            if let Err(error) = publisher.wait_for_fold().await {
                task_error.get_or_insert(error);
            }
        }
        for publisher in &publishers {
            if let Err(error) = publisher.wait_for_compaction().await {
                task_error.get_or_insert(error);
            }
        }
        let panicked = self.shared.panicked_units.load(Ordering::SeqCst);
        if panicked > 0 {
            return Err(Error::RuntimeTask(format!(
                "{panicked} publisher task(s) panicked"
            )));
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(())
    }
}
