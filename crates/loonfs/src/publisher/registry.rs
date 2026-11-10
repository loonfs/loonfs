//! The table of live writer sessions, and the changes that mutations outside
//! the publisher make to a namespace.

use super::admission::{AdmissionPermit, PublicationAdmission};
use super::{lock_state, NamespacePublisher, NamespaceTurns, PublisherContext};
use crate::publish::CommitCandidate;
use crate::runtime_core::{HeadPositions, ReadPositionSlot, RuntimeCore};
use crate::trace::phase_event;
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, ErrorCode, Result,
    SharedObjectStore, WriterState,
};
use futures::future::BoxFuture;
use loonfs_core::control::LoadedManifest;
use loonfs_core::publish::{NamespaceCommitEngine, WriterPosition};
use loonfs_core::{FloorObjectReport, Phase, PublicationReport, PublishedManifest};
use loonfs_types::Commit;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
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
    /// The session admits no more work. It was closed, its namespace was
    /// deleted, or the runtime is shutting down.
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
    /// contain panics to keep the registry usable, so this — not a task
    /// join error — is what a drain reports.
    pub(super) panicked_units: AtomicUsize,
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
pub(crate) struct MergeReservation(Arc<AdmissionPermit>);

impl MergeReservation {
    /// Returns the merge allowance with this reservation, which the merge
    /// planner checks and grows.
    pub(crate) fn allowance(&self) -> loonfs_core::MergeAllowance {
        self.0.merge_allowance()
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
    /// planned. When the reservations of other merges, into any namespace
    /// that shares the execution budget, leave no room, the merge gets
    /// `server_busy`.
    pub(crate) fn reserve_merge(&self) -> Result<MergeReservation> {
        Ok(MergeReservation(self.publisher.reserve_merge()?))
    }

    /// Submits a merge under `reservation`. Admitted work continues if the
    /// caller is cancelled.
    pub(crate) async fn submit_merge(
        &self,
        candidate: CommitCandidate,
        reservation: &MergeReservation,
    ) -> Result<Commit> {
        self.publisher.submit_merge(candidate, &reservation.0).await
    }

    /// Submits a namespace deletion, sequenced as a barrier: mutations
    /// admitted before it publish first, and mutations admitted after it
    /// fail once the delete succeeds.
    pub(crate) async fn submit_delete(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.publisher.submit_delete(options).await
    }

    pub(crate) fn state(&self) -> NamespaceSessionStatus {
        self.publisher.session_status()
    }

    pub(crate) fn last_published_seq(&self) -> Option<ChangeSeq> {
        self.publisher
            .lock_state()
            .last_published
            .map(|last| last.seq)
    }

    /// Whether the session has a writer position whose last check is within
    /// the revalidation bound.
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
        publisher.compaction_cancellation.cancel();
        publisher.forget_if_ended();
        publisher.wait_for_worker().await;
        if let Err(error) = publisher.wait_for_fold().await {
            phase_event!(
                publisher.context.runtime_core,
                Phase::WalFold,
                publisher.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "wal fold failed while the namespace session closed"
            );
        }
        if let Err(error) = publisher.wait_for_compaction().await {
            phase_event!(
                publisher.context.runtime_core,
                Phase::MetadataCompaction,
                publisher.namespace_id,
                tracing::Level::WARN,
                error = %error.public_message(),
                "metadata compaction failed while the namespace session closed"
            );
        }
        publisher.forget_if_ended();
        Ok(CloseNamespaceReport {
            drained_commits,
            fenced: publisher.session_is_fenced(),
        })
    }
}

impl RegistryShared {
    pub(super) fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        lock_state(&self.state)
    }

    fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| live.publisher.clone())
    }

    /// Raises the namespace's hint to `manifest_no`, a manifest that this
    /// runtime published, with one read of the hint and one compare-and-swap
    /// (format spec, "The current manifest and WAL tip").
    ///
    /// A failed raise, or one that loses its compare-and-swap, is logged and
    /// ignored. The hint can then stay behind the newest manifest until the
    /// next publication raises it, and discovery probes past a hint that lags.
    pub(super) async fn raise_hint(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        if let Err(error) = self
            .context
            .runtime_core
            .namespace_store(namespace_id)
            .raise_hint(manifest_no, None)
            .await
        {
            tracing::debug!(
                namespace_id = %namespace_id,
                manifest_no = %manifest_no,
                error = %error,
                "the namespace hint raise failed, and a later publication raises the hint"
            );
        }
    }
}

impl HeadPositions for RegistryShared {
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>> {
        self.lock_state()
            .sessions
            .get(namespace_id)
            .map(|live| Arc::clone(&live.publisher.read_position))
    }

    fn drop_position<'a>(&'a self, namespace_id: &'a NamespaceId) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(publisher) = self.live_publisher(namespace_id) {
                publisher.change_position(PositionChange::Drop).await;
            }
        })
    }

    fn move_position<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
        published: PublishedManifest,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(publisher) = self.live_publisher(namespace_id) {
                publisher
                    .change_position(PositionChange::Move(published))
                    .await;
            }
        })
    }
}

impl PublisherRegistry {
    /// Creates the registry a writer owns. Batches publish through each
    /// publisher's own commit engine and writer session.
    pub(crate) fn new(
        runtime_core: RuntimeCore,
        writer: Weak<WriterState>,
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
                    writer,
                    runtime,
                    admission,
                }),
                state: Mutex::new(RegistryState {
                    closed: false,
                    sessions: HashMap::new(),
                    turns: HashMap::new(),
                }),
                panicked_units: AtomicUsize::new(0),
            }),
        }
    }

    /// Returns the session for `namespace_id`, starting one when the table
    /// has none.
    ///
    /// A session whose last handle dropped stays in the table until its
    /// admitted work finishes, and an open before then continues it: a
    /// second session would acquire a new epoch and fence that work. A fenced
    /// session cannot continue: an open fails while its work ends, then starts
    /// a new session even while old handles still exist.
    pub(crate) fn open_session(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<Arc<NamespaceSession>, CoreError> {
        let mut state = self.shared.lock_state();
        if state.closed {
            return Err(CoreError::ShuttingDown);
        }
        if let Some(live) = state.sessions.get(namespace_id) {
            if live.publisher.session_is_fenced() {
                if !live.publisher.lock_state().has_ended(false) {
                    return Err(CoreError::WriterSessionClosed {
                        namespace_id: namespace_id.clone(),
                    });
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

    /// Tells the namespace's publisher how a mutation outside the publisher
    /// changed the namespace, and drops the namespace's cached head when the
    /// mutation may have changed the namespace in ways this runtime
    /// hasn't recorded.
    pub(crate) async fn invalidate_namespace(
        &self,
        namespace_id: &NamespaceId,
        change: NamespaceChange,
    ) {
        match change {
            NamespaceChange::Published(published) => {
                self.record_manifest_published(namespace_id, published)
                    .await;
            }
            NamespaceChange::Unchanged => {}
            NamespaceChange::Unknown => {
                self.shared
                    .context
                    .runtime_core
                    .invalidate_namespace_read_cache(namespace_id);
                if let Some(publisher) = self.live_publisher(namespace_id) {
                    publisher.change_position(PositionChange::Drop).await;
                }
            }
        }
    }

    /// Runs [`Self::invalidate_namespace`] after a mutation's result when
    /// [`should_invalidate_after_result`] selects it. `change` describes a successful result. A failed result
    /// may have changed the namespace in ways this runtime hasn't recorded.
    pub(crate) async fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
        change: impl FnOnce(&T) -> NamespaceChange,
    ) -> Result<T> {
        if should_invalidate_after_result(&result) {
            let change = result.as_ref().map_or(NamespaceChange::Unknown, change);
            self.invalidate_namespace(namespace_id, change).await;
        }
        result
    }

    /// Runs [`Self::finish_namespace_mutation`] after a maintenance call
    /// whose report says which manifest it published, and returns the
    /// report's response.
    pub(crate) async fn finish_publication<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<PublicationReport<T>>,
    ) -> Result<T> {
        self.finish_namespace_mutation(namespace_id, result, NamespaceChange::of_report)
            .await
            .map(|report| report.response)
    }

    /// Moves this runtime's views of the namespace onto a manifest that this
    /// runtime published (see [`Self::move_views`]), and then raises the
    /// namespace's hint to the manifest (see [`Self::raise_hint`]).
    ///
    /// The hint of a namespace without a live session is raised too, because
    /// a maintenance call or a deletion can publish a manifest in that
    /// namespace.
    pub(crate) async fn record_manifest_published(
        &self,
        namespace_id: &NamespaceId,
        published: PublishedManifest,
    ) {
        let manifest_no = published.manifest.manifest_no;
        self.move_views(namespace_id, published).await;
        self.raise_hint(namespace_id, manifest_no).await;
    }

    /// Moves the namespace's cached head and its publisher's position onto
    /// `published`, a manifest that this runtime published or found current,
    /// or drops each one that can't move there (see
    /// [`HeadAndWalTailCache::move_head`](loonfs_core::cache::HeadAndWalTailCache::move_head)),
    /// so that later commits and reads in this runtime start from the newest
    /// manifest.
    pub(super) async fn move_views(
        &self,
        namespace_id: &NamespaceId,
        published: PublishedManifest,
    ) {
        self.shared
            .context
            .runtime_core
            .head_and_wal_tail_cache()
            .move_head(namespace_id, &published);
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher
                .change_position(PositionChange::Move(published))
                .await;
        }
    }

    pub(super) fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.shared.live_publisher(namespace_id)
    }

    /// Submits an advance of the retention floor to `target`, planned from
    /// `planned`, to the queue of the namespace's writer session in this
    /// runtime, and returns `None` without submitting when the runtime has
    /// no writer session for the namespace that admits work and isn't
    /// fenced.
    ///
    /// The queue orders the advance like a deletion: commits admitted before
    /// it publish first, and commits admitted after it are validated against
    /// the new floor.
    pub(crate) async fn submit_retention_advance(
        &self,
        namespace_id: &NamespaceId,
        planned: LoadedManifest,
        target: ChangeSeq,
    ) -> Option<Result<FloorObjectReport>> {
        let publisher = self.live_publisher(namespace_id)?;
        publisher.submit_retention_advance(planned, target).await
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

    /// Returns the namespace's writer position while its last check is
    /// within the revalidation bound (see
    /// [`NamespacePublisher::current_position`]), or `None` when this runtime
    /// has no such position for the namespace.
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
    /// finish, and with them the hint raises that their publications started.
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

    /// Raises the namespace's hint to `manifest_no`, a manifest that this
    /// runtime published, after the publication has returned.
    pub(crate) async fn raise_hint(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        self.shared.raise_hint(namespace_id, manifest_no).await;
    }
}

/// Whether a mutation with `result` may have changed the namespace in ways
/// this runtime hasn't recorded: a success, or a stale head or a fence that
/// another writer's change caused.
fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(Error::Core(error))
            if matches!(error.code(), ErrorCode::StaleHead | ErrorCode::WriterFenced) =>
        {
            true
        }
        _ => false,
    }
}

/// How a mutation outside the publisher, such as a maintenance call, changed
/// a namespace, as the namespace's publisher needs to record it.
#[derive(Debug, Clone)]
pub(crate) enum NamespaceChange {
    /// The mutation published this manifest. The publisher's position and
    /// the cached head move to it, or each one that can't move there is
    /// dropped.
    Published(PublishedManifest),
    /// The mutation published no manifest, such as a garbage collection
    /// or a change to a pin. The publisher keeps its position, and the
    /// cached head stays confirmed, because the head didn't move.
    Unchanged,
    /// The mutation may have changed the namespace in ways this runtime
    /// hasn't recorded, such as creating it or failing with a stale head. The
    /// publisher drops its position, and the runtime drops its cached head.
    Unknown,
}

impl NamespaceChange {
    /// Returns the change that a maintenance call's report describes.
    fn of_report<T>(report: &PublicationReport<T>) -> Self {
        report
            .published
            .clone()
            .map_or(Self::Unchanged, Self::Published)
    }
}

/// A change to the engine's position that a mutation outside the publisher
/// asked for.
#[derive(Debug)]
pub(super) enum PositionChange {
    /// Moves the position onto a manifest above it under the session's
    /// writer epoch, or drops the position when it can't move there.
    Move(PublishedManifest),
    /// Drops the position.
    Drop,
}

impl PositionChange {
    pub(super) fn apply(self, engine: &mut NamespaceCommitEngine<SharedObjectStore>) {
        match self {
            Self::Move(published) => engine.record_manifest_published(published),
            Self::Drop => engine.invalidate_projection(),
        }
    }
}
