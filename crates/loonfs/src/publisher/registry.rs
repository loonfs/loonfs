//! The table of live writer sessions, and the changes that mutations outside
//! the publisher make to a namespace.

use super::admission::PublicationAdmission;
use super::{lock_state, FoldTrigger, NamespacePublisher, PublisherContext};
use crate::publish::CommitCandidate;
use crate::runtime_core::{HeadPositions, ReadPositionSlot, RuntimeCore};
use crate::trace::phase_event;
use crate::{
    CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Error, ErrorCode,
    MetadataMaintenanceOutcome, Result, SharedObjectStore, WriterState,
};
use futures::future::BoxFuture;
use loonfs_core::limits::HINT_RAISE_INTERVAL_MS;
use loonfs_core::publish::{NamespaceCommitEngine, WriterPosition};
use loonfs_core::{Phase, PublicationReport, PublishedManifest};
use loonfs_types::Commit;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
use std::collections::hash_map::Entry;
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
/// session table, contained panic count, and the hint raises that this
/// runtime's manifest publications started.
pub(super) struct RegistryShared {
    pub(super) context: Arc<PublisherContext>,
    state: Mutex<RegistryState>,
    /// Publication and deletion units whose panic a task survived. Tasks
    /// contain panics to keep the registry usable, so this — not a task
    /// join error — is what a drain reports.
    pub(super) panicked_units: AtomicUsize,
    hint_raises: HintRaises,
}

/// The raises of namespace hints that this runtime started, so it raises
/// each namespace's hint at most once per `HINT_RAISE_INTERVAL_MS`.
#[derive(Default)]
struct HintRaises {
    /// When each raise that started less than `HINT_RAISE_INTERVAL_MS` ago
    /// started, on the monotonic timer. Each raise removes the older
    /// entries, so the map lists at most the namespaces whose hints this
    /// runtime raised in the last interval.
    started_at_ms: Mutex<HashMap<NamespaceId, u64>>,
}

impl HintRaises {
    /// Records that a raise of the namespace's hint starts at `now_ms`, on
    /// the monotonic timer, and returns whether it starts: `false` when this
    /// runtime started a raise of that hint less than `HINT_RAISE_INTERVAL_MS`
    /// ago.
    fn start(&self, namespace_id: &NamespaceId, now_ms: u64) -> bool {
        let mut started_at_ms = lock_state(&self.started_at_ms);
        started_at_ms.retain(|_, at_ms| now_ms < at_ms.saturating_add(HINT_RAISE_INTERVAL_MS));
        match started_at_ms.entry(namespace_id.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(now_ms);
                true
            }
        }
    }
}

pub(super) struct RegistryState {
    pub(super) closed: bool,
    pub(super) sessions: HashMap<NamespaceId, LiveSession>,
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

    pub(crate) fn last_published_at_ms(&self) -> Option<u64> {
        self.publisher
            .lock_state()
            .last_published
            .map(|last| last.at_ms)
    }

    pub(crate) fn metadata_caught_up(&self) -> bool {
        self.publisher.lock_state().metadata_caught_up
    }

    pub(crate) fn idle_fold_due_at_ms(&self) -> Option<u64> {
        let published_at_ms = self.last_published_at_ms()?;
        FoldTrigger::new(self.publisher.context.runtime_core.config())
            .idle_fold_due_at_ms(published_at_ms)
    }

    pub(crate) fn record_metadata_maintenance(
        &self,
        expected_seq: Option<ChangeSeq>,
        outcome: MetadataMaintenanceOutcome,
    ) {
        self.publisher
            .record_metadata_maintenance(expected_seq, outcome);
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
    /// runtime published, with one compare-and-swap, unless this runtime
    /// started a raise of the namespace's hint less than
    /// `HINT_RAISE_INTERVAL_MS` ago (format spec, "The current manifest and
    /// WAL tip").
    ///
    /// A skipped raise returns at once, and a failed raise is logged and
    /// ignored. Either leaves the hint behind the newest manifest until a
    /// later publication raises it, and discovery probes past a hint that
    /// lags.
    pub(super) async fn raise_hint(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        let runtime_core = &self.context.runtime_core;
        if !self
            .hint_raises
            .start(namespace_id, runtime_core.timer().monotonic_now_ms())
        {
            return;
        }
        if let Err(error) = runtime_core
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
                }),
                panicked_units: AtomicUsize::new(0),
                hint_raises: HintRaises::default(),
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
        let publisher = NamespacePublisher::new(namespace_id.clone(), &self.shared);
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

    /// Moves the namespace's publisher to a manifest that this runtime
    /// published, or drops the publisher's position when it can't move
    /// there, so that later commits and reads in this runtime start from the
    /// newest manifest, and then raises the namespace's hint to the manifest
    /// (see [`Self::raise_hint`]).
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
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher
                .change_position(PositionChange::Move(published))
                .await;
        }
        self.raise_hint(namespace_id, manifest_no).await;
    }

    pub(super) fn live_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.shared.live_publisher(namespace_id)
    }

    /// Returns the writer sessions that this runtime's reads start from.
    pub(crate) fn head_positions(&self) -> Weak<dyn HeadPositions> {
        Arc::downgrade(&self.shared) as Weak<dyn HeadPositions>
    }

    pub(crate) async fn wal_tail_inline_bytes(&self, namespace_id: &NamespaceId) -> Option<usize> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.wal_tail_inline_bytes()
    }

    /// Returns the namespace's writer position that reads in this runtime
    /// start from, as a fold input, or `None` when this runtime has no
    /// position for the namespace or the namespace's session is fenced.
    ///
    /// Unlike [`Self::wal_fold_input`], this function doesn't wait for a
    /// publication that the session's engine is running, so the position
    /// doesn't include that publication's commits.
    pub(crate) fn read_position_fold_input(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<WriterPosition> {
        let position = self.shared.read_position_slot(namespace_id)?.current()?;
        Some(WriterPosition::clone(&position))
    }

    /// Returns the namespace's writer position as a fold input: the manifest
    /// it plans against and the unfolded WAL tail above that manifest. Returns
    /// `None` when this runtime has no position for the namespace.
    pub(crate) async fn wal_fold_input(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<WriterPosition> {
        let publisher = self.live_publisher(namespace_id)?;
        let slot = publisher.lock_engine().await;
        slot.engine
            .as_ref()
            .and_then(NamespaceCommitEngine::writer_position)
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
    /// runtime published, after the publication has returned, unless this
    /// runtime started a raise of that hint less than
    /// `HINT_RAISE_INTERVAL_MS` ago.
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
    /// The mutation published this manifest. The publisher moves to it, or
    /// drops its position when it can't move there. The cached head stays,
    /// because the manifest is at the head that the namespace already had.
    Published(PublishedManifest),
    /// The mutation published no manifest, such as a garbage collection pass
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
