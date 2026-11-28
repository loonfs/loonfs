//! The one fold path of each namespace in a writable runtime, the snapshot
//! that a fold serves, and the locks that keep a namespace's folds apart and
//! its compaction units apart.

use super::registry::NamespaceChange;
use super::worker::elapsed_ms_from;
use super::{NamespacePublisher, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::phase_span;
use crate::{CoreError, Error, FoldWalOutcome, Result};
use loonfs_core::control::{
    Held, LoadedManifest, NamespaceReadAnchor, NamespaceStorageDiagnostics,
};
use loonfs_core::limits::{fold_at_inline_bytes, FOLD_AT_WAL_BYTES, MAX_CONTENTION_ATTEMPTS};
use loonfs_core::publish::{LaneGuard, LaneTurn, WalTailReport, WriterPosition};
use loonfs_core::time::OperationClock;
use loonfs_core::{FoldWriteOutcome, FoldedWalTail, MaintenanceCancellation, Phase};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Mutex as AsyncMutex;
use tracing::Instrument;

/// The fold lock, the fold rounds, and the compaction lock of one namespace
/// in this runtime.
///
/// A fold takes the fold lock before LoonFS checks whether the fold is due,
/// and keeps it until it has moved the session's position, so two folds
/// never fold the same tail. The fold lock comes before the session's publication
/// lane, which a fold takes for its publication, and an operation that keeps
/// the lane never waits for the fold lock. The fold rounds record which fold
/// served a request: a fold that took its input after the request arrived
/// covers every commit acknowledged before the request, so the requests
/// that wait for one fold share the next one. A compaction unit takes the
/// compaction lock before it plans, so two units never plan from the same
/// manifest. The fold lock and the compaction lock are `tokio` locks,
/// because a fold and a unit keep them across store requests.
#[derive(Default)]
pub(crate) struct NamespaceTurns {
    pub(super) fold: AsyncMutex<()>,
    rounds: Mutex<FoldRounds>,
    pub(crate) compaction: AsyncMutex<()>,
}

#[derive(Default)]
struct FoldRounds {
    started: u64,
    last: Option<(u64, FoldedWalTail)>,
}

impl NamespaceTurns {
    fn started(&self) -> u64 {
        self.lock_rounds().started
    }

    fn folded_after(&self, arrival: u64) -> Option<FoldedWalTail> {
        self.lock_rounds()
            .last
            .as_ref()
            .filter(|(round, _)| *round > arrival)
            .map(|(_, folded)| folded.clone())
    }

    fn start_round(&self) -> u64 {
        let mut rounds = self.lock_rounds();
        rounds.started += 1;
        rounds.started
    }

    fn record(&self, round: u64, folded: &FoldedWalTail) {
        self.lock_rounds().last = Some((round, folded.clone()));
    }

    fn lock_rounds(&self) -> MutexGuard<'_, FoldRounds> {
        self.rounds
            .lock()
            .expect("the fold rounds lock should not be poisoned")
    }
}

/// Which WAL tails [`NamespacePublisher::fold`] folds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FoldWhen {
    /// Any tail. A tail that's already folded is reported as
    /// [`FoldWalOutcome::AlreadyCurrent`]. A fold of the namespace in this
    /// runtime that took its input after the call arrived serves the call,
    /// which then returns that fold's result and folds nothing.
    Always,
    /// A tail of at least `max_wal_tail_objects` WAL objects, a tail of at
    /// least `FOLD_AT_WAL_BYTES` unfolded WAL bytes, a tail of at least
    /// `fold_at_inline_bytes` of the inline tail limit in inline content
    /// bytes, or a tail whose oldest commit has reached the runtime's fold
    /// age.
    Due { max_wal_tail_objects: u64 },
}

/// The size of a WAL tail and the time of its oldest commit, which decide
/// whether a fold of the tail is due (see [`fold_due`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct WalTailState {
    /// Unfolded WAL objects, including fences.
    objects: u64,
    /// Stored bytes of the unfolded WAL objects with commits.
    wal_bytes: usize,
    /// Inline content bytes of the unfolded WAL objects.
    inline_bytes: usize,
    /// The earliest `committed_at_ms` of the tail's commits, or `None` for a
    /// tail without a commit.
    oldest_commit_at_ms: Option<u64>,
    /// Whether a floor object in the tail raised the view's floor above the
    /// manifest's, so that no manifest records the view's floor yet.
    raises_retention_floor: bool,
}

impl WalTailState {
    /// Whether the tail has a commit or a floor object that raises the
    /// view's floor, which a fold publishes. A tail of only fences, or of
    /// floor objects that the manifest's floor already covers, changes
    /// nothing that a manifest records, so it counts as empty.
    fn has_change(&self) -> bool {
        self.oldest_commit_at_ms.is_some() || self.raises_retention_floor
    }
}

impl From<&WriterPosition> for WalTailState {
    fn from(position: &WriterPosition) -> Self {
        Self {
            objects: position.wal_tail_objects(),
            wal_bytes: position.wal_tail_bytes(),
            inline_bytes: position.wal_tail_inline_bytes(),
            oldest_commit_at_ms: position.wal_tail_oldest_commit_at_ms(),
            raises_retention_floor: position.read_basis().head().raises_retention_floor(),
        }
    }
}

impl From<&WalTailReport> for WalTailState {
    fn from(report: &WalTailReport) -> Self {
        Self {
            objects: report.objects,
            wal_bytes: report.wal_bytes,
            inline_bytes: report.inline_bytes,
            oldest_commit_at_ms: report.oldest_commit_at_ms,
            raises_retention_floor: report.raises_retention_floor,
        }
    }
}

impl From<&NamespaceReadAnchor> for WalTailState {
    fn from(anchor: &NamespaceReadAnchor) -> Self {
        let status = NamespaceStorageDiagnostics::from(anchor);
        Self {
            objects: status.wal_tail_objects,
            wal_bytes: anchor.wal_tail_bytes(),
            inline_bytes: anchor.wal_tail_inline_bytes(),
            oldest_commit_at_ms: status.wal_tail_oldest_commit_at_ms,
            raises_retention_floor: anchor.read_state().raises_retention_floor(),
        }
    }
}

/// When a fold of a WAL tail is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FoldDue {
    /// The tail has reached a fold threshold or the fold age, or a floor
    /// object in it raised the view's floor above the manifest's.
    Now,
    /// The tail's oldest commit reaches the fold age at `due_at_ms`, on the
    /// wall clock.
    At { due_at_ms: u64 },
    /// The tail has no commit, so only a larger tail makes a fold due.
    OnGrowth,
}

/// Returns when a fold of `tail` is due at `now_ms`, on the runtime's wall
/// clock: once the tail reaches `max_wal_tail_objects` WAL objects,
/// `FOLD_AT_WAL_BYTES`, or the inline fold trigger of the runtime's inline
/// tail limit (see `fold_at_inline_bytes`), once its oldest commit is the
/// runtime's fold age old, or at once when a floor object in it raised the
/// view's floor above the manifest's, such as after a retention advance
/// whose fold failed, because no manifest records that floor yet.
///
/// The writer session's check after each publication and every maintenance
/// check use this one rule, with their own object threshold.
pub(crate) fn fold_due(
    core: &RuntimeCore,
    max_wal_tail_objects: u64,
    tail: &WalTailState,
    now_ms: u64,
) -> FoldDue {
    let due_now = tail.raises_retention_floor
        || tail.objects >= max_wal_tail_objects
        || tail.wal_bytes >= FOLD_AT_WAL_BYTES
        || tail.inline_bytes
            >= fold_at_inline_bytes(core.config().inline_content.inline_content_tail_limit_bytes);
    let due_at_ms = tail
        .oldest_commit_at_ms
        .map(|committed_at_ms| committed_at_ms.saturating_add(core.config().fold_age_ms.get()));
    match due_at_ms {
        _ if due_now => FoldDue::Now,
        Some(due_at_ms) if now_ms >= due_at_ms => FoldDue::Now,
        Some(due_at_ms) => FoldDue::At { due_at_ms },
        None => FoldDue::OnGrowth,
    }
}

/// What one [`NamespacePublisher::fold`] call did.
pub(crate) enum NamespaceFold {
    /// No fold was due.
    NotDue {
        /// When the tail's oldest commit reaches the fold age, so that a fold
        /// becomes due without another commit, on the runtime's monotonic
        /// timer. `None` for a tail without a commit, such as a tail of
        /// fences alone.
        fold_due_at_ms: Option<u64>,
        /// The current manifest, when the check loaded it from the store,
        /// for a compaction unit to plan from.
        observed: Option<Held<LoadedManifest>>,
    },
    /// The call folded the tail, or found it folded.
    Folded(Box<FoldedWalTail>),
}

impl NamespaceFold {
    /// Returns the fold, or `None` when no fold was due.
    pub(crate) fn folded(self) -> Option<FoldedWalTail> {
        match self {
            Self::NotDue { .. } => None,
            Self::Folded(folded) => Some(*folded),
        }
    }
}

impl NamespacePublisher {
    /// Folds the WAL tail of this session's namespace when `when` finds a
    /// fold due.
    ///
    /// Every fold of a namespace in this runtime runs here, as a fold of the
    /// namespace's writer session, with the namespace's fold lock, so a fold
    /// waits for a fold in progress instead of folding the same tail. Under
    /// [`FoldWhen::Always`], a fold that took its input after the call
    /// arrived serves the call. A fenced session returns `writer_fenced`
    /// before it checks anything.
    ///
    /// The check of whether a fold is due reads the session's position (see
    /// [`Self::current_position`]) and takes no fold lock and no fold permit
    /// when nothing is due, and the check runs again under the fold lock,
    /// because a fold that had the lock may have folded the tail. Without a
    /// position, the check loads the tail from the store under the fold lock,
    /// and claims nothing. A fold that is due is an item of the session's
    /// publication lane, which starts from the session's position, so a
    /// session without its writer epoch claims it first (see
    /// [`Self::run_from_position`] and [`Self::lane_turn`]). A check that loads the
    /// tail takes a read permit, which it releases before the fold waits for
    /// its fold permit.
    ///
    /// The fold waits for a fold permit, and in the lane it records the
    /// manifest that covers the tail, whether it published that manifest or
    /// found it current, as the session's newest published manifest. A fold
    /// that publishes then wakes the commits that wait at the write stop.
    pub(super) async fn fold(
        &self,
        when: FoldWhen,
        stop: &MaintenanceCancellation,
    ) -> Result<NamespaceFold> {
        drop(self.unfenced_session()?);
        let turns = &self.turns;
        let arrival = turns.started();
        if self.current_position().is_some() {
            if let Some(not_due) = self.fold_not_due(when).await? {
                return Ok(not_due);
            }
        }
        let _turn = self
            .unless_stopped(
                stop,
                turns.fold.lock().instrument(phase_span!(
                    self.context.runtime_core,
                    Phase::WaitForFoldLock,
                    self.namespace_id
                )),
            )
            .await?;
        if let FoldWhen::Always = when {
            if let Some(folded) = turns.folded_after(arrival) {
                return Ok(NamespaceFold::Folded(Box::new(folded)));
            }
        }
        if let Some(not_due) = self.fold_not_due(when).await? {
            return Ok(not_due);
        }
        if let (FoldWhen::Always, None) = (when, self.current_position()) {
            // A session without a position reads the tail first, so a fold
            // of a tail with no WAL object publishes nothing and claims
            // nothing. The manifest that the read found may be another
            // writer's, so it isn't the session's newest published manifest.
            let anchor = self.load_tail_anchor().await?;
            if let Some(current) = FoldedWalTail::current_when_no_wal_object(&anchor) {
                self.registry()?
                    .record_manifest_published(&self.namespace_id, &current.current);
                return Ok(NamespaceFold::Folded(Box::new(current)));
            }
        }
        self.fold_under_turn(stop)
            .await
            .map(|folded| NamespaceFold::Folded(Box::new(folded)))
    }

    /// Waits for `wait`, a lock or a permit that a fold waits for, or returns
    /// the session's refusal of new work (see [`Self::stopped_refusal`]) once
    /// `stop` is set, so that a host's fold that hasn't started doesn't keep
    /// the session's close or the runtime's drain waiting. `stop` is the
    /// session's stop, or a token that is never set for the session's own
    /// fold.
    async fn unless_stopped<T>(
        &self,
        stop: &MaintenanceCancellation,
        wait: impl Future<Output = T>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            () = stop.cancelled() => Err(self.stopped_refusal()),
            value = wait => Ok(value),
        }
    }

    /// Folds the tail from the session's position, with the fold lock that
    /// the caller has.
    ///
    /// The fold writes its content objects and segments from the session's
    /// position before it takes the session's publication lane (see
    /// [`NamespaceStore::write_fold_segments`](loonfs_core::NamespaceStore::write_fold_segments)),
    /// and then publishes them in one turn of the lane, over the session's
    /// newest published manifest, which has the head that the fold started
    /// from (see
    /// [`NamespaceStore::publish_fold_in_turn`](loonfs_core::NamespaceStore::publish_fold_in_turn)).
    /// So the fold keeps the lane only for its put and the boundary read
    /// after it, and a claim waits for that turn alone. The lane comes after
    /// the fold lock and the fold permit, and before the engine, which the
    /// fold locks only to load a position that the session doesn't keep. A
    /// fold that has its permit runs to its end, so the session's stop
    /// doesn't end its wait for the lane.
    async fn fold_under_turn(&self, stop: &MaintenanceCancellation) -> Result<FoldedWalTail> {
        let core = &self.context.runtime_core;
        let namespace_id = &self.namespace_id;
        let _permit = self
            .unless_stopped(
                stop,
                core.fold_permit().instrument(phase_span!(
                    core,
                    Phase::WaitForFoldPermit,
                    namespace_id
                )),
            )
            .await?;
        let round = self.turns.start_round();
        let started_at_ms = core.timer().monotonic_now_ms();
        let fold_span = phase_span!(
            core,
            Phase::WalFold,
            namespace_id,
            outcome = tracing::field::Empty
        );
        // Commits that the session published while the fold waited for its
        // permit are in the position now, so the fold folds them too.
        let namespace = core.namespace_store(namespace_id);
        let clock = OperationClock::start(core.now_ms(), Arc::clone(core.timer()));
        let result: Result<FoldedWalTail> = async {
            let writes = self
                .run_from_position(|position| {
                    let (namespace, clock) = (&namespace, &clock);
                    async move { namespace.write_fold_segments(&position, clock).await }
                })
                .await?;
            let writes = match writes {
                FoldWriteOutcome::AlreadyFolded(folded) => return Ok(folded),
                FoldWriteOutcome::Written(writes) => writes,
            };
            let lane = self.lane.lock().await;
            let turn = self.lane_turn(&lane).await?;
            Ok(namespace.publish_fold_in_turn(turn, writes, &clock).await?)
        }
        .instrument(fold_span.clone())
        .await;
        if let Ok(folded) = &result {
            fold_span.record("outcome", tracing::field::debug(folded.response.outcome));
        }
        core.instruments()
            .publisher
            .wal_fold_duration(elapsed_ms_from(
                started_at_ms,
                core.timer().monotonic_now_ms(),
            ));
        let registry = self.registry()?;
        let folded = registry
            .finish_namespace_mutation(namespace_id, result, |_| NamespaceChange::Unchanged)
            .await?;
        registry.record_manifest_published(namespace_id, &folded.current);
        self.turns.record(round, &folded);
        if folded.response.outcome == FoldWalOutcome::Published {
            self.fold_published.send_replace(());
        }
        Ok(folded)
    }

    /// Runs `create` at the manifest that covers the tail of this session's
    /// namespace, as one item of the session's publication lane, and repeats
    /// it while the snapshot finds that manifest replaced before its
    /// verification, for at most `MAX_CONTENTION_ATTEMPTS` snapshots (format
    /// spec, "Creating and verifying a snapshot").
    ///
    /// The manifest that covers the tail is a fold's when the session has a
    /// position, or when the tail has a commit or a floor object that raises
    /// the view's floor above its manifest's, the tail that a fold publishes
    /// (see `WalTailState::has_change`), so such a snapshot folds, and a
    /// session without its writer epoch claims it first. A tail of only
    /// fences counts as empty. Otherwise the snapshot is taken at the current
    /// manifest that the head was read from, and the call claims nothing.
    /// The check of the tail takes a read permit, which it releases before
    /// the fold waits for its fold permit.
    /// Each attempt uses a fold that took its input after the attempt
    /// arrived, and folds only when no such fold has completed (see
    /// [`Self::fold`]).
    ///
    /// The attempt then keeps the session's publication lane while `create`
    /// writes the snapshot's record and verifies it, and takes the newer of
    /// that manifest and the newest manifest that the session published,
    /// which also covers the tail (see
    /// [`LaneGuard::basis_over`](loonfs_core::publish::LaneGuard::basis_over)).
    /// So no publication of the session replaces the snapshot's manifest
    /// before the snapshot is verified, and snapshots take their turns in the
    /// lane. Another process can still replace the
    /// manifest. Each attempt's result goes through
    /// [`PublisherRegistry::finish_namespace_mutation`], so an attempt that
    /// finds its manifest replaced drops the session's position and the
    /// cached head, and the next attempt reads the newest manifest.
    pub(super) async fn snapshot_after_fold<T, SnapshotFuture>(
        &self,
        mut create: impl FnMut(Held<LoadedManifest>) -> SnapshotFuture,
        stop: &MaintenanceCancellation,
    ) -> Result<T>
    where
        SnapshotFuture: Future<Output = loonfs_core::Result<T>>,
    {
        let mut attempts = 1;
        loop {
            let arrival = self.turns.started();
            let basis = self.snapshot_basis(arrival, stop).await?;
            let lane = self.unless_stopped(stop, self.lane.lock()).await?;
            let basis = lane.basis_over(basis);
            let created = create(basis).await.map_err(Error::from);
            drop(lane);
            let created = self
                .registry()?
                .finish_namespace_mutation(&self.namespace_id, created, |_| {
                    NamespaceChange::Unchanged
                })
                .await;
            match created {
                Err(Error::Core(CoreError::HeadMoved(_))) if attempts < MAX_CONTENTION_ATTEMPTS => {
                    attempts += 1;
                }
                created => return created,
            }
        }
    }

    /// Returns the manifest that covers the tail for a snapshot attempt that
    /// arrived at `arrival` (see [`Self::snapshot_after_fold`]), under the
    /// namespace's fold lock, which the call releases before the snapshot
    /// takes the publication lane. `stop` ends the waits for the fold lock
    /// and for a fold permit, as it does for [`Self::fold`].
    async fn snapshot_basis(
        &self,
        arrival: u64,
        stop: &MaintenanceCancellation,
    ) -> Result<Held<LoadedManifest>> {
        let turns = &self.turns;
        let _turn = self.unless_stopped(stop, turns.fold.lock()).await?;
        if let Some(folded) = turns.folded_after(arrival) {
            return Ok(folded.current);
        }
        drop(self.unfenced_session()?);
        if self.current_position().is_none() {
            let anchor = self.load_tail_anchor().await?;
            if !WalTailState::from(&anchor).has_change() {
                return Ok(anchor.held().clone());
            }
        }
        Ok(self.fold_under_turn(stop).await?.current)
    }

    /// Returns what a fold under `when` reports without folding, or `None`
    /// when a fold is due.
    ///
    /// When the session has a position, the tail is the position's.
    /// Otherwise the check loads the current manifest and the tail from the
    /// store, and claims nothing. Either way, the check dates the tail's
    /// oldest commit by its `committed_at_ms`, on the runtime's wall clock.
    async fn fold_not_due(&self, when: FoldWhen) -> Result<Option<NamespaceFold>> {
        let FoldWhen::Due {
            max_wal_tail_objects,
        } = when
        else {
            return Ok(None);
        };
        let core = &self.context.runtime_core;
        let (tail, observed) = match self.current_position() {
            Some(position) => (WalTailState::from(&position), None),
            None => {
                let anchor = self.load_tail_anchor().await?;
                (WalTailState::from(&anchor), Some(anchor.held().clone()))
            }
        };
        let now_ms = core.now_ms();
        let fold_due_at_ms = match fold_due(core, max_wal_tail_objects, &tail, now_ms) {
            FoldDue::Now => return Ok(None),
            FoldDue::At { due_at_ms } => Some(due_at_ms),
            FoldDue::OnGrowth => None,
        };
        // The tail's commits are dated on the wall clock, so the due time
        // moves onto the monotonic timer by its distance from the check's own
        // reading.
        let monotonic_now_ms = core.timer().monotonic_now_ms();
        Ok(Some(NamespaceFold::NotDue {
            fold_due_at_ms: fold_due_at_ms
                .map(|due_at_ms| monotonic_now_ms.saturating_add(due_at_ms.saturating_sub(now_ms))),
            observed,
        }))
    }

    /// Loads the namespace's current manifest and WAL tail for a check of
    /// whether a fold is due by a session without a position, with a read
    /// permit from the runtime's execution budget, which the check releases
    /// before a fold waits for its fold permit.
    async fn load_tail_anchor(&self) -> Result<NamespaceReadAnchor> {
        let core = &self.context.runtime_core;
        let _permit = core.read_permit().await;
        Ok(core
            .namespace_store(&self.namespace_id)
            .load_live_read_anchor(core.observation())
            .await?)
    }

    /// Returns the registry that this session belongs to, or
    /// `shutting_down` once the registry is gone.
    pub(super) fn registry(&self) -> Result<PublisherRegistry> {
        let shared = self.shared.upgrade().ok_or(CoreError::ShuttingDown)?;
        Ok(PublisherRegistry { shared })
    }

    /// Returns the turn of a publication of the session's lane, which the
    /// caller keeps as `lane`, over the session's newest published manifest,
    /// after a claim when the session has no writer epoch (see
    /// `NamespaceCommitEngine::lane_turn`), which locks the engine. A fenced
    /// session returns `writer_fenced`. Every publication of the lane starts its turn here,
    /// so each checks the session's epoch in this one place.
    ///
    /// The caller keeps the lane and then locks the engine, which is the
    /// order of the session's locks, so a claim keeps both queues.
    pub(super) async fn lane_turn<'g>(&self, lane: &'g LaneGuard<'_>) -> Result<LaneTurn<'g>> {
        drop(self.unfenced_session()?);
        if let Some(turn) = lane.turn() {
            return Ok(turn);
        }
        let mut engine = self.lock_engine().await;
        engine.lane_turn(lane).await.map_err(Error::from)
    }

    /// Returns the position that the writes of a publication of the session
    /// start from outside its publication lane, such as a fold's segments or
    /// a compaction unit's output segments, before the publication takes the lane for
    /// its put: the position that the session keeps, or,
    /// when it keeps none, the position that the engine loads as a commit
    /// loads its view. The load publishes nothing, so it takes the engine
    /// alone, after a claim when the session has no writer epoch, which
    /// keeps the publication lane and the engine (see
    /// [`Self::lock_engine_for_unit`]). A fenced session returns
    /// `writer_fenced`.
    pub(super) async fn writer_position(&self) -> Result<WriterPosition> {
        drop(self.unfenced_session()?);
        if let Some(position) = self.current_position() {
            return Ok(position);
        }
        let (mut engine, start) = self.lock_engine_for_unit().await?;
        engine.load_position(start).await.map_err(Error::from)
    }

    /// Returns the position that reads in this runtime start from.
    ///
    /// The position is the engine's as of the engine's last release, brought
    /// up to the newest manifest that this runtime published, so a fold from
    /// it doesn't wait for a publication that the engine is running, and that
    /// publication's commits stay in the tail above the fold's manifest.
    pub(super) fn current_position(&self) -> Option<WriterPosition> {
        self.read_position
            .current()
            .map(|position| WriterPosition::clone(&position))
    }
}
