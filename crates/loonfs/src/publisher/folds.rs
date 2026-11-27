//! The one fold path of each namespace in a writable runtime, and the locks
//! that keep a namespace's folds apart from each other and from its snapshots, and
//! its compaction units apart.

use super::registry::NamespaceChange;
use super::worker::elapsed_ms_from;
use super::{NamespacePublisher, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::phase_span;
use crate::{CoreError, Error, FoldWalOutcome, NamespaceId, Result};
use loonfs_core::control::{
    Held, LoadedManifest, NamespaceReadAnchor, NamespaceStorageDiagnostics,
};
use loonfs_core::limits::{fold_at_inline_bytes, FOLD_AT_WAL_BYTES, MAX_CONTENTION_ATTEMPTS};
use loonfs_core::publish::{WalTailReport, WriterPosition};
use loonfs_core::time::OperationClock;
use loonfs_core::{FoldedWalTail, MetadataCompactionCancellation, Phase};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{Mutex as AsyncMutex, RwLock, RwLockReadGuard};
use tracing::Instrument;

/// The fold lock, the fold rounds, and the compaction lock of one namespace
/// in this runtime.
///
/// A fold takes the fold lock in exclusive mode before it decides that a
/// fold is due, and keeps it until it has moved the session's position, so
/// two folds never fold the same tail. A snapshot takes the lock in shared mode
/// from the moment it has a fold's manifest until its verification, so a
/// fold in this runtime never replaces that manifest before the snapshot is
/// verified, and any number of snapshots run at once. The fold rounds record
/// which fold served a request: a fold that took its input after the
/// request arrived covers every commit acknowledged before the request, so
/// the requests that wait for one fold share the next one. A compaction
/// unit takes the compaction lock before it plans, so two units never plan
/// from the same manifest. The fold lock and the compaction lock are `tokio`
/// locks, because a fold, a snapshot, and a unit keep them across store requests.
#[derive(Default)]
pub(crate) struct NamespaceTurns {
    pub(super) fold: RwLock<()>,
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

/// Which WAL tails [`PublisherRegistry::fold_namespace`] folds.
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
pub(super) struct WalTailState {
    /// Unfolded WAL objects, including fences.
    objects: u64,
    /// Stored bytes of the unfolded WAL objects with commits.
    wal_bytes: usize,
    /// Inline content bytes of the unfolded WAL objects.
    inline_bytes: usize,
    /// The earliest `committed_at_ms` of the tail's commits, or `None` for a
    /// tail without a commit.
    oldest_commit_at_ms: Option<u64>,
}

impl From<&WriterPosition> for WalTailState {
    fn from(position: &WriterPosition) -> Self {
        Self {
            objects: position.wal_tail_objects(),
            wal_bytes: position.wal_tail_bytes(),
            inline_bytes: position.wal_tail_inline_bytes(),
            oldest_commit_at_ms: position.wal_tail_oldest_commit_at_ms(),
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
        }
    }
}

/// When a fold of a WAL tail is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FoldDue {
    /// The tail has reached a fold threshold or the fold age.
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
/// tail limit (see `fold_at_inline_bytes`), or once its oldest commit is the
/// runtime's fold age old.
///
/// The writer session's check after each publication and every maintenance
/// check use this one rule, with their own object threshold.
pub(super) fn fold_due(
    core: &RuntimeCore,
    max_wal_tail_objects: u64,
    tail: &WalTailState,
    now_ms: u64,
) -> FoldDue {
    let reached_size = tail.objects >= max_wal_tail_objects
        || tail.wal_bytes >= FOLD_AT_WAL_BYTES
        || tail.inline_bytes
            >= fold_at_inline_bytes(core.config().inline_content.inline_content_tail_limit_bytes);
    let due_at_ms = tail
        .oldest_commit_at_ms
        .map(|committed_at_ms| committed_at_ms.saturating_add(core.config().fold_age_ms.get()));
    match due_at_ms {
        _ if reached_size => FoldDue::Now,
        Some(due_at_ms) if now_ms >= due_at_ms => FoldDue::Now,
        Some(due_at_ms) => FoldDue::At { due_at_ms },
        None => FoldDue::OnGrowth,
    }
}

/// What one [`PublisherRegistry::fold_namespace`] call did.
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

impl PublisherRegistry {
    /// Folds the WAL tail of `namespace_id` when `when` finds a fold due.
    ///
    /// Every fold of a namespace in this runtime runs here, with the
    /// namespace's fold lock in exclusive mode, so a fold waits for a fold in
    /// progress instead of folding the same tail, and for the snapshots in
    /// progress (see [`Self::snapshot_after_fold`]). Under [`FoldWhen::Always`], a
    /// fold that took its input after the call arrived serves the call. The
    /// fold's input is the position of the
    /// namespace's writer session while the session isn't fenced and has a
    /// position (see [`NamespacePublisher::current_position`]),
    /// and otherwise the fold loads the tail from the store. A check against
    /// the position takes no fold lock and no fold permit when nothing is
    /// due, and the check runs again under the fold lock, because a fold that
    /// had the lock may have folded the tail. Without a position, the check
    /// loads the tail from the store, under the fold lock.
    ///
    /// The fold waits for a fold permit, and it records the manifest that
    /// covers the tail, whether it published that manifest or found it
    /// current (see [`Self::record_manifest_published`]). A fold that
    /// publishes then wakes the commits that wait at the write stop.
    pub(crate) async fn fold_namespace(
        &self,
        namespace_id: &NamespaceId,
        when: FoldWhen,
    ) -> Result<NamespaceFold> {
        let turns = self.namespace_turns(namespace_id);
        let arrival = turns.started();
        let session = self.unfenced_publisher(namespace_id);
        let session = session.as_ref();
        if session
            .and_then(NamespacePublisher::current_position)
            .is_some()
        {
            if let Some(not_due) = self.fold_not_due(namespace_id, when, session).await? {
                return Ok(not_due);
            }
        }
        let _turn = turns
            .fold
            .write()
            .instrument(phase_span!(
                self.shared.context.runtime_core,
                Phase::WaitForFoldLock,
                namespace_id
            ))
            .await;
        if let FoldWhen::Always = when {
            if let Some(folded) = turns.folded_after(arrival) {
                return Ok(NamespaceFold::Folded(Box::new(folded)));
            }
        }
        if let Some(not_due) = self.fold_not_due(namespace_id, when, session).await? {
            return Ok(not_due);
        }
        self.fold_under_turn(namespace_id, session, &turns)
            .await
            .map(|folded| NamespaceFold::Folded(Box::new(folded)))
    }

    async fn fold_under_turn(
        &self,
        namespace_id: &NamespaceId,
        session: Option<&NamespacePublisher>,
        turns: &NamespaceTurns,
    ) -> Result<FoldedWalTail> {
        let core = &self.shared.context.runtime_core;
        let _permit = core
            .fold_permit()
            .instrument(phase_span!(core, Phase::WaitForFoldPermit, namespace_id))
            .await;
        let round = turns.start_round();
        // Commits that the session published while the fold waited for its
        // permit are in the position now, so the fold folds them too.
        let input = session.and_then(NamespacePublisher::current_position);
        let started_at_ms = core.timer().monotonic_now_ms();
        let fold_span = phase_span!(
            core,
            Phase::WalFold,
            namespace_id,
            outcome = tracing::field::Empty
        );
        let result = core
            .namespace_store(namespace_id)
            .fold_wal_tail(
                input,
                &OperationClock::start(core.now_ms(), Arc::clone(core.timer())),
            )
            .instrument(fold_span.clone())
            .await
            .map_err(Error::from);
        if let Ok(folded) = &result {
            fold_span.record("outcome", tracing::field::debug(folded.response.outcome));
        }
        core.instruments()
            .publisher
            .wal_fold_duration(elapsed_ms_from(
                started_at_ms,
                core.timer().monotonic_now_ms(),
            ));
        let folded = self
            .finish_namespace_mutation(namespace_id, result, |_| NamespaceChange::Unchanged)
            .await?;
        self.record_manifest_published(namespace_id, &folded.current);
        turns.record(round, &folded);
        if folded.response.outcome == FoldWalOutcome::Published {
            if let Some(publisher) = self.live_publisher(namespace_id) {
                publisher.fold_published.send_replace(());
            }
        }
        Ok(folded)
    }

    /// Folds the WAL tail of `namespace_id` and runs `create` at the manifest
    /// that covers the tail, and repeats both while the snapshot finds that
    /// manifest replaced before its verification, for at most
    /// `MAX_CONTENTION_ATTEMPTS` snapshots.
    ///
    /// Each attempt uses a fold that took its input after the attempt
    /// arrived, and folds only when no such fold has completed (see
    /// [`Self::fold_namespace`]). The attempt keeps the namespace's fold lock
    /// in shared mode from the moment it has the fold's manifest until `create`
    /// returns, so no fold in this runtime replaces the manifest before the
    /// snapshot is verified, and other snapshots run at the same time. A compaction
    /// unit, an extension's publication, or another process can still
    /// replace the manifest. Each attempt's result goes through
    /// [`Self::finish_namespace_mutation`], so an attempt that finds its
    /// manifest replaced drops the session's position and the cached head,
    /// and the next attempt's fold loads the newest manifest from the store.
    pub(crate) async fn snapshot_after_fold<T, SnapshotFuture>(
        &self,
        namespace_id: &NamespaceId,
        mut create: impl FnMut(Held<LoadedManifest>) -> SnapshotFuture,
    ) -> Result<T>
    where
        SnapshotFuture: Future<Output = loonfs_core::Result<T>>,
    {
        let turns = self.namespace_turns(namespace_id);
        let mut attempts = 1;
        loop {
            let arrival = turns.started();
            let (basis, turn) = self
                .fold_for_snapshot(namespace_id, &turns, arrival)
                .await?;
            let created = create(basis).await.map_err(Error::from);
            drop(turn);
            let created = self
                .finish_namespace_mutation(namespace_id, created, |_| NamespaceChange::Unchanged)
                .await;
            match created {
                Err(Error::Core(CoreError::HeadMoved(_))) if attempts < MAX_CONTENTION_ATTEMPTS => {
                    attempts += 1;
                }
                created => return created,
            }
        }
    }

    async fn fold_for_snapshot<'a>(
        &self,
        namespace_id: &NamespaceId,
        turns: &'a NamespaceTurns,
        arrival: u64,
    ) -> Result<(Held<LoadedManifest>, RwLockReadGuard<'a, ()>)> {
        let shared = turns.fold.read().await;
        if let Some(folded) = turns.folded_after(arrival) {
            return Ok((folded.current, shared));
        }
        drop(shared);
        // The fold lock is released from exclusive mode, not downgraded to
        // shared mode. If a snapshot kept the lock from its fold, the next
        // waiter's exclusive lock would wait for that snapshot, and the waiters
        // would take their snapshots one at a time.
        {
            let _exclusive = turns.fold.write().await;
            if turns.folded_after(arrival).is_none() {
                let session = self.unfenced_publisher(namespace_id);
                self.fold_under_turn(namespace_id, session.as_ref(), turns)
                    .await?;
            }
        }
        let shared = turns.fold.read().await;
        let folded = turns
            .folded_after(arrival)
            .expect("a fold after the arrival should have completed under the fold lock");
        Ok((folded.current, shared))
    }

    fn unfenced_publisher(&self, namespace_id: &NamespaceId) -> Option<NamespacePublisher> {
        self.live_publisher(namespace_id)
            .filter(|publisher| !publisher.session_is_fenced())
    }

    /// Returns what a fold under `when` reports without folding, or `None`
    /// when a fold is due.
    ///
    /// When `session` has a position, the tail is the position's. Otherwise
    /// the check loads the current manifest and the tail from the store.
    /// Either way, the check dates the tail's oldest commit by its
    /// `committed_at_ms`, on the runtime's wall clock.
    async fn fold_not_due(
        &self,
        namespace_id: &NamespaceId,
        when: FoldWhen,
        session: Option<&NamespacePublisher>,
    ) -> Result<Option<NamespaceFold>> {
        let FoldWhen::Due {
            max_wal_tail_objects,
        } = when
        else {
            return Ok(None);
        };
        let core = &self.shared.context.runtime_core;
        let position = session.and_then(NamespacePublisher::current_position);
        let (tail, observed) = match position {
            Some(position) => (WalTailState::from(&position), None),
            None => {
                let anchor = core
                    .namespace_store(namespace_id)
                    .load_live_read_anchor(core.observation())
                    .await?;
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

    /// Returns the fold lock and the compaction lock of `namespace_id`.
    pub(crate) fn namespace_turns(&self, namespace_id: &NamespaceId) -> Arc<NamespaceTurns> {
        self.shared.lock_state().turns(namespace_id)
    }

    /// Returns the compaction cancellation of the namespace's writer
    /// session, which a close or a delete of the session cancels, or `None`
    /// when this runtime has no session for the namespace.
    pub(crate) fn session_compaction_cancellation(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<MetadataCompactionCancellation> {
        self.live_publisher(namespace_id)
            .map(|publisher| publisher.compaction_cancellation.clone())
    }
}

impl NamespacePublisher {
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
