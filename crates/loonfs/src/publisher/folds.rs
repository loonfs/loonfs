//! The one fold path of each namespace in a writable runtime, and the locks
//! that keep a namespace's folds apart and its compaction units apart.

use super::registry::{NamespaceChange, PositionChange};
use super::worker::elapsed_ms_from;
use super::{FoldTrigger, NamespacePublisher, PublisherRegistry};
use crate::trace::phase_span;
use crate::{CoreError, Error, FoldWalOutcome, NamespaceId, Result};
use loonfs_core::control::{NamespaceStorageDiagnostics, PublicationBasis};
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::publish::WriterPosition;
use loonfs_core::time::OperationClock;
use loonfs_core::{FoldedWalTail, MetadataCompactionCancellation, Phase};
use loonfs_types::format::control::ManifestRef;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::Instrument;

/// The fold lock and the compaction lock of one namespace in this runtime.
///
/// A fold takes the fold lock before it decides that a fold is due, and
/// keeps it until it has moved the session's position, so two folds never
/// fold the same tail. A compaction unit takes the compaction lock before it
/// plans, so two units never plan from the same manifest. Both are `tokio`
/// mutexes, because a fold and a unit keep their lock across store requests.
#[derive(Default)]
pub(crate) struct NamespaceTurns {
    pub(super) fold: AsyncMutex<()>,
    pub(crate) compaction: AsyncMutex<()>,
}

/// Which WAL tails [`PublisherRegistry::fold_namespace`] folds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FoldWhen {
    /// Any tail. A tail that's already folded is reported as
    /// [`FoldWalOutcome::AlreadyCurrent`].
    Always,
    /// A tail of at least `max_wal_tail_objects` WAL objects, a tail with the
    /// runtime's inline fold size of inline content, or a tail whose newest
    /// commit has reached the runtime's idle fold age.
    Due { max_wal_tail_objects: u64 },
}

/// What one [`PublisherRegistry::fold_namespace`] call did.
pub(crate) enum NamespaceFold {
    /// No fold was due.
    NotDue {
        /// Whether the tail has a commit that reaches the idle fold age
        /// later, so that a fold becomes due without another commit.
        idle_fold_pending: bool,
        /// The current manifest, when the check loaded it from the store,
        /// for a compaction unit to plan from.
        observed: Option<PublicationBasis>,
    },
    /// The call folded the tail, or found it folded.
    Folded(FoldedWalTail),
}

impl NamespaceFold {
    /// Returns the fold, or `None` when no fold was due.
    pub(crate) fn folded(self) -> Option<FoldedWalTail> {
        match self {
            Self::NotDue { .. } => None,
            Self::Folded(folded) => Some(folded),
        }
    }
}

impl PublisherRegistry {
    /// Folds the WAL tail of `namespace_id` when `when` finds a fold due.
    ///
    /// Every fold of a namespace in this runtime runs here, under the
    /// namespace's fold lock, so a fold waits for a fold in progress instead
    /// of folding the same tail. The fold's input is the position of the
    /// namespace's writer session while the session isn't fenced and the
    /// position is current (see [`NamespacePublisher::current_position`]),
    /// and otherwise the fold loads the tail from the store. A check that
    /// the position answers takes no lock and no fold permit when nothing is
    /// due, and the check runs again under the lock, because a fold that had
    /// the lock may have folded the tail. Without a position, the check loads
    /// the tail from the store, under the lock.
    ///
    /// The fold waits for a fold permit, and it moves the session's position
    /// onto the manifest that covers the tail, whether it published that
    /// manifest or found it current. A fold that publishes then wakes the
    /// commits that wait at the write stop, and raises the namespace's hint.
    pub(crate) async fn fold_namespace(
        &self,
        namespace_id: &NamespaceId,
        when: FoldWhen,
    ) -> Result<NamespaceFold> {
        let session = self
            .live_publisher(namespace_id)
            .filter(|publisher| !publisher.session_is_fenced());
        let session = session.as_ref();
        if session
            .and_then(NamespacePublisher::current_position)
            .is_some()
        {
            if let Some(not_due) = self.fold_not_due(namespace_id, when, session).await? {
                return Ok(not_due);
            }
        }
        let turns = self.namespace_turns(namespace_id);
        let _turn = turns.fold.lock().await;
        if let Some(not_due) = self.fold_not_due(namespace_id, when, session).await? {
            return Ok(not_due);
        }
        let core = &self.shared.context.runtime_core;
        let _permit = core.fold_permit().await;
        // Commits that the session published while the fold waited for its
        // permit are in the position now, so the fold folds them too.
        let input = session.and_then(NamespacePublisher::current_position);
        let started_at_ms = core.timer().monotonic_now_ms();
        let result = core
            .namespace_store(namespace_id)
            .fold_wal_tail(
                input,
                &OperationClock::start(core.now_ms(), Arc::clone(core.timer())),
            )
            .instrument(phase_span!(core, Phase::WalFold, namespace_id))
            .await
            .map_err(Error::from);
        core.instruments()
            .publisher
            .wal_fold_duration(elapsed_ms_from(
                started_at_ms,
                core.timer().monotonic_now_ms(),
            ));
        let folded = self
            .finish_namespace_mutation(namespace_id, result, |_| NamespaceChange::Unchanged)
            .await?;
        let published = folded.response.outcome == FoldWalOutcome::Published;
        if let Some(publisher) = self.live_publisher(namespace_id) {
            publisher
                .change_position(PositionChange::Move(folded.manifest.clone()))
                .await;
            if published {
                publisher.fold_published.send_replace(());
            }
        }
        if published {
            self.raise_hint(namespace_id, folded.response.manifest_no)
                .await;
        }
        Ok(NamespaceFold::Folded(folded))
    }

    /// Folds the WAL tail of `namespace_id` and runs `pin` at the manifest
    /// that covers the tail, and repeats both while the pin finds that
    /// manifest replaced before its verification, for at most
    /// `MAX_CONTENTION_ATTEMPTS` pins.
    ///
    /// Each fold is one of the namespace's folds (see
    /// [`Self::fold_namespace`]), so a fold after a replaced pin waits for a
    /// fold in progress instead of folding the same tail. The fold lock isn't
    /// held while `pin` runs.
    pub(crate) async fn pin_after_fold<T, Pin>(
        &self,
        namespace_id: &NamespaceId,
        mut pin: impl FnMut(ManifestRef) -> Pin,
    ) -> Result<T>
    where
        Pin: Future<Output = loonfs_core::Result<T>>,
    {
        let mut pins = 1;
        loop {
            let folded = self
                .fold_namespace(namespace_id, FoldWhen::Always)
                .await?
                .folded()
                .expect("a fold of any tail runs");
            match pin(folded.manifest.manifest).await {
                Err(CoreError::CheckpointUnavailable(_)) if pins < MAX_CONTENTION_ATTEMPTS => {
                    pins += 1;
                }
                pinned => return pinned.map_err(Error::from),
            }
        }
    }

    /// Returns what a fold under `when` reports without folding, or `None`
    /// when a fold is due.
    ///
    /// When `session` has a current position, the tail is the position's,
    /// and its newest commit is the session's last publish, on the runtime's
    /// monotonic clock. Otherwise the check loads the current manifest and
    /// the tail from the store, the tail's newest commit is dated on the
    /// wall clock, and the tail's inline bytes are those of the session's
    /// position whatever the age of its last check, or zero without a
    /// position. A position past the revalidation bound can only count more
    /// inline bytes than the tail has, because another process may have
    /// folded since.
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
        let (objects, inline_bytes, newest_commit_at_ms, now_ms, observed) = match position {
            Some(position) => (
                position.wal_tail_objects(),
                position.wal_tail_inline_bytes(),
                session.and_then(NamespacePublisher::last_published_at_ms),
                core.timer().monotonic_now_ms(),
                None,
            ),
            None => {
                let anchor = core
                    .namespace_store(namespace_id)
                    .load_live_read_anchor(core.observation())
                    .await?;
                let status = NamespaceStorageDiagnostics::from(&anchor);
                (
                    status.wal_tail_objects,
                    session
                        .and_then(|session| session.read_position.current())
                        .map_or(0, |position| position.wal_tail_inline_bytes()),
                    status.wal_tail_newest_commit_at_ms,
                    core.now_ms(),
                    Some(anchor.publication_basis()),
                )
            }
        };
        let trigger = FoldTrigger::new(core.config());
        let idle_fold_due_at_ms = newest_commit_at_ms
            .filter(|_| objects > 0)
            .and_then(|committed_at_ms| trigger.idle_fold_due_at_ms(committed_at_ms));
        if trigger.fold_is_due(max_wal_tail_objects, objects, inline_bytes)
            || idle_fold_due_at_ms.is_some_and(|due_at_ms| now_ms >= due_at_ms)
        {
            return Ok(None);
        }
        Ok(Some(NamespaceFold::NotDue {
            idle_fold_pending: objects > 0 && trigger.folds_idle_tails(),
            observed,
        }))
    }

    /// Returns the fold and compaction locks of `namespace_id`.
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
    /// Returns the position that reads in this runtime start from while its
    /// last check is within the revalidation bound. A fold from an older
    /// position would load the tail from the store anyway, because another
    /// writer may have committed since the check.
    ///
    /// The position is the engine's as of the engine's last release, so a
    /// fold from it doesn't wait for a publication that the engine is
    /// running, and that publication's commits stay in the tail above the
    /// fold's manifest.
    pub(super) fn current_position(&self) -> Option<WriterPosition> {
        self.read_position
            .current()
            .filter(|position| position.basis_checked.is_within_revalidation_bound())
            .map(|position| WriterPosition::clone(&position))
    }
}
