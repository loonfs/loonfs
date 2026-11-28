//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use super::RuntimeCore;
use crate::trace::phase_span;
use crate::NamespaceId;
use futures::future::BoxFuture;
use loonfs_core::cache::{CachedHead, HeadAndWalTailCache, SharedCheck, WalTailCacheKey};
use loonfs_core::control::{Held, HeldManifestForm, LoadedManifest};
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::publish::{
    FollowOutcome, PositionCheck, PublishedLock, PublishedManifest, WriterPosition,
};
use loonfs_core::CoreError;
use loonfs_core::Phase;
use loonfs_types::ManifestNo;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::Instrument;

/// The writer sessions of a writable runtime, as its reads see them.
///
/// The publisher registry implements this trait. The runtime core keeps a
/// weak reference to the registry, because the registry keeps the core.
/// Reads take a session's position and check it. A read never changes the
/// position itself, because only the session's queues do, and a read whose
/// check finds a newer manifest asks the session to settle its position.
pub(crate) trait HeadPositions: Send + Sync {
    /// Returns the position slot of the namespace's writer session, or
    /// `None` when the runtime has no session for the namespace.
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>>;

    /// Settles the position of the namespace's writer session after a read's
    /// check found its manifest, manifest `replaced`, replaced (see
    /// `NamespacePublisher::settle_replaced_position`). A runtime without a
    /// session for the namespace does nothing.
    fn settle_replaced_position<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
        replaced: ManifestNo,
    ) -> BoxFuture<'a, Result<(), CoreError>>;
}

/// What the session's check of its position found for a read (see
/// [`RuntimeCore::checked_writer_position`]).
#[derive(Debug)]
pub(crate) enum CheckedPosition {
    /// The runtime has no writer session for the namespace, or the session
    /// has no position, so the read starts from the read cache as a read in
    /// a runtime without the session does.
    Absent,
    /// The read starts from this position, which a check confirmed within
    /// the read's maximum staleness.
    Confirmed(Arc<WriterPosition>),
    /// The session has a position that its check didn't confirm, after the
    /// session settled it on each check. The read loads the namespace's head from the
    /// store as a `strong` read does, because the read cache's head can be
    /// older than commits that the session acknowledged.
    Unconfirmed,
}

/// The position of one writer session's commit engine, kept where reads can
/// take it without locking the engine.
///
/// The publisher stores the engine's position here each time a task
/// releases the engine, so a read after a commit's acknowledgement sees the
/// commit. A manifest that this runtime publishes moves the position that
/// the slot returns at once, without waiting for the engine, because the
/// slot brings the position up to the session's newest published manifest
/// each time it returns the position (see [`PublishedManifest::follow`]).
#[derive(Debug)]
pub(crate) struct ReadPositionSlot {
    /// The runtime's read cache, where [`Self::clear`] installs the position
    /// as the namespace's cached head, with the position's tail.
    head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
    /// The newest manifest that the session's runtime published.
    published: PublishedManifest,
    /// The position as the engine's last release stored it, or as the slot
    /// last brought it up to `published`.
    position: Mutex<Option<Arc<WriterPosition>>>,
    /// The session's checks of the position, which the reads that need a
    /// check share, and whether each confirmed the position.
    check: SharedCheck<Result<bool, CoreError>>,
}

/// What [`ReadPositionSlot::follow_stored`] replaced in the slot.
#[derive(Debug)]
pub(crate) enum FollowedSlot {
    /// A position that moved onto the published manifest replaced this one.
    Replaced(Arc<WriterPosition>),
    /// The slot removed this position, which can't move onto the published
    /// manifest.
    Removed(Arc<WriterPosition>),
}

impl ReadPositionSlot {
    /// Returns an empty slot for a writer session in the runtime whose read
    /// cache is `head_and_wal_tail_cache`, and whose newest published
    /// manifest is `published`.
    pub(crate) fn new(
        head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
        published: PublishedManifest,
    ) -> Self {
        Self {
            head_and_wal_tail_cache,
            published,
            position: Mutex::default(),
            check: SharedCheck::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<WriterPosition>>> {
        // A critical section under this lock moves an `Arc`, or follows the
        // published manifest and stores the result, and calls no code outside
        // the position types, so it can't panic and poison the lock.
        self.position
            .lock()
            .expect("the read position lock should not be poisoned")
    }

    /// Returns the newest manifest that the session's runtime published,
    /// which the slot's position follows.
    pub(crate) fn published(&self) -> &PublishedManifest {
        &self.published
    }

    /// Brings the stored position up to the manifest that `published`, the
    /// slot's locked [`PublishedManifest`], records, and stores it, or
    /// removes it when it can't move there, and returns the position that
    /// it replaced.
    ///
    /// The slot stays locked from the read of the stored position to the
    /// store, so a read's check that replaces the position meanwhile (see
    /// [`Self::record_check`]) either comes first, and its position is the
    /// one that follows, or comes after, and finds the position replaced.
    /// The caller passes the result to [`Self::finish_follow`] once it has
    /// unlocked `published`, because dropping the replaced position can free
    /// a large WAL tail, and a removed position's tail goes into the read
    /// cache.
    #[must_use = "finish the follow once the published manifest is unlocked"]
    pub(crate) fn follow_stored(&self, published: &PublishedLock<'_>) -> Option<FollowedSlot> {
        assert!(
            published.is_lock_of(&self.published),
            "a slot follows its own session's published manifest"
        );
        let mut slot = self.lock();
        let stored = slot.as_ref()?;
        match published.follow(stored) {
            FollowOutcome::Unchanged => None,
            FollowOutcome::Moved(moved) => {
                slot.replace(Arc::new(*moved)).map(FollowedSlot::Replaced)
            }
            FollowOutcome::Dropped => slot.take().map(FollowedSlot::Removed),
        }
    }

    /// Finishes [`Self::follow_stored`] once the published manifest is
    /// unlocked: drops the replaced position, or hands a removed position
    /// over as [`Self::current`] does.
    pub(crate) fn finish_follow(&self, followed: Option<FollowedSlot>) {
        match followed {
            Some(FollowedSlot::Replaced(replaced)) => drop(replaced),
            Some(FollowedSlot::Removed(removed)) => self.hand_over_removed(&removed),
            None => {}
        }
    }

    /// Returns the position that reads start from, brought up to the
    /// session's newest published manifest, or `None` when reads take the
    /// read cache.
    ///
    /// A position that can't move onto that manifest is removed, and the
    /// namespace's cached head is dropped, as the engine's release does when
    /// it finds no position to store: the position's tail goes into the read
    /// cache, and the next read loads the namespace by discovery. The
    /// engine's own position stays until the engine's release, because every
    /// change to it waits for the engine's turn, and a unit can keep the
    /// engine across its WAL put. That position can't move there either, so
    /// the release drops it (see `NamespacePublisher::follow_published`).
    pub(crate) fn current(&self) -> Option<Arc<WriterPosition>> {
        let stored = self.lock().clone()?;
        self.apply(&stored, self.published.follow(&stored))
    }

    /// Applies `outcome`, where `stored` goes once it follows the session's
    /// newest published manifest, and returns the position that reads start
    /// from.
    fn apply(
        &self,
        stored: &Arc<WriterPosition>,
        outcome: FollowOutcome,
    ) -> Option<Arc<WriterPosition>> {
        match outcome {
            FollowOutcome::Unchanged => Some(Arc::clone(stored)),
            FollowOutcome::Moved(moved) => {
                let moved = Arc::new(*moved);
                self.replace(stored, Some(Arc::clone(&moved)));
                Some(moved)
            }
            FollowOutcome::Dropped => {
                if self.replace(stored, None) {
                    self.hand_over_removed(stored);
                }
                None
            }
        }
    }

    /// Puts the tail of `removed`, a position that can't follow the
    /// session's newest published manifest, into the read cache, and drops
    /// the namespace's cached head, as the engine's release does when it
    /// finds no position to store.
    fn hand_over_removed(&self, removed: &WriterPosition) {
        self.insert_tail(removed);
        self.head_and_wal_tail_cache
            .invalidate_head(&removed.read_basis().head().namespace_id);
    }

    /// Replaces `current` with `updated`, and returns whether it did, unless
    /// the slot has stored or cleared a position since it returned `current`.
    fn replace(&self, current: &Arc<WriterPosition>, updated: Option<Arc<WriterPosition>>) -> bool {
        let mut slot = self.lock();
        let replaced = slot
            .as_ref()
            .is_some_and(|position| Arc::ptr_eq(position, current));
        if replaced {
            *slot = updated;
        }
        replaced
    }

    /// Inserts `position`'s WAL tail into the read cache, under the key of
    /// the position's manifest and head.
    fn insert_tail(&self, position: &WriterPosition) {
        let read_basis = position.read_basis();
        self.head_and_wal_tail_cache.insert_tail(
            WalTailCacheKey {
                namespace_id: read_basis.head().namespace_id.clone(),
                manifest_no: read_basis.basis().manifest_no(),
                head_seq: read_basis.head().seq,
                wal_no: read_basis.head().wal_no,
            },
            Arc::clone(position.tail()),
        );
    }

    /// Returns the position as the slot stores it, without following the
    /// session's newest published manifest, for a test of what the slot
    /// keeps in memory.
    #[cfg(test)]
    pub(crate) fn stored(&self) -> Option<Arc<WriterPosition>> {
        self.lock().clone()
    }

    /// Returns the engine's position as the slot will store it.
    ///
    /// A read's check of the position that the slot has now stays when the
    /// engine's own check is older (see [`Held::merge`]).
    pub(crate) fn prepare(&self, position: Option<WriterPosition>) -> Option<Arc<WriterPosition>> {
        let mut position = position?;
        if let Some(current) = self.current() {
            position.merge_check(current.basis());
        }
        Some(Arc::new(position))
    }

    /// Stores a position that [`Self::prepare`] returned. Only
    /// [`Self::clear`] removes a position.
    pub(crate) fn store(&self, position: Arc<WriterPosition>) {
        *self.lock() = Some(position);
    }

    /// Removes the position, so reads take the namespace's cached head until
    /// the engine's next release stores one again, and returns whether the
    /// slot had a position.
    ///
    /// The position becomes the namespace's cached head first, through
    /// [`HeadAndWalTailCache::replace_head`](loonfs_core::cache::HeadAndWalTailCache::replace_head),
    /// with both checks at the position's last successor check, because
    /// that check confirmed a head that this runtime wrote itself. So the
    /// first read after the position starts from a head at least as new as
    /// the reads from the position returned, whatever older load finishes
    /// after it.
    ///
    /// The position's WAL tail goes into the read cache before the head,
    /// under the key of the position's manifest and head, so the next read
    /// from that head takes the tail without replaying the WAL. A tail under
    /// its key is never wrong (see
    /// [`RuntimeCore::invalidate_namespace_read_cache`]), so the slot hands
    /// the tail over at every end of a position. After another writer's
    /// claim, the caller then drops the cached head, and the tail stays
    /// unused unless a later load finds the same head. A position that can't
    /// move onto the session's newest published manifest never becomes the
    /// cached head, because [`Self::current`] removes it first.
    pub(crate) fn clear(&self) -> bool {
        let Some(position) = self.current() else {
            return false;
        };
        self.insert_tail(&position);
        self.head_and_wal_tail_cache.replace_head(CachedHead::new(
            position.basis().clone(),
            position.basis().checked().clone(),
        ));
        *self.lock() = None;
        true
    }

    /// Records `check`, a check that found its manifest the newest, on the
    /// position that the slot has now (see [`Held::merge`]).
    fn record_check<M: HeldManifestForm>(&self, check: &Held<M>) {
        let Some(current) = self.current() else {
            return;
        };
        let mut updated = WriterPosition::clone(&current);
        if !updated.merge_check(check) {
            return;
        }
        self.replace(&current, Some(Arc::new(updated)));
    }
}

impl RuntimeCore {
    /// Returns the position of the namespace's writer session in this
    /// runtime for a read with `max_staleness_ms` to start from, or how the
    /// read starts from the read cache instead.
    ///
    /// A read takes the position as it is while the position's last check is
    /// younger than `max_staleness_ms`. A read without one (a
    /// `strong` read), or past it, needs the session's check of the position
    /// (see [`Self::check_writer_position`]). It waits for the check in
    /// flight, and then takes the position if that check confirmed it within
    /// the maximum staleness, or else shares the next check or runs it
    /// (see [`SharedCheck`]). So a wave of concurrent reads costs at most two
    /// checks.
    pub(crate) async fn checked_writer_position(
        &self,
        namespace_id: &NamespaceId,
        max_staleness_ms: Option<u64>,
    ) -> Result<CheckedPosition, CoreError> {
        let Some(positions) = self
            .inner
            .head_positions
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            return Ok(CheckedPosition::Absent);
        };
        let Some(slot) = positions.read_position_slot(namespace_id) else {
            return Ok(CheckedPosition::Absent);
        };
        let Some(position) = slot.current() else {
            return Ok(CheckedPosition::Absent);
        };
        let fresh = |position: &WriterPosition| {
            max_staleness_ms.is_some_and(|max_staleness_ms| {
                position.basis().checked().age_ms() < max_staleness_ms
            })
        };
        if fresh(&position) {
            return Ok(CheckedPosition::Confirmed(position));
        }
        let confirmed = slot
            .check
            .run(
                || {
                    slot.current()
                        .filter(|position| fresh(position))
                        .map(|_| Ok(true))
                },
                || {
                    self.check_writer_position(namespace_id, positions.as_ref(), &slot)
                        .instrument(phase_span!(
                            self,
                            Phase::PositionManifestProbe,
                            namespace_id
                        ))
                },
            )
            .await?;
        Ok(match slot.current() {
            Some(position) if confirmed => CheckedPosition::Confirmed(position),
            Some(_) => CheckedPosition::Unconfirmed,
            None => CheckedPosition::Absent,
        })
    }

    /// Records `check`, a check that found its manifest the newest, on the
    /// position of the namespace's writer session (see
    /// [`ReadPositionSlot::record_check`]), and returns the position when a
    /// check that began at or after `check`'s confirms it.
    ///
    /// A check by another call, such as a snapshot's verification after its
    /// record write, then serves a `strong` read that started before
    /// `check`, as the session's own check does. A runtime without the
    /// namespace's writer session, a position on an older manifest, and a
    /// position that the check doesn't confirm return `None`.
    pub(crate) fn record_writer_position_check(
        &self,
        namespace_id: &NamespaceId,
        check: &Held<LoadedManifest>,
    ) -> Option<Arc<WriterPosition>> {
        let slot = self
            .inner
            .head_positions
            .get()
            .and_then(std::sync::Weak::upgrade)?
            .read_position_slot(namespace_id)?;
        slot.record_check(check);
        slot.current()
            .filter(|position| position.basis().is_confirmed_by(check))
    }

    /// Checks the position (see [`WriterPosition::check`]), records a check
    /// that found the position's manifest the newest, and returns whether a
    /// check did.
    ///
    /// Each check that finds a newer manifest asks the session to settle its
    /// position (see [`HeadPositions::settle_replaced_position`]), and then
    /// checks the position that the session has once more. The newer manifest
    /// is the session's own publication between its put and its record, so
    /// the position follows it once the publication records it and the next
    /// check confirms it, or another writer's claim, so the settle's writer's
    /// load fences the session, which then has no position. When the
    /// position followed the session's own manifest and the next check finds
    /// that manifest replaced too, the next settle finds that the position
    /// follows nothing newer, so its load finds the other writer's claim. The
    /// check runs at most `MAX_CONTENTION_ATTEMPTS` times, and returns
    /// `false` when the last one finds a newer manifest. A settle that finds
    /// the session fenced returns `false` too, so the read starts from the
    /// read cache.
    async fn check_writer_position(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
    ) -> Result<bool, CoreError> {
        let namespace = self.namespace_store(namespace_id);
        for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
            let Some(position) = slot.current() else {
                return Ok(false);
            };
            match position.check(&namespace, self.observation()).await? {
                PositionCheck::Newest(checked) => {
                    slot.record_check(&checked);
                    return Ok(true);
                }
                PositionCheck::Replaced => {
                    let replaced = position.basis().manifest_no();
                    match positions
                        .settle_replaced_position(namespace_id, replaced)
                        .await
                    {
                        Ok(()) => {}
                        Err(CoreError::WriterFenced(_)) => return Ok(false),
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(false)
    }
}
