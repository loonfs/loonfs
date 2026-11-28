//! [`PublishedManifest`], a writer session's status with the newest manifest
//! that the session published under its writer epoch, and [`KeptPosition`],
//! the commit engine's position, which follows that manifest each time the
//! engine reads or takes it.

use super::engine::WriterSessionStatus;
use super::publication_lane::PublicationLane;
use super::publish_view::WriterPosition;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::writer_epoch::AcquiredWriter;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// One writer session's status with its newest published manifest, which
/// the session's commit engine and the runtime share.
///
/// While the session has its writer epoch, the status includes the newest
/// manifest that the session published for its namespace. A claim sets the
/// session's epoch with the claim's manifest. A publication of the session's
/// lane records its own manifest here as soon as its put and its boundary
/// read succeed, before the lane takes the next publication, without waiting
/// for a publication unit that has the engine.
/// Both keep the lane while they record, and nothing else records a manifest
/// here: a load of the session's position never moves it, so it never lists
/// a manifest that isn't this session's (see [`PublicationLane`]). A put of
/// either of the session's queues whose outcome is unknown ends the epoch,
/// and the manifest goes with it (see
/// [`SessionEpoch::send_put`](super::session_epoch::SessionEpoch::send_put)).
///
/// The engine's position still changes only on the engine's turn: the
/// session's runtime brings the three copies of the position (the engine's,
/// the lane's, and the one that reads start from) up to this manifest at
/// once when no task has the engine, and at the engine's release otherwise,
/// and every read of a copy follows it too (see [`Self::follow`]). So a unit
/// that planned from an older manifest doesn't undo a publication when it
/// puts its position back, a read never starts from a manifest below the
/// session's own newest one, and a session without its epoch has no
/// position. A manifest at or below the recorded one never replaces it.
///
/// Every taker of the mutex, this type's and the runtime's through
/// [`WriterSessionStatusView`](super::engine::WriterSessionStatusView),
/// takes the status even when the mutex is poisoned. The runtime contains
/// a panic in a unit of work and keeps running, and it keeps the view's
/// guard across its own code, so a panic there poisons the mutex. A put's
/// end runs while its task unwinds, too (see
/// [`SessionEpoch`](super::session_epoch::SessionEpoch)), and a second
/// panic there would abort the process. Each change under the mutex is one
/// assignment of the status, so a poisoned mutex still has a status that
/// was set whole.
#[derive(Debug, Clone, Default)]
pub struct PublishedManifest(pub(super) Arc<Mutex<WriterSessionStatus>>);

/// Where [`PublishedManifest::follow`] leaves a position.
#[derive(Debug)]
pub enum FollowOutcome {
    /// The position is at or above the recorded manifest, so it stays where
    /// it is.
    Unchanged,
    /// The position moved onto the recorded manifest.
    Moved(Box<WriterPosition>),
    /// The session drops the position: the position is below the recorded
    /// manifest and can't move onto it (see `WriterPosition::move_to`), or
    /// the session doesn't have the writer epoch that the position was
    /// loaded under. The next load starts after the manifest was recorded,
    /// so it finds that manifest or a later one, and a session without its
    /// epoch claims before it loads.
    Dropped,
}

impl PublishedManifest {
    /// Locks the session's status. While the caller has the lock, no
    /// publication records a manifest and the epoch neither starts nor ends,
    /// so a caller that brings a position up to the recorded manifest and
    /// stores it under the lock can't miss a publication that's recorded
    /// meanwhile, or an end of the epoch.
    pub fn lock(&self) -> PublishedLock<'_> {
        PublishedLock {
            published: self,
            // Dropping a replaced position and handing a dropped one's tail
            // to the read cache happen after the unlock.
            status: self.0.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }

    /// Records `published` (see [`PublishedLock::record`]). Only an item of
    /// the session's publication lane records a manifest (see
    /// [`LaneGuard::record`](super::publication_lane::LaneGuard::record)).
    pub(crate) fn record(&self, published: &Held<LoadedManifest>) {
        self.lock().record(published);
    }

    /// Returns the recorded manifest, or `None` when the session doesn't
    /// have its writer epoch.
    pub(crate) fn recorded(&self) -> Option<Held<LoadedManifest>> {
        match &*self.lock() {
            WriterSessionStatus::Acquired { published, .. } => Some(published.clone()),
            _ => None,
        }
    }

    /// Returns where `position` goes once it's brought up to the recorded
    /// manifest (see [`PublishedLock::follow`]).
    pub fn follow(&self, position: &WriterPosition) -> FollowOutcome {
        self.lock().follow(position)
    }

    /// Settles a put of the session at its end: a put whose outcome isn't
    /// `known` ends the session's writer epoch, so the session's next
    /// publication claims (see
    /// [`SessionEpoch::send_put`](super::session_epoch::SessionEpoch::send_put)).
    /// A fenced session, or one whose namespace is gone, keeps its status.
    /// Returns whether the session still has its epoch.
    ///
    /// The status is the one that the put was sent under, or that an
    /// earlier unknown outcome left, because a claim waits until neither of
    /// the session's queues has a put in flight.
    pub(crate) fn settle_put(&self, known: bool) -> bool {
        let mut status = self.lock();
        if !known && matches!(*status, WriterSessionStatus::Acquired { .. }) {
            status.set(WriterSessionStatus::Unacquired);
        }
        matches!(*status, WriterSessionStatus::Acquired { .. })
    }
}

/// A locked [`PublishedManifest`], from [`PublishedManifest::lock`], which
/// derefs to the session's status for reading.
#[derive(Debug)]
pub struct PublishedLock<'a> {
    published: &'a PublishedManifest,
    status: MutexGuard<'a, WriterSessionStatus>,
}

impl std::ops::Deref for PublishedLock<'_> {
    type Target = WriterSessionStatus;

    fn deref(&self) -> &WriterSessionStatus {
        &self.status
    }
}

impl PublishedLock<'_> {
    /// Returns whether this is the lock of `published`.
    pub fn is_lock_of(&self, published: &PublishedManifest) -> bool {
        Arc::ptr_eq(&self.published.0, &published.0)
    }

    /// Sets the session's status to `status`.
    pub(crate) fn set(&mut self, status: WriterSessionStatus) {
        *self.status = status;
    }

    /// Starts the writer epoch `writer`, which a claim acquired, with
    /// `claimed`, the claim's manifest, as the session's newest published
    /// manifest.
    pub(crate) fn acquire(&mut self, writer: AcquiredWriter, claimed: Held<LoadedManifest>) {
        self.set(WriterSessionStatus::Acquired {
            writer,
            published: claimed,
        });
    }

    /// Records `published`, a manifest that the session's runtime published
    /// or found current, while the session has its writer epoch. A manifest
    /// above the recorded one replaces it, and the recorded one otherwise
    /// takes `published`'s check (see [`Held::merge`]). A session without
    /// its epoch records nothing, because a known outcome of one put never
    /// restores an epoch that an unknown outcome of another put ended.
    pub(crate) fn record(&mut self, published: &Held<LoadedManifest>) {
        let WriterSessionStatus::Acquired {
            published: recorded,
            ..
        } = &mut *self.status
        else {
            return;
        };
        if recorded.manifest_no() >= published.manifest_no() {
            recorded.merge(published);
        } else {
            *recorded = published.clone();
        }
    }

    /// Returns where `position` goes once it's brought up to the recorded
    /// manifest. A session without its writer epoch drops every position,
    /// and so does a session whose epoch isn't the one that `position` was
    /// loaded under. A position below the recorded manifest moves onto it by
    /// the rule of `WriterPosition::move_to`, or it's dropped when it can't
    /// move there. A position on that manifest takes the manifest's check
    /// when it's younger (see [`WriterPosition::merge_check`]), so a read
    /// doesn't repeat a check that a publication just made. A position above
    /// it stays where it is.
    pub fn follow(&self, position: &WriterPosition) -> FollowOutcome {
        let WriterSessionStatus::Acquired {
            writer,
            published: recorded,
        } = &*self.status
        else {
            return FollowOutcome::Dropped;
        };
        if position.read_basis().head.writer_epoch != writer.writer_epoch {
            return FollowOutcome::Dropped;
        }
        let recorded_no = recorded.manifest_no();
        let position_no = position.basis.manifest_no();
        if recorded_no < position_no
            || (recorded_no == position_no && !position.basis.would_merge(recorded))
        {
            return FollowOutcome::Unchanged;
        }
        let mut moved = position.clone();
        if recorded_no == position_no {
            moved.merge_check(recorded);
            FollowOutcome::Moved(Box::new(moved))
        } else if moved.move_to(recorded) {
            FollowOutcome::Moved(Box::new(moved))
        } else {
            FollowOutcome::Dropped
        }
    }
}

/// The commit engine's position, which the engine can read only brought up
/// to the session's [`PublishedManifest`].
///
/// The position is private to this module, so every read of it goes through
/// [`Self::get`] or [`Self::take`], which follow the recorded manifest.
/// Every store follows too, and so does [`Self::follow_stored`], which the
/// session's runtime runs after each publication, so the stored position
/// doesn't keep the part of the tail that a publication folded. That
/// follow, which the runtime runs at each release of the engine, also
/// leaves a copy of the stored position in the session's publication lane,
/// and each drop removes the copy. The lane's
/// publications build on the copy (see
/// [`LaneGuard::turn`](super::publication_lane::LaneGuard::turn)), so they
/// don't wait for a unit that has the engine, and a unit's own changes to
/// its position don't copy the tail that the lane's copy shares.
#[derive(Debug)]
pub(crate) struct KeptPosition {
    position: Option<WriterPosition>,
    lane: PublicationLane,
}

impl KeptPosition {
    /// Returns no position, which follows the newest manifest that `lane`,
    /// the session's publication lane, records.
    pub(crate) fn new(lane: PublicationLane) -> Self {
        Self {
            position: None,
            lane,
        }
    }

    /// Returns a copy of the position brought up to the recorded manifest,
    /// or `None` when there is no position or it can't move there.
    pub(crate) fn get(&self) -> Option<WriterPosition> {
        self.followed(self.position.clone()?)
    }

    /// Takes the position brought up to the recorded manifest, and leaves no
    /// position, or returns `None` when there is no position or it can't
    /// move there.
    pub(crate) fn take(&mut self) -> Option<WriterPosition> {
        let position = self.position.take()?;
        self.followed(position)
    }

    /// Stores `position` brought up to the recorded manifest, or drops it
    /// when it can't move there.
    ///
    /// Reads would be right without this follow, because every read follows.
    /// It's here for memory: a position put back below a fold's manifest
    /// would otherwise keep the tail that the fold covered, inline content
    /// included, until the engine's next unit.
    pub(crate) fn set(&mut self, position: WriterPosition) {
        self.position = self.followed(position);
    }

    /// Brings the stored position up to the manifest that `published`, this
    /// position's locked [`PublishedManifest`], records, stores it, leaves a
    /// copy of it in the session's publication lane, and returns the
    /// positions that it replaced. A session without its writer epoch, such
    /// as a fenced one, keeps neither (see [`PublishedLock::follow`]).
    pub(crate) fn follow_stored(&mut self, published: &PublishedLock<'_>) -> ReplacedPositions {
        assert!(
            published.is_lock_of(self.lane.published_manifest()),
            "a position follows its own session's published manifest"
        );
        let engine = match self
            .position
            .as_ref()
            .map(|position| published.follow(position))
        {
            None | Some(FollowOutcome::Unchanged) => None,
            Some(FollowOutcome::Moved(moved)) => self.position.replace(*moved),
            Some(FollowOutcome::Dropped) => self.position.take(),
        };
        ReplacedPositions {
            _engine: engine,
            _lane: self.lane.keep(self.position.clone()),
        }
    }

    /// Returns the position as it's stored, without following the recorded
    /// manifest, for a test of what the engine keeps in memory.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn stored(&self) -> Option<&WriterPosition> {
        self.position.as_ref()
    }

    /// Drops the position and the lane's copy of it.
    pub(crate) fn clear(&mut self) {
        self.position = None;
        drop(self.lane.keep(None));
    }

    /// Returns `position` brought up to the recorded manifest, or `None` when
    /// it can't move there. The published manifest is locked only for the
    /// follow, and `position` is dropped after the unlock.
    fn followed(&self, position: WriterPosition) -> Option<WriterPosition> {
        match self.lane.published_manifest().follow(&position) {
            FollowOutcome::Unchanged => Some(position),
            FollowOutcome::Moved(moved) => Some(*moved),
            FollowOutcome::Dropped => None,
        }
    }
}

/// The positions that a follow of the stored position replaced (see
/// [`KeptPosition::follow_stored`]): the engine's, and the copy in the
/// session's publication lane.
///
/// The caller drops this once it has unlocked the session's published
/// manifest, because either position can be the last owner of a large WAL
/// tail.
#[derive(Debug)]
#[must_use = "drop the replaced positions once the published manifest is unlocked"]
pub struct ReplacedPositions {
    _engine: Option<WriterPosition>,
    _lane: Option<WriterPosition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Observation;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_types::format::manifest::NamespaceManifestPayload;
    use loonfs_types::ManifestNo;

    /// Checks that a manifest at or below the recorded one leaves the
    /// recorded one in place, and that a later manifest replaces it.
    #[test]
    fn the_recorded_manifest_only_moves_forward() {
        let checked = Observation::now(Arc::new(ManualClock::new(1_000)));
        let manifest = |manifest_no| {
            crate::test_support::held_manifest(
                NamespaceManifestPayload {
                    manifest_no: ManifestNo(manifest_no),
                    ..NamespaceManifestPayload::initial(
                        loonfs_test_support::ids::namespace_id("demo"),
                        loonfs_test_support::ids::numbered_pool_id(1),
                        1_000,
                        loonfs_test_support::test_actor(),
                    )
                },
                checked.clone(),
            )
        };
        let published = PublishedManifest::default();
        published.lock().acquire(
            AcquiredWriter {
                writer_id: loonfs_test_support::ids::writer_id("writer-a"),
                writer_epoch: loonfs_types::WriterEpoch(1),
            },
            manifest(1),
        );
        for (recorded, expected) in [(3, 3), (2, 3), (3, 3), (4, 4)] {
            published.record(&manifest(recorded));
            assert_eq!(
                published.recorded().as_ref().map(Held::manifest_no),
                Some(ManifestNo(expected)),
                "after recording manifest {recorded}"
            );
        }
    }
}
