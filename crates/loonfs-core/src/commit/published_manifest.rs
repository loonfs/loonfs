//! [`PublishedManifest`], the newest manifest that a writer session's runtime
//! published, and [`KeptPosition`], the commit engine's position, which
//! follows that manifest each time the engine reads or takes it.

use super::publish_view::WriterPosition;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use std::sync::{Arc, Mutex, MutexGuard};

/// The newest manifest that the runtime of one writer session published for
/// the session's namespace, or found current, shared by the session's commit
/// engine and the runtime.
///
/// A fold or a maintenance call records its manifest here as soon as its put
/// and its boundary read succeed, without waiting for a publication unit
/// that has the engine. The engine's position still changes only on the
/// engine's turn: the session's runtime brings both copies of the position
/// (the engine's and the one that reads start from) up to this manifest at
/// once when no task has the engine, and at the engine's release otherwise,
/// and every read of either copy follows it too (see [`Self::follow`]). So
/// a unit that planned from an older manifest doesn't undo a publication
/// when it puts its position back, and a read never starts from a manifest
/// below the session's own newest one. A manifest at or below the recorded
/// one never replaces it.
#[derive(Debug, Clone, Default)]
pub struct PublishedManifest(Arc<Mutex<Option<Held<LoadedManifest>>>>);

/// Where [`PublishedManifest::follow`] leaves a position.
#[derive(Debug)]
pub enum FollowOutcome {
    /// The position is at or above the recorded manifest, so it stays where
    /// it is.
    Unchanged,
    /// The position moved onto the recorded manifest.
    Moved(Box<WriterPosition>),
    /// The position is below the recorded manifest and can't move onto it,
    /// so the session drops it (see `WriterPosition::move_to`). The next
    /// load starts after the manifest was recorded, so it finds that manifest
    /// or a later one.
    Dropped,
}

impl PublishedManifest {
    /// Locks the recorded manifest. While the caller has the lock, no
    /// publication records a manifest, so a caller that brings a position up
    /// to the recorded manifest and stores it under the lock can't miss a
    /// publication that's recorded meanwhile.
    pub fn lock(&self) -> PublishedLock<'_> {
        PublishedLock {
            published: self,
            // A critical section under this lock records a manifest, follows
            // positions and stores them, checks that a lock is its own
            // cell's, and unlocks the engine. It calls no code outside these
            // types, so a panic under it is a bug, and so is a poisoned lock.
            // Dropping a replaced position and handing a dropped one's tail
            // to the read cache happen after the unlock.
            recorded: self
                .0
                .lock()
                .expect("the published manifest lock should not be poisoned"),
        }
    }

    /// Records `published` (see [`PublishedLock::record`]).
    pub fn record(&self, published: &Held<LoadedManifest>) {
        self.lock().record(published);
    }

    /// Returns where `position` goes once it's brought up to the recorded
    /// manifest (see [`PublishedLock::follow`]).
    pub fn follow(&self, position: &WriterPosition) -> FollowOutcome {
        self.lock().follow(position)
    }
}

/// A locked [`PublishedManifest`], from [`PublishedManifest::lock`].
#[derive(Debug)]
pub struct PublishedLock<'a> {
    published: &'a PublishedManifest,
    recorded: MutexGuard<'a, Option<Held<LoadedManifest>>>,
}

impl PublishedLock<'_> {
    /// Returns whether this is the lock of `published`.
    pub fn is_lock_of(&self, published: &PublishedManifest) -> bool {
        Arc::ptr_eq(&self.published.0, &published.0)
    }

    /// Records `published`, a manifest that the session's runtime published
    /// or found current. A manifest above the recorded one replaces it, and
    /// the recorded one otherwise takes `published`'s check (see
    /// [`Held::merge`]).
    pub fn record(&mut self, published: &Held<LoadedManifest>) {
        match self.recorded.as_mut() {
            Some(recorded) if recorded.manifest_no() >= published.manifest_no() => {
                recorded.merge(published);
            }
            _ => *self.recorded = Some(published.clone()),
        }
    }

    /// Returns where `position` goes once it's brought up to the recorded
    /// manifest. A position below that manifest moves onto it by the rule of
    /// `WriterPosition::move_to`, or it's dropped when it can't move there.
    /// A position on that manifest takes the manifest's check when it's
    /// younger (see [`WriterPosition::merge_check`]), so a read doesn't
    /// repeat a check that a publication just made. A position above it
    /// stays where it is.
    pub fn follow(&self, position: &WriterPosition) -> FollowOutcome {
        let Some(recorded) = self.recorded.as_ref() else {
            return FollowOutcome::Unchanged;
        };
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
/// doesn't keep the part of the tail that a publication folded.
#[derive(Debug, Default)]
pub(crate) struct KeptPosition {
    position: Option<WriterPosition>,
    published: PublishedManifest,
}

impl KeptPosition {
    /// Returns the newest manifest that the session's runtime published.
    pub(crate) fn published(&self) -> &PublishedManifest {
        &self.published
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
    /// position's locked [`PublishedManifest`], records, stores it, and
    /// returns the position that it replaced, or `None` when the position
    /// stays. The caller drops the replaced position once it has unlocked
    /// `published`, because the replaced position can be the last owner of a
    /// large WAL tail.
    #[must_use = "drop the replaced position once the published manifest is unlocked"]
    pub(crate) fn follow_stored(
        &mut self,
        published: &PublishedLock<'_>,
    ) -> Option<WriterPosition> {
        assert!(
            published.is_lock_of(&self.published),
            "a position follows its own session's published manifest"
        );
        match published.follow(self.position.as_ref()?) {
            FollowOutcome::Unchanged => None,
            FollowOutcome::Moved(moved) => self.position.replace(*moved),
            FollowOutcome::Dropped => self.position.take(),
        }
    }

    /// Records `published`, a manifest that the session published without a
    /// runtime around the engine (see [`PublishedLock::record`]), and brings
    /// the stored position up to it, as the runtime's release of the engine
    /// does. Returns the position that the follow replaced, which the caller
    /// drops.
    #[must_use = "drop the replaced position once the published manifest is unlocked"]
    pub(crate) fn record_published(
        &mut self,
        published: &Held<LoadedManifest>,
    ) -> Option<WriterPosition> {
        let manifest = self.published.clone();
        let mut recorded = manifest.lock();
        recorded.record(published);
        self.follow_stored(&recorded)
    }

    /// Returns the position as it's stored, without following the recorded
    /// manifest, for a test of what the engine keeps in memory.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn stored(&self) -> Option<&WriterPosition> {
        self.position.as_ref()
    }

    /// Drops the position.
    pub(crate) fn clear(&mut self) {
        self.position = None;
    }

    /// Returns `position` brought up to the recorded manifest, or `None` when
    /// it can't move there. The published manifest is locked only for the
    /// follow, and `position` is dropped after the unlock.
    fn followed(&self, position: WriterPosition) -> Option<WriterPosition> {
        match self.published.follow(&position) {
            FollowOutcome::Unchanged => Some(position),
            FollowOutcome::Moved(moved) => Some(*moved),
            FollowOutcome::Dropped => None,
        }
    }
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
        for (recorded, expected) in [(3, 3), (2, 3), (3, 3), (4, 4)] {
            published.record(&manifest(recorded));
            assert_eq!(
                published.lock().recorded.as_ref().map(Held::manifest_no),
                Some(ManifestNo(expected)),
                "after recording manifest {recorded}"
            );
        }
    }
}
