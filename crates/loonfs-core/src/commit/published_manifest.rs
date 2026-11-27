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
/// engine's turn, and each copy of the position (the engine's and the one
/// that reads start from) is brought up to this manifest when it's next used
/// (see [`Self::follow`]). So a unit that planned from an older manifest
/// doesn't undo a publication when it puts its position back, and a read
/// never starts from a manifest below the session's own newest one. A
/// manifest at or below the recorded one never replaces it.
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
    fn lock(&self) -> MutexGuard<'_, Option<Held<LoadedManifest>>> {
        // A critical section under this lock compares manifest numbers and
        // clones or merges a holder, so it can't panic and poison the lock.
        self.0
            .lock()
            .expect("the published manifest lock should not be poisoned")
    }

    /// Records `published`, a manifest that the session's runtime published
    /// or found current. A manifest above the recorded one replaces it, and
    /// the recorded one otherwise takes `published`'s check (see
    /// [`Held::merge`]).
    pub fn record(&self, published: &Held<LoadedManifest>) {
        let mut recorded = self.lock();
        match recorded.as_mut() {
            Some(recorded) if recorded.manifest_no() >= published.manifest_no() => {
                recorded.merge(published);
            }
            _ => *recorded = Some(published.clone()),
        }
    }

    /// Returns where `position` goes once it's brought up to the recorded
    /// manifest: it stays when it's at or above that manifest, and otherwise
    /// it moves onto the manifest by the rule of `WriterPosition::move_to`,
    /// or it's dropped when it can't move there.
    pub fn follow(&self, position: &WriterPosition) -> FollowOutcome {
        let published = match self.lock().as_ref() {
            Some(published) if published.manifest_no() > position.basis.manifest_no() => {
                published.clone()
            }
            _ => return FollowOutcome::Unchanged,
        };
        let mut moved = position.clone();
        if moved.move_to(&published) {
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
/// [`Self::get`] or [`Self::take`], which follow the recorded manifest. A
/// store needs no follow, because every later read follows.
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

    /// Stores `position`.
    pub(crate) fn set(&mut self, position: WriterPosition) {
        self.position = Some(position);
    }

    /// Drops the position.
    pub(crate) fn clear(&mut self) {
        self.position = None;
    }

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
                published.lock().as_ref().map(Held::manifest_no),
                Some(ManifestNo(expected)),
                "after recording manifest {recorded}"
            );
        }
    }
}
