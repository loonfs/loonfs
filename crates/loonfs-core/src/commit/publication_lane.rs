//! [`PublicationLane`], the serial queue through which every manifest
//! publication of one writer session goes, the guard of the kept lane, and
//! the turn that one publication takes.

use super::publish_view::WriterPosition;
use super::published_manifest::{FollowOutcome, PublishedManifest};
use super::session_epoch::SessionEpoch;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use std::sync::{Arc, Mutex};

/// The publication lane of one writer session, a serial queue beside the
/// session's commit queue.
///
/// Every manifest publication of the session other than a claim goes
/// through the lane: a fold, a compaction unit's publication, an extension
/// entry, a retention window change, a deletion's manifests, and a
/// snapshot's record put with its verification. A publication keeps the lane
/// from the moment it takes the position that it builds on until its put and
/// the boundary read after it have returned and it has recorded its
/// manifest, so no two manifest puts of the session race for one manifest
/// number, and the next publication builds on the manifest that the one
/// before it published. A commit's WAL put never waits for the lane.
///
/// The lane keeps the session's status with the newest manifest that the
/// session published, which every copy of the session's position follows,
/// and a copy of the position that the session's commit engine keeps, which
/// the lane's publications build on (see [`LaneGuard::turn`]). Only a
/// publication of the lane and a claim record a manifest, each while it
/// keeps the lane, and a load of the position never does. A claim isn't a publication of the lane: it keeps
/// the lane and the commit queue while it runs, so it waits for the
/// publication in flight, and no publication puts while it runs. The order of
/// the locks is the namespace's fold lock, then the lane, then the commit
/// queue, and no code takes the lane while it has the commit queue.
#[derive(Debug, Clone)]
pub struct PublicationLane(Arc<LaneState>);

#[derive(Debug, Default)]
struct LaneState {
    /// A `tokio` mutex, because a publication keeps the lane across its
    /// manifest put and the boundary read after it.
    turn: tokio::sync::Mutex<()>,
    published: PublishedManifest,
    /// The position that the session's commit engine kept at its last
    /// release, or `None` after the engine dropped it or the session lost
    /// its writer epoch. A caller locks it only to store or copy the position, so it
    /// never waits for `published` with this locked.
    kept: Mutex<Option<WriterPosition>>,
}

impl PublicationLane {
    /// Returns the lane of a new writer session, which its commit engine
    /// creates.
    pub(crate) fn new() -> Self {
        Self(Arc::default())
    }

    /// Waits for the lane and keeps it until the caller drops the guard.
    pub async fn lock(&self) -> LaneGuard<'_> {
        LaneGuard {
            lane: self,
            _turn: self.0.turn.lock().await,
        }
    }

    /// Returns the session's status with the newest manifest that the
    /// session published, which every copy of the session's position
    /// follows.
    pub fn published_manifest(&self) -> &PublishedManifest {
        &self.0.published
    }

    /// Stores `position`, the position that the session's commit engine
    /// keeps at a release, or `None` when the engine dropped its position,
    /// and returns the copy that it replaced. The caller drops the replaced
    /// copy once it has unlocked the session's published manifest, because
    /// the copy can be the last owner of a large WAL tail.
    #[must_use = "drop the replaced copy once the published manifest is unlocked"]
    pub(crate) fn keep(&self, position: Option<WriterPosition>) -> Option<WriterPosition> {
        std::mem::replace(
            &mut *self
                .0
                .kept
                .lock()
                .expect("the kept position lock should not be poisoned"),
            position,
        )
    }
}

/// The publication lane, kept by one publication or claim until the caller
/// drops this guard, from [`PublicationLane::lock`].
#[derive(Debug)]
pub struct LaneGuard<'a> {
    lane: &'a PublicationLane,
    _turn: tokio::sync::MutexGuard<'a, ()>,
}

impl LaneGuard<'_> {
    /// Returns whether this guard keeps `lane`.
    pub(crate) fn is_guard_of(&self, lane: &PublicationLane) -> bool {
        Arc::ptr_eq(&self.lane.0, &lane.0)
    }

    /// Records `published`, a manifest that a publication of the lane
    /// published, as the session's newest published manifest.
    pub(crate) fn record(&self, published: &Held<LoadedManifest>) {
        self.lane.0.published.record(published);
    }

    /// Returns the epoch of the session of `namespace`, which the lane's
    /// publications send their manifest puts through.
    pub(crate) fn epoch<'a, S>(&'a self, namespace: &'a NamespaceStore<S>) -> SessionEpoch<'a> {
        SessionEpoch::new(&self.lane.0.published, namespace)
    }

    /// Returns the manifest that a publication planned from `planned`
    /// builds on: the newer of `planned` and the session's newest published
    /// manifest.
    ///
    /// A publication that plans outside the lane, such as a compaction
    /// unit's or a snapshot's, calls this once it keeps the lane, so it
    /// never puts at the number of a manifest that the session published
    /// after it planned. A manifest above the newest published one would be
    /// another writer's claim, which the put finds.
    pub fn basis_over(&self, planned: Held<LoadedManifest>) -> Held<LoadedManifest> {
        match self.lane.0.published.recorded() {
            Some(recorded) if recorded.manifest_no() > planned.manifest_no() => recorded,
            _ => planned,
        }
    }

    /// Returns the turn of a publication: the position that the session's
    /// commit engine kept at its last release, brought up to the session's
    /// newest published manifest, or `None` when the session keeps no
    /// position, so the publication loads one first (see
    /// [`NamespaceCommitEngine::lane_turn`](crate::publish::NamespaceCommitEngine::lane_turn)).
    pub fn turn(&self) -> Option<LaneTurn<'_>> {
        let kept = self
            .lane
            .0
            .kept
            .lock()
            .expect("the kept position lock should not be poisoned")
            .clone()?;
        let position = match self.lane.0.published.follow(&kept) {
            FollowOutcome::Unchanged => kept,
            FollowOutcome::Moved(moved) => *moved,
            FollowOutcome::Dropped => return None,
        };
        Some(self.turn_from(position))
    }

    /// Returns the turn of a publication that builds on `position`, a
    /// position of the session that the caller loaded while it kept the
    /// lane, such as a deletion's.
    pub(crate) fn turn_from(&self, position: WriterPosition) -> LaneTurn<'_> {
        LaneTurn {
            guard: self,
            position,
        }
    }
}

/// One publication's turn in the publication lane: the kept lane, and the
/// session's position that the publication builds on, from
/// [`LaneGuard::turn`].
///
/// Every core function that publishes a manifest of an active namespace
/// other than a claim takes a turn, so only a publication of the session's
/// lane can call it, it builds on the session's own position, and it records
/// the manifest that it published before the lane takes the next
/// publication.
#[derive(Debug)]
pub struct LaneTurn<'g> {
    guard: &'g LaneGuard<'g>,
    position: WriterPosition,
}

/// The position that the publication builds on (see [`LaneTurn::position`]).
impl AsRef<WriterPosition> for LaneTurn<'_> {
    fn as_ref(&self) -> &WriterPosition {
        &self.position
    }
}

impl LaneTurn<'_> {
    /// Returns the position that the publication builds on.
    pub fn position(&self) -> &WriterPosition {
        &self.position
    }

    /// Returns the epoch of the session of `namespace`, which the
    /// publication sends its manifest put through (see [`LaneGuard::epoch`]).
    pub(crate) fn epoch<'a, S>(&'a self, namespace: &'a NamespaceStore<S>) -> SessionEpoch<'a> {
        self.guard.epoch(namespace)
    }

    /// Records `published` (see [`LaneGuard::record`]) and ends the turn.
    pub(crate) fn record(self, published: &Held<LoadedManifest>) {
        self.guard.record(published);
    }
}
