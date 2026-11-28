//! [`PublicationLane`], the serial queue through which every manifest
//! publication of one writer session goes, the guard of the kept lane, and
//! the turn that one publication takes.

use super::published_manifest::PublishedManifest;
use super::session_epoch::SessionEpoch;
use crate::error::CoreError;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::AcquiredWriter;
use std::sync::Arc;

/// The publication lane of one writer session, a serial queue beside the
/// session's commit queue.
///
/// Every manifest publication of the session other than a claim goes
/// through the lane: a fold, a compaction unit's publication, an extension
/// entry, a retention window change, a deletion's manifests, and a
/// snapshot's record put with its verification. The heavy work of a
/// publication, such as a fold's segments or a compaction unit's output segments, is
/// written before the publication takes the lane. In its turn, the
/// publication builds on the session's newest published manifest, which
/// the session has in memory, puts once at the next manifest number, and
/// records its manifest once the put and the boundary read after it have
/// returned, so no two manifest puts of the session race for one manifest
/// number, and the next publication builds on the manifest that the one
/// before it published. A commit's WAL put never waits for the lane.
///
/// The lane keeps the session's status with the newest manifest that the
/// session published, which every copy of the session's position follows
/// (see [`PublishedManifest`]). Only a publication of the lane and a claim
/// record a manifest, each while it keeps the lane, and a load of the
/// position never does. A claim isn't a publication of the lane: it keeps
/// the lane and the commit queue while it runs, so it waits for the
/// publication in flight, and no publication puts while it runs. The order
/// of the locks is the namespace's fold lock, then the lane, then the commit
/// queue, and no code takes the lane while it has the commit queue.
#[derive(Debug, Clone)]
pub struct PublicationLane(Arc<LaneState>);

#[derive(Debug, Default)]
struct LaneState {
    /// A `tokio` mutex, because a publication keeps the lane across its
    /// manifest put and the boundary read after it.
    turn: tokio::sync::Mutex<()>,
    published: PublishedManifest,
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

    /// Returns the manifest that a snapshot planned from `planned` writes
    /// its record on: the newer of `planned` and the session's newest
    /// published manifest.
    ///
    /// A snapshot plans outside the lane and writes a side record, not a
    /// manifest, so it calls this once it keeps the lane, and a publication
    /// of the session that followed its plan doesn't refuse its
    /// verification. A manifest that a discovery found above the newest
    /// published one is another writer's, which the snapshot can write its
    /// record on as any process can.
    pub fn basis_over(&self, planned: Held<LoadedManifest>) -> Held<LoadedManifest> {
        match self.lane.0.published.recorded() {
            Some(recorded) if recorded.manifest_no() > planned.manifest_no() => recorded,
            _ => planned,
        }
    }

    /// Returns the turn of a publication, which builds on the session's
    /// newest published manifest, or `None` when the session doesn't have
    /// its writer epoch, so the publication claims first (see
    /// [`NamespaceCommitEngine::lane_turn`](crate::publish::NamespaceCommitEngine::lane_turn)).
    pub fn turn(&self) -> Option<LaneTurn<'_>> {
        let status = self.lane.0.published.lock();
        let (writer, basis) = status.acquired()?;
        Some(LaneTurn {
            guard: self,
            writer: writer.clone(),
            basis: basis.clone(),
        })
    }

    /// Returns the turn of a publication whose caller keeps the lane and
    /// the session's commit engine, and loaded the session's position under
    /// its writer epoch, such as a deletion's.
    ///
    /// # Panics
    ///
    /// Panics if the session doesn't have its writer epoch. While the caller
    /// keeps both queues, no put of the session is in flight, so the epoch
    /// that the load ran under is still the session's.
    pub(crate) fn acquired_turn(&self) -> LaneTurn<'_> {
        self.turn()
            .expect("a session that keeps both queues keeps the epoch that it loaded under")
    }
}

/// One publication's turn in the publication lane, from [`LaneGuard::turn`]:
/// the kept lane, the session's writer epoch, and the session's newest
/// published manifest, which the publication builds on.
///
/// Every core function that publishes a manifest of an active namespace
/// other than a claim takes a turn, so only a publication of the session's
/// lane can call it, it puts once at the number after the session's newest
/// published manifest, and it records the manifest that it published before
/// the lane takes the next publication (see
/// [`NamespaceStore::publish_manifest_in_turn`]).
#[derive(Debug)]
pub struct LaneTurn<'g> {
    guard: &'g LaneGuard<'g>,
    writer: AcquiredWriter,
    basis: Held<LoadedManifest>,
}

impl LaneTurn<'_> {
    /// Returns the session's newest published manifest, which the
    /// publication builds on.
    pub fn basis(&self) -> &Held<LoadedManifest> {
        &self.basis
    }

    /// Returns the session's writer epoch.
    pub(crate) fn writer(&self) -> &AcquiredWriter {
        &self.writer
    }

    /// Returns the epoch of the session of `namespace`, which the
    /// publication sends its manifest put through.
    pub(crate) fn epoch<'a, S>(&'a self, namespace: &'a NamespaceStore<S>) -> SessionEpoch<'a> {
        SessionEpoch::new(&self.guard.lane.0.published, namespace)
    }

    /// Records `published`, the manifest that the publication published, as
    /// the session's newest published manifest, and ends the turn.
    pub(crate) fn record(self, published: &Held<LoadedManifest>) {
        self.guard.lane.0.published.record(published);
    }

    /// Records what `error`, the error of the publication's put, says about
    /// the session, by the rule that the commit queue records by (see
    /// [`PublishedManifest::record_session_error`]), and ends the turn. A
    /// fence then stops both queues of the session.
    pub(crate) fn record_error(self, error: &CoreError) {
        self.guard.lane.0.published.record_session_error(error);
    }
}
