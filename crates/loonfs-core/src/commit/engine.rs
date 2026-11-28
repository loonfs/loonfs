//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one numbered WAL put, then returns
//! one result per candidate.

use super::batch::{PublishBatchReport, PublishViewEffect};
use super::publication_lane::{LaneGuard, LaneTurn, PublicationLane};
use super::publish_view::{ViewStart, WriterPosition};
use super::published_manifest::{
    KeptPosition, PublishedLock, PublishedManifest, ReplacedPositions,
};
use super::session_epoch::SessionEpoch;
use crate::engine::{Administrator, NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::publish::ManifestPublicationOutcome;
use crate::manifest::FloorObjectReport;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::held::Held;
use crate::namespace::read_anchor::WriterAnchor;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::time::{Observation, OperationClock};
use crate::wal::{prepare_wal_object, publish_wal_object, WalPublishError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::options::DeleteNamespaceOptions;
use loonfs_types::{Commit, CommitSeq, DeleteNamespaceResponse, NamespaceId, WriterId};
use std::sync::{Arc, Mutex, MutexGuard};

mod candidate;

pub use self::candidate::{CommitCandidate, ContentPreparationError};
pub(crate) use self::candidate::{ContentPreparation, MergeIntent, TemporaryMove};

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishOutcome {
    pub results: Vec<Result<Commit>>,
    /// The WAL tail this attempt loaded, for opportunistic maintenance
    /// scheduling, or `None` when it loaded no projection.
    pub wal_tail: Option<WalTailReport>,
}

/// The WAL tail one publish attempt loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalTailReport {
    /// Unfolded WAL objects.
    pub objects: u64,
    /// Stored bytes of the unfolded WAL objects with commits that the
    /// position records, leaving out a put whose outcome is unknown.
    pub wal_bytes: usize,
    /// Inline content bytes of the unfolded WAL objects that the position
    /// records, leaving out a put whose outcome is unknown.
    pub inline_bytes: usize,
    /// The earliest `committed_at_ms` of the commits that the position
    /// records, or `None` when the tail has no commit, such as a tail of
    /// fences alone.
    pub oldest_commit_at_ms: Option<u64>,
    /// Whether a floor object in the tail raised the view's floor above the
    /// manifest's (see
    /// [`NamespaceReadState::raises_retention_floor`](crate::control::NamespaceReadState::raises_retention_floor)).
    pub raises_retention_floor: bool,
    /// Where the attempt found the tail.
    pub source: TailSource,
}

/// Where a publish attempt found the WAL tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailSource {
    /// The engine's position, which kept the tail from an earlier
    /// publication unit.
    Cached,
    /// The store.
    Store,
}

/// Tracks writer state for one namespace session: unacquired, acquired,
/// permanently fenced, or without a namespace that the store still has.
///
/// Object storage can't reconstruct whether the current session was fenced,
/// so a runtime keeps the session's engine for as long as the session
/// stays open. Each engine creates its own status, which the session's
/// [`PublishedManifest`] keeps, and a runtime reads the status through a
/// [`WriterSessionStatusView`].
#[derive(Debug, Default)]
pub enum WriterSessionStatus {
    /// No epoch: the session's next publish acquires one with a claim and a
    /// fence. A new session starts here, and so does a session after a put
    /// of either of its queues whose outcome is unknown (format spec,
    /// "Failed and unknown outcomes").
    #[default]
    Unacquired,
    /// Writer epoch acquired by a claim and a fence, and reused for the
    /// session's later publishes until a put of the session has an unknown
    /// outcome.
    Acquired {
        writer: AcquiredWriter,
        /// The newest manifest that the session published under the epoch,
        /// the claim's or a later one, which every copy of the session's
        /// position follows (see [`PublishedManifest::follow`]).
        published: Held<LoadedManifest>,
    },
    /// Permanent fencing record for this session. Later publishes return
    /// `writer_fenced` without accessing the store. Acquiring a new writer epoch
    /// requires an explicit caller action.
    Fenced(WriterFence),
    /// The session's last read of its namespace for a publish found the
    /// namespace deleted or missing. A runtime that keeps sessions open reads
    /// this record to stop keeping the session. The record changes no later
    /// publish: each one reads the store again, and acquires a writer epoch
    /// if the namespace exists.
    NamespaceGone,
}

impl WriterSessionStatus {
    /// Returns the session's writer epoch and its newest published manifest,
    /// or `None` when the session doesn't have its epoch.
    pub(crate) fn acquired(&self) -> Option<(&AcquiredWriter, &Held<LoadedManifest>)> {
        match self {
            Self::Acquired { writer, published } => Some((writer, published)),
            _ => None,
        }
    }
}

/// A read-only view of one engine's [`WriterSessionStatus`], from
/// [`NamespaceCommitEngine::session_status`].
///
/// A runtime reads the status through this view while the engine is busy
/// with a unit, such as to refuse new work for a fenced session. The view
/// can't change the status or attach it to another engine, so the status
/// changes only through its engine.
#[derive(Debug, Clone)]
pub struct WriterSessionStatusView(Arc<Mutex<WriterSessionStatus>>);

impl WriterSessionStatusView {
    /// Locks the status for reading. The engine can't change the status
    /// until the caller drops the guard.
    pub fn lock(&self) -> WriterSessionStatusGuard<'_> {
        // A poisoned lock still returns the status (see `PublishedManifest`).
        WriterSessionStatusGuard(
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// A locked [`WriterSessionStatusView`], which derefs to the status for
/// reading.
#[derive(Debug)]
pub struct WriterSessionStatusGuard<'a>(MutexGuard<'a, WriterSessionStatus>);

impl std::ops::Deref for WriterSessionStatusGuard<'_> {
    type Target = WriterSessionStatus;

    fn deref(&self) -> &WriterSessionStatus {
        &self.0
    }
}

/// A writer session for one namespace: it publishes batches of candidates
/// through numbered WAL puts, and keeps the position each batch leaves.
#[derive(Debug)]
pub struct NamespaceCommitEngine<S> {
    /// The namespace's components, which never change. A publication unit's
    /// view borrows the store and the segment cache from here while the unit
    /// moves `position`.
    namespace: NamespaceStore<S>,
    /// The writer that the session acquires its epoch for.
    writer_id: WriterId,
    /// The session's publication lane, through which every manifest
    /// publication of the session goes, with the newest manifest that the
    /// session published, which `position` follows.
    lane: PublicationLane,
    position: PositionState,
}

/// The position that each publication unit moves, with the writer session
/// that the units publish under.
#[derive(Debug)]
struct PositionState {
    /// The position as the last unit, drop or move left it, which the
    /// engine reads only brought up to the newest manifest that the
    /// session's runtime published.
    current: KeptPosition,
    /// This session's epoch and fencing for the namespace, with the newest
    /// manifest that it published (see [`WriterSessionStatus`]), which the
    /// session's publication lane shares. The engine creates the status,
    /// and no other engine can reach it.
    session: PublishedManifest,
}

impl<S: ObjectStore> NamespaceCommitEngine<S> {
    /// Returns a commit engine over `namespace` that commits as `writer_id`,
    /// with a new writer session. Building the engine sends no request to
    /// the store, and the session acquires its writer epoch on its first
    /// publish.
    pub fn new(namespace: NamespaceStore<S>, writer_id: WriterId) -> Self {
        let lane = PublicationLane::new();
        Self {
            namespace,
            writer_id,
            position: PositionState {
                current: KeptPosition::new(lane.clone()),
                session: lane.published_manifest().clone(),
            },
            lane,
        }
    }

    /// Returns a commit engine whose session already has the epoch
    /// `acquired`, with `published` as its newest published manifest, for a
    /// test that acquires the writer epoch through the store before the
    /// engine's first publish.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_acquired_session(
        namespace: NamespaceStore<S>,
        writer_id: WriterId,
        acquired: AcquiredWriter,
        published: Held<LoadedManifest>,
    ) -> Self {
        let engine = Self::new(namespace, writer_id);
        engine.position.lock_session().acquire(acquired, published);
        engine
    }

    /// Returns a read-only view of this engine's writer session status.
    pub fn session_status(&self) -> WriterSessionStatusView {
        WriterSessionStatusView(Arc::clone(&self.position.session.0))
    }

    /// Returns the session's publication lane, through which the session's
    /// runtime publishes every manifest of the session (see
    /// [`PublicationLane`]).
    pub fn publication_lane(&self) -> PublicationLane {
        self.lane.clone()
    }

    /// Returns the session's status with the newest manifest that the
    /// session published, which each item of the session's publication lane
    /// records without locking the engine. The engine brings its position up
    /// to it each time it reads the position (see
    /// [`PublishedManifest::follow`]).
    pub fn published_manifest(&self) -> PublishedManifest {
        self.lane.published_manifest().clone()
    }

    /// Brings the position up to the manifest that `published`, this
    /// engine's locked [`Self::published_manifest`], records, and stores it,
    /// so it doesn't keep the part of the tail that a publication folded. A
    /// session without its writer epoch keeps no position. The session's
    /// runtime calls this after each publication, and when it releases the
    /// engine. Returns the position that the call replaced, which the caller
    /// drops once it has unlocked `published`.
    pub fn follow_published(&mut self, published: &PublishedLock<'_>) -> ReplacedPositions {
        self.position.current.follow_stored(published)
    }

    /// Drops the position and its tail, so the next unit reads the tail from
    /// the store. Writer epoch and fencing remain in session state and are
    /// not reset.
    pub fn invalidate_projection(&mut self) {
        self.position.invalidate_projection();
    }

    /// Unfolded WAL objects as of the last publication unit, or `None` when
    /// the engine has no position.
    pub fn wal_tail_objects(&self) -> Option<u64> {
        self.writer_position()
            .as_ref()
            .map(WriterPosition::wal_tail_objects)
    }

    /// Inline content bytes in the unfolded WAL tail as of the last
    /// publication unit, or `None` when the engine has no position.
    pub fn wal_tail_inline_bytes(&self) -> Option<usize> {
        self.writer_position()
            .as_ref()
            .map(WriterPosition::wal_tail_inline_bytes)
    }

    /// Unfolded WAL bytes as of the last publication unit, or `None` when
    /// the engine has no position.
    pub fn wal_tail_bytes(&self) -> Option<usize> {
        self.writer_position()
            .as_ref()
            .map(WriterPosition::wal_tail_bytes)
    }

    /// Returns the engine's position, brought up to the newest manifest that
    /// the session's runtime published, which reads in this runtime start
    /// from, or `None` when the engine has no position or the session
    /// doesn't have its writer epoch.
    ///
    /// A fenced session publishes nothing more, so its position would stay
    /// on a head that the new writer has moved past, and after a put whose
    /// outcome is unknown the position could be one manifest or one WAL
    /// object behind a written put.
    pub fn writer_position(&self) -> Option<WriterPosition> {
        self.position.current.get()
    }

    /// Returns the engine's position as it's stored, without bringing it up
    /// to the session's newest published manifest, for a test of what the
    /// engine keeps in memory.
    #[cfg(any(test, feature = "test-support"))]
    pub fn stored_position(&self) -> Option<WriterPosition> {
        self.position.current.stored().cloned()
    }

    /// Moves the position onto `manifest`, a manifest above it such as one
    /// that a read's check found, or drops the position when it can't move
    /// there (see `WriterPosition::move_to` for the rule).
    ///
    /// Later commits then plan against the new manifest, and reads in this
    /// runtime start from it. The position keeps the WAL tail above the new
    /// manifest, and it merges `manifest`'s check (see [`Held::merge`]). The
    /// session's runtime records the manifests that it publishes itself in
    /// [`Self::published_manifest`] instead, which doesn't wait for the
    /// engine.
    pub fn move_position_onto(&mut self, manifest: Held<LoadedManifest>) {
        let state = &mut self.position;
        let Some(mut position) = state.current.take() else {
            return;
        };
        if position.move_to(&manifest) {
            state.current.set(position);
        } else {
            state.invalidate_projection();
        }
    }

    /// Deletes the namespace using this writer session (format spec,
    /// "Deleting a namespace").
    ///
    /// Deletion publishes a new manifest with the same writer-session checks as
    /// [`Self::publish_batch`]. Fenced sessions fail before accessing the store,
    /// and a put that another writer's claim won fences the session. A
    /// session without its writer epoch claims it first, and the deletion
    /// then starts from the claim's discovery. A deletion first folds the WAL
    /// tail under the namespace's run policy, and then publishes the
    /// tombstone over the fold's manifest (see
    /// [`NamespaceStore::delete_namespace`]).
    ///
    /// An initializing namespace has no writer session, so its deletion is a
    /// plain manifest publication of its tombstone (see
    /// [`NamespaceStore::tombstone_initializing_namespace`]). When manifest 2
    /// with the status `active` wins that publication, the call deletes the
    /// namespace with a writer session.
    ///
    /// The caller keeps `lane`, the session's publication lane, and the
    /// engine for the whole deletion, so both of the deletion's manifests
    /// are publications of the lane, and no commit is written between the
    /// fold and the tombstone.
    ///
    /// `administrator` proves the caller administers the namespace, because
    /// deletion is only for the administrator.
    pub async fn delete_namespace(
        &mut self,
        lane: &LaneGuard<'_>,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        administrator.assert_for(&self.namespace.namespace_id);
        let clock = self.namespace.start_clock();
        let position = match self.load_position_in_lane(lane).await {
            Ok(position) => position,
            Err(error @ CoreError::NamespaceNotFound { .. }) => {
                let NewestManifest::Manifest(current) =
                    self.namespace.find_newest_manifest(clock.origin()).await?
                else {
                    return Err(error);
                };
                let payload = current.manifest().payload();
                if !matches!(payload.status, NamespaceStatus::Initializing {}) {
                    return Err(error);
                }
                if let Some(expected) = options.expected_head_seq {
                    if payload.head_seq != expected {
                        return Err(CoreError::StaleHeadPrecondition {
                            expected,
                            actual: payload.head_seq,
                        });
                    }
                }
                let winner = match self
                    .namespace
                    .tombstone_initializing_namespace(current, &clock)
                    .await?
                {
                    ManifestPublicationOutcome::Published(tombstone) => {
                        return Ok(DeleteNamespaceResponse {
                            namespace_id: self.namespace.namespace_id.clone(),
                            head_seq: tombstone.manifest().payload().head_seq,
                        });
                    }
                    ManifestPublicationOutcome::Lost(winner) => winner,
                };
                match winner.manifest().payload().status {
                    NamespaceStatus::Active {} => self.load_position_in_lane(lane).await?,
                    NamespaceStatus::Deleted { .. } => {
                        return Err(CoreError::NamespaceDeleted {
                            namespace_id: self.namespace.namespace_id.clone(),
                        })
                    }
                    NamespaceStatus::Initializing {} => {
                        return Err(CoreError::NamespaceCorrupt(format!(
                            "manifest {} of namespace `{}` is initializing after a fork's \
                             manifest 2",
                            winner.manifest_no(),
                            self.namespace.namespace_id
                        )))
                    }
                }
            }
            Err(error) => return Err(error),
        };
        let deleted = self
            .namespace
            .delete_namespace(options, position, lane, &clock)
            .await;
        self.position.invalidate_projection();
        deleted.inspect_err(|error| self.position.record_session_error(error))
    }

    /// Returns the session's position, loaded while the caller keeps `lane`,
    /// the session's publication lane, and the engine: the position that the
    /// session keeps, or the position from the load of the namespace from
    /// the store under the session's writer epoch, or, for a session without
    /// its epoch, the position of the discovery of a claim in `lane` (see
    /// [`Self::load_position`]). A deletion starts from it, and so does a
    /// publication of the lane that loads its position again after a check
    /// found it behind.
    pub async fn load_position_in_lane(&mut self, lane: &LaneGuard<'_>) -> Result<WriterPosition> {
        let start = self.unit_start(lane).await?;
        self.load_position(start).await
    }

    /// Returns where the session's next unit of the commit queue or of the
    /// publication lane starts when the session has its writer epoch, or
    /// `None` when the session has none, so the unit claims one first (see
    /// [`Self::claim`]). A fenced session returns `writer_fenced`.
    pub fn kept_start(&self) -> Result<Option<UnitStart>> {
        match &*self.position.lock_session() {
            WriterSessionStatus::Acquired { writer, .. } => {
                Ok(Some(UnitStart::Kept(writer.clone())))
            }
            WriterSessionStatus::Fenced(fence) => Err(CoreError::WriterFenced(fence.clone())),
            WriterSessionStatus::Unacquired | WriterSessionStatus::NamespaceGone => Ok(None),
        }
    }

    /// Returns where the session's next unit starts (see
    /// [`Self::kept_start`]), after a claim in `lane`, the session's
    /// publication lane, when the session has no writer epoch. The caller
    /// keeps `lane` and the engine, as [`Self::claim`] requires.
    pub async fn unit_start(&mut self, lane: &LaneGuard<'_>) -> Result<UnitStart> {
        match self.kept_start()? {
            Some(start) => Ok(start),
            None => Ok(UnitStart::Claimed(self.claim(lane).await?)),
        }
    }

    /// Acquires a writer epoch for a session that has none (see
    /// [`Self::kept_start`]) with a claim and a fence, and returns the claim
    /// for the next unit to start from. The claim doesn't compare the newest
    /// manifest's epoch with the epoch the session used before, and an
    /// acquisition that returns `writer_fenced` records the session as
    /// fenced.
    ///
    /// The caller keeps `lane`, the session's publication lane, and the
    /// engine, so the claim keeps both queues while it runs, and no put of
    /// the session is in flight. A put whose future was dropped can still
    /// reach the store, but its drop ended the epoch before its queue was
    /// released, and the claim's own puts either collide with it or take
    /// its number first (format spec, "Failed and unknown outcomes"). The
    /// claim records its manifest as the session's newest published
    /// manifest, which a publication that planned before the claim then
    /// builds on. A position of the session's earlier epoch never follows
    /// it, and the next unit starts from the claim's discovery.
    pub async fn claim(&mut self, lane: &LaneGuard<'_>) -> Result<Claim> {
        assert!(
            lane.is_guard_of(&self.lane),
            "a session claims in its own publication lane"
        );
        assert!(
            matches!(self.kept_start(), Ok(None)),
            "a session claims only without an epoch"
        );
        let acquisition = self
            .namespace
            .acquire_writer(&self.writer_id)
            .await
            .inspect_err(|error| self.position.record_session_error(error))?;
        self.position.lock_session().acquire(
            acquisition.writer.clone(),
            acquisition.anchor.held().clone(),
        );
        Ok(Claim {
            writer: acquisition.writer,
            anchor: Box::new(acquisition.anchor),
        })
    }

    /// Returns this session's position, loaded as a commit loads its view
    /// from `start`: the claim's discovery, the engine's position once the
    /// store shows that it's still current, or the writer's load. The engine
    /// keeps the position, and a failed load drops it.
    ///
    /// The load publishes nothing, so a session that has its epoch loads
    /// with the engine alone. While the session has its epoch, a load can't
    /// find a manifest above the session's newest published manifest except
    /// another writer's claim, which fences the load, and the loaded position
    /// follows the newest published manifest as every copy of the position
    /// does. Every publication of the session that runs beside its commits,
    /// such as a fold or a compaction unit, starts from a position that this
    /// stored, so it publishes only while the session has its epoch.
    pub async fn load_position(&mut self, start: UnitStart) -> Result<WriterPosition> {
        let clock = self.namespace.start_clock();
        let Self {
            namespace,
            position,
            ..
        } = self;
        let held = position
            .load_publish_view(namespace, &clock, start)
            .await?
            .view
            .into_position();
        position.current.set(held.clone());
        Ok(held)
    }

    /// Returns the turn of a publication of the session's lane, which `lane`
    /// keeps (see [`LaneGuard::turn`]), after a claim when the session has
    /// no writer epoch. The engine keeps the position of the claim's
    /// discovery. A fenced session returns `writer_fenced`.
    pub async fn lane_turn<'g>(&mut self, lane: &'g LaneGuard<'_>) -> Result<LaneTurn<'g>> {
        assert!(
            lane.is_guard_of(&self.lane),
            "a publication takes its turn in its own session's lane"
        );
        if self.kept_start()?.is_none() {
            let claim = self.claim(lane).await?;
            self.load_position(UnitStart::Claimed(claim)).await?;
        }
        Ok(lane.acquired_turn())
    }

    /// Advances the retention floor to `target` with a floor object, as one
    /// unit of the session's commit queue, so the floor object never
    /// collides with the session's own commits (format spec, "Advancing the
    /// retention floor").
    ///
    /// The call first checks that the segments of `planned`, the manifest
    /// that the advance was planned from, exist. The session loads its view
    /// from `start` as a commit does, and it puts the floor object at the
    /// number after its position's tip, as it puts a commit, with no check
    /// after the put. The position then replays the floor object, so its tip
    /// is the floor object's number and its floor is raised. A session that
    /// discovery finds fenced returns `writer_fenced`. A put that finds the
    /// number taken drops the position and runs the writer's load, which
    /// returns `writer_fenced` when another writer's fence took the number,
    /// and otherwise the put's error, so the caller plans again. The floor
    /// object's put is sent under the session's epoch, as a commit's WAL put
    /// is, so a put whose outcome is unknown ends the epoch, and the
    /// session's next unit claims. The call writes nothing when the view's
    /// floor is already at or above `target`.
    pub async fn publish_floor_object(
        &mut self,
        start: UnitStart,
        planned: &LoadedManifest,
        target: CommitSeq,
        clock: &OperationClock,
    ) -> Result<FloorObjectReport> {
        self.namespace
            .current_segments(planned)
            .segments
            .verify_segments_exist()
            .await?;
        let Self {
            namespace,
            position,
            ..
        } = self;
        let loaded = position.load_publish_view(namespace, clock, start).await?;
        position
            .publish_floor_object_against(namespace, loaded, target)
            .await
    }

    /// Publishes `candidates` as one batch from `start`. Retries share
    /// `clock`, so the batch keeps one commit time, and content evidence
    /// continues aging.
    ///
    /// The caller takes `start` from [`Self::kept_start`], which returns
    /// `writer_fenced` for a fenced session, or, for a session without its
    /// writer epoch, from a claim (see [`Self::claim`]). A claimed unit
    /// starts from the claim's discovery, and any other unit starts from the
    /// session's position, or from the writer's load, with the engine alone.
    pub async fn publish_batch(
        &mut self,
        start: UnitStart,
        candidates: impl AsRef<[CommitCandidate]>,
        clock: &OperationClock,
    ) -> NamespaceCommitEnginePublishOutcome {
        let candidates = candidates.as_ref();
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishOutcome {
                results: Vec::new(),
                wal_tail: None,
            };
        }
        let Self {
            namespace,
            position,
            ..
        } = self;
        match position.load_publish_view(namespace, clock, start).await {
            Ok(loaded) => {
                position
                    .publish_against(namespace, loaded, candidates, clock)
                    .await
            }
            Err(error) => failed_publication(candidates.len(), error),
        }
    }
}

/// A claim of the session's writer epoch, from
/// [`NamespaceCommitEngine::claim`], with the discovery that the claim's
/// fence was put after, which the next unit starts from.
#[derive(Debug)]
pub struct Claim {
    writer: AcquiredWriter,
    anchor: Box<WriterAnchor>,
}

/// Where a unit of the session's commit queue, or a publication that loads
/// the session's position, starts its view.
#[derive(Debug)]
pub enum UnitStart {
    /// The session has this writer epoch, and the unit starts from the
    /// session's position or from the writer's load (see
    /// [`NamespaceCommitEngine::kept_start`]).
    Kept(AcquiredWriter),
    /// The caller claimed the writer epoch for the unit, which starts from
    /// the claim's discovery.
    Claimed(Claim),
}

/// The forms that a test uses whose engine no other task uses, so the engine
/// takes its own publication lane whenever it claims, as the session's
/// runtime does with the engine released first.
#[cfg(any(test, feature = "test-support"))]
impl<S: ObjectStore> NamespaceCommitEngine<S> {
    /// Returns where the next unit starts, after a claim in the session's
    /// own publication lane when the session has no writer epoch.
    pub async fn start_alone(&mut self) -> Result<UnitStart> {
        if let Some(start) = self.kept_start()? {
            return Ok(start);
        }
        let lane = self.publication_lane();
        let guard = lane.lock().await;
        Ok(UnitStart::Claimed(self.claim(&guard).await?))
    }

    /// Loads the session's position, after a claim when the session needs
    /// one (see [`Self::load_position`]).
    pub async fn load_position_alone(&mut self) -> Result<WriterPosition> {
        let start = self.start_alone().await?;
        self.load_position(start).await
    }

    /// Publishes `candidates` as one batch, after a claim when the session
    /// needs one (see [`Self::publish_batch`]).
    pub async fn publish_batch_alone(
        &mut self,
        candidates: impl AsRef<[CommitCandidate]>,
        clock: &OperationClock,
    ) -> NamespaceCommitEnginePublishOutcome {
        let candidates = candidates.as_ref();
        match self.start_alone().await {
            // Boxing keeps the futures of the tests that call this from
            // exceeding rustc's type-recursion depth, as the runtime's worker
            // boxes its publish.
            Ok(start) => Box::pin(self.publish_batch(start, candidates, clock)).await,
            Err(error) => failed_publication(candidates.len(), error),
        }
    }

    /// Advances the retention floor, after a claim when the session needs
    /// one (see [`Self::publish_floor_object`]).
    pub async fn publish_floor_object_alone(
        &mut self,
        planned: &LoadedManifest,
        target: CommitSeq,
        clock: &OperationClock,
    ) -> Result<FloorObjectReport> {
        let start = self.start_alone().await?;
        self.publish_floor_object(start, planned, target, clock)
            .await
    }

    /// Deletes the namespace in the session's own publication lane (see
    /// [`Self::delete_namespace`]).
    pub async fn delete_namespace_alone(
        &mut self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let lane = self.publication_lane();
        let guard = lane.lock().await;
        self.delete_namespace(&guard, administrator, options).await
    }

    /// Brings the stored position up to the manifest that a publication of
    /// the session's lane recorded, as the runtime's release of the engine
    /// does, so the engine keeps no part of the tail that the publication
    /// folded.
    fn follow_alone(&mut self) {
        let published = self.published_manifest();
        let recorded = published.lock();
        let replaced = self.follow_published(&recorded);
        drop(recorded);
        drop(replaced);
    }

    /// Folds the WAL tail from this session's position, and publishes the
    /// next manifest as one publication of its lane (format spec,
    /// "Publishing a manifest").
    ///
    /// A session without its writer epoch claims it first. The fold writes
    /// its segments from the session's position (see
    /// [`NamespaceStore::write_fold_segments`]), and then publishes in its
    /// turn (see [`NamespaceStore::publish_fold_in_turn`]), which records the
    /// manifest as the session's newest published manifest, so the position
    /// moves onto it and later commits plan against it. A failed fold drops
    /// the position. The stored position follows the manifest at once, so
    /// the engine keeps no part of the tail that the fold covered.
    pub async fn fold(&mut self) -> Result<crate::manifest::FoldedWalTail> {
        let clock = self.namespace.start_clock();
        let lane = self.publication_lane();
        let folded = async {
            let position = self.load_position_alone().await?;
            match self
                .namespace
                .write_fold_segments(&position, &clock)
                .await?
            {
                crate::manifest::FoldWriteOutcome::AlreadyFolded(folded) => Ok(folded),
                crate::manifest::FoldWriteOutcome::Written(writes) => {
                    let guard = lane.lock().await;
                    let turn = self.lane_turn(&guard).await?;
                    self.namespace
                        .publish_fold_in_turn(turn, writes, &clock)
                        .await
                }
            }
        }
        .await;
        let folded = folded.inspect_err(|error| {
            self.position.record_session_error(error);
            self.position.invalidate_projection();
        })?;
        self.follow_alone();
        Ok(folded)
    }

    /// Sets the retention window as one publication of this session's lane
    /// (see [`NamespaceStore::set_retention_window_from`]).
    pub async fn set_retention_window(
        &mut self,
        retention_window_ms: Option<loonfs_types::RetentionWindow>,
    ) -> crate::Result<
        crate::manifest::published::PublicationReport<loonfs_types::SetRetentionWindowResponse>,
    > {
        let lane = self.publication_lane();
        let guard = lane.lock().await;
        let turn = self.lane_turn(&guard).await?;
        let report = self
            .namespace
            .set_retention_window_from(turn, retention_window_ms)
            .await?;
        drop(guard);
        self.follow_alone();
        Ok(report)
    }

    /// Publishes an extension's change as one publication of this session's
    /// lane (see [`NamespaceStore::publish_extension_from`]). A basis of
    /// another namespace returns `invalid_request` before the session claims
    /// anything (see [`ExtensionBasis::ensure_for`](crate::manifest::extension::ExtensionBasis::ensure_for)).
    pub async fn publish_extension(
        &mut self,
        basis: &crate::manifest::extension::ExtensionBasis,
        name: &loonfs_types::ExtensionName,
        entry: loonfs_types::format::manifest::ExtensionEntry,
    ) -> crate::Result<
        crate::manifest::published::PublicationReport<
            crate::manifest::extension::ExtensionPublication,
        >,
    > {
        basis.ensure_for(&self.namespace.namespace_id)?;
        let lane = self.publication_lane();
        let guard = lane.lock().await;
        let turn = self.lane_turn(&guard).await?;
        let report = self
            .namespace
            .publish_extension_from(turn, basis, name, entry)
            .await?;
        drop(guard);
        self.follow_alone();
        Ok(report)
    }
}

impl PositionState {
    fn lock_session(&self) -> PublishedLock<'_> {
        self.session.lock()
    }

    fn invalidate_projection(&mut self) {
        self.current.clear();
    }

    /// Records what `error` says about the session (see
    /// [`PublishedManifest::record_session_error`]).
    fn record_session_error(&self, error: &CoreError) {
        self.session.record_session_error(error);
    }

    /// Loads the view that one publication unit plans against, from
    /// `start`: the claim's discovery, the position once the store shows
    /// that it's still current, or the namespace's current manifest and WAL
    /// tail.
    ///
    /// The view borrows `namespace` rather than this state, so the caller
    /// can read the view and then publish against it.
    async fn load_publish_view<'a, S: ObjectStore>(
        &mut self,
        namespace: &'a NamespaceStore<S>,
        clock: &OperationClock,
        start: UnitStart,
    ) -> Result<LoadedPublishView<'a, S>> {
        let attempt = clock.observe();
        let (acquired_writer, start) = match start {
            UnitStart::Kept(acquired_writer) => {
                let start = match self.current.take() {
                    Some(position) => ViewStart::Position(Box::new(position)),
                    None => ViewStart::Load,
                };
                (acquired_writer, start)
            }
            UnitStart::Claimed(claim) => (claim.writer, ViewStart::Anchor(claim.anchor)),
        };
        let loaded = namespace
            .load_publish_metadata_view(acquired_writer, start, attempt.clone())
            .await;
        let view = match loaded {
            Ok(view) => view,
            Err(error) => {
                self.invalidate_projection();
                self.record_session_error(&error);
                return Err(error);
            }
        };
        Ok(LoadedPublishView { view, attempt })
    }

    /// Publishes `candidates` against `loaded` as one batch, and keeps the
    /// position that the batch leaves.
    async fn publish_against<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        loaded: LoadedPublishView<'_, S>,
        candidates: &[CommitCandidate],
        clock: &OperationClock,
    ) -> NamespaceCommitEnginePublishOutcome {
        let LoadedPublishView {
            view: publish_view,
            attempt,
        } = loaded;
        let wal_tail_discovered = publish_view.tail_discovered;
        let epoch = SessionEpoch::new(&self.session, namespace);
        let published = super::batch::publish_namespace_commits_batch_against_publish_view(
            candidates,
            &publish_view,
            super::batch::PublicationClock {
                batch: clock,
                attempt,
            },
            &epoch,
        )
        .await;
        let position = publish_view.into_position();
        let published = drop_replaced_basis(published);
        let update = self.update_publish_tail_projection(
            &namespace.namespace_id,
            position,
            published.effect,
        );
        NamespaceCommitEnginePublishOutcome {
            results: published.results,
            wal_tail: Some(WalTailReport {
                objects: update.wal_tail_objects,
                wal_bytes: update.wal_tail_bytes,
                inline_bytes: update.wal_tail_inline_bytes,
                oldest_commit_at_ms: update.wal_tail_oldest_commit_at_ms,
                raises_retention_floor: update.raises_retention_floor,
                source: if wal_tail_discovered {
                    TailSource::Store
                } else {
                    TailSource::Cached
                },
            }),
        }
    }

    /// Puts a floor object for `target` at the number after the tip of
    /// `loaded`, unless the view's floor is already at or above `target`, and
    /// keeps the position that the put leaves.
    async fn publish_floor_object_against<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        loaded: LoadedPublishView<'_, S>,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        let LoadedPublishView { view, .. } = loaded;
        let head = view.head().clone();
        let manifest_retention_floor_seq = view.manifest_retention_floor_seq();
        if head.retention_floor_seq >= target {
            self.current.set(view.into_position());
            return Ok(FloorObjectReport {
                retention_floor_seq: head.retention_floor_seq,
                manifest_retention_floor_seq,
            });
        }
        let floor_object = prepare_wal_object(
            &head,
            view.acquired_writer.writer_epoch,
            WalObjectBody::Floor {
                retention_floor_seq: target,
            },
        )?;
        let acquired_writer = view.acquired_writer.clone();
        let published = SessionEpoch::new(&self.session, namespace)
            .send_put(publish_wal_object(view.store(), &floor_object))
            .await;
        let position = view.into_position();
        if let Err(error) = published {
            // The session's next unit loads the namespace from the store.
            self.invalidate_projection();
            if matches!(error, CoreError::WalPublish(WalPublishError::NumberTaken)) {
                // While the session has its epoch, only another writer's
                // fence takes the session's next number, so the writer's load
                // finds that writer's claim and returns `writer_fenced`.
                let loaded = namespace
                    .load_writer_read_anchor(&acquired_writer, namespace.start_clock().observe())
                    .await;
                if let Err(fenced) = loaded {
                    self.record_session_error(&fenced);
                    return Err(fenced);
                }
            }
            return Err(error);
        }
        let head = head.after_wal_object(floor_object.envelope().payload());
        let retention_floor_seq = head.retention_floor_seq;
        self.update_publish_tail_projection(
            &namespace.namespace_id,
            position,
            PublishViewEffect::Advanced {
                body: floor_object.envelope().payload().body.clone(),
                head,
                wal_bytes: floor_object.as_bytes().len(),
            },
        );
        Ok(FloorObjectReport {
            retention_floor_seq,
            manifest_retention_floor_seq,
        })
    }

    /// Folds one batch's effect into the unit's position and its tail, keeps
    /// the position, and reports the WAL-tail length the caller schedules
    /// maintenance on.
    fn update_publish_tail_projection(
        &mut self,
        namespace_id: &NamespaceId,
        mut position: WriterPosition,
        effect: PublishViewEffect,
    ) -> TailProjectionUpdate {
        match effect {
            PublishViewEffect::Unchanged => {}
            PublishViewEffect::Invalidated => {
                self.invalidate_projection();
                return TailProjectionUpdate::of(&position);
            }
            PublishViewEffect::Advanced {
                body,
                head,
                wal_bytes,
            } => {
                position.basis.read_basis_mut().head = head;
                if let Err(error) =
                    Arc::make_mut(&mut position.tail).push_object(namespace_id, &body, wal_bytes)
                {
                    // The WAL is already durable. Discard the position;
                    // replay and folding will report the accounting error.
                    tracing::warn!(
                        namespace_id = %namespace_id,
                        error = %error,
                        "could not update the committed WAL projection"
                    );
                    self.invalidate_projection();
                    return TailProjectionUpdate::of(&position);
                }
            }
        }
        self.current.set(position);
        // The report describes the position that the engine keeps, which a
        // manifest published meanwhile may have moved.
        self.current
            .get()
            .as_ref()
            .map_or_else(TailProjectionUpdate::default, TailProjectionUpdate::of)
    }
}

/// The view that one publication unit plans against, with the observation
/// that its publication clock starts from.
struct LoadedPublishView<'a, S: ObjectStore + ?Sized> {
    view: super::publish_view::PublishMetadataView<'a, S>,
    /// When the unit began.
    attempt: Observation,
}

/// What folding one batch into the tail projection left: the WAL-tail length
/// the caller schedules maintenance on. The default is the report of a
/// position that the engine dropped, which leaves nothing to fold.
#[derive(Default)]
struct TailProjectionUpdate {
    wal_tail_objects: u64,
    wal_tail_bytes: usize,
    wal_tail_inline_bytes: usize,
    wal_tail_oldest_commit_at_ms: Option<u64>,
    raises_retention_floor: bool,
}

impl TailProjectionUpdate {
    fn of(position: &WriterPosition) -> Self {
        Self {
            wal_tail_objects: position.wal_tail_objects(),
            wal_tail_bytes: position.wal_tail_bytes(),
            wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
            wal_tail_oldest_commit_at_ms: position.wal_tail_oldest_commit_at_ms(),
            raises_retention_floor: position.read_basis().head().raises_retention_floor(),
        }
    }
}

/// Returns `report` with the batch's effect set to drop the position when a
/// result is `ManifestReplaced`, so the next unit plans on the current
/// manifest, where the caller publishes that result's candidate again.
fn drop_replaced_basis(mut report: PublishBatchReport) -> PublishBatchReport {
    let replaced = report
        .results
        .iter()
        .any(|result| matches!(result, Err(CoreError::ManifestReplaced { .. })));
    if replaced {
        report.effect = PublishViewEffect::Invalidated;
    }
    report
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<Commit>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// The result of a unit that failed before it published anything.
fn failed_publication(count: usize, error: CoreError) -> NamespaceCommitEnginePublishOutcome {
    NamespaceCommitEnginePublishOutcome {
        results: repeated_error(count, error),
        wal_tail: None,
    }
}

impl<S: ObjectStore + Clone> NamespaceEngine<S, Writable> {
    /// Deletes the current namespace by publishing a tombstone manifest, in
    /// a new writer session that keeps its own publication lane for the
    /// deletion. Earlier committed changes remain durable. Later operations
    /// return `namespace_deleted`. `administrator` proves the caller
    /// administers the namespace.
    pub async fn delete_namespace(
        &self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let mut engine = self.commit_engine();
        let lane = engine.publication_lane();
        let guard = lane.lock().await;
        engine
            .delete_namespace(&guard, administrator, options)
            .await
    }
}

#[cfg(test)]
mod tests;
