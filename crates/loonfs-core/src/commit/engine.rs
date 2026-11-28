//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one numbered WAL put, then returns
//! one result per candidate.

use super::batch::{PublishBatchReport, PublishViewEffect};
use super::publish_view::WriterPosition;
use super::published_manifest::{KeptPosition, PublishedLock, PublishedManifest};
use super::session_epoch::SessionEpoch;
use crate::engine::{Administrator, NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::{FloorObjectReport, FoldedWalTail};
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::delete::TombstoneOutcome;
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
/// stays open. Each engine creates its own status, and a runtime reads the
/// status through a [`WriterSessionStatusView`].
#[derive(Debug, Default)]
pub enum WriterSessionStatus {
    /// No epoch: the session's next publish acquires one with a claim and a
    /// fence. A new session starts here, and so does a session from the
    /// moment it sends a commit's WAL put or a tombstone's put until it
    /// learns the put's outcome (format spec, "Failed and unknown outcomes").
    #[default]
    Unacquired,
    /// Writer epoch acquired by a claim and a fence, and reused for the
    /// session's later publishes.
    Acquired(AcquiredWriter),
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
        // The runtime catches a panic in a unit of work and keeps running,
        // so a poisoned lock still returns the status.
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
    acquired_anchor: Option<WriterAnchor>,
    /// This session's epoch and fencing for the namespace; see
    /// [`WriterSessionStatus`]. The engine creates the status, and no other
    /// engine can reach it.
    session: Arc<Mutex<WriterSessionStatus>>,
}

impl<S: ObjectStore> NamespaceCommitEngine<S> {
    /// Returns a commit engine over `namespace` that commits as `writer_id`,
    /// with a new writer session. Building the engine sends no request to
    /// the store, and the session acquires its writer epoch on its first
    /// publish.
    pub fn new(namespace: NamespaceStore<S>, writer_id: WriterId) -> Self {
        Self {
            namespace,
            writer_id,
            position: PositionState {
                current: KeptPosition::default(),
                acquired_anchor: None,
                session: Arc::default(),
            },
        }
    }

    /// Returns a commit engine whose session already holds `acquired`, for a
    /// test that acquires the writer epoch through the store before the
    /// engine's first publish.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_acquired_session(
        namespace: NamespaceStore<S>,
        writer_id: WriterId,
        acquired: AcquiredWriter,
    ) -> Self {
        let engine = Self::new(namespace, writer_id);
        *engine.position.lock_session() = WriterSessionStatus::Acquired(acquired);
        engine
    }

    /// Returns a read-only view of this engine's writer session status.
    pub fn session_status(&self) -> WriterSessionStatusView {
        WriterSessionStatusView(Arc::clone(&self.position.session))
    }

    /// Returns the newest manifest that the session's runtime published,
    /// where the runtime records each fold and maintenance call without
    /// locking the engine. The engine brings its position up to it each
    /// time it reads the position (see [`PublishedManifest::follow`]).
    pub fn published_manifest(&self) -> PublishedManifest {
        self.position.current.published().clone()
    }

    /// Brings the position up to the manifest that `published`, this
    /// engine's locked [`Self::published_manifest`], records, and stores it,
    /// so the engine doesn't keep the part of the tail that a publication
    /// folded. The session's runtime calls this after each publication, and
    /// when it releases the engine. Returns the position that the call
    /// replaced, which the caller drops once it has unlocked `published`.
    #[must_use = "drop the replaced position once the published manifest is unlocked"]
    pub fn follow_published(&mut self, published: &PublishedLock<'_>) -> Option<WriterPosition> {
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
    /// the session's runtime published, whether or not the session is fenced,
    /// or `None` when the engine has no position.
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

    /// Returns the position that reads in this runtime start from, or `None`
    /// when the engine has no position or its session is fenced.
    ///
    /// A fenced session publishes nothing more, so its position would stay
    /// on a head that the new writer has moved past.
    pub fn read_position(&self) -> Option<WriterPosition> {
        if matches!(
            *self.position.lock_session(),
            WriterSessionStatus::Fenced(_)
        ) {
            return None;
        }
        self.position.current.get()
    }

    /// Deletes the namespace using this writer session (format spec,
    /// "Deleting a namespace").
    ///
    /// Deletion publishes a new manifest with the same writer-session checks as
    /// [`Self::publish_batch`]. Fenced sessions fail before accessing the store,
    /// and a takeover detected during manifest publication permanently fences the
    /// session. A deletion first folds the WAL tail under the namespace's run
    /// policy. Both publications build on the position's manifest. The
    /// tombstone's put releases the session's epoch, as a commit's WAL put
    /// does, so after an unknown outcome the session's next publication
    /// claims a new epoch and writes a fence.
    ///
    /// An initializing namespace has no writer session, so its deletion is a
    /// plain manifest publication of its tombstone (see
    /// [`NamespaceStore::tombstone_initializing_namespace`]). When manifest 2
    /// with the status `active` wins that publication, the call deletes the
    /// namespace with a writer session.
    ///
    /// `administrator` proves the caller administers the namespace, because
    /// deletion is only for the administrator.
    pub async fn delete_namespace(
        &mut self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        administrator.assert_for(&self.namespace.namespace_id);
        let clock = self.namespace.start_clock();
        let acquired_writer = match self
            .position
            .session_writer_epoch(&self.namespace, &self.writer_id)
            .await
        {
            Ok(acquired_writer) => acquired_writer,
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
                let update = self
                    .namespace
                    .tombstone_initializing_namespace(current, &clock)
                    .await?;
                match update.response {
                    TombstoneOutcome::Published(response) => return Ok(response),
                    TombstoneOutcome::Deleted => {
                        return Err(CoreError::NamespaceDeleted {
                            namespace_id: self.namespace.namespace_id.clone(),
                        })
                    }
                    TombstoneOutcome::Active => {
                        self.position
                            .session_writer_epoch(&self.namespace, &self.writer_id)
                            .await?
                    }
                }
            }
            Err(error) => return Err(error),
        };
        let deleted = self
            .namespace
            .delete_namespace(
                options,
                SessionEpoch::new(&self.position.session, acquired_writer),
                self.writer_position(),
                &clock,
            )
            .await;
        self.position.invalidate_projection();
        deleted
            .map(|report| report.response)
            .inspect_err(|error| self.position.record_session_error(error))
    }

    /// Returns this session's position, loaded as a commit loads its view: a
    /// session without its writer epoch claims it first, and the position is
    /// the claim's discovery, the engine's position once the store shows that
    /// it's still current, or the writer's load. The engine keeps the
    /// position, and a failed load drops it.
    ///
    /// Every publication of the session that runs beside its commits, such
    /// as a fold or a compaction unit, starts from a position that this
    /// returns, so it publishes only while the session has its epoch.
    pub async fn load_position(&mut self) -> Result<WriterPosition> {
        let clock = self.namespace.start_clock();
        let Self {
            namespace,
            writer_id,
            position,
        } = self;
        let held = position
            .load_publish_view(namespace, writer_id, &clock)
            .await?
            .view
            .into_position();
        position.current.set(held.clone());
        Ok(held)
    }

    /// Folds the WAL tail from this session's position and publishes the
    /// next manifest (format spec, "Publishing a manifest").
    ///
    /// The fold starts from [`Self::load_position`], so a session without its
    /// writer epoch claims it first. The fold records the manifest that
    /// covers the tail as the session's newest published manifest, as every
    /// publication does (see [`Self::published_manifest`]), so the position
    /// moves onto it and later commits plan against it, and a failed fold
    /// drops the position. The stored position follows the manifest at once,
    /// so the engine keeps no part of the tail that the fold covered. A fold
    /// whose manifest put loses to another process's claim still builds
    /// again on that claim through the publication loop of
    /// [`NamespaceStore::fold_wal_tail_from`].
    pub async fn fold(&mut self) -> Result<FoldedWalTail> {
        let held = self.load_position().await?;
        let clock = self.namespace.start_clock();
        let folded = self.namespace.fold_wal_tail_from(&held, &clock).await;
        let folded = folded.inspect_err(|error| {
            self.position.record_session_error(error);
            self.position.invalidate_projection();
        })?;
        drop(self.position.current.record_published(&folded.current));
        Ok(folded)
    }

    /// Advances the retention floor to `target` with a floor object, as one
    /// unit of the session's publication queue, so the floor object never
    /// collides with the session's own commits (format spec, "Advancing the
    /// retention floor").
    ///
    /// The call first checks that the segments of `planned`, the manifest
    /// that the advance was planned from, exist. The session loads its view
    /// as a commit does, so a session without its writer epoch claims it
    /// first, and it puts the floor object at the number after its position's
    /// tip, as it puts a commit, with no check after the put. The position
    /// then replays the floor object, so its tip is the floor object's number
    /// and its floor is raised. A session that discovery finds fenced returns
    /// `writer_fenced`. A put that finds the number taken drops the position
    /// and runs the writer's load, which returns `writer_fenced` when another
    /// writer's fence took the number, and otherwise the put's error, so the
    /// caller plans again. The
    /// floor object's put doesn't change the session's status, because
    /// collection never deletes a fence or a floor object (format spec,
    /// "Advancing the retention floor"), and a put whose outcome is unknown
    /// drops the position. The call writes nothing when the view's floor is
    /// already at or above `target`.
    pub async fn publish_floor_object(
        &mut self,
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
            writer_id,
            position,
        } = self;
        let loaded = position
            .load_publish_view(namespace, writer_id, clock)
            .await?;
        position
            .publish_floor_object_against(namespace, loaded, target)
            .await
    }

    /// Publishes `candidates` as one batch. Retries share `clock`, so the
    /// batch keeps one commit time, and content evidence continues aging.
    pub async fn publish_batch(
        &mut self,
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
            writer_id,
            position,
        } = self;
        match position
            .load_publish_view(namespace, writer_id, clock)
            .await
        {
            Ok(loaded) => {
                position
                    .publish_against(namespace, loaded, candidates, clock)
                    .await
            }
            Err(error) => failed_publication(candidates.len(), error),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S: ObjectStore> NamespaceCommitEngine<S> {
    /// Records the manifest that `report` says a publication from this
    /// session's position published as the session's newest published
    /// manifest, the last step of the one path by which a test publishes as a
    /// session, the way the session's runtime publishes a window change or an
    /// extension's entry: load the position (claiming first when the session
    /// has no epoch, see [`Self::load_position`]), publish from it without the
    /// engine, and record the manifest.
    fn record_publication<T>(
        &mut self,
        report: crate::manifest::published::PublicationReport<T>,
    ) -> crate::manifest::published::PublicationReport<T> {
        if let Some(manifest) = &report.published {
            drop(self.position.current.record_published(manifest));
        }
        report
    }

    /// Sets the retention window as this session (see
    /// [`Self::record_publication`] and
    /// [`NamespaceStore::set_retention_window_from`]).
    pub async fn set_retention_window(
        &mut self,
        retention_window_ms: Option<loonfs_types::RetentionWindow>,
    ) -> crate::Result<
        crate::manifest::published::PublicationReport<loonfs_types::SetRetentionWindowResponse>,
    > {
        let position = self.load_position().await?;
        let report = self
            .namespace
            .set_retention_window_from(&position, retention_window_ms)
            .await?;
        Ok(self.record_publication(report))
    }

    /// Publishes an extension's change as this session (see
    /// [`Self::record_publication`] and
    /// [`NamespaceStore::publish_extension_from`]). A basis of another
    /// namespace returns `invalid_request` before the session claims
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
        let position = self.load_position().await?;
        let report = self
            .namespace
            .publish_extension_from(&position, basis, name, entry)
            .await?;
        Ok(self.record_publication(report))
    }
}

impl PositionState {
    fn lock_session(&self) -> std::sync::MutexGuard<'_, WriterSessionStatus> {
        // Treat a poisoned lock as fatal because another thread panicked while
        // updating the session state.
        self.session
            .lock()
            .expect("writer session state lock should not be poisoned")
    }

    fn invalidate_projection(&mut self) {
        self.current.clear();
        self.acquired_anchor = None;
    }

    /// Records that the session was fenced, or found its namespace deleted
    /// or missing, when `error` says so. Every caller passes an error from a
    /// read of the session's own namespace, so an error about another
    /// namespace, such as a merge's fork, is never recorded. A fence stays
    /// recorded.
    fn record_session_error(&self, error: &CoreError) {
        let mut session = self.lock_session();
        match error {
            CoreError::WriterFenced(fence) => {
                *session = WriterSessionStatus::Fenced(fence.clone());
            }
            CoreError::NamespaceDeleted { .. } | CoreError::NamespaceNotFound { .. }
                if !matches!(*session, WriterSessionStatus::Fenced(_)) =>
            {
                *session = WriterSessionStatus::NamespaceGone;
            }
            _ => {}
        }
    }

    /// Returns the session's writer epoch, and acquires one with a claim and
    /// a fence when the session has none. The claim doesn't compare the
    /// newest manifest's epoch with the epoch the session used before. A
    /// fenced session fails immediately without accessing the store or
    /// acquiring a new epoch, and an acquisition that returns
    /// `writer_fenced` records the session as fenced.
    async fn session_writer_epoch<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        writer_id: &WriterId,
    ) -> Result<AcquiredWriter> {
        match &*self.lock_session() {
            WriterSessionStatus::Fenced(fence) => {
                return Err(CoreError::WriterFenced(fence.clone()))
            }
            WriterSessionStatus::Acquired(acquired_writer) => return Ok(acquired_writer.clone()),
            WriterSessionStatus::Unacquired | WriterSessionStatus::NamespaceGone => {}
        }
        let acquisition = namespace
            .acquire_writer(writer_id)
            .await
            .inspect_err(|error| self.record_session_error(error))?;
        self.acquired_anchor = Some(acquisition.anchor);
        *self.lock_session() = WriterSessionStatus::Acquired(acquisition.writer.clone());
        Ok(acquisition.writer)
    }

    /// Loads the view that one publication unit plans against: the
    /// position, once the store shows that it's still current, or the
    /// namespace's current manifest and WAL tail.
    ///
    /// The view borrows `namespace` rather than this state, so the caller
    /// can read the view and then publish against it.
    async fn load_publish_view<'a, S: ObjectStore>(
        &mut self,
        namespace: &'a NamespaceStore<S>,
        writer_id: &WriterId,
        clock: &OperationClock,
    ) -> Result<LoadedPublishView<'a, S>> {
        let attempt = clock.observe();
        let acquired_writer = self.session_writer_epoch(namespace, writer_id).await?;

        let acquired_anchor = self.acquired_anchor.take();
        let cached_position = match acquired_anchor {
            Some(_) => {
                self.current.clear();
                None
            }
            None => self.current.take(),
        };
        let loaded = namespace
            .load_publish_metadata_view(
                acquired_writer,
                cached_position,
                acquired_anchor,
                attempt.clone(),
            )
            .await;
        let view = match loaded {
            Ok(value) => value,
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
        let epoch = SessionEpoch::new(&self.session, publish_view.acquired_writer.clone());
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
        // Unlike a commit's put, this put leaves the session's epoch alone,
        // because collection never deletes a fence or a floor object
        // (collection recognizes them by their size and keeps them, see
        // `gc/families.rs`). The format spec, "Advancing the retention
        // floor", explains why that is enough.
        let acquired_writer = view.acquired_writer.clone();
        let published = publish_wal_object(view.store(), &floor_object).await;
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
            PublishViewEffect::Invalidated | PublishViewEffect::OutcomeUnknown => {
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
    // A put with an unknown outcome in the same batch still leaves the
    // session without its epoch.
    if replaced && !matches!(report.effect, PublishViewEffect::OutcomeUnknown) {
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
    /// Deletes the current namespace by publishing a tombstone manifest.
    /// Earlier committed changes remain durable. Later operations return `namespace_deleted`.
    /// `administrator` proves the caller administers the namespace.
    pub async fn delete_namespace(
        &self,
        administrator: &Administrator,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.commit_engine()
            .delete_namespace(administrator, options)
            .await
    }
}

#[cfg(test)]
mod tests;
