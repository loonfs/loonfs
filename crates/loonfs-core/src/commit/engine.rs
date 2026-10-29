//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one numbered WAL put, then returns
//! one result per candidate.

use super::batch::PublishViewEffect;
use super::publish_view::WriterPosition;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::PublishedManifest;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::options::DeleteNamespaceOptions;
use crate::time::{Observation, OperationClock};
use crate::wal::settle_publish_attempt;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{Commit, CommitId, DeleteNamespaceResponse, ManifestNo, NamespaceId, WriterId};
use std::sync::{Arc, Mutex};

mod candidate;

pub(crate) use self::candidate::ContentPreparation;
pub use self::candidate::{CommitCandidate, ContentPreparationError};

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
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
    /// Inline content bytes in the unfolded WAL objects that the position
    /// records, leaving out a put whose outcome is unknown.
    pub inline_bytes: usize,
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

/// Tracks writer state for one namespace session: unacquired, acquired, or
/// permanently fenced.
///
/// Object storage cannot reconstruct whether the current session was fenced.
/// Runtimes should therefore share one instance across engines for the same
/// namespace and keep it outside rebuildable caches. An engine without shared
/// state treats each one-shot commit as a separate session.
#[derive(Debug, Default)]
pub enum WriterSessionStatus {
    /// No epoch yet: the session's first publish acquires one.
    #[default]
    Unacquired,
    /// Writer epoch acquired on the first publish and reused for the rest of
    /// the session.
    Acquired(AcquiredWriter),
    /// Permanent fencing record for this session. Later publishes return
    /// `writer_fenced` without accessing the store. Acquiring a new writer epoch
    /// requires an explicit caller action.
    Fenced(WriterFence),
}

/// Shared handle to one namespace's [`WriterSessionStatus`].
pub type SharedWriterSessionStatus = Arc<Mutex<WriterSessionStatus>>;

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
    current: Option<WriterPosition>,
    projection_observed: Option<Observation>,
    acquired_anchor: Option<NamespaceReadAnchor>,
    /// This session's epoch and fencing for the namespace; see
    /// [`WriterSessionStatus`].
    session: SharedWriterSessionStatus,
    /// The newest manifest that this engine published itself (the claim of
    /// the session's writer epoch, or a tombstone) and that
    /// [`NamespaceCommitEngine::take_published_manifest_no`] hasn't returned yet.
    published_manifest_no: Option<ManifestNo>,
}

impl<S: ObjectStore> NamespaceCommitEngine<S> {
    /// Returns a commit engine over `namespace` that commits as `writer_id`,
    /// with a writer session that no other engine shares.
    pub fn new(namespace: NamespaceStore<S>, writer_id: WriterId) -> Self {
        Self {
            namespace,
            writer_id,
            position: PositionState {
                current: None,
                projection_observed: None,
                acquired_anchor: None,
                session: SharedWriterSessionStatus::default(),
                published_manifest_no: None,
            },
        }
    }

    /// Uses runtime-managed session state so the writer epoch and fenced status
    /// persist across engine instances.
    pub fn writer_session(mut self, session: SharedWriterSessionStatus) -> Self {
        self.position.session = session;
        self
    }

    /// Drops the position and its tail, so the next unit reads the tail from
    /// the store. Writer epoch and fencing remain in session state and are
    /// not reset.
    pub fn invalidate_projection(&mut self) {
        self.position.invalidate_projection();
    }

    /// Checks the position's WAL tail for the commit's receipt, without
    /// store reads or writer acquisition.
    pub fn retains_commit_receipt(&self, commit_id: &CommitId) -> bool {
        self.position
            .current
            .as_ref()
            .is_some_and(|position| position.tail.find_commit_receipt(commit_id).is_some())
    }

    /// Unfolded WAL objects as of the last publication unit, or `None` when
    /// the engine has no position.
    pub fn wal_tail_objects(&self) -> Option<u64> {
        self.position
            .current
            .as_ref()
            .map(WriterPosition::wal_tail_objects)
    }

    /// Inline content bytes in the unfolded WAL tail as of the last
    /// publication unit, or `None` when the engine has no position.
    pub fn wal_tail_inline_bytes(&self) -> Option<usize> {
        self.position
            .current
            .as_ref()
            .map(WriterPosition::wal_tail_inline_bytes)
    }

    /// Returns the engine's position, whether or not the session is fenced,
    /// or `None` when the engine has no position.
    pub fn writer_position(&self) -> Option<WriterPosition> {
        self.position.current.clone()
    }

    /// Moves the position onto `manifest`, a manifest that this runtime
    /// published or found current (the session's own fold, or a maintenance
    /// call), or that a read found above the position, or drops the
    /// position when it can't move there (see
    /// `WriterPosition::move_to` for the rule).
    ///
    /// Later commits then plan against the new manifest, and reads in this
    /// runtime start from it, so a read's manifest check doesn't find the
    /// new manifest as a successor and drop the position. The position keeps
    /// the WAL tail above the new manifest.
    ///
    /// The last check of the position's manifest stays valid after a move.
    /// That check found no successor of the old manifest, so it happened
    /// before the new manifest was published, and no manifest after the new
    /// one existed then either. Once the position is on a manifest that this
    /// runtime published or found current, the start of that call is a check
    /// of the manifest too, because no manifest followed it then, so the
    /// position keeps the younger of the two checks.
    pub fn record_manifest_published(&mut self, manifest: PublishedManifest) {
        let state = &mut self.position;
        let Some(position) = state.current.as_mut() else {
            return;
        };
        if !position.move_to(&manifest) {
            state.invalidate_projection();
            return;
        }
        let Some(started) = manifest.started else {
            return;
        };
        if position.read_basis.basis.manifest() == &manifest.manifest
            && started.age_ms() < position.basis_checked.age_ms()
        {
            position.basis_checked = started;
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
        self.position.current.clone()
    }

    /// Returns the number of the newest manifest that this engine published
    /// since the last call, so that the runtime can raise the namespace's hint
    /// to that manifest.
    ///
    /// The engine publishes a manifest when it acquires the session's writer
    /// epoch and when it deletes the namespace.
    pub fn take_published_manifest_no(&mut self) -> Option<ManifestNo> {
        self.position.published_manifest_no.take()
    }

    /// Deletes the namespace using this writer session (format spec,
    /// "Deleting a namespace").
    ///
    /// Deletion publishes a new manifest with the same writer-session checks as
    /// [`Self::publish_batch`]. Fenced sessions fail before accessing the store,
    /// and a takeover detected during manifest publication permanently fences the
    /// session. A deletion first folds the WAL tail under the namespace's run
    /// policy. Both publications build on the position's manifest while the
    /// position's last check is within the revalidation bound.
    pub async fn delete_namespace(
        &mut self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let clock = self.namespace.start_clock();
        let acquired_writer = self
            .position
            .session_writer_epoch(&self.namespace, &self.writer_id)
            .await?;
        let deleted = self
            .namespace
            .delete_namespace(options, acquired_writer, self.writer_position(), &clock)
            .await;
        self.position.invalidate_projection();
        match deleted {
            Ok(report) => {
                if let Some(tombstone) = report.published {
                    self.position
                        .record_published_manifest_no(tombstone.manifest.manifest_no);
                }
                Ok(report.response)
            }
            Err(error) => {
                if let CoreError::WriterFenced(fence) = &error {
                    *self.position.lock_session() = WriterSessionStatus::Fenced(fence.clone());
                }
                Err(error)
            }
        }
    }

    /// Publishes `candidates` as one batch. Retries share `clock`, so the
    /// batch keeps one commit time, and content evidence continues aging.
    pub async fn publish_batch(
        &mut self,
        candidates: impl AsRef<[CommitCandidate]>,
        clock: &OperationClock,
    ) -> NamespaceCommitEnginePublishResult {
        let candidates = candidates.as_ref();
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
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

    /// Publishes `candidates` as one batch, and publishes again each
    /// candidate whose attempt the store settled as retryable, until every
    /// candidate settles or the attempts run out. The attempts share
    /// `clock` and keep this engine's position.
    pub async fn publish_batch_until_settled(
        &mut self,
        candidates: Vec<CommitCandidate>,
        clock: &OperationClock,
    ) -> Vec<Result<Commit>> {
        let mut results = vec![None; candidates.len()];
        let mut pending: Vec<_> = candidates.into_iter().enumerate().collect();
        for _ in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
            let (indices, attempted): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
            let observed = self.publish_batch(&attempted, clock).await.results;
            pending =
                settle_publish_attempt(&mut results, indices.into_iter().zip(attempted), observed);
            if pending.is_empty() {
                break;
            }
        }
        results
            .into_iter()
            .map(|result| result.expect("each candidate should receive a publication result"))
            .collect()
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
        self.current = None;
        self.acquired_anchor = None;
        self.projection_observed = None;
    }

    /// The basis manifest that needs a successor check, either because its
    /// last confirmation expired or because the tip observation expired, so
    /// that the check also confirms the tip.
    fn unconfirmed_basis(&self, attempt: &Observation, tip_expired: bool) -> Option<ManifestNo> {
        let position = self.current.as_ref()?;
        let confirmed_recently = !tip_expired
            && position.basis_checked.age_at(attempt) <= crate::limits::WAL_PUBLISH_BUDGET_MS;
        (!confirmed_recently).then(|| position.read_basis.basis.manifest_no())
    }

    fn record_published_manifest_no(&mut self, manifest_no: ManifestNo) {
        self.published_manifest_no = Some(
            self.published_manifest_no
                .map_or(manifest_no, |newest| newest.max(manifest_no)),
        );
    }

    /// Returns the session's writer epoch, acquiring it on first use. A fenced
    /// session fails immediately without accessing the store or acquiring a new
    /// epoch.
    async fn session_writer_epoch<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        writer_id: &WriterId,
    ) -> Result<AcquiredWriter> {
        let already_acquired = match &*self.lock_session() {
            WriterSessionStatus::Fenced(fence) => {
                return Err(CoreError::WriterFenced(fence.clone()))
            }
            WriterSessionStatus::Acquired(acquired_writer) => Some(acquired_writer.clone()),
            WriterSessionStatus::Unacquired => None,
        };
        if let Some(acquired_writer) = already_acquired {
            return Ok(acquired_writer);
        }
        let acquisition = namespace.acquire_writer(writer_id).await?;
        let acquired_writer = acquisition.writer;
        self.record_published_manifest_no(acquisition.claim.manifest.manifest_no);
        self.acquired_anchor = Some(acquisition.anchor);
        let mut session = self.lock_session();
        if let WriterSessionStatus::Fenced(fence) = &*session {
            // Another engine fenced the shared session while this engine was acquiring
            // the epoch. Preserve the fenced state.
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        *session = WriterSessionStatus::Acquired(acquired_writer.clone());
        Ok(acquired_writer)
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

        let tip_expired = self.projection_observed.as_ref().is_some_and(|observed| {
            observed.age_at(&attempt) > crate::limits::WAL_PUBLISH_BUDGET_MS
        });
        // A response can't confirm a basis whose last confirmation is at the
        // revalidation bound, so the position is dropped without a probe.
        if tip_expired
            && !self.current.as_ref().is_some_and(|position| {
                position.basis_checked.age_at(&attempt) < crate::limits::READ_REVALIDATION_BOUND_MS
            })
        {
            self.invalidate_projection();
        }
        // A put that took effect confirms the tip, not the manifest the batch was planned
        // against: a fold or compaction (including our own) may have succeeded.
        // Another writer must claim a successor manifest before it writes a WAL
        // object, so the probe also confirms the tip. A WAL object that another
        // writer creates after the probe is too young to be collected before
        // the put, so the put collides with it. After this session's own put
        // with an unknown outcome, the engine has already dropped the
        // position. Bound the response by the previous basis confirmation so a
        // collected successor can't look absent. A probe that finds a manifest
        // or fails reloads the view.
        if let Some(manifest_no) = self.unconfirmed_basis(&attempt, tip_expired) {
            let successor = namespace.manifest_has_successor(manifest_no).await;
            match (successor, self.current.as_mut()) {
                (Ok(false), Some(position))
                    if position.basis_checked.is_within_revalidation_bound() =>
                {
                    position.basis_checked = attempt.clone();
                    if tip_expired {
                        self.projection_observed = Some(attempt.clone());
                    }
                }
                _ => self.invalidate_projection(),
            }
        }
        let acquired_anchor = self.acquired_anchor.take();
        let cached_position = match acquired_anchor {
            Some(_) => {
                self.current = None;
                None
            }
            None => self.current.take(),
        };
        let loaded = namespace
            .load_publish_metadata_view(acquired_writer, cached_position, acquired_anchor)
            .await;
        let mut view = match loaded {
            Ok(value) => value,
            Err(error) => {
                self.invalidate_projection();
                if let CoreError::WriterFenced(fence) = &error {
                    *self.lock_session() = WriterSessionStatus::Fenced(fence.clone());
                }
                return Err(error);
            }
        };
        let projection_observed = if view.tail_discovered {
            // Discovery confirmed the basis and observed the tip no earlier
            // than this attempt began.
            view.position.basis_checked = attempt.clone();
            attempt.clone()
        } else {
            self.projection_observed
                .clone()
                .unwrap_or_else(|| attempt.clone())
        };
        Ok(LoadedPublishView {
            view,
            attempt,
            projection_observed,
        })
    }

    /// Publishes `candidates` against `loaded` as one batch, and keeps the
    /// position that the batch leaves.
    async fn publish_against<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        loaded: LoadedPublishView<'_, S>,
        candidates: &[CommitCandidate],
        clock: &OperationClock,
    ) -> NamespaceCommitEnginePublishResult {
        let LoadedPublishView {
            view: publish_view,
            attempt,
            projection_observed,
        } = loaded;
        let wal_tail_discovered = publish_view.tail_discovered;
        let published = super::batch::publish_namespace_commits_batch_against_publish_view(
            candidates,
            &publish_view,
            super::batch::PublicationClock {
                batch: clock,
                attempt: attempt.clone(),
                tip: projection_observed.clone(),
            },
        )
        .await;
        let position = publish_view.into_position();
        self.projection_observed = Some(projection_observed);
        let update = self.update_publish_tail_projection(
            &namespace.namespace_id,
            position,
            published.effect,
        );
        // A put that took effect is itself an observation of the tip it created, made no
        // earlier than this attempt began; the next put's budget runs from it.
        if update.landed && self.current.is_some() {
            self.projection_observed = Some(attempt);
        }
        NamespaceCommitEnginePublishResult {
            results: published.results,
            wal_tail: Some(WalTailReport {
                objects: update.wal_tail_objects,
                inline_bytes: update.wal_tail_inline_bytes,
                source: if wal_tail_discovered {
                    TailSource::Store
                } else {
                    TailSource::Cached
                },
            }),
        }
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
        let advanced = matches!(effect, PublishViewEffect::Advanced { .. });
        match effect {
            PublishViewEffect::Unchanged => {}
            PublishViewEffect::Invalidated => {
                self.invalidate_projection();
                return TailProjectionUpdate {
                    wal_tail_objects: position.wal_tail_objects(),
                    wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
                    landed: false,
                };
            }
            PublishViewEffect::Advanced { records, head } => {
                position.read_basis.head = head;
                if let Err(error) =
                    Arc::make_mut(&mut position.tail).push_object(namespace_id, &records)
                {
                    // The WAL is already durable. Discard the position;
                    // replay and folding will report the accounting error.
                    tracing::warn!(
                        namespace_id = %namespace_id,
                        error = %error,
                        "could not update the committed WAL projection"
                    );
                    self.invalidate_projection();
                    return TailProjectionUpdate {
                        wal_tail_objects: position.wal_tail_objects(),
                        wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
                        landed: false,
                    };
                }
            }
        }
        let update = TailProjectionUpdate {
            wal_tail_objects: position.wal_tail_objects(),
            wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
            landed: advanced,
        };
        self.current = Some(position);
        update
    }
}

/// The view that one publication unit plans against, with the
/// observations that its publication clock starts from.
struct LoadedPublishView<'a, S: ObjectStore + ?Sized> {
    view: super::publish_view::PublishMetadataView<'a, S>,
    /// When the unit began.
    attempt: Observation,
    /// When the store last showed the view's WAL tip current.
    projection_observed: Observation,
}

/// What folding one batch into the tail projection left: the WAL-tail length
/// the caller schedules maintenance on, and whether the batch's put took effect
/// and moved the position.
struct TailProjectionUpdate {
    wal_tail_objects: u64,
    wal_tail_inline_bytes: usize,
    landed: bool,
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<Commit>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// The result of a unit that failed before it published anything.
fn failed_publication(count: usize, error: CoreError) -> NamespaceCommitEnginePublishResult {
    NamespaceCommitEnginePublishResult {
        results: repeated_error(count, error),
        wal_tail: None,
    }
}

impl<S: ObjectStore + Clone> NamespaceEngine<S, Writable> {
    /// Publishes already-classified mutation candidates as one batch: one WAL
    /// object, one numbered WAL put, one result per candidate in order.
    pub async fn publish_namespace_commits_batch(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Result<Vec<Result<Commit>>> {
        Ok(self
            .commit_engine()
            .publish_batch_until_settled(candidates, &self.namespace.start_clock())
            .await)
    }

    /// Deletes the current namespace by publishing a tombstone manifest.
    /// Earlier committed changes remain durable. Later operations return `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.commit_engine().delete_namespace(options).await
    }
}

#[cfg(test)]
mod tests;
