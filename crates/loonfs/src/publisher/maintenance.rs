//! The maintenance that a host drives through a writer session: its
//! admission, which the session's close and drain wait for, and the one way
//! that each of its publications starts from the session's position.

use super::registry::NamespaceSession;
use super::{FoldWhen, NamespaceFold, NamespacePublisher};
use crate::maintenance::MetadataCompactionOutcome;
use crate::{CoreError, Error, MaintenanceCancellation, MetadataMaintenanceResponse, Result};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::{LaneGuard, LaneTurn, WriterPosition};
use loonfs_core::{ExtensionBasis, ExtensionPublication, FloorObjectReport, PublicationReport};
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{CommitSeq, ExtensionName, RetentionWindow, SetRetentionWindowResponse};
use std::future::Future;

/// Maintenance that a host drives through a writer session, such as a fold,
/// a compaction unit, a window change, an extension's entry, a snapshot or a
/// retention advance, admitted to the session as a commit is (see
/// [`NamespaceSession::admit`]).
///
/// Every maintenance call of a writable handle runs its work through one of
/// these, so none runs for a session that is fenced, closed, or shutting
/// down. The session's close and the runtime's drain wait for it, and the
/// table keeps the session while it runs, so no second session of the
/// namespace starts in this runtime meanwhile.
pub(crate) struct AdmittedMaintenance<'a> {
    session: &'a NamespaceSession,
}

impl Drop for AdmittedMaintenance<'_> {
    fn drop(&mut self) {
        let publisher = &self.session.publisher;
        publisher
            .lock_state()
            .maintenance
            .send_modify(|running| *running -= 1);
        publisher.forget_if_ended();
    }
}

impl NamespaceSession {
    /// Admits maintenance to this session with the check that admits a
    /// commit (see `NamespacePublisher::admitted_session`), so a fenced
    /// session returns `writer_fenced`, a closed one `writer_session_closed`,
    /// and one that is shutting down `shutting_down`.
    pub(crate) fn admit(&self) -> std::result::Result<AdmittedMaintenance<'_>, CoreError> {
        let publisher = &self.publisher;
        let state = publisher.lock_state();
        let _session = publisher.admitted_session(&state)?;
        state.maintenance.send_modify(|running| *running += 1);
        Ok(AdmittedMaintenance { session: self })
    }
}

impl AdmittedMaintenance<'_> {
    fn publisher(&self) -> &NamespacePublisher {
        &self.session.publisher
    }

    /// Folds the WAL tail when `when` finds a fold due, as
    /// [`NamespacePublisher::fold`] describes. The session's stop (its close,
    /// its fence or the runtime's shutdown) ends the fold's waits for the
    /// fold lock and a fold permit with the session's refusal, and a fold
    /// that has its permit runs to its end, which the close and the drain
    /// wait for.
    pub(crate) async fn fold(&self, when: FoldWhen) -> Result<NamespaceFold> {
        self.publisher().fold(when, &self.publisher().stop).await
    }

    /// Takes a snapshot at the manifest that covers the tail, as
    /// [`NamespacePublisher::snapshot_after_fold`] describes.
    pub(crate) async fn snapshot_after_fold<T, SnapshotFuture>(
        &self,
        create: impl FnMut(Held<LoadedManifest>) -> SnapshotFuture,
    ) -> Result<T>
    where
        SnapshotFuture: Future<Output = loonfs_core::Result<T>>,
    {
        self.publisher()
            .snapshot_after_fold(create, &self.publisher().stop)
            .await
    }

    /// Runs compaction units while a unit is due, as
    /// [`NamespacePublisher::compact_namespace`] describes, stopped by
    /// `cancellation` or by the session's stop. A loop that ends once the
    /// session's stop is set returns the session's refusal.
    pub(crate) async fn compact_namespace(
        &self,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        let publisher = self.publisher();
        let response = publisher
            .compact_namespace(observed, &cancellation.joined(&publisher.stop))
            .await?;
        publisher.ensure_not_stopped()?;
        Ok(response)
    }

    /// Runs one compaction unit, as [`NamespacePublisher::compact_once`]
    /// describes, stopped by `cancellation` or by the session's stop. A unit
    /// that publishes nothing once the session's stop is set returns the
    /// session's refusal.
    pub(crate) async fn compact_once(
        &self,
        observed: Option<Held<LoadedManifest>>,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionOutcome> {
        let publisher = self.publisher();
        let outcome = publisher
            .compact_once(observed, &cancellation.joined(&publisher.stop))
            .await?;
        if !matches!(outcome, MetadataCompactionOutcome::Published { .. }) {
            publisher.ensure_not_stopped()?;
        }
        Ok(outcome)
    }

    /// Enqueues an advance of the retention floor to `target`, planned from
    /// `planned`, in the session's queue (see
    /// [`NamespacePublisher::submit_retention_advance`]).
    pub(crate) async fn submit_retention_advance(
        &self,
        planned: LoadedManifest,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        self.publisher()
            .submit_retention_advance(planned, target)
            .await
    }

    /// Sets the namespace's retention window (see
    /// [`NamespaceStore::set_retention_window_from`](loonfs_core::NamespaceStore::set_retention_window_from))
    /// as a publication of the session's lane (see
    /// [`NamespacePublisher::publish_in_lane`]). The change puts its manifest
    /// without the engine, so no commit waits for it, and a change that the
    /// session's newest published manifest already has is confirmed first. A session without
    /// a position first reads the newest manifest, with a read permit, and
    /// returns the unchanged window without a claim when that manifest
    /// already has it. The caller brings the session's positions up to the
    /// manifest that the report includes, through
    /// [`PublisherRegistry::finish_publication`](super::PublisherRegistry::finish_publication).
    pub(crate) async fn set_retention_window(
        &self,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        let publisher = self.publisher();
        let namespace = publisher.namespace_store();
        if publisher.current_position().is_none() {
            let core = &publisher.context.runtime_core;
            let _permit = core.read_permit().await;
            let newest = namespace.load_live_basis(core.observation()).await?;
            let window_ms = newest.manifest().payload().retention_window_ms;
            if window_ms == retention_window_ms.map(std::num::NonZeroU64::from) {
                return Ok(PublicationReport {
                    response: SetRetentionWindowResponse {
                        namespace_id: publisher.namespace_id.clone(),
                        retention_window_ms: window_ms.map(std::num::NonZeroU64::get),
                    },
                    published: None,
                });
            }
        }
        let lane = publisher.lane.lock().await;
        publisher
            .publish_in_lane(
                &lane,
                |turn| {
                    let namespace = &namespace;
                    async move {
                        namespace
                            .set_retention_window_from(turn, retention_window_ms)
                            .await
                    }
                },
                |report| report.published.is_none(),
            )
            .await
    }

    /// Loads what a result without a put starts from, such as an extension's
    /// basis or the view that a retention advance plans from: `from_position`
    /// of the session's position after the check that confirms it (see
    /// [`NamespacePublisher::confirmed_position`]), or `newest`, a fresh read
    /// of the newest manifest, for a session without a position, which
    /// claims nothing. The load publishes nothing, so it takes no turn in the
    /// session's publication lane.
    pub(crate) async fn load_confirmed<T, Fut>(
        &self,
        from_position: impl FnMut(WriterPosition) -> Fut,
        newest: impl Future<Output = Result<T>>,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let publisher = self.publisher();
        if publisher.current_position().is_none() {
            return newest.await;
        }
        publisher.confirmed_position().await?;
        publisher.run_from_position(from_position).await
    }

    /// Publishes an extension's change (see
    /// [`NamespaceStore::publish_extension_from`](loonfs_core::NamespaceStore::publish_extension_from))
    /// as a publication of the session's lane, the way
    /// [`Self::set_retention_window`] publishes a window change. A change
    /// that the session's newest published manifest refuses is confirmed first. A basis of
    /// another namespace returns `invalid_request` before anything is
    /// claimed (see [`ExtensionBasis::ensure_for`]).
    pub(crate) async fn publish_extension(
        &self,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<PublicationReport<ExtensionPublication>> {
        let publisher = self.publisher();
        basis.ensure_for(&publisher.namespace_id)?;
        let namespace = publisher.namespace_store();
        let lane = publisher.lane.lock().await;
        publisher
            .publish_in_lane(
                &lane,
                |turn| {
                    let namespace = &namespace;
                    let entry = entry.clone();
                    async move {
                        namespace
                            .publish_extension_from(turn, basis, name, entry)
                            .await
                    }
                },
                |report| report.published.is_none(),
            )
            .await
    }
}

impl NamespacePublisher {
    /// Returns this session's namespace in the store.
    fn namespace_store(&self) -> loonfs_core::NamespaceStore<crate::SharedObjectStore> {
        self.context
            .runtime_core
            .namespace_store(&self.namespace_id)
    }

    /// Runs `run`, the writes of a fold or a compaction unit before its put,
    /// or a load of what a result without a put starts from, from the
    /// session's position (see [`Self::writer_position`]), and runs it once
    /// more from a newly loaded position when the first position turns out
    /// to be behind. The writes then take the session's publication lane for
    /// their put. The loads of [`AdmittedMaintenance::load_confirmed`] run
    /// here too.
    ///
    /// The position is behind when `run` returns `head_moved`, because the
    /// position's manifest is missing from the store and at or below the
    /// manifest boundary (see
    /// [`NamespaceStore::load_publication_basis`](loonfs_core::NamespaceStore::load_publication_basis)),
    /// or `manifest_replaced`, because a segment of that manifest is missing
    /// and a newer manifest replaced it (see `SegmentKeeper::load_error` in
    /// core). A collection deletes a manifest or its segments only after a
    /// newer manifest replaced it, and only another writer's claim publishes
    /// after the session's newest published manifest. So the session drops
    /// the position (see [`Self::drop_position`]) and loads it again from the
    /// store under its writer epoch, and that load returns `writer_fenced`
    /// when another writer claimed the namespace. When the second run finds
    /// its position behind too, the session loads its position once more and
    /// returns that load's error, such as `writer_fenced`, or the second
    /// run's error when the session is still the writer.
    pub(super) async fn run_from_position<T, Fut>(
        &self,
        mut run: impl FnMut(WriterPosition) -> Fut,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let next = match run(self.writer_position().await?).await {
            Err(CoreError::HeadMoved(_) | CoreError::ManifestReplaced { .. }) => {
                self.drop_position().await;
                self.writer_position().await?
            }
            settled => return settled.map_err(Error::from),
        };
        match run(next).await {
            Err(behind @ (CoreError::HeadMoved(_) | CoreError::ManifestReplaced { .. })) => {
                self.drop_position().await;
                self.writer_position().await?;
                Err(behind.into())
            }
            settled => settled.map_err(Error::from),
        }
    }

    /// Runs `publish`, a publication of the session's lane, which the
    /// caller keeps as `lane`, in its turn (see [`Self::lane_turn`]), and
    /// confirms a result without a put before the session returns it.
    ///
    /// When `unconfirmed` selects the result, a result without a put that
    /// the publication computed from the session's newest published manifest
    /// (such as a window that the manifest already has), the session
    /// confirms its position first (see [`Self::confirmed_position`]),
    /// because a session that another process displaced doesn't learn that
    /// it was displaced until its next put. When the check moved the
    /// position or dropped it, the session loads its position again with the
    /// lane that the caller keeps (see `NamespaceCommitEngine::load_position_in_lane`):
    /// from the store under its writer epoch, which returns `writer_fenced`
    /// when another writer claimed the namespace, or after a claim when the
    /// session lost its epoch meanwhile. It then runs `publish` once more in a
    /// new turn, whose result needs no check. So a result without a put is
    /// never computed from a manifest that another writer replaced.
    pub(super) async fn publish_in_lane<'g, T, Fut>(
        &self,
        lane: &'g LaneGuard<'_>,
        mut publish: impl FnMut(LaneTurn<'g>) -> Fut,
        unconfirmed: impl Fn(&T) -> bool,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let turn = self.lane_turn(lane).await?;
        let manifest_no = turn.basis().manifest_no();
        let result = publish(turn).await?;
        if !unconfirmed(&result) {
            return Ok(result);
        }
        match self.confirmed_position().await? {
            Some(checked) if checked.read_basis().basis().manifest_no() == manifest_no => {
                return Ok(result);
            }
            _ => {
                drop(self.unfenced_session()?);
                let mut engine = self.lock_engine().await;
                engine.load_position_in_lane(lane).await?;
            }
        }
        Ok(publish(self.lane_turn(lane).await?).await?)
    }

    /// Returns the session's position for a result that the session returns
    /// without a put, after the check of the position that a strong read
    /// makes (see
    /// [`RuntimeCore::checked_writer_position`](crate::runtime_core::RuntimeCore::checked_writer_position)):
    /// the position when the check finds its manifest the newest, or the
    /// position that the check moved onto a newer manifest of the session's
    /// writer epoch. Returns `None` for a session without a position, and
    /// when the check drops the position, so the caller loads it again with
    /// the writer's load, which returns `writer_fenced` when another writer
    /// claimed the namespace.
    ///
    /// The check takes no turn in the session's publication lane, so an item
    /// of the lane confirms its result in its own turn. It locks the engine
    /// to move the position, which comes after the lane in the order of the
    /// session's locks. The check costs one HEAD of the successor of the
    /// position's manifest and one read of the manifest boundary.
    async fn confirmed_position(&self) -> Result<Option<WriterPosition>> {
        if self.current_position().is_none() {
            return Ok(None);
        }
        let checked = self
            .context
            .runtime_core
            .checked_writer_position(&self.namespace_id, None)
            .await?;
        Ok(checked.map(|position| WriterPosition::clone(&position)))
    }

    /// Returns the error with which this session refuses new work once the
    /// session's stop is set (see [`Self::stopped_refusal`]), and `Ok` while
    /// the stop isn't set.
    fn ensure_not_stopped(&self) -> Result<()> {
        if self.stop.is_cancelled() {
            return Err(self.stopped_refusal());
        }
        Ok(())
    }

    /// Returns the error with which this session, whose stop is set, refuses
    /// new work: `writer_fenced`, `writer_session_closed`, `shutting_down` or
    /// `namespace_deleted`.
    pub(super) fn stopped_refusal(&self) -> Error {
        let state = self.lock_state();
        self.admitted_session(&state)
            .map(drop)
            .expect_err("the stop is set only once the session admits no more work")
            .into()
    }

    /// Waits until no maintenance that hosts run on this session is left
    /// (see [`AdmittedMaintenance`]).
    pub(super) async fn wait_for_maintenance(&self) {
        let mut running = self.lock_state().maintenance.subscribe();
        // The publisher keeps the sender, so the wait ends only once the
        // count reaches zero.
        let _ = running.wait_for(|running| *running == 0).await;
    }
}
