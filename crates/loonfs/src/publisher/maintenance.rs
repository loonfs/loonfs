//! The maintenance that a host drives through a writer session: its
//! admission, which the session's close and drain wait for, and the one way
//! that each of its publications starts from the session's position.

use super::registry::NamespaceSession;
use super::{FoldWhen, NamespaceFold, NamespacePublisher};
use crate::maintenance::MetadataCompactionOutcome;
use crate::{CoreError, Error, MaintenanceCancellation, MetadataMaintenanceResponse, Result};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::WriterPosition;
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
    /// as one item of the session's publication lane (see
    /// [`NamespacePublisher::publish_from`]). The change puts its manifest
    /// without the engine, so no commit waits for it, and a change that the
    /// position's manifest already has is confirmed first. A session without
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
            .publish_from(
                || publisher.lane_turn(&lane),
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
        publisher
            .publish_from(|| publisher.writer_position(), from_position, |_| false)
            .await
    }

    /// Publishes an extension's change (see
    /// [`NamespaceStore::publish_extension_from`](loonfs_core::NamespaceStore::publish_extension_from))
    /// as one item of the session's publication lane, the way
    /// [`Self::set_retention_window`] publishes a window change. A change
    /// that the position's manifest refuses is confirmed first. A basis of
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
            .publish_from(
                || publisher.lane_turn(&lane),
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

    /// Runs `publish`, a publication of this session beside its commits,
    /// from what `start` returns, and runs it once more from a new start
    /// when the first turns out to be behind. The fold, the window change
    /// and the extension's entry run here as one item of the session's
    /// publication lane: `start` takes the turn (see [`Self::lane_turn`]),
    /// and the caller keeps the lane for both runs and the confirmation
    /// between them. The compaction unit runs here from the session's
    /// position (see [`Self::writer_position`]), and takes the lane only for
    /// its put. The snapshot and the deletion don't run here: a snapshot
    /// publishes no manifest of its own, and the deletion folds and puts its
    /// tombstone in one turn of the lane through its own loop.
    ///
    /// The start is behind when `publish` returns `head_moved`: its
    /// manifest is gone from the store, or a manifest that won the
    /// publication's put ended it without a put (see
    /// [`NamespaceStore::load_publication_basis`](loonfs_core::NamespaceStore::load_publication_basis)).
    /// Within one process only another writer's manifest can win the put,
    /// because the session's own manifest publications take their turns in
    /// its lane. The session then drops the position (see
    /// [`Self::drop_position`]), so `start` loads it again with the writer's
    /// load, which returns `writer_fenced` when another writer claimed the
    /// namespace. The drop and the load are two turns of the engine, which
    /// is harmless: the drop clears every copy of the position, and a unit
    /// that has the engine in between finds no position and loads one from
    /// the store too. When `unconfirmed` selects the result, a result without
    /// a put that the publication computed from the start (such as a window
    /// that the position's manifest already has), the session confirms the
    /// position first (see [`Self::confirmed_position`]), because a session
    /// that another process displaced doesn't learn of it until it puts, and
    /// runs `publish` once more from the position that the check moved, or
    /// from the writer's load when the check dropped it. So a result without
    /// a put is never computed from a manifest that the session didn't
    /// publish. A put that loses to another writer's claim still builds again
    /// on the claim for now, as [Basis, collision, and boundary
    /// checks](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#basis-collision-and-boundary-checks-for-every-manifest-publication)
    /// describes. The second run's start was just confirmed or loaded, so
    /// its result needs no check. When the second run finds its start
    /// behind too, the session loads its position once more and returns that
    /// load's error, such as `writer_fenced`, or `head_moved` when the
    /// session is still the writer.
    pub(super) async fn publish_from<S, T, StartFut, Fut>(
        &self,
        mut start: impl FnMut() -> StartFut,
        mut publish: impl FnMut(S) -> Fut,
        unconfirmed: impl Fn(&T) -> bool,
    ) -> Result<T>
    where
        S: AsRef<WriterPosition>,
        StartFut: Future<Output = Result<S>>,
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let first = start().await?;
        let manifest_no = first.as_ref().read_basis().basis().manifest_no();
        let next = match publish(first).await {
            Err(CoreError::HeadMoved(_)) => {
                self.drop_position().await;
                start().await?
            }
            Ok(result) if unconfirmed(&result) => match self.confirmed_position().await? {
                Some(checked) if checked.read_basis().basis().manifest_no() == manifest_no => {
                    return Ok(result);
                }
                // The check moved the position, or dropped it, and `start`
                // returns what the check left or loads the position again.
                _ => start().await?,
            },
            settled => return settled.map_err(Error::from),
        };
        match publish(next).await {
            // Behind again: when another writer claimed meanwhile, the
            // writer's load returns `writer_fenced`. When it finds the
            // session still the writer, the session lost twice to manifests
            // that it published, such as puts of its own whose outcome was
            // unknown, and `head_moved` stands.
            Err(CoreError::HeadMoved(message)) => {
                self.drop_position().await;
                start().await?;
                Err(CoreError::HeadMoved(message).into())
            }
            settled => settled.map_err(Error::from),
        }
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
