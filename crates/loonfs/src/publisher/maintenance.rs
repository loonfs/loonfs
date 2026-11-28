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
    /// from the session's position (see
    /// [`NamespacePublisher::publish_from_position`]). The change puts its
    /// manifest without the engine, so no commit waits for it, and a change
    /// that the position's manifest already has is confirmed first. A
    /// session without a position first reads the newest manifest, with a
    /// read permit, and returns the unchanged window without a claim when
    /// that manifest already has it. The caller records the manifest that the
    /// report includes, through
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
        publisher
            .publish_from_position(
                |position| {
                    let namespace = &namespace;
                    async move {
                        namespace
                            .set_retention_window_from(&position, retention_window_ms)
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
    /// claims nothing.
    pub(crate) async fn load_confirmed<T, Fut>(
        &self,
        from_position: impl FnMut(WriterPosition) -> Fut,
        newest: impl Future<Output = Result<T>>,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let publisher = self.publisher();
        match publisher.confirmed_position().await? {
            Some(position) => {
                publisher
                    .publish_from(position, from_position, |_| false)
                    .await
            }
            None => newest.await,
        }
    }

    /// Publishes an extension's change (see
    /// [`NamespaceStore::publish_extension_from`](loonfs_core::NamespaceStore::publish_extension_from))
    /// from the session's position, the way [`Self::set_retention_window`]
    /// publishes a window change. A change that the position's manifest
    /// refuses is confirmed first. A basis of another namespace returns
    /// `invalid_request` before anything is claimed (see
    /// [`ExtensionBasis::ensure_for`]).
    pub(crate) async fn publish_extension(
        &self,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<PublicationReport<ExtensionPublication>> {
        let publisher = self.publisher();
        basis.ensure_for(&publisher.namespace_id)?;
        let namespace = publisher.namespace_store();
        publisher
            .publish_from_position(
                |position| {
                    let namespace = &namespace;
                    let entry = entry.clone();
                    async move {
                        namespace
                            .publish_extension_from(&position, basis, name, entry)
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
    /// from the session's position (see [`Self::writer_position`]), as
    /// [`Self::publish_from`] describes.
    pub(super) async fn publish_from_position<T, Fut>(
        &self,
        publish: impl FnMut(WriterPosition) -> Fut,
        unconfirmed: impl Fn(&T) -> bool,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let position = self.writer_position().await?;
        self.publish_from(position, publish, unconfirmed).await
    }

    /// Runs `publish` from `position`, and runs it once more from another
    /// position when `position` turns out to be behind.
    ///
    /// The position is behind when `publish` returns `head_moved`: its
    /// manifest is gone from the store, or a manifest that won the
    /// publication's put ended it without a put (see
    /// [`NamespaceStore::load_publication_basis`](loonfs_core::NamespaceStore::load_publication_basis)).
    /// The session then drops the position and loads it again with the
    /// writer's load, which returns `writer_fenced` when another writer
    /// claimed the namespace. When `unconfirmed` selects the result, a
    /// result without a put that the publication computed from the position
    /// (such as a window that the position's manifest already has), the
    /// session confirms the position first (see [`Self::confirmed_position`]),
    /// because a session that another process displaced doesn't learn of it
    /// until it puts, and runs `publish` once more from the confirmed
    /// position when that is on a newer manifest. So a result without a put
    /// is never computed from a manifest that the session didn't publish. A
    /// put that loses to another writer's claim still builds again on the
    /// claim for now, as [Basis, collision, and boundary
    /// checks](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#basis-collision-and-boundary-checks-for-every-manifest-publication)
    /// describes. The second run's position was just confirmed or loaded, so
    /// its result needs no check. When the second run finds its position
    /// behind too, the session loads it once more with the writer's load and
    /// returns that load's error, such as `writer_fenced`, or `head_moved`
    /// when the session is still the writer.
    async fn publish_from<T, Fut>(
        &self,
        position: WriterPosition,
        mut publish: impl FnMut(WriterPosition) -> Fut,
        unconfirmed: impl Fn(&T) -> bool,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let manifest_no = position.read_basis().basis().manifest_no();
        let position = match publish(position).await {
            Err(CoreError::HeadMoved(_)) => self.reload_writer_position().await?,
            Ok(result) if unconfirmed(&result) => match self.confirmed_position().await? {
                Some(checked) if checked.read_basis().basis().manifest_no() == manifest_no => {
                    return Ok(result);
                }
                Some(checked) => checked,
                None => self.writer_position().await?,
            },
            settled => return settled.map_err(Error::from),
        };
        match publish(position).await {
            // Behind again: when another writer claimed meanwhile, the
            // writer's load returns `writer_fenced`. When it finds the
            // session still the writer, the session lost to its own
            // publication twice, and `head_moved` stands.
            Err(CoreError::HeadMoved(message)) => {
                self.reload_writer_position().await?;
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
    /// writer epoch. When the check drops the position, the session loads it
    /// again with the writer's load, which returns `writer_fenced` when
    /// another writer claimed the namespace. Returns `None` for a session
    /// without a position, which returns a result from a fresh read of the
    /// newest manifest instead and claims nothing.
    ///
    /// The check costs one HEAD of the successor of the position's manifest
    /// and one read of the manifest boundary.
    async fn confirmed_position(&self) -> Result<Option<WriterPosition>> {
        if self.current_position().is_none() {
            return Ok(None);
        }
        let checked = self
            .context
            .runtime_core
            .checked_writer_position(&self.namespace_id, None)
            .await?;
        match checked {
            Some(position) => Ok(Some(WriterPosition::clone(&position))),
            None => self.reload_writer_position().await.map(Some),
        }
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
