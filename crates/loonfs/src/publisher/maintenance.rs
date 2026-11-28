//! The maintenance that a host drives through a writer session, and its
//! admission, which the session's close and drain wait for.

use super::registry::NamespaceSession;
use super::{FoldWhen, NamespaceFold, NamespacePublisher};
use crate::maintenance::MetadataCompactionOutcome;
use crate::{CoreError, Error, MaintenanceCancellation, MetadataMaintenanceResponse, Result};
use loonfs_core::control::{Held, LoadedManifest};
use loonfs_core::publish::{UnitBasis, UnitReport, WriterPosition};
use loonfs_core::{ExtensionBasis, ExtensionPublication, FloorObjectReport};
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
    /// as a publication of the session's lane. The change puts its manifest
    /// without the engine, so no commit waits for it, and a change that the
    /// session's newest published manifest already has is confirmed first
    /// (see [`NamespacePublisher::run_confirmed`]). A session without a
    /// position first reads the newest manifest, with a read permit, and
    /// returns the unchanged window without a claim when that manifest
    /// already has it.
    pub(crate) async fn set_retention_window(
        &self,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<SetRetentionWindowResponse> {
        let publisher = self.publisher();
        let namespace = &publisher.namespace_store();
        if publisher.current_position().is_none() {
            let core = &publisher.context.runtime_core;
            let _permit = core.read_permit().await;
            let newest = namespace.load_live_basis(core.observation()).await?;
            let window_ms = newest.manifest().payload().retention_window_ms;
            if window_ms == retention_window_ms.map(std::num::NonZeroU64::from) {
                return Ok(SetRetentionWindowResponse {
                    namespace_id: publisher.namespace_id.clone(),
                    retention_window_ms: window_ms.map(std::num::NonZeroU64::get),
                });
            }
        }
        publisher
            .run_confirmed(move || async move {
                let lane = publisher.lock_lane().await;
                let turn = publisher.lane_turn(&lane).await?;
                let report = namespace
                    .set_retention_window_from(turn, retention_window_ms)
                    .await?;
                Ok(UnitReport::from_publication(report))
            })
            .await
    }

    /// Loads what a result without a put starts from, such as an extension's
    /// basis or the view that a retention advance plans from: `from_position`
    /// of the session's position (see
    /// [`NamespacePublisher::run_from_position`]), confirmed before the call
    /// returns it (see [`NamespacePublisher::run_confirmed`]), or `newest`, a
    /// fresh read of the newest manifest, for a session without a position
    /// and when the check doesn't confirm the position, unless the check
    /// found the session fenced, which returns `writer_fenced`. The load
    /// claims nothing, and it publishes nothing, so it takes no turn in the
    /// session's publication lane.
    pub(crate) async fn load_confirmed<T, Fut>(
        &self,
        from_position: impl Fn(WriterPosition) -> Fut,
        newest: impl Future<Output = Result<T>>,
    ) -> Result<T>
    where
        Fut: Future<Output = loonfs_core::Result<T>>,
    {
        let publisher = self.publisher();
        if publisher.current_position().is_none() {
            return newest.await;
        }
        let from_position = &from_position;
        let mut newest = Some(newest);
        let mut first = true;
        publisher
            .run_confirmed(move || {
                let newest = if first { None } else { newest.take() };
                first = false;
                async move {
                    if let Some(newest) = newest {
                        drop(publisher.unfenced_session()?);
                        return Ok(UnitReport::from_store(newest.await?));
                    }
                    Ok(UnitReport {
                        value: publisher.run_from_position(from_position).await?,
                        basis: UnitBasis::Session,
                    })
                }
            })
            .await
    }

    /// Publishes an extension's change (see
    /// [`NamespaceStore::publish_extension_from`](loonfs_core::NamespaceStore::publish_extension_from))
    /// as a publication of the session's lane, the way
    /// [`Self::set_retention_window`] publishes a window change. A change
    /// that is refused against the session's newest published manifest is
    /// confirmed by the session's check before the call returns it. A basis of another namespace returns `invalid_request` before
    /// anything is claimed (see [`ExtensionBasis::ensure_for`]).
    pub(crate) async fn publish_extension(
        &self,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<ExtensionPublication> {
        let publisher = self.publisher();
        basis.ensure_for(&publisher.namespace_id)?;
        let namespace = &publisher.namespace_store();
        publisher
            .run_confirmed(move || {
                let entry = entry.clone();
                async move {
                    let lane = publisher.lock_lane().await;
                    let turn = publisher.lane_turn(&lane).await?;
                    let report = namespace
                        .publish_extension_from(turn, basis, name, entry)
                        .await?;
                    Ok(UnitReport::from_publication(report))
                }
            })
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
