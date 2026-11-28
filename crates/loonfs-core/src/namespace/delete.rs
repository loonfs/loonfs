//! Namespace deletion status published as successive manifests.

use crate::commit::publication_lane::LaneGuard;
use crate::commit::publish_view::{load_writer_position, WriterPosition};
use crate::commit::session_epoch::SessionEpoch;
use crate::error::{CoreError, Result};
use crate::manifest::publish::{ManifestChange, ManifestSender, ManifestUpdate};
use crate::manifest::PublicationReport;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::options::DeleteNamespaceOptions;
use loonfs_types::DeleteNamespaceResponse;
use std::sync::Mutex;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes the namespace's tombstone, after a fold of the WAL tail when
    /// the current manifest doesn't include the tail.
    ///
    /// A writer session passes its writer position as `position`. An attempt
    /// that builds on the position's manifest takes the head from the position
    /// instead of reading the WAL tail, and any other attempt takes the head
    /// and the tail from the writer's load. The attempt's fold folds that
    /// tail. After that fold, the position moves onto the fold's
    /// manifest by [`WriterPosition::move_to`], and the deletion drops the
    /// position when the position can't move.
    ///
    /// The caller keeps `lane`, the session's publication lane, for the
    /// whole deletion, so the fold and the tombstone are items of the lane.
    /// The tombstone's put is sent through `epoch` (see
    /// [`SessionEpoch::send_put`]).
    pub(crate) async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
        epoch: SessionEpoch<'_>,
        position: Option<WriterPosition>,
        lane: &LaneGuard<'_>,
        clock: &OperationClock,
    ) -> Result<PublicationReport<DeleteNamespaceResponse>> {
        let basis = match &position {
            Some(position) => self.load_position_basis(position).await?,
            None => None,
        };
        // Each attempt's future reads and updates the position through this
        // lock, because a future that a closure returns can't borrow the
        // closure's own state.
        let position = Mutex::new(position);
        let sender = ManifestSender::Session(&epoch);
        self.update_manifest_as(basis, clock, sender, |current: LoadedManifest| {
            let acquired_writer = epoch.writer();
            let position = &position;
            async move {
                let held = position
                    .lock()
                    .expect("the deletion's position lock should not be poisoned")
                    .clone()
                    .filter(|held| held.read_basis().basis.manifest() == &current.manifest());
                let held = match held {
                    Some(held) => held,
                    // The writer anchor's load returns `writer_fenced` for
                    // another writer's epoch, `namespace_corrupt` for a lower
                    // one, and then `namespace_deleted` for a tombstone (see
                    // `WriterAnchor`).
                    None => {
                        let anchor = self
                            .load_writer_read_anchor(acquired_writer, clock.observe())
                            .await?;
                        load_writer_position(
                            &anchor,
                            &self.current_segments(anchor.current_manifest()),
                        )?
                    }
                };
                let head = held.read_basis().head.clone();
                if let Some(expected) = options.expected_head_seq {
                    if head.seq != expected {
                        return Err(CoreError::StaleHeadPrecondition {
                            expected,
                            actual: head.seq,
                        });
                    }
                }
                if current.payload().folded_wal_no < head.wal_no {
                    clock.ensure_metadata_publication_budget(&self.namespace_id)?;
                    let mut held = held;
                    let folded = self
                        .fold_wal_tail_from(lane.turn_from(held.clone()), clock)
                        .await;
                    let moved = match folded {
                        Ok(folded) => held.move_to(&folded.current).then_some(held),
                        // The position is behind: its manifest is gone, or a
                        // manifest that won the fold's put ended the fold
                        // without a put. The next attempt drops it and loads
                        // the writer's anchor, which returns `writer_fenced`
                        // when another writer claimed the namespace.
                        Err(CoreError::HeadMoved(_)) => None,
                        Err(error) => return Err(error),
                    };
                    *position
                        .lock()
                        .expect("the deletion's position lock should not be poisoned") = moved;
                    return Ok(ManifestChange::Again);
                }
                let payload = current.payload().tombstone(clock.request_at_ms());
                let response = DeleteNamespaceResponse {
                    namespace_id: self.namespace_id.clone(),
                    head_seq: payload.head_seq,
                };
                Ok(ManifestChange::Next(Box::new(payload), response))
            }
        })
        .await
        .map(ManifestUpdate::into_report)
    }

    /// Publishes the tombstone of a namespace whose newest manifest, `current`,
    /// is initializing, as the next manifest, without a writer session: the
    /// namespace has no writer and no WAL objects, so there is nothing to fence
    /// or fold. The tombstone competes with manifest 2 for one number, so the
    /// namespace becomes deleted or active, never both.
    pub(crate) async fn tombstone_initializing_namespace(
        &self,
        current: Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<ManifestUpdate<TombstoneOutcome>> {
        self.update_manifest(Some(current), clock, |current: LoadedManifest| {
            let payload = current.payload();
            let change = match payload.status {
                NamespaceStatus::Initializing {} => ManifestChange::Next(
                    Box::new(payload.tombstone(clock.request_at_ms())),
                    TombstoneOutcome::Published(DeleteNamespaceResponse {
                        namespace_id: self.namespace_id.clone(),
                        head_seq: payload.head_seq,
                    }),
                ),
                NamespaceStatus::Active {} => ManifestChange::Finished(TombstoneOutcome::Active),
                NamespaceStatus::Deleted { .. } => {
                    ManifestChange::Finished(TombstoneOutcome::Deleted)
                }
            };
            async move { Ok(change) }
        })
        .await
    }
}

/// How a tombstone publication for an initializing namespace ended.
pub(crate) enum TombstoneOutcome {
    /// The call published the tombstone.
    Published(DeleteNamespaceResponse),
    /// Manifest 2 with the status `active` was published first, so the
    /// namespace is active.
    Active,
    /// The namespace was already deleted.
    Deleted,
}
