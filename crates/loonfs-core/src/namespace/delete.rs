//! Namespace deletion status published as successive manifests.

use crate::commit::publish_view::WriterPosition;
use crate::error::{CoreError, Result};
use crate::manifest::publish::{ManifestChange, ManifestUpdate};
use crate::manifest::PublicationReport;
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
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
    /// instead of reading the WAL tail, and the attempt's fold folds the
    /// position's tail. After that fold, the position moves onto the fold's
    /// manifest by [`WriterPosition::move_to`], and the deletion drops the
    /// position when the position can't move.
    pub(crate) async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
        acquired_writer: AcquiredWriter,
        position: Option<WriterPosition>,
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
        self.update_manifest(basis, clock, |current: LoadedManifest| {
            let acquired_writer = &acquired_writer;
            let position = &position;
            async move {
                let held = position
                    .lock()
                    .expect("the deletion's position lock should not be poisoned")
                    .clone()
                    .filter(|held| held.read_basis.basis.manifest() == &current.manifest());
                let head = match &held {
                    Some(held) => held.read_basis.head.clone(),
                    None => self.load_read_anchor(clock.observe()).await?.read_state,
                };
                super::control::ensure_namespace_live(&head.namespace_id, head.status)?;
                ensure_writer_not_fenced(&head, acquired_writer)?;
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
                    let folded = self.fold_wal_tail(held.clone(), clock).await?;
                    *position
                        .lock()
                        .expect("the deletion's position lock should not be poisoned") =
                        held.and_then(|mut held| held.move_to(&folded.manifest).then_some(held));
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
        current: PublicationBasis,
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
