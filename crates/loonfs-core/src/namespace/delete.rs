//! Namespace deletion status published as successive manifests.

use crate::commit::publish_view::WriterPosition;
use crate::error::{CoreError, Result};
use crate::manifest::publish::{ManifestChange, ManifestUpdate};
use crate::manifest::{load_position_basis, PublicationReport};
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::options::DeleteNamespaceOptions;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::{DeleteNamespaceResponse, DeletionId};
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
    /// position when the position can't move. The tombstone records
    /// `options`' deletion ID, or a generated one when `options` has none.
    pub(crate) async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
        acquired_writer: AcquiredWriter,
        position: Option<WriterPosition>,
        clock: &OperationClock,
    ) -> Result<PublicationReport<DeleteNamespaceResponse>> {
        let basis = match &position {
            Some(position) => {
                load_position_basis(&self.store, &self.segment_cache, position).await?
            }
            None => None,
        };
        // Each attempt's future reads and updates the position through this
        // lock, because a future that a closure returns can't borrow the
        // closure's own state.
        let position = Mutex::new(position);
        let deletion_id = options.deletion_id_or_generate();
        self.update_manifest(basis, clock, |current: LoadedManifest| {
            let acquired_writer = &acquired_writer;
            let position = &position;
            let deletion_id = &deletion_id;
            async move {
                let mut payload = current.payload().clone();
                let held = position
                    .lock()
                    .expect("the deletion's position lock should not be poisoned")
                    .clone()
                    .filter(|held| held.read_basis.basis.manifest() == &current.manifest());
                let head = match &held {
                    Some(held) => held.read_basis.head.clone(),
                    None => self.load_read_anchor(clock.observe()).await?.read_state,
                };
                super::control::ensure_namespace_live(&head.namespace_id, &head.status)?;
                ensure_writer_not_fenced(&head, acquired_writer)?;
                if let Some(expected) = options.expected_head_seq {
                    if head.seq != expected {
                        return Err(CoreError::StaleHeadPrecondition {
                            expected,
                            actual: head.seq,
                        });
                    }
                }
                if payload.folded_wal_no < head.wal_no {
                    clock.ensure_metadata_publication_budget(&self.namespace_id)?;
                    let folded = self.fold_wal_tail(held.clone(), clock).await?;
                    *position
                        .lock()
                        .expect("the deletion's position lock should not be poisoned") =
                        held.and_then(|mut held| held.move_to(&folded.manifest).then_some(held));
                    return Ok(ManifestChange::Again);
                }
                payload.status = NamespaceStatus::Deleted {
                    deleted_at_ms: clock.request_at_ms(),
                    deletion_id: deletion_id.clone(),
                };
                // A tombstone lists no extension segments, so they're
                // collected once no pin keeps a manifest that lists them
                // (format spec, "Extensions").
                for entry in payload.extensions.values_mut() {
                    entry.runs.clear();
                }
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

    /// Returns the namespace ID and the final head when the namespace's
    /// current manifest is a tombstone with `deletion_id`, and `None` when
    /// the namespace is live, missing, or deleted under another deletion ID.
    ///
    /// A delete that finds a tombstone calls this, so a retry of the delete
    /// that published the tombstone returns what the first delete returned
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/retrying-creates-and-deletes.md#deleting-a-namespace)).
    /// The retry doesn't check `expected_head_seq` again.
    pub async fn find_deletion(
        &self,
        deletion_id: &DeletionId,
    ) -> Result<Option<DeleteNamespaceResponse>> {
        let current = self
            .load_current_manifest_if_present()
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        Ok(current
            .filter(|current| current.payload().status.deletion_id() == Some(deletion_id))
            .map(|tombstone| DeleteNamespaceResponse {
                namespace_id: self.namespace_id.clone(),
                head_seq: tombstone.payload().head_seq,
            }))
    }
}
