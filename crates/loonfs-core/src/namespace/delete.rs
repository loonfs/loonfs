//! Namespace deletion status published as successive manifests.

use crate::commit::publish_view::WriterPosition;
use crate::error::{CoreError, Result};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::publish::ManifestChange;
use crate::manifest::{load_position_basis, PublicationReport, PublishedManifest};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::options::DeleteNamespaceOptions;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
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
    /// manifest when that manifest has folded the WAL through the position's
    /// head, under the session's writer epoch.
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
        self.update_manifest(
            basis,
            clock,
            MAX_CONTENTION_ATTEMPTS,
            |current: LoadedManifest| {
                let acquired_writer = &acquired_writer;
                let position = &position;
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
                    if payload.folded_wal_no < head.wal_no {
                        clock.ensure_metadata_publication_budget(&self.namespace_id)?;
                        let folded = self.fold_wal_tail(held.clone(), clock).await?;
                        *position
                            .lock()
                            .expect("the deletion's position lock should not be poisoned") = held
                            .filter(|_| folds_head(&folded.manifest, &head))
                            .map(|mut held| {
                                held.read_basis.basis =
                                    MetadataBasis::new(folded.manifest.manifest.clone());
                                held
                            });
                        return Ok(ManifestChange::Again);
                    }
                    payload.status = NamespaceStatus::Deleted {
                        deleted_at_ms: clock.request_at_ms(),
                    };
                    let response = DeleteNamespaceResponse {
                        namespace_id: self.namespace_id.clone(),
                        head_seq: payload.head_seq,
                    };
                    Ok(ManifestChange::Next(Box::new(payload), response))
                }
            },
        )
        .await
        .map(|(response, tombstone, _)| PublicationReport {
            response,
            published: Some(PublishedManifest::new(
                &tombstone.envelope,
                Some(clock.origin()),
            )),
        })
    }
}

/// Returns whether `manifest` is an active manifest, with the writer epoch
/// of `head`, that has folded the WAL through `head`, so `head` is also the
/// head at `manifest`.
fn folds_head(
    manifest: &PublishedManifest,
    head: &crate::namespace::state::NamespaceReadState,
) -> bool {
    !manifest.status.is_deleted()
        && manifest.writer_epoch == head.writer_epoch
        && manifest.folded_wal_no == head.wal_no
}
