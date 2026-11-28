//! Namespace deletion status published as successive manifests.

use crate::commit::publication_lane::LaneGuard;
use crate::commit::publish_view::WriterPosition;
use crate::error::Result;
use crate::manifest::fold::FoldWriteOutcome;
use crate::manifest::publish::ManifestPublicationOutcome;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::DeleteNamespaceResponse;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes the namespace's tombstone from `position`, a writer
    /// session's position whose head the caller checked against the
    /// request's expected head, after a fold of the position's WAL tail when
    /// the position's manifest doesn't include the tail.
    ///
    /// The caller keeps `lane`, the publication lane of the session, and the
    /// session's commit queue for the whole deletion, so the head is exact,
    /// the fold and the tombstone are publications of the lane, and no
    /// commit is written between them. Each publication puts once (see
    /// [`NamespaceStore::publish_manifest_in_turn`]), and the tombstone builds
    /// on the fold's manifest.
    pub(crate) async fn delete_namespace(
        &self,
        position: WriterPosition,
        lane: &LaneGuard<'_>,
        clock: &OperationClock,
    ) -> Result<DeleteNamespaceResponse> {
        // The deletion's whole run is bounded by the publication budget, so
        // a deletion past it writes nothing.
        clock.ensure_metadata_publication_budget(&self.namespace_id)?;
        if let FoldWriteOutcome::Written(writes) =
            self.write_fold_segments(&position, clock).await?
        {
            self.publish_fold_in_turn(lane.acquired_turn(), writes, clock)
                .await?;
        }
        let turn = lane.acquired_turn();
        let payload = turn
            .basis()
            .manifest()
            .payload()
            .tombstone(clock.request_at_ms());
        let head_seq = payload.head_seq;
        self.publish_manifest_in_turn(turn, payload, clock).await?;
        Ok(DeleteNamespaceResponse {
            namespace_id: self.namespace_id.clone(),
            head_seq,
        })
    }

    /// Publishes the tombstone of a namespace whose newest manifest, `current`,
    /// is initializing, as the next manifest, without a writer session: the
    /// namespace has no writer and no WAL objects, so there is nothing to fence
    /// or fold. The tombstone competes with manifest 2 for one number, so the
    /// namespace becomes deleted or active, never both, and the caller returns
    /// a result that depends on the manifest that won a lost put (see
    /// [`Self::publish_over_initializing`]).
    pub(crate) async fn tombstone_initializing_namespace(
        &self,
        current: Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<ManifestPublicationOutcome> {
        let deleted_at_ms = clock.request_at_ms();
        self.publish_over_initializing(current, clock, |payload| payload.tombstone(deleted_at_ms))
            .await
    }
}
