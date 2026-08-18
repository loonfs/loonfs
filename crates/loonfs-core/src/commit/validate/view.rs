//! The metadata view used while validating a commit for publication.

use super::super::metadata_overlay::CommitOverlayRows;
use super::super::ValidatedOp;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::{ActorRef, ChangeSeq, CommitId};
use loonfs_objectstore::ObjectStore;

/// The publish view: it holds the loaded [`MetadataView`] plus the
/// accumulating commit overlay (seeded from the rows already accepted earlier
/// in the batch), rebuilding an overlaid view for each lookup so object-store
/// failures surface as [`CoreError`].
pub(crate) struct PublishValidationView<'a, S: ObjectStore + ?Sized> {
    base_view: MetadataView<'a, 'a, S>,
    committed_seq: ChangeSeq,
    overlay: CommitOverlayRows,
}

impl<'a, S: ObjectStore + ?Sized> PublishValidationView<'a, S> {
    pub(crate) fn new(
        base_view: MetadataView<'a, 'a, S>,
        accepted_rows: &MetadataState,
        committed_seq: ChangeSeq,
    ) -> Self {
        Self {
            base_view,
            committed_seq,
            overlay: CommitOverlayRows::from_rows(accepted_rows),
        }
    }

    /// The metadata the next operation resolves against: the loaded publish
    /// view plus every row this commit's earlier operations would persist.
    /// The planner reads through this so an operation resolves paths against
    /// what its predecessors did.
    pub(crate) fn view(&self) -> MetadataView<'_, 'a, S> {
        self.base_view
            .with_overlay(self.overlay.rows(), self.committed_seq)
    }
}

impl<S: ObjectStore + ?Sized> PublishValidationView<'_, S> {
    pub(crate) fn apply_validated_op_mut(
        &mut self,
        committed_seq: ChangeSeq,
        commit_id: &CommitId,
        actor: &ActorRef,
        committed_at_ms: u64,
        op: &ValidatedOp,
    ) {
        self.overlay
            .apply_validated_op_mut(committed_seq, commit_id, actor, committed_at_ms, op);
    }
}
