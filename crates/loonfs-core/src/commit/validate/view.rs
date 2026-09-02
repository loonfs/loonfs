//! The metadata view used while validating a commit for publication.

use super::super::materialize::materialize_validated_op;
use super::super::ValidatedOp;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::{ActorRef, ChangeSeq, CommitId};
use loonfs_objectstore::ObjectStore;

/// The publish view: it holds the loaded [`MetadataView`] plus the
/// accumulating commit overlay, rebuilding an overlaid view for each lookup.
pub(crate) struct PublishValidationView<'a, S: ObjectStore + ?Sized> {
    base_view: MetadataView<'a, 'a, S>,
    batch_accepted: &'a MetadataState,
    committed_seq: ChangeSeq,
    overlay: MetadataState,
}

impl<'a, S: ObjectStore + ?Sized> PublishValidationView<'a, S> {
    pub(crate) fn new(
        base_view: MetadataView<'a, 'a, S>,
        accepted_rows: &'a MetadataState,
        committed_seq: ChangeSeq,
    ) -> Self {
        Self {
            base_view,
            batch_accepted: accepted_rows,
            committed_seq,
            overlay: MetadataState::default(),
        }
    }

    /// The metadata the next operation resolves against: the loaded publish
    /// view plus every row this commit's earlier operations would persist.
    /// The planner reads through this so an operation resolves paths against
    /// what its predecessors did.
    pub(crate) fn view(&self) -> MetadataView<'_, 'a, S> {
        self.base_view
            .with_overlay(&self.overlay, self.batch_accepted, self.committed_seq)
    }
}

impl<S: ObjectStore + ?Sized> PublishValidationView<'_, S> {
    pub(crate) fn committed_seq(&self) -> ChangeSeq {
        self.committed_seq
    }

    pub(crate) fn apply_validated_op_mut(
        &mut self,
        commit_id: &CommitId,
        actor: &ActorRef,
        committed_at_ms: u64,
        op: &ValidatedOp,
    ) {
        for delta in &materialize_validated_op(op) {
            self.overlay.apply_committed_wal_delta_mut(
                self.committed_seq,
                commit_id,
                actor,
                committed_at_ms,
                &delta.wal_delta,
            );
        }
    }
}

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;
