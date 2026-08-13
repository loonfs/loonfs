//! The metadata view used while validating a commit for publication.

use super::super::metadata_overlay::CommitOverlayRows;
use super::super::ValidatedOp;
use crate::error::CoreError;
use crate::metadata::{
    DirentryBindRecord, InodeRecord, MetadataState, MetadataView, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::{
    ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, InodeId, NameKey, RevisionNo,
};
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
    pub(crate) async fn inode_at_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        self.view().inode_at_seq(inode_id).await
    }

    pub(crate) async fn visible_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        self.view().visible_inode(inode_id).await
    }

    pub(crate) async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.view().visible_child(parent_inode_id, name_key).await
    }

    pub(crate) async fn has_visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<bool, CoreError> {
        self.view().has_visible_children(parent_inode_id).await
    }

    pub(crate) async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        self.view().latest_revision_record(inode_id).await
    }

    pub(crate) async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        self.view().revision_at_head(inode_id, revision_no).await
    }

    pub(crate) async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.view()
            .current_parent_binding_for_child(child_inode_id)
            .await
    }

    pub(crate) async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        self.view().covering_subtree_tombstone(inode_id).await
    }

    pub(crate) async fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        self.view().active_subtree_tombstone(root_inode_id).await
    }

    pub(crate) async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool, CoreError> {
        self.view()
            .would_create_directory_cycle(inode_id, new_parent_inode_id)
            .await
    }

    pub(crate) async fn attributes_at_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<(AttributeRevisionNo, Attributes), CoreError> {
        self.view().attributes_at_visible_seq(inode_id).await
    }

    pub(crate) fn apply_validated_op_mut(
        &mut self,
        committed_seq: ChangeSeq,
        actor: &ActorRef,
        committed_at_ms: u64,
        op: &ValidatedOp,
    ) {
        self.overlay
            .apply_validated_op_mut(committed_seq, actor, committed_at_ms, op);
    }
}
