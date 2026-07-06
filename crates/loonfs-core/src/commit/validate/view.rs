//! Metadata lookup adapters for commit validation.
//!
//! The validator runs the same checks against either in-memory rows or an
//! object-store-backed metadata view, while preserving each entry point's error
//! type.

use super::super::metadata_overlay::CommitOverlayRows;
use super::super::{CommitValidationError, ValidatedOp};
use crate::error::CoreError;
use crate::metadata::{
    DirentryBindRecord, InMemoryMetadataView, InodeRecord, MetadataState, MetadataView,
    RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_api::{ChangeSeq, InodeId, NamePolicy, RevisionNo};
use loonfs_objectstore::ObjectStore;

/// Seq-scoped metadata lookups used by the shared commit validator.
///
/// Implementations include the accumulating commit overlay, so later ops see
/// rows earlier ops would persist.
pub(super) trait CommitValidationView {
    type Error: From<CommitValidationError>;

    async fn inode_at_seq(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error>;

    async fn visible_inode(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error>;

    async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>;

    async fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, Self::Error>;

    async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, Self::Error>;

    async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, Self::Error>;

    async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>;

    async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error>;

    async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool, Self::Error>;

    fn apply_validated_op_mut(&mut self, committed_seq: ChangeSeq, op: &ValidatedOp);
}

/// Maps shared view errors into the in-memory validator's error type.
fn commit_validation_from_core(error: CoreError) -> CommitValidationError {
    match error {
        CoreError::CommitValidation(error) => error,
        error => CommitValidationError::ValidatedPreviewApplyFailed(error.to_string()),
    }
}

/// In-memory validator view over base rows plus the current commit overlay.
pub(super) struct InMemoryValidationView<'a> {
    metadata_state: &'a MetadataState,
    committed_seq: ChangeSeq,
    name_policy: NamePolicy,
    overlay: CommitOverlayRows,
}

impl<'a> InMemoryValidationView<'a> {
    pub(super) fn new(
        metadata_state: &'a MetadataState,
        committed_seq: ChangeSeq,
        name_policy: NamePolicy,
    ) -> Self {
        Self {
            metadata_state,
            committed_seq,
            name_policy,
            overlay: CommitOverlayRows::new(),
        }
    }

    fn view(&self) -> InMemoryMetadataView<'_> {
        InMemoryMetadataView::in_memory(
            self.metadata_state,
            Some(self.overlay.rows()),
            self.committed_seq,
            self.name_policy,
        )
    }
}

impl CommitValidationView for InMemoryValidationView<'_> {
    type Error = CommitValidationError;

    async fn inode_at_seq(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.view()
            .inode_at_seq(inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn visible_inode(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.view()
            .visible_inode(inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view()
            .visible_child(parent_inode_id, name_key)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, Self::Error> {
        self.view()
            .visible_children(parent_inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        self.view()
            .latest_revision_record(inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        self.view()
            .revision_at_head(inode_id, revision_no)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view()
            .current_parent_binding_for_child(child_inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.view()
            .covering_subtree_tombstone(inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool, Self::Error> {
        self.view()
            .would_create_directory_cycle(inode_id, new_parent_inode_id)
            .await
            .map_err(commit_validation_from_core)
    }

    fn apply_validated_op_mut(&mut self, committed_seq: ChangeSeq, op: &ValidatedOp) {
        self.overlay.apply_validated_op_mut(committed_seq, op);
    }
}

/// Publish validator view over manifest-backed metadata plus accepted batch rows.
pub(super) struct PublishValidationView<'a, S: ObjectStore + ?Sized> {
    base_view: MetadataView<'a, 'a, S>,
    committed_seq: ChangeSeq,
    overlay: CommitOverlayRows,
}

impl<'a, S: ObjectStore + ?Sized> PublishValidationView<'a, S> {
    pub(super) fn new(
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

    fn view(&self) -> MetadataView<'_, 'a, S> {
        self.base_view
            .with_overlay(self.overlay.rows(), self.committed_seq)
    }
}

impl<S: ObjectStore + ?Sized> CommitValidationView for PublishValidationView<'_, S> {
    type Error = CoreError;

    async fn inode_at_seq(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.view().inode_at_seq(inode_id).await
    }

    async fn visible_inode(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.view().visible_inode(inode_id).await
    }

    async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view().visible_child(parent_inode_id, name_key).await
    }

    async fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, Self::Error> {
        self.view().visible_children(parent_inode_id).await
    }

    async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        self.view().latest_revision_record(inode_id).await
    }

    async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        self.view().revision_at_head(inode_id, revision_no).await
    }

    async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view()
            .current_parent_binding_for_child(child_inode_id)
            .await
    }

    async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.view().covering_subtree_tombstone(inode_id).await
    }

    async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool, Self::Error> {
        self.view()
            .would_create_directory_cycle(inode_id, new_parent_inode_id)
            .await
    }

    fn apply_validated_op_mut(&mut self, committed_seq: ChangeSeq, op: &ValidatedOp) {
        self.overlay.apply_validated_op_mut(committed_seq, op);
    }
}
