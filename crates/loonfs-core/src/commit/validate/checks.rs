//! The state checks that commit operation validators run: target
//! visibility, revision preconditions, binding state, and cycles.

use super::super::{CommitValidationError, ResolvedBinding};
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::error::{CoreError, Result};
use crate::metadata::{DirentryBindingRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeltaPosition, DirentryRevision};

use loonfs_types::{
    AccessRevisionNo, AttributesRevisionNo, ChangeSeq, DisplayName, InodeId, InodeKind, NameKey,
    RevisionNo,
};

#[derive(Debug, Default)]
pub(super) struct CommitNumbering {
    next_op_index: u32,
    next_delta_index: u32,
}

impl CommitNumbering {
    pub(super) fn reserve_op_index(&mut self) -> std::result::Result<u32, CommitValidationError> {
        let op_index = self.next_op_index;
        self.next_op_index = self
            .next_op_index
            .checked_add(1)
            .ok_or(CommitValidationError::OperationIndexOverflow)?;
        Ok(op_index)
    }

    pub(super) fn reserve_delta_index(
        &mut self,
    ) -> std::result::Result<u32, CommitValidationError> {
        let delta_index = self.next_delta_index;
        self.next_delta_index = self
            .next_delta_index
            .checked_add(1)
            .ok_or(CommitValidationError::DeltaIndexOverflow)?;
        Ok(delta_index)
    }
}

impl<S: ObjectStore + ?Sized> CommitValidationSession<'_, S> {
    /// Returns the `set` event of the deletion that `deletion_seq` identifies,
    /// which has to be the root's latest tombstone event and above the retention
    /// floor of the writer's basis
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    /// A request for a deletion that has expired, that a later event revoked,
    /// or that a later deletion replaced, fails here, so it can't cancel a
    /// different deletion.
    ///
    /// The floor check comes first, so a request for an expired deletion reads
    /// no tombstone. A base compaction relies on this rule: the WAL object of
    /// every accepted undelete records a basis floor below the deletion.
    pub(super) async fn resolve_recoverable_deletion(
        &self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
    ) -> Result<SubtreeTombstoneRecord> {
        if deletion_seq <= self.view().retention_floor_seq() {
            return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
        }
        let Some(deletion) = self.view().recoverable_deletion(inode_id).await? else {
            return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
        };
        if deletion.committed_seq >= self.committed_seq {
            return Err(CommitValidationError::UndeleteTargetsCurrentCommit { inode_id }.into());
        }
        if deletion.committed_seq != deletion_seq {
            return Err(CommitValidationError::UndeleteSequenceMismatch {
                inode_id,
                requested_seq: deletion_seq,
                active_seq: deletion.committed_seq,
            }
            .into());
        }
        Ok(deletion)
    }

    pub(super) async fn ensure_target_visible(
        &self,
        inode_id: InodeId,
        operand: CommitOperand,
    ) -> Result<()> {
        if self.view().visible_inode(inode_id).await?.is_none() {
            return Err(CommitValidationError::InodeMissing { operand, inode_id }.into());
        }
        Ok(())
    }

    pub(super) async fn ensure_inode_attributes_revision_is(
        &self,
        inode_id: InodeId,
        expected: AttributesRevisionNo,
    ) -> Result<()> {
        let actual = self
            .view()
            .attributes(inode_id)
            .await?
            .attributes_revision_no;
        if actual != expected {
            return Err(
                CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                    inode_id,
                    expected,
                    actual: Some(actual),
                }
                .into(),
            );
        }
        Ok(())
    }

    pub(super) async fn ensure_inode_access_revision_is(
        &self,
        inode_id: InodeId,
        expected: AccessRevisionNo,
    ) -> Result<()> {
        let actual = self
            .view()
            .latest_access_revision(inode_id)
            .await?
            .map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
        if actual != expected {
            return Err(CommitValidationError::UpdateAccessBaseRevisionMismatch {
                inode_id,
                expected,
                actual: Some(actual),
            }
            .into());
        }
        Ok(())
    }

    pub(super) async fn resolve_inode_kind(
        &self,
        inode_id: InodeId,
        expected: InodeKind,
        operand: CommitOperand,
    ) -> Result<InodeRecord> {
        let inode = self
            .view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CommitValidationError::InodeMissing { operand, inode_id })?;
        if inode.inode_kind != expected {
            return Err(CommitValidationError::InodeWrongKind {
                operand,
                inode_id,
                expected,
                actual: inode.inode_kind,
            }
            .into());
        }
        Ok(inode)
    }

    pub(super) async fn ensure_not_covered_by_tombstone(
        &self,
        inode_id: InodeId,
        operand: CommitOperand,
    ) -> Result<()> {
        if let Some(tombstone) = self.view().covering_subtree_tombstone(inode_id).await? {
            return Err(CommitValidationError::TargetUnderSubtreeTombstone {
                operand,
                inode_id,
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.committed_seq,
            }
            .into());
        }
        Ok(())
    }

    pub(super) async fn resolve_name_absent(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        rebinding_inode_id: Option<InodeId>,
        operand: CommitOperand,
    ) -> Result<NameKey> {
        self.resolve_inode_kind(parent_inode_id, InodeKind::Directory, operand)
            .await?;

        let name_key = NameKey::for_display_name(display_name);
        if let Some(existing) = self
            .view()
            .visible_child(parent_inode_id, &name_key)
            .await?
        {
            if rebinding_inode_id != Some(existing.child_inode_id) {
                return Err(CommitValidationError::NameTaken {
                    operand,
                    parent_inode_id,
                    name_key,
                    child_inode_id: existing.child_inode_id,
                }
                .into());
            }
        }
        Ok(name_key)
    }

    /// One parent per child: a bind names an inode with no current binding, or
    /// with the binding its own operation unbinds first.
    pub(super) async fn ensure_child_unbound(
        &self,
        child_inode_id: InodeId,
        unbinding: Option<DeltaPosition>,
    ) -> Result<()> {
        match self
            .view()
            .current_parent_binding_for_child(child_inode_id)
            .await?
        {
            Some(current) if Some(current.position()) != unbinding => {
                Err(CoreError::NamespaceCorrupt(format!(
                    "a commit binds inode `{child_inode_id}`, which is already bound as `{}` under \
                     parent inode `{}`",
                    current.name_key, current.parent_inode_id
                )))
            }
            _ => Ok(()),
        }
    }

    pub(super) async fn ensure_source_binding(&self, expected: &ResolvedBinding) -> Result<()> {
        let Some(existing) = self
            .view()
            .visible_child(expected.parent_inode_id, &expected.name_key)
            .await?
        else {
            return Err(CommitValidationError::BindingPreconditionMissing {
                parent_inode_id: expected.parent_inode_id,
                name_key: expected.name_key.clone(),
            }
            .into());
        };
        if existing.parent_inode_id != expected.parent_inode_id
            || existing.name_key != expected.name_key
            || existing.child_inode_id != expected.child_inode_id
            || existing.position() != expected.position
        {
            return Err(CommitValidationError::BindingPreconditionMismatch {
                target: format!(
                    "name `{}` under parent inode `{}`",
                    expected.name_key, expected.parent_inode_id
                ),
                expected_inode_id: Some(expected.child_inode_id),
                actual_inode_id: Some(existing.child_inode_id),
            }
            .into());
        }
        Ok(())
    }

    pub(super) async fn ensure_file_base_revision_is(
        &self,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        operand: CommitOperand,
    ) -> Result<()> {
        self.resolve_inode_kind(inode_id, InodeKind::File, operand)
            .await?;
        let actual = self
            .view()
            .latest_revision_record(inode_id)
            .await?
            .map(|revision| revision.revision_no);
        if actual != Some(expected_revision_no) {
            return Err(CommitValidationError::BaseRevisionMismatch {
                inode_id,
                expected: expected_revision_no,
                actual,
            }
            .into());
        }
        Ok(())
    }

    /// Returns the current binding of the file whose revision an operation
    /// writes. The revision's delta includes the binding's edge, and replay
    /// stores the revision under that edge
    /// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
    pub(super) async fn resolve_current_binding(
        &self,
        inode_id: InodeId,
        operand: CommitOperand,
    ) -> Result<DirentryBindingRecord> {
        Ok(self
            .view()
            .current_parent_binding_for_child(inode_id)
            .await?
            .ok_or(CommitValidationError::InodeMissing { operand, inode_id })?)
    }

    /// Returns the current revision that a bind of `inode` includes, which is
    /// `None` for a directory.
    pub(super) async fn resolve_bind_revision(
        &self,
        inode: &InodeRecord,
    ) -> Result<Option<DirentryRevision>> {
        if inode.inode_kind == InodeKind::Directory {
            return Ok(None);
        }
        let revision = self
            .view()
            .latest_revision_record(inode.inode_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "file `{}` has no revision to bind",
                    inode.inode_id
                ))
            })?;
        Ok(Some(DirentryRevision {
            revision_no: revision.revision_no,
            content_ref: revision.content_ref,
            revision_committed_by: revision.committed_by,
            revision_committed_at_ms: revision.committed_at_ms,
        }))
    }

    pub(super) async fn resolve_restore_source_revision(
        &self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    ) -> Result<RevisionRecord> {
        Ok(self
            .view()
            .revision_at_head(inode_id, source_revision_no)
            .await?
            .ok_or(
                CommitValidationError::RestoreRevisionSourceRevisionMissing {
                    inode_id,
                    source_revision_no,
                },
            )?)
    }

    pub(super) async fn ensure_rename_does_not_cycle(
        &self,
        inode: &InodeRecord,
        new_parent_inode_id: InodeId,
    ) -> Result<()> {
        if inode.inode_kind == InodeKind::Directory
            && self
                .view()
                .would_create_directory_cycle(inode.inode_id, new_parent_inode_id)
                .await?
        {
            return Err(CommitValidationError::RenameWouldCycleDirectory {
                inode_id: inode.inode_id,
                new_parent_inode_id,
            }
            .into());
        }
        Ok(())
    }
}
