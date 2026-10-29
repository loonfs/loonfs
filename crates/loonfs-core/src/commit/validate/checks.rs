//! Operation validation for one commit.

use super::super::{CommitOperation, CommitValidationError, NewChild, ResolvedBinding};
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::error::{CoreError, Result};
use crate::metadata::{InodeRecord, RevisionRecord, SubtreeTombstoneRecord};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition};
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    next_public_ordinal, AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo,
    ChangeSeq, ContentRef, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
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

    fn reserve_delta_index(&mut self) -> std::result::Result<u32, CommitValidationError> {
        let delta_index = self.next_delta_index;
        self.next_delta_index = self
            .next_delta_index
            .checked_add(1)
            .ok_or(CommitValidationError::DeltaIndexOverflow)?;
        Ok(delta_index)
    }
}

impl<S: ObjectStore + ?Sized> CommitValidationSession<'_, S> {
    pub(super) async fn validate_operation(
        &mut self,
        op: &CommitOperation,
    ) -> Result<Vec<WalDelta>> {
        match op {
            CommitOperation::CreateDirectory {
                child_inode_id,
                parent_inode_id,
                display_name,
            } => {
                self.validate_create(
                    *child_inode_id,
                    *parent_inode_id,
                    display_name,
                    NewChild::Directory,
                )
                .await
            }
            CommitOperation::CreateFile {
                child_inode_id,
                parent_inode_id,
                display_name,
                content_ref,
            } => {
                self.validate_create(
                    *child_inode_id,
                    *parent_inode_id,
                    display_name,
                    NewChild::File(content_ref.clone()),
                )
                .await
            }
            CommitOperation::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
                append_offset,
            } => {
                self.validate_replace_file(
                    *inode_id,
                    *base_revision_no,
                    content_ref,
                    *append_offset,
                )
                .await
            }
            CommitOperation::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                self.validate_restore_revision(*inode_id, *source_revision_no, *base_revision_no)
                    .await
            }
            CommitOperation::DeleteFile {
                inode_id,
                source_binding,
            } => self.validate_delete_file(*inode_id, source_binding).await,
            CommitOperation::Rename {
                inode_id,
                source_binding,
                new_parent_inode_id,
                new_display_name,
            } => {
                self.validate_rename(
                    *inode_id,
                    source_binding,
                    *new_parent_inode_id,
                    new_display_name,
                )
                .await
            }
            CommitOperation::DeleteSubtree {
                root_inode_id,
                source_binding,
                require_empty,
            } => {
                self.validate_delete_subtree(*root_inode_id, source_binding, *require_empty)
                    .await
            }
            CommitOperation::Undelete {
                inode_id,
                deletion_seq,
                parent_inode_id,
                display_name,
            } => {
                self.validate_undelete(*inode_id, *deletion_seq, *parent_inode_id, display_name)
                    .await
            }
            CommitOperation::UpdateAttributes {
                inode_id,
                base_attributes_revision_no,
                attributes,
            } => {
                self.validate_update_attributes(*inode_id, *base_attributes_revision_no, attributes)
                    .await
            }
            CommitOperation::UpdateAccess {
                inode_id,
                base_access_revision_no,
                boundary,
                grants,
            } => {
                self.validate_update_access(*inode_id, *base_access_revision_no, *boundary, grants)
                    .await
            }
        }
    }

    async fn validate_create(
        &mut self,
        child_inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        child: NewChild,
    ) -> Result<Vec<WalDelta>> {
        let name_key = self
            .resolve_name_absent(
                parent_inode_id,
                display_name,
                None,
                CommitOperand::CreateParent,
            )
            .await?;
        self.ensure_child_unbound(child_inode_id, None).await?;
        self.ensure_not_covered_by_tombstone(parent_inode_id, CommitOperand::CreateParent)
            .await?;
        let inode_kind = match child {
            NewChild::Directory => InodeKind::Directory,
            NewChild::File(_) => InodeKind::File,
        };
        let mut deltas = vec![
            WalDelta::CreateInode {
                delta_index: self.numbering.reserve_delta_index()?,
                inode_id: child_inode_id,
                inode_kind,
            },
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id,
                name_key,
                display_name: display_name.clone(),
                child_inode_id,
                child_kind: inode_kind,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
            },
        ];
        if let NewChild::File(content_ref) = child {
            deltas.push(WalDelta::AppendFileRevision {
                delta_index: self.numbering.reserve_delta_index()?,
                inode_id: child_inode_id,
                revision_no: RevisionNo(1),
                content_ref,
                layout: None,
                append_offset: None,
            });
        }
        Ok(deltas)
    }

    async fn validate_replace_file(
        &mut self,
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_ref: &ContentRef,
        append_offset: Option<u64>,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_file_base_revision_is(inode_id, base_revision_no, CommitOperand::ReplaceTarget)
            .await?;
        let revision_no = next_revision_no(inode_id, base_revision_no)?;
        self.ensure_not_covered_by_tombstone(inode_id, CommitOperand::ReplaceTarget)
            .await?;
        Ok(vec![WalDelta::AppendFileRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            revision_no,
            content_ref: content_ref.clone(),
            layout: None,
            append_offset,
        }])
    }

    async fn validate_restore_revision(
        &mut self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_file_base_revision_is(inode_id, base_revision_no, CommitOperand::RestoreTarget)
            .await?;
        let source_revision = self
            .resolve_restore_source_revision(inode_id, source_revision_no)
            .await?;
        let revision_no = next_revision_no(inode_id, base_revision_no)?;
        self.ensure_not_covered_by_tombstone(inode_id, CommitOperand::RestoreTarget)
            .await?;
        Ok(vec![WalDelta::AppendFileRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            revision_no,
            content_ref: source_revision.content_ref,
            layout: None,
            append_offset: None,
        }])
    }

    async fn validate_delete_file(
        &mut self,
        inode_id: InodeId,
        source_binding: &ResolvedBinding,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_source_binding(source_binding).await?;
        let inode = self
            .resolve_inode_kind(inode_id, InodeKind::File, CommitOperand::DeleteTarget)
            .await?;
        self.ensure_not_covered_by_tombstone(inode_id, CommitOperand::DeleteTarget)
            .await?;
        delete_deltas(&mut self.numbering, &inode, source_binding)
    }

    async fn validate_rename(
        &mut self,
        inode_id: InodeId,
        source_binding: &ResolvedBinding,
        new_parent_inode_id: InodeId,
        new_display_name: &DisplayName,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_source_binding(source_binding).await?;
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::RenameSource,
                inode_id,
            },
        )?;
        let new_name_key = self
            .resolve_name_absent(
                new_parent_inode_id,
                new_display_name,
                Some(inode_id),
                CommitOperand::RenameTargetParent,
            )
            .await?;
        self.ensure_child_unbound(inode_id, Some(source_binding.position))
            .await?;
        self.ensure_rename_does_not_cycle(&inode, new_parent_inode_id)
            .await?;
        self.ensure_not_covered_by_tombstone(inode_id, CommitOperand::RenameSource)
            .await?;
        self.ensure_not_covered_by_tombstone(
            new_parent_inode_id,
            CommitOperand::RenameTargetParent,
        )
        .await?;
        Ok(vec![
            unbind_delta(&mut self.numbering, source_binding, &inode)?,
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id: new_parent_inode_id,
                name_key: new_name_key,
                display_name: new_display_name.clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
            },
        ])
    }

    async fn validate_delete_subtree(
        &mut self,
        root_inode_id: InodeId,
        source_binding: &ResolvedBinding,
        require_empty: bool,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_source_binding(source_binding).await?;
        let operand = if require_empty {
            CommitOperand::EmptyDirectoryTarget
        } else {
            CommitOperand::SubtreeRoot
        };
        let inode = self
            .resolve_inode_kind(root_inode_id, InodeKind::Directory, operand)
            .await?;
        if require_empty && self.view().has_visible_children(root_inode_id).await? {
            return Err(CommitValidationError::DirectoryNotEmpty {
                inode_id: root_inode_id,
            }
            .into());
        }
        self.ensure_not_covered_by_tombstone(root_inode_id, CommitOperand::SubtreeRoot)
            .await?;
        delete_deltas(&mut self.numbering, &inode, source_binding)
    }

    async fn validate_undelete(
        &mut self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Vec<WalDelta>> {
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::UndeleteTarget,
                inode_id,
            },
        )?;
        let deletion = self
            .resolve_recoverable_deletion(inode_id, deletion_seq)
            .await?;
        let name_key = self
            .resolve_name_absent(
                parent_inode_id,
                display_name,
                None,
                CommitOperand::UndeleteTarget,
            )
            .await?;
        self.ensure_child_unbound(inode_id, None).await?;
        self.ensure_not_covered_by_tombstone(parent_inode_id, CommitOperand::UndeleteTarget)
            .await?;
        Ok(vec![
            WalDelta::RevokeSubtreeTombstone {
                delta_index: self.numbering.reserve_delta_index()?,
                root_inode_id: inode_id,
                target: deletion.position(),
            },
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id,
                name_key,
                display_name: display_name.clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
            },
        ])
    }

    async fn validate_update_attributes(
        &mut self,
        inode_id: InodeId,
        base_attributes_revision_no: AttributesRevisionNo,
        attributes: &Attributes,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_target_visible(inode_id, CommitOperand::AttributeTarget)
            .await?;
        self.ensure_inode_attributes_revision_is(inode_id, base_attributes_revision_no)
            .await?;
        let attributes_revision_no =
            next_attributes_revision_no(inode_id, base_attributes_revision_no)?;
        Ok(vec![WalDelta::AppendAttributesRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            attributes_revision_no,
            attributes: attributes.clone(),
        }])
    }

    async fn validate_update_access(
        &mut self,
        inode_id: InodeId,
        base_access_revision_no: AccessRevisionNo,
        boundary: bool,
        grants: &AccessGrants,
    ) -> Result<Vec<WalDelta>> {
        self.ensure_target_visible(inode_id, CommitOperand::AccessTarget)
            .await?;
        self.ensure_inode_access_revision_is(inode_id, base_access_revision_no)
            .await?;
        let access_revision_no = next_access_revision_no(inode_id, base_access_revision_no)?;
        Ok(vec![WalDelta::AppendAccessRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            access_revision_no,
            boundary,
            grants: grants.clone(),
        }])
    }

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
    async fn resolve_recoverable_deletion(
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

    async fn ensure_target_visible(&self, inode_id: InodeId, operand: CommitOperand) -> Result<()> {
        if self.view().visible_inode(inode_id).await?.is_none() {
            return Err(CommitValidationError::InodeMissing { operand, inode_id }.into());
        }
        Ok(())
    }

    async fn ensure_inode_attributes_revision_is(
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

    async fn ensure_inode_access_revision_is(
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

    async fn resolve_inode_kind(
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

    async fn ensure_not_covered_by_tombstone(
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

    async fn resolve_name_absent(
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
    async fn ensure_child_unbound(
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

    async fn ensure_source_binding(&self, expected: &ResolvedBinding) -> Result<()> {
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

    async fn ensure_file_base_revision_is(
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

    async fn resolve_restore_source_revision(
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

    async fn ensure_rename_does_not_cycle(
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

fn unbind_delta(
    numbering: &mut CommitNumbering,
    binding: &ResolvedBinding,
    inode: &InodeRecord,
) -> std::result::Result<WalDelta, CommitValidationError> {
    Ok(WalDelta::UnbindDirentry {
        delta_index: numbering.reserve_delta_index()?,
        parent_inode_id: binding.parent_inode_id,
        name_key: binding.name_key.clone(),
        display_name: binding.display_name.clone(),
        child_inode_id: binding.child_inode_id,
        child_kind: inode.inode_kind,
        child_created_by: inode.committed_by.clone(),
        child_created_at_ms: inode.committed_at_ms,
        target: binding.position,
    })
}

fn delete_deltas(
    numbering: &mut CommitNumbering,
    inode: &InodeRecord,
    binding: &ResolvedBinding,
) -> Result<Vec<WalDelta>> {
    Ok(vec![
        unbind_delta(numbering, binding, inode)?,
        WalDelta::TombstoneSubtree {
            delta_index: numbering.reserve_delta_index()?,
            root_inode_id: inode.inode_id,
            deleted_binding: DeletedBinding {
                parent_inode_id: binding.parent_inode_id,
                name_key: binding.name_key.clone(),
                display_name: binding.display_name.clone(),
            },
        },
    ])
}

fn next_access_revision_no(
    inode_id: InodeId,
    base_access_revision_no: AccessRevisionNo,
) -> std::result::Result<AccessRevisionNo, CommitValidationError> {
    next_public_ordinal(base_access_revision_no.0)
        .map(AccessRevisionNo)
        .ok_or(CommitValidationError::UpdateAccessRevisionOverflow {
            inode_id,
            base_access_revision_no,
        })
}

fn next_revision_no(
    inode_id: InodeId,
    base_revision_no: RevisionNo,
) -> std::result::Result<RevisionNo, CommitValidationError> {
    next_public_ordinal(base_revision_no.0)
        .map(RevisionNo)
        .ok_or(CommitValidationError::RevisionOverflow {
            inode_id,
            base_revision_no,
        })
}

fn next_attributes_revision_no(
    inode_id: InodeId,
    base_attributes_revision_no: AttributesRevisionNo,
) -> std::result::Result<AttributesRevisionNo, CommitValidationError> {
    next_public_ordinal(base_attributes_revision_no.0)
        .map(AttributesRevisionNo)
        .ok_or(CommitValidationError::UpdateAttributesRevisionOverflow {
            inode_id,
            base_attributes_revision_no,
        })
}

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use loonfs_types::MAX_PUBLIC_INTEGER;
    use std::assert_matches;

    #[test]
    fn revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        let at_maximum = next_revision_no(InodeId(2), RevisionNo(MAX_PUBLIC_INTEGER - 1))
            .expect("advance to public maximum");
        assert_eq!(at_maximum, RevisionNo(MAX_PUBLIC_INTEGER));

        assert_matches!(
            next_revision_no(InodeId(2), RevisionNo(MAX_PUBLIC_INTEGER)),
            Err(CommitValidationError::RevisionOverflow {
                inode_id: InodeId(2),
                base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
            })
        );
    }

    #[test]
    fn attribute_revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_attributes_revision_no(InodeId(2), AttributesRevisionNo(MAX_PUBLIC_INTEGER - 1),)
                .expect("advance to public maximum"),
            AttributesRevisionNo(MAX_PUBLIC_INTEGER)
        );
        assert_matches!(
            next_attributes_revision_no(InodeId(2), AttributesRevisionNo(MAX_PUBLIC_INTEGER),),
            Err(CommitValidationError::UpdateAttributesRevisionOverflow {
                inode_id: InodeId(2),
                base_attributes_revision_no: AttributesRevisionNo(MAX_PUBLIC_INTEGER),
            })
        );
    }
}
