//! The validator of each commit operation: the checks an operation runs and
//! the WAL deltas it produces.

use super::super::{CommitOperation, CommitValidationError, NewChild, ResolvedBinding};
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::commit::validate::checks::CommitNumbering;
use crate::error::Result;
use crate::metadata::{DirentryBindingRecord, InodeRecord};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition, RevisionSummary};
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    next_public_ordinal, AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo,
    ChangeSeq, ContentRef, DisplayName, InodeId, InodeKind, RevisionNo,
};

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
        let (inode_kind, revision) = match &child {
            NewChild::Directory => (InodeKind::Directory, None),
            NewChild::File(content_ref) => (
                InodeKind::File,
                Some(RevisionSummary {
                    revision_no: RevisionNo(1),
                    size_bytes: content_ref.size_bytes,
                    revision_committed_by: self.actor_id.clone(),
                    revision_committed_at_ms: self.committed_at_ms,
                }),
            ),
        };
        let create_index = self.numbering.reserve_delta_index()?;
        let bind_position = DeltaPosition {
            seq: self.committed_seq,
            delta_index: self.numbering.reserve_delta_index()?,
        };
        let mut deltas = vec![
            WalDelta::CreateInode {
                delta_index: create_index,
                inode_id: child_inode_id,
                inode_kind,
            },
            WalDelta::BindDirentry {
                delta_index: bind_position.delta_index,
                parent_inode_id,
                name_key: name_key.clone(),
                display_name: display_name.clone(),
                child_inode_id,
                child_kind: inode_kind,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
                revision,
            },
        ];
        if let NewChild::File(content_ref) = child {
            deltas.push(WalDelta::AppendFileRevision {
                delta_index: self.numbering.reserve_delta_index()?,
                inode_id: child_inode_id,
                parent_inode_id,
                name_key,
                display_name: display_name.clone(),
                bind_position,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
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
        let edge = self
            .resolve_current_binding(inode_id, CommitOperand::ReplaceTarget)
            .await?;
        Ok(vec![revision_delta(
            &mut self.numbering,
            &edge,
            revision_no,
            content_ref.clone(),
            append_offset,
        )?])
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
        let edge = self
            .resolve_current_binding(inode_id, CommitOperand::RestoreTarget)
            .await?;
        Ok(vec![revision_delta(
            &mut self.numbering,
            &edge,
            revision_no,
            source_revision.content_ref,
            None,
        )?])
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
        self.ensure_child_unbound(inode_id, Some(source_binding.bind_position))
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
        let revision = self.resolve_revision_summary(&inode).await?;
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
                revision,
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
        let revision = self.resolve_revision_summary(&inode).await?;
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
                revision,
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
}

/// The delta of a file's next revision, which writes the revision's summary
/// row under the file's current binding `edge`.
fn revision_delta(
    numbering: &mut CommitNumbering,
    edge: &DirentryBindingRecord,
    revision_no: RevisionNo,
    content_ref: ContentRef,
    append_offset: Option<u64>,
) -> std::result::Result<WalDelta, CommitValidationError> {
    Ok(WalDelta::AppendFileRevision {
        delta_index: numbering.reserve_delta_index()?,
        inode_id: edge.child_inode_id,
        parent_inode_id: edge.parent_inode_id,
        name_key: edge.name_key.clone(),
        display_name: edge
            .display_name()
            .expect("a current binding should be bound")
            .clone(),
        bind_position: edge.bind_position,
        child_created_by: edge.child_created_by.clone(),
        child_created_at_ms: edge.child_created_at_ms,
        revision_no,
        content_ref,
        layout: None,
        append_offset,
    })
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
        target: binding.bind_position,
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
            root_kind: inode.inode_kind,
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
