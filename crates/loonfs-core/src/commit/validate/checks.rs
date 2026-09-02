//! Operation validation for one commit.

use super::super::{CommitOp, CommitValidationError, ResolvedBinding, ValidatedOp};
use super::error::CommitOperand;
use super::view::PublishValidationView;
use crate::error::CoreError;
use crate::metadata::{BindingIdentity, InodeRecord, RevisionRecord, SubtreeTombstoneRecord};
use loonfs_api::{
    next_public_ordinal, ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, CommitId,
    ContentRef, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Default)]
pub(crate) struct CommitNumbering {
    next_op_index: u32,
    next_delta_index: u32,
}

impl CommitNumbering {
    fn reserve_op_index(&mut self) -> Result<u32, CommitValidationError> {
        let op_index = self.next_op_index;
        self.next_op_index = self
            .next_op_index
            .checked_add(1)
            .ok_or(CommitValidationError::OpIndexOverflow)?;
        Ok(op_index)
    }

    fn reserve_delta_index(&mut self) -> Result<u32, CommitValidationError> {
        let delta_index = self.next_delta_index;
        self.next_delta_index = self
            .next_delta_index
            .checked_add(1)
            .ok_or(CommitValidationError::DeltaIndexOverflow)?;
        Ok(delta_index)
    }
}

pub(crate) async fn validate_ops<S: ObjectStore + ?Sized>(
    ops: &[CommitOp],
    view: &mut PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    commit_id: &CommitId,
    actor: &ActorRef,
    committed_at_ms: u64,
) -> Result<Vec<ValidatedOp>, CoreError> {
    let mut validated_ops = Vec::with_capacity(ops.len());

    for op in ops {
        let op_index = numbering.reserve_op_index()?;
        let validated_op = match op {
            CommitOp::CreateDirectory {
                child_inode_id,
                parent_inode_id,
                display_name,
            } => {
                validate_create_directory(
                    view,
                    numbering,
                    op_index,
                    *child_inode_id,
                    *parent_inode_id,
                    display_name,
                )
                .await?
            }
            CommitOp::CreateFile {
                child_inode_id,
                parent_inode_id,
                display_name,
                content_ref,
            } => {
                validate_create_file(
                    view,
                    numbering,
                    op_index,
                    *child_inode_id,
                    *parent_inode_id,
                    display_name,
                    content_ref,
                )
                .await?
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                validate_replace_file(
                    view,
                    numbering,
                    op_index,
                    *inode_id,
                    *base_revision_no,
                    content_ref,
                )
                .await?
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                validate_restore_revision(
                    view,
                    numbering,
                    op_index,
                    *inode_id,
                    *source_revision_no,
                    *base_revision_no,
                )
                .await?
            }
            CommitOp::DeleteFile {
                inode_id,
                source_binding,
            } => validate_delete_file(view, numbering, op_index, *inode_id, source_binding).await?,
            CommitOp::Rename {
                inode_id,
                source_binding,
                new_parent_inode_id,
                new_display_name,
            } => {
                validate_rename(
                    view,
                    numbering,
                    op_index,
                    *inode_id,
                    source_binding,
                    *new_parent_inode_id,
                    new_display_name,
                )
                .await?
            }
            CommitOp::DeleteSubtree {
                root_inode_id,
                source_binding,
                require_empty,
            } => {
                validate_delete_subtree(
                    view,
                    numbering,
                    op_index,
                    *root_inode_id,
                    source_binding,
                    *require_empty,
                )
                .await?
            }
            CommitOp::Undelete {
                inode_id,
                deletion_seq,
                parent_inode_id,
                display_name,
            } => {
                validate_undelete(
                    view,
                    numbering,
                    op_index,
                    *inode_id,
                    *deletion_seq,
                    *parent_inode_id,
                    display_name,
                )
                .await?
            }
            CommitOp::UpdateAttributes {
                inode_id,
                base_attributes_revision_no,
                attributes,
            } => {
                validate_update_attributes(
                    view,
                    numbering,
                    op_index,
                    *inode_id,
                    *base_attributes_revision_no,
                    attributes,
                )
                .await?
            }
        };
        view.apply_validated_op_mut(commit_id, actor, committed_at_ms, &validated_op);
        validated_ops.push(validated_op);
    }

    Ok(validated_ops)
}

async fn validate_create_directory<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    child_inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<ValidatedOp, CoreError> {
    let name_key = validate_name_absent(
        view,
        parent_inode_id,
        display_name,
        None,
        CommitOperand::CreateParent,
    )
    .await?;
    validate_not_covered_by_tombstone(view, parent_inode_id, CommitOperand::CreateParent).await?;
    Ok(ValidatedOp::CreateDir {
        op_index,
        parent_inode_id,
        display_name: display_name.clone(),
        name_key,
        child_inode_id,
        create_inode_delta_index: numbering.reserve_delta_index()?,
        bind_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_create_file<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    child_inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    content_ref: &ContentRef,
) -> Result<ValidatedOp, CoreError> {
    let name_key = validate_name_absent(
        view,
        parent_inode_id,
        display_name,
        None,
        CommitOperand::CreateParent,
    )
    .await?;
    validate_not_covered_by_tombstone(view, parent_inode_id, CommitOperand::CreateParent).await?;
    Ok(ValidatedOp::CreateFile {
        op_index,
        parent_inode_id,
        display_name: display_name.clone(),
        name_key,
        child_inode_id,
        content_ref: content_ref.clone(),
        create_inode_delta_index: numbering.reserve_delta_index()?,
        bind_delta_index: numbering.reserve_delta_index()?,
        revision_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_replace_file<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    content_ref: &ContentRef,
) -> Result<ValidatedOp, CoreError> {
    validate_file_base_revision_is(
        view,
        inode_id,
        base_revision_no,
        CommitOperand::ReplaceTarget,
    )
    .await?;
    let revision_no =
        next_revision_no(inode_id, base_revision_no, |inode_id, base_revision_no| {
            CommitValidationError::ReplaceFileRevisionOverflow {
                inode_id,
                base_revision_no,
            }
        })?;
    validate_not_covered_by_tombstone(view, inode_id, CommitOperand::ReplaceTarget).await?;
    Ok(ValidatedOp::ReplaceFile {
        op_index,
        inode_id,
        revision_no,
        content_ref: content_ref.clone(),
        revision_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_restore_revision<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
    base_revision_no: RevisionNo,
) -> Result<ValidatedOp, CoreError> {
    validate_file_base_revision_is(
        view,
        inode_id,
        base_revision_no,
        CommitOperand::RestoreTarget,
    )
    .await?;
    let source_revision =
        validate_restore_source_revision(view, inode_id, source_revision_no).await?;
    let revision_no =
        next_revision_no(inode_id, base_revision_no, |inode_id, base_revision_no| {
            CommitValidationError::RestoreRevisionOverflow {
                inode_id,
                base_revision_no,
            }
        })?;
    validate_not_covered_by_tombstone(view, inode_id, CommitOperand::RestoreTarget).await?;
    Ok(ValidatedOp::RestoreRevision {
        op_index,
        inode_id,
        source_revision_no,
        revision_no,
        content_ref: source_revision.content_ref,
        revision_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_delete_file<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    source_binding: &ResolvedBinding,
) -> Result<ValidatedOp, CoreError> {
    validate_source_binding(view, source_binding).await?;
    validate_inode_kind(view, inode_id, InodeKind::File, CommitOperand::DeleteTarget).await?;
    validate_not_covered_by_tombstone(view, inode_id, CommitOperand::DeleteTarget).await?;
    Ok(ValidatedOp::DeleteFile {
        op_index,
        inode_id,
        source_binding: source_binding.clone(),
        unbind_delta_index: numbering.reserve_delta_index()?,
        tombstone_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_rename<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    source_binding: &ResolvedBinding,
    new_parent_inode_id: InodeId,
    new_display_name: &DisplayName,
) -> Result<ValidatedOp, CoreError> {
    validate_source_binding(view, source_binding).await?;
    let inode =
        view.view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CommitValidationError::InodeMissing {
                operand: CommitOperand::RenameSource,
                inode_id,
            })?;
    let new_name_key = validate_name_absent(
        view,
        new_parent_inode_id,
        new_display_name,
        Some(inode_id),
        CommitOperand::RenameTargetParent,
    )
    .await?;
    validate_rename_does_not_cycle(view, &inode, new_parent_inode_id).await?;
    validate_not_covered_by_tombstone(view, inode_id, CommitOperand::RenameSource).await?;
    validate_not_covered_by_tombstone(view, new_parent_inode_id, CommitOperand::RenameTargetParent)
        .await?;
    Ok(ValidatedOp::Rename {
        op_index,
        inode_id,
        source_binding: source_binding.clone(),
        new_parent_inode_id,
        new_display_name: new_display_name.clone(),
        new_name_key,
        unbind_delta_index: numbering.reserve_delta_index()?,
        bind_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_delete_subtree<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    root_inode_id: InodeId,
    source_binding: &ResolvedBinding,
    require_empty: bool,
) -> Result<ValidatedOp, CoreError> {
    validate_source_binding(view, source_binding).await?;
    let operand = if require_empty {
        CommitOperand::EmptyDirectoryTarget
    } else {
        CommitOperand::SubtreeRoot
    };
    validate_inode_kind(view, root_inode_id, InodeKind::Directory, operand).await?;
    if require_empty && view.view().has_visible_children(root_inode_id).await? {
        return Err(CommitValidationError::DirectoryNotEmpty {
            inode_id: root_inode_id,
        }
        .into());
    }
    validate_not_covered_by_tombstone(view, root_inode_id, CommitOperand::SubtreeRoot).await?;
    Ok(ValidatedOp::DeleteSubtree {
        op_index,
        root_inode_id,
        source_binding: source_binding.clone(),
        unbind_delta_index: numbering.reserve_delta_index()?,
        tombstone_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_undelete<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<ValidatedOp, CoreError> {
    let active = validate_undelete_target(view, inode_id, deletion_seq).await?;
    let name_key = validate_name_absent(
        view,
        parent_inode_id,
        display_name,
        None,
        CommitOperand::UndeleteTarget,
    )
    .await?;
    validate_not_covered_by_tombstone(view, parent_inode_id, CommitOperand::UndeleteTarget).await?;
    Ok(ValidatedOp::Undelete {
        op_index,
        inode_id,
        parent_inode_id,
        display_name: display_name.clone(),
        name_key,
        target: active.generation,
        revoke_tombstone_delta_index: numbering.reserve_delta_index()?,
        bind_delta_index: numbering.reserve_delta_index()?,
    })
}

async fn validate_update_attributes<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    numbering: &mut CommitNumbering,
    op_index: u32,
    inode_id: InodeId,
    base_attributes_revision_no: AttributeRevisionNo,
    attributes: &Attributes,
) -> Result<ValidatedOp, CoreError> {
    validate_attributes_target_visible(view, inode_id).await?;
    validate_inode_attributes_revision_is(view, inode_id, base_attributes_revision_no).await?;
    let attributes_revision_no =
        next_attributes_revision_no(inode_id, base_attributes_revision_no)?;
    validate_not_covered_by_tombstone(view, inode_id, CommitOperand::AttributeTarget).await?;
    Ok(ValidatedOp::UpdateAttributes {
        op_index,
        inode_id,
        attributes_revision_no,
        attributes: attributes.clone(),
        attributes_delta_index: numbering.reserve_delta_index()?,
    })
}

fn next_revision_no(
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    overflow: impl FnOnce(InodeId, RevisionNo) -> CommitValidationError,
) -> Result<RevisionNo, CommitValidationError> {
    next_public_ordinal(base_revision_no.0)
        .map(RevisionNo)
        .ok_or_else(|| overflow(inode_id, base_revision_no))
}

fn next_attributes_revision_no(
    inode_id: InodeId,
    base_attributes_revision_no: AttributeRevisionNo,
) -> Result<AttributeRevisionNo, CommitValidationError> {
    next_public_ordinal(base_attributes_revision_no.0)
        .map(AttributeRevisionNo)
        .ok_or(CommitValidationError::UpdateAttributesRevisionOverflow {
            inode_id,
            base_attributes_revision_no,
        })
}

async fn validate_undelete_target<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
) -> Result<SubtreeTombstoneRecord, CoreError> {
    let committed_seq = view.committed_seq();
    if view.view().inode_at_seq(inode_id).await?.is_none() {
        return Err(CommitValidationError::InodeMissing {
            operand: CommitOperand::UndeleteTarget,
            inode_id,
        }
        .into());
    }
    if deletion_seq >= committed_seq {
        return Err(CommitValidationError::UndeleteTargetsCurrentCommit {
            inode_id,
            requested_seq: deletion_seq,
        }
        .into());
    }
    let Some(active) = view.view().active_subtree_tombstone(inode_id).await? else {
        return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
    };
    if active.generation.seq != deletion_seq {
        return Err(CommitValidationError::UndeleteGenerationMismatch {
            inode_id,
            requested_seq: deletion_seq,
            active_seq: active.generation.seq,
        }
        .into());
    }

    Ok(active)
}

async fn validate_attributes_target_visible<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
) -> Result<(), CoreError> {
    if view.view().visible_inode(inode_id).await?.is_none() {
        return Err(CommitValidationError::InodeMissing {
            operand: CommitOperand::AttributeTarget,
            inode_id,
        }
        .into());
    }
    Ok(())
}

async fn validate_inode_attributes_revision_is<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected: AttributeRevisionNo,
) -> Result<Attributes, CoreError> {
    let (actual, attributes) = view.view().attributes_at_visible_seq(inode_id).await?;
    if actual != expected {
        return Err(
            CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            }
            .into(),
        );
    }
    Ok(attributes)
}

async fn validate_inode_kind<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected: InodeKind,
    operand: CommitOperand,
) -> Result<InodeRecord, CoreError> {
    let inode = view
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

async fn validate_not_covered_by_tombstone<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    operand: CommitOperand,
) -> Result<(), CoreError> {
    if let Some(tombstone) = view.view().covering_subtree_tombstone(inode_id).await? {
        return Err(CommitValidationError::TargetUnderSubtreeTombstone {
            operand,
            inode_id,
            root_inode_id: tombstone.root_inode_id,
            tombstone_seq: tombstone.generation.seq,
        }
        .into());
    }
    Ok(())
}

async fn validate_name_absent<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    rebinding_inode_id: Option<InodeId>,
    operand: CommitOperand,
) -> Result<NameKey, CoreError> {
    validate_inode_kind(view, parent_inode_id, InodeKind::Directory, operand).await?;

    let name_key = NameKey::for_display_name(display_name);
    if let Some(existing) = view
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

async fn validate_source_binding<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    expected: &ResolvedBinding,
) -> Result<(), CoreError> {
    let Some(existing) = view
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
    let expected_identity = BindingIdentity {
        parent_inode_id: expected.parent_inode_id,
        name_key: expected.name_key.clone(),
        child_inode_id: expected.child_inode_id,
        bind_seq: expected.bind_seq,
        bind_delta_index: expected.bind_delta_index,
    };
    if BindingIdentity::from(&existing) != expected_identity {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            parent_inode_id: expected.parent_inode_id,
            name_key: expected.name_key.clone(),
            expected_child_inode_id: expected.child_inode_id,
            actual_child_inode_id: existing.child_inode_id,
        }
        .into());
    }
    Ok(())
}

async fn validate_file_base_revision_is<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    operand: CommitOperand,
) -> Result<(), CoreError> {
    validate_inode_kind(view, inode_id, InodeKind::File, operand).await?;
    let actual = view
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

async fn validate_restore_source_revision<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
) -> Result<RevisionRecord, CoreError> {
    Ok(view
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

async fn validate_rename_does_not_cycle<S: ObjectStore + ?Sized>(
    view: &PublishValidationView<'_, S>,
    inode: &InodeRecord,
    new_parent_inode_id: InodeId,
) -> Result<(), CoreError> {
    if inode.inode_kind == InodeKind::Directory
        && view
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

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use loonfs_api::MAX_PUBLIC_INTEGER;

    #[test]
    fn revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        let at_maximum = next_revision_no(
            InodeId(2),
            RevisionNo(MAX_PUBLIC_INTEGER - 1),
            |inode_id, base_revision_no| CommitValidationError::RestoreRevisionOverflow {
                inode_id,
                base_revision_no,
            },
        )
        .expect("advance to public maximum");
        assert_eq!(at_maximum, RevisionNo(MAX_PUBLIC_INTEGER));

        assert!(matches!(
            next_revision_no(
                InodeId(2),
                RevisionNo(MAX_PUBLIC_INTEGER),
                |inode_id, base_revision_no| CommitValidationError::RestoreRevisionOverflow {
                    inode_id,
                    base_revision_no,
                },
            ),
            Err(CommitValidationError::RestoreRevisionOverflow {
                inode_id: InodeId(2),
                base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
            })
        ));
    }

    #[test]
    fn attribute_revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_attributes_revision_no(InodeId(2), AttributeRevisionNo(MAX_PUBLIC_INTEGER - 1),)
                .expect("advance to public maximum"),
            AttributeRevisionNo(MAX_PUBLIC_INTEGER)
        );
        assert!(matches!(
            next_attributes_revision_no(InodeId(2), AttributeRevisionNo(MAX_PUBLIC_INTEGER),),
            Err(CommitValidationError::UpdateAttributesRevisionOverflow {
                inode_id: InodeId(2),
                base_attributes_revision_no: AttributeRevisionNo(MAX_PUBLIC_INTEGER),
            })
        ));
    }
}
