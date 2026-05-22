use super::frame::validate_commit_request_frame;
use super::prepared::materialize_commit_op;
use super::{
    push_unique_invariant, CommitMaterializationError, CommitOp, CommitPlan, CommitRequest,
    CommitValidationContext, CommitValidationError, Precondition, ResolvedBinding,
};
use crate::invariants::INVARIANTS;
use crate::metadata::MetadataState;
use loon_api::{
    name_key_for_display_name, ChangeSeq, ContentRef, InodeId, InodeKind, NamePolicy, RevisionNo,
};
use std::collections::BTreeMap;

struct CommitShape {
    assigned_seq: ChangeSeq,
    allocated_inode_ids: Vec<InodeId>,
    resulting_next_inode_id: InodeId,
}

struct ResolvedMetadataPreconditions {
    resolved_restore_content_refs: Vec<Option<ContentRef>>,
    resolved_source_bindings: Vec<Option<ResolvedBinding>>,
}

pub(crate) fn resolve_restore_content_refs(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Vec<Option<ContentRef>> {
    let mut resolved_request_revisions = BTreeMap::<(InodeId, RevisionNo), ContentRef>::new();

    request
        .ops
        .iter()
        .map(|op| match op {
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                if let Some(next_revision) = base_revision_no.0.checked_add(1).map(RevisionNo) {
                    resolved_request_revisions
                        .insert((*inode_id, next_revision), content_ref.clone());
                }
                None
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                let resolved = resolved_request_revisions
                    .get(&(*inode_id, *source_revision_no))
                    .cloned()
                    .or_else(|| {
                        context
                            .metadata_state
                            .revision_at_seq(*inode_id, *source_revision_no, context.head.seq)
                            .map(|revision| revision.content_ref)
                    });
                if let (Some(next_revision), Some(content_ref)) = (
                    base_revision_no.0.checked_add(1).map(RevisionNo),
                    resolved.clone(),
                ) {
                    resolved_request_revisions.insert((*inode_id, next_revision), content_ref);
                }
                resolved
            }
            _ => None,
        })
        .collect()
}

pub fn build_commit_plan(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitPlan, CommitValidationError> {
    validate_commit_request_frame(request, context)?;

    let mut checked_invariants = INVARIANTS
        .iter()
        .copied()
        .filter(|name| {
            matches!(
                *name,
                "stale_writer_cannot_publish"
                    | "head_and_lease_fence_tokens_agree"
                    | "next_inode_id_is_monotonic"
            )
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let shape = compute_commit_shape(request, context)?;
    let resolved_metadata = validate_metadata_preconditions(
        request,
        &context.metadata_state,
        shape.assigned_seq,
        &shape.allocated_inode_ids,
        &mut checked_invariants,
    )?;

    if !shape.allocated_inode_ids.is_empty() {
        push_unique_invariant(
            &mut checked_invariants,
            "create_mutation_consumes_next_inode_id",
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::CreateFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            "create_file_requires_durable_content",
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::ReplaceFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            "replace_file_requires_durable_content",
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::RestoreRevision { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            "restore_revision_requires_durable_content",
        );
    }

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        apply_after_seq: context.head.seq,
        assigned_seq: shape.assigned_seq,
        allocated_inode_ids: shape.allocated_inode_ids,
        resolved_restore_content_refs: resolved_metadata.resolved_restore_content_refs,
        resolved_source_bindings: resolved_metadata.resolved_source_bindings,
        resulting_next_inode_id: shape.resulting_next_inode_id,
        metadata_preconditions: request.preconditions.clone(),
        checked_invariants,
    })
}

fn compute_commit_shape(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitShape, CommitValidationError> {
    let assigned_seq = context
        .head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;
    let create_op_count = request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    let allocated_inode_ids = (0..create_op_count)
        .map(|offset| {
            let offset =
                u64::try_from(offset).map_err(|_| CommitValidationError::NextInodeOverflow)?;
            context
                .head
                .next_inode_id
                .0
                .checked_add(offset)
                .map(InodeId)
                .ok_or(CommitValidationError::NextInodeOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resulting_next_inode_id = context
        .head
        .next_inode_id
        .0
        .checked_add(
            u64::try_from(create_op_count).map_err(|_| CommitValidationError::NextInodeOverflow)?,
        )
        .map(InodeId)
        .ok_or(CommitValidationError::NextInodeOverflow)?;

    Ok(CommitShape {
        assigned_seq,
        allocated_inode_ids,
        resulting_next_inode_id,
    })
}

fn validate_metadata_preconditions(
    request: &CommitRequest,
    metadata_state: &MetadataState,
    committed_seq: ChangeSeq,
    allocated_inode_ids: &[InodeId],
    checked_invariants: &mut Vec<String>,
) -> Result<ResolvedMetadataPreconditions, CommitValidationError> {
    let mut ephemeral_metadata_state = metadata_state.clone();
    let mut allocated_inode_ids = allocated_inode_ids.iter().copied();
    let mut resolved_restore_content_refs = Vec::with_capacity(request.ops.len());
    let mut resolved_source_bindings = Vec::with_capacity(request.ops.len());
    let mut next_delta_index = 0u32;

    validate_explicit_preconditions(
        &request.preconditions,
        &ephemeral_metadata_state,
        committed_seq,
        checked_invariants,
    )?;

    for (op_index, op) in request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitValidationError::OpIndexOverflow)?;
        let resolved_restore_content_ref = match op {
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                validate_restore_target(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )?;
                let source_revision_no = validate_restore_source_revision(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *source_revision_no,
                    committed_seq,
                )?;
                if base_revision_no.0.checked_add(1).is_none() {
                    return Err(CommitValidationError::RestoreRevisionOverflow {
                        inode_id: *inode_id,
                        base_revision_no: *base_revision_no,
                    });
                }
                validate_restore_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                Some(source_revision_no.content_ref)
            }
            _ => None,
        };
        let resolved_source_binding = match op {
            CommitOp::DeleteFile { inode_id } => Some(resolve_current_binding_for_mutation(
                &ephemeral_metadata_state,
                *inode_id,
                committed_seq,
            )?),
            CommitOp::Rename { inode_id, mode, .. } => {
                if *mode != loon_api::v0::RenameMode::NoReplace {
                    return Err(CommitValidationError::UnsupportedRenameMode { mode: *mode });
                }
                Some(resolve_current_binding_for_mutation(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                )?)
            }
            CommitOp::DeleteSubtree { root_inode } => Some(resolve_current_binding_for_mutation(
                &ephemeral_metadata_state,
                *root_inode,
                committed_seq,
            )?),
            _ => None,
        };

        match op {
            CommitOp::CreateDir {
                parent_inode,
                display_name,
            }
            | CommitOp::CreateFile {
                parent_inode,
                display_name,
                ..
            } => {
                validate_child_name_absent(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    display_name,
                    committed_seq,
                )?;
                validate_ancestors_not_subtree_deleted(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    committed_seq,
                    checked_invariants,
                    true,
                )?;
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                ..
            } => {
                validate_inode_revision_is(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )?;
                if base_revision_no.0.checked_add(1).is_none() {
                    return Err(CommitValidationError::ReplaceFileRevisionOverflow {
                        inode_id: *inode_id,
                        base_revision_no: *base_revision_no,
                    });
                }
                validate_ancestors_not_subtree_deleted(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                    false,
                )?;
            }
            CommitOp::RestoreRevision { .. } => {}
            CommitOp::DeleteFile { inode_id } => {
                validate_delete_file_target(&ephemeral_metadata_state, *inode_id, committed_seq)?;
                validate_delete_file_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
            }
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
                ..
            } => {
                validate_rename_source(&ephemeral_metadata_state, *inode_id, committed_seq)?;
                validate_rename_target_name_absent(
                    &ephemeral_metadata_state,
                    *new_parent_inode,
                    new_display_name,
                    committed_seq,
                )?;
                validate_rename_does_not_cycle(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *new_parent_inode,
                    committed_seq,
                )?;
                validate_rename_inode_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                validate_rename_target_parent_not_covered(
                    &ephemeral_metadata_state,
                    *new_parent_inode,
                    committed_seq,
                    checked_invariants,
                )?;
            }
            CommitOp::DeleteSubtree { root_inode } => {
                validate_delete_subtree_root(
                    &ephemeral_metadata_state,
                    *root_inode,
                    committed_seq,
                )?;
                validate_delete_subtree_not_covered(
                    &ephemeral_metadata_state,
                    *root_inode,
                    committed_seq,
                    checked_invariants,
                )?;
            }
        }
        resolved_restore_content_refs.push(resolved_restore_content_ref.clone());
        resolved_source_bindings.push(resolved_source_binding.clone());

        let (deltas, _result) = materialize_commit_op(
            op,
            op_index,
            resolved_restore_content_ref.as_ref(),
            resolved_source_binding.as_ref(),
            &mut allocated_inode_ids,
            &mut next_delta_index,
        )
        .map_err(|err| materialization_error_to_validation_error(err, op))?;
        let applied_metadata = ephemeral_metadata_state
            .apply_committed_wal_ops(
                committed_seq,
                &deltas
                    .iter()
                    .map(|delta| delta.wal_op.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("validated commit ops should always apply into ephemeral metadata state");
        ephemeral_metadata_state = applied_metadata.metadata_state;
    }

    Ok(ResolvedMetadataPreconditions {
        resolved_restore_content_refs,
        resolved_source_bindings,
    })
}

fn materialization_error_to_validation_error(
    error: CommitMaterializationError,
    op: &CommitOp,
) -> CommitValidationError {
    match (error, op) {
        (
            CommitMaterializationError::MissingResolvedRestoreContentRef { .. },
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                ..
            },
        ) => CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: *inode_id,
            source_revision_no: *source_revision_no,
        },
        (
            CommitMaterializationError::ReplaceRevisionOverflow { .. },
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                ..
            },
        ) => CommitValidationError::ReplaceFileRevisionOverflow {
            inode_id: *inode_id,
            base_revision_no: *base_revision_no,
        },
        (
            CommitMaterializationError::RestoreRevisionOverflow { .. },
            CommitOp::RestoreRevision {
                inode_id,
                base_revision_no,
                ..
            },
        ) => CommitValidationError::RestoreRevisionOverflow {
            inode_id: *inode_id,
            base_revision_no: *base_revision_no,
        },
        (CommitMaterializationError::OpIndexOverflow, _) => CommitValidationError::OpIndexOverflow,
        (CommitMaterializationError::DeltaIndexOverflow, _) => {
            CommitValidationError::OpIndexOverflow
        }
        (CommitMaterializationError::MissingResolvedSourceBinding { op_index }, _) => {
            CommitValidationError::SourceBindingMissing {
                inode_id: request_op_inode(op).unwrap_or(InodeId(u64::from(op_index))),
            }
        }
        _ => CommitValidationError::NextInodeOverflow,
    }
}

fn request_op_inode(op: &CommitOp) -> Option<InodeId> {
    match op {
        CommitOp::ReplaceFile { inode_id, .. }
        | CommitOp::RestoreRevision { inode_id, .. }
        | CommitOp::DeleteFile { inode_id }
        | CommitOp::Rename { inode_id, .. } => Some(*inode_id),
        CommitOp::DeleteSubtree { root_inode } => Some(*root_inode),
        CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. } => None,
    }
}

fn validate_explicit_preconditions(
    preconditions: &[Precondition],
    metadata_state: &MetadataState,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    for precondition in preconditions {
        match precondition {
            Precondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => {
                validate_inode_revision_is(metadata_state, *inode_id, *revision_no, base_seq)?;
            }
            Precondition::AncestorsNotSubtreeDeleted { inode_id } => {
                validate_ancestors_not_subtree_deleted(
                    metadata_state,
                    *inode_id,
                    base_seq,
                    checked_invariants,
                    false,
                )?;
            }
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key,
            } => {
                validate_child_name_absent_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    base_seq,
                )?;
            }
            Precondition::ChildNameIs {
                parent_inode,
                name_key,
                child_inode,
            } => {
                validate_child_name_is_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    *child_inode,
                    base_seq,
                )?;
            }
            Precondition::BindingIs {
                parent_inode,
                name_key,
                child_inode,
                bind_seq,
                bind_delta_index,
            } => {
                validate_binding_is_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    *child_inode,
                    *bind_seq,
                    *bind_delta_index,
                    base_seq,
                )?;
            }
            Precondition::DirectoryEmpty { inode_id } => {
                validate_directory_empty_precondition(metadata_state, *inode_id, base_seq)?;
            }
        }
    }

    Ok(())
}

fn validate_child_name_absent_precondition(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    name_key: &str,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::ChildNamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::ChildNamePreconditionParentNotDirectory {
                parent_inode,
                actual_kind: parent.inode_kind,
            },
        );
    }

    if let Some(existing) = metadata_state.visible_child(parent_inode, name_key, base_seq) {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key: name_key.to_owned(),
            child_inode: existing.child_inode_id,
        });
    }

    Ok(())
}

fn validate_child_name_is_precondition(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    name_key: &str,
    child_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::ChildNamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::ChildNamePreconditionParentNotDirectory {
                parent_inode,
                actual_kind: parent.inode_kind,
            },
        );
    }

    let Some(existing) = metadata_state.visible_child(parent_inode, name_key, base_seq) else {
        return Err(CommitValidationError::ChildNamePreconditionMissing {
            parent_inode,
            name_key: name_key.to_owned(),
        });
    };
    if existing.child_inode_id != child_inode {
        return Err(CommitValidationError::ChildNamePreconditionMismatch {
            parent_inode,
            name_key: name_key.to_owned(),
            expected_child_inode: child_inode,
            actual_child_inode: existing.child_inode_id,
        });
    }

    Ok(())
}

fn resolve_current_binding_for_mutation(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<ResolvedBinding, CommitValidationError> {
    let binding = metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .ok_or(CommitValidationError::SourceBindingMissing { inode_id })?;
    Ok(ResolvedBinding {
        parent_inode: binding.parent_inode_id,
        name_key: binding.name_key,
        display_name: binding.display_name,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

fn validate_binding_is_precondition(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    name_key: &str,
    child_inode: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::ChildNamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::ChildNamePreconditionParentNotDirectory {
                parent_inode,
                actual_kind: parent.inode_kind,
            },
        );
    }

    let Some(existing) = metadata_state.visible_child(parent_inode, name_key, base_seq) else {
        return Err(CommitValidationError::BindingPreconditionMissing {
            parent_inode,
            name_key: name_key.to_owned(),
        });
    };
    if existing.child_inode_id != child_inode
        || existing.bind_seq != bind_seq
        || existing.bind_delta_index != bind_delta_index
    {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            parent_inode,
            name_key: name_key.to_owned(),
            expected_child_inode: child_inode,
            actual_child_inode: Some(existing.child_inode_id),
        });
    }

    Ok(())
}

fn validate_directory_empty_precondition(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .visible_inode(inode_id, base_seq)
        .ok_or(CommitValidationError::DirectoryEmptyPreconditionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::DirectoryEmptyPreconditionInodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            },
        );
    }

    if !metadata_state
        .visible_children(inode_id, base_seq)
        .is_empty()
    {
        return Err(CommitValidationError::DirectoryEmptyPreconditionNotEmpty { inode_id });
    }

    Ok(())
}

fn validate_child_name_absent(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::CreateParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::CreateParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    if let Some(existing) = metadata_state.visible_child(parent_inode, display_name, base_seq) {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key: name_key_for_display_name(NamePolicy::default(), display_name),
            child_inode: existing.child_inode_id,
        });
    }

    Ok(())
}

fn validate_inode_revision_is(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::ReplaceFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::ReplaceFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        });
    }

    Ok(())
}

fn validate_restore_target(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RestoreRevisionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        });
    }

    Ok(())
}

fn validate_restore_source_revision(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<crate::metadata::RevisionRecord, CommitValidationError> {
    metadata_state
        .revision_at_seq(inode_id, source_revision_no, base_seq)
        .ok_or(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id,
                source_revision_no,
            },
        )
}

fn validate_ancestors_not_subtree_deleted(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
    is_create: bool,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return if is_create {
            Err(CommitValidationError::CreateUnderSubtreeTombstone {
                parent_inode: inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            })
        } else {
            Err(CommitValidationError::ReplaceFileUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            })
        };
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}

fn validate_restore_not_covered(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(
            CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            },
        );
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}

fn validate_delete_subtree_root(
    metadata_state: &MetadataState,
    root_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(root_inode, base_seq)
        .ok_or(CommitValidationError::DeleteSubtreeRootMissing { root_inode })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::DeleteSubtreeRootNotDirectory {
            root_inode,
            actual_kind: inode.inode_kind,
        });
    }

    Ok(())
}

fn validate_delete_file_target(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::DeleteFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::DeleteFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    Ok(())
}

fn validate_delete_file_not_covered(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(CommitValidationError::DeleteFileCoveredByTombstone {
            inode_id,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}

fn validate_delete_subtree_not_covered(
    metadata_state: &MetadataState,
    root_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(root_inode, base_seq) {
        return Err(CommitValidationError::DeleteSubtreeRootCoveredByTombstone {
            root_inode,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}

fn validate_rename_source(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .is_none()
    {
        return Err(CommitValidationError::RenameSourceBindingMissing { inode_id });
    }

    Ok(())
}

fn validate_rename_target_name_absent(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::RenameTargetParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::RenameTargetParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    if let Some(existing) = metadata_state.visible_child(parent_inode, display_name, base_seq) {
        return Err(CommitValidationError::RenameTargetNameCollision {
            parent_inode,
            name_key: name_key_for_display_name(NamePolicy::default(), display_name),
            child_inode: existing.child_inode_id,
        });
    }

    Ok(())
}

fn validate_rename_does_not_cycle(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    new_parent_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Ok(());
    }
    if metadata_state.would_create_directory_cycle(inode_id, new_parent_inode, base_seq) {
        return Err(CommitValidationError::RenameWouldCycleDirectory {
            inode_id,
            new_parent_inode,
        });
    }

    Ok(())
}

fn validate_rename_inode_not_covered(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(CommitValidationError::RenameInodeUnderSubtreeTombstone {
            inode_id,
            root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}

fn validate_rename_target_parent_not_covered(
    metadata_state: &MetadataState,
    parent_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<String>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(parent_inode, base_seq) {
        return Err(
            CommitValidationError::RenameTargetParentUnderSubtreeTombstone {
                parent_inode,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            },
        );
    }

    push_unique_invariant(
        checked_invariants,
        "subtree_tombstone_blocks_descendant_mutation",
    );
    Ok(())
}
