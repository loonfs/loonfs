use super::frame::validate_commit_request_frame;
use super::{
    push_unique_invariant, CommitOp, CommitPlan, CommitRequest, CommitValidationContext,
    CommitValidationError,
};
use crate::invariants::INVARIANTS;
use crate::metadata::MetadataState;
use loon_api::{
    name_key_for_display_name, ChangeSeq, InodeId, InodeKind, NamePolicy, RevisionNo, WalOp,
};
use std::collections::BTreeMap;

struct CommitShape {
    next_seq: ChangeSeq,
    allocated_inode_ids: Vec<InodeId>,
    resulting_next_inode_id: InodeId,
}

pub(crate) fn resolve_restore_content_manifest_digests(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Vec<Option<String>> {
    let mut resolved_request_revisions = BTreeMap::<(InodeId, RevisionNo), String>::new();

    request
        .ops
        .iter()
        .map(|op| match op {
            CommitOp::ReplaceFile {
                inode_id,
                base_revision,
                content_manifest_digest,
            } => {
                if let Some(next_revision) = base_revision.0.checked_add(1).map(RevisionNo) {
                    resolved_request_revisions
                        .insert((*inode_id, next_revision), content_manifest_digest.clone());
                }
                None
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision,
                base_revision,
            } => {
                let resolved = resolved_request_revisions
                    .get(&(*inode_id, *source_revision))
                    .cloned()
                    .or_else(|| {
                        context
                            .metadata_state
                            .revision_at_seq(*inode_id, *source_revision, context.head.seq)
                            .map(|revision| revision.content_manifest_digest)
                    });
                if let (Some(next_revision), Some(content_manifest_digest)) = (
                    base_revision.0.checked_add(1).map(RevisionNo),
                    resolved.clone(),
                ) {
                    resolved_request_revisions
                        .insert((*inode_id, next_revision), content_manifest_digest);
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
    let resolved_restore_content_manifest_digests = validate_metadata_preconditions(
        request,
        &context.metadata_state,
        shape.next_seq,
        &shape.allocated_inode_ids,
        &mut checked_invariants,
    )?;

    let durable_content_required = request.ops.iter().any(|op| {
        matches!(
            op,
            CommitOp::CreateFile { .. }
                | CommitOp::ReplaceFile { .. }
                | CommitOp::RestoreRevision { .. }
        )
    });
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
        commit_id: request.request_id.clone(),
        base_head_seq: request.planned_head_seq,
        next_seq: shape.next_seq,
        allocated_inode_ids: shape.allocated_inode_ids,
        resolved_restore_content_manifest_digests,
        resulting_next_inode_id: shape.resulting_next_inode_id,
        durable_content_required,
        wal_object_must_be_written: true,
        head_cas_must_succeed: true,
        metadata_preconditions: request.preconditions.clone(),
        checked_invariants,
    })
}

fn compute_commit_shape(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitShape, CommitValidationError> {
    let next_seq = context
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
        next_seq,
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
) -> Result<Vec<Option<String>>, CommitValidationError> {
    let mut ephemeral_metadata_state = metadata_state.clone();
    let mut allocated_inode_ids = allocated_inode_ids.iter().copied();
    let mut resolved_restore_content_manifest_digests = Vec::with_capacity(request.ops.len());

    for (op_index, op) in request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitValidationError::OpIndexOverflow)?;
        let resolved_restore_content_manifest_digest = match op {
            CommitOp::RestoreRevision {
                inode_id,
                source_revision,
                base_revision,
            } => {
                validate_restore_target(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision,
                    committed_seq,
                )?;
                let source_revision = validate_restore_source_revision(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *source_revision,
                    committed_seq,
                )?;
                if base_revision.0.checked_add(1).is_none() {
                    return Err(CommitValidationError::RestoreRevisionOverflow {
                        inode_id: *inode_id,
                        base_revision: *base_revision,
                    });
                }
                validate_restore_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                Some(source_revision.content_manifest_digest)
            }
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
                base_revision,
                ..
            } => {
                validate_inode_revision_is(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision,
                    committed_seq,
                )?;
                if base_revision.0.checked_add(1).is_none() {
                    return Err(CommitValidationError::ReplaceFileRevisionOverflow {
                        inode_id: *inode_id,
                        base_revision: *base_revision,
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
        resolved_restore_content_manifest_digests
            .push(resolved_restore_content_manifest_digest.clone());

        let applied_metadata = ephemeral_metadata_state
            .apply_committed_wal_ops(
                committed_seq,
                &[materialize_simulated_wal_op(
                    op,
                    op_index,
                    resolved_restore_content_manifest_digest.as_deref(),
                    &mut allocated_inode_ids,
                )?],
            )
            .expect("validated commit ops should always apply into ephemeral metadata state");
        ephemeral_metadata_state = applied_metadata.metadata_state;
    }

    Ok(resolved_restore_content_manifest_digests)
}

fn materialize_simulated_wal_op(
    op: &CommitOp,
    op_index: u32,
    resolved_restore_content_manifest_digest: Option<&str>,
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
) -> Result<WalOp, CommitValidationError> {
    Ok(match op {
        CommitOp::CreateDir {
            parent_inode,
            display_name,
        } => WalOp::CreateDir {
            op_index,
            inode_id: allocated_inode_ids
                .next()
                .ok_or(CommitValidationError::NextInodeOverflow)?,
            parent_inode: *parent_inode,
            display_name: display_name.clone(),
        },
        CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_manifest_digest,
        } => WalOp::CreateFile {
            op_index,
            inode_id: allocated_inode_ids
                .next()
                .ok_or(CommitValidationError::NextInodeOverflow)?,
            parent_inode: *parent_inode,
            display_name: display_name.clone(),
            content_manifest_digest: content_manifest_digest.clone(),
        },
        CommitOp::ReplaceFile {
            inode_id,
            base_revision,
            content_manifest_digest,
        } => WalOp::ReplaceFile {
            op_index,
            inode_id: *inode_id,
            base_revision: *base_revision,
            content_manifest_digest: content_manifest_digest.clone(),
        },
        CommitOp::RestoreRevision {
            inode_id,
            source_revision,
            base_revision,
        } => WalOp::RestoreRevision {
            op_index,
            inode_id: *inode_id,
            source_revision_no: *source_revision,
            base_revision: *base_revision,
            content_manifest_digest: resolved_restore_content_manifest_digest
                .ok_or(
                    CommitValidationError::RestoreRevisionSourceRevisionMissing {
                        inode_id: *inode_id,
                        source_revision: *source_revision,
                    },
                )?
                .to_owned(),
        },
        CommitOp::DeleteFile { inode_id } => WalOp::DeleteFile {
            op_index,
            inode_id: *inode_id,
        },
        CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        } => WalOp::Rename {
            op_index,
            inode_id: *inode_id,
            new_parent_inode: *new_parent_inode,
            new_display_name: new_display_name.clone(),
        },
        CommitOp::DeleteSubtree { root_inode } => WalOp::DeleteSubtree {
            op_index,
            root_inode: *root_inode,
        },
    })
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
    expected_revision: RevisionNo,
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

    let actual_revision = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision != Some(expected_revision) {
        return Err(CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected: expected_revision,
            actual: actual_revision,
        });
    }

    Ok(())
}

fn validate_restore_target(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    expected_revision: RevisionNo,
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

    let actual_revision = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision != Some(expected_revision) {
        return Err(CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected: expected_revision,
            actual: actual_revision,
        });
    }

    Ok(())
}

fn validate_restore_source_revision(
    metadata_state: &MetadataState,
    inode_id: InodeId,
    source_revision: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<crate::metadata::RevisionRecord, CommitValidationError> {
    metadata_state
        .revision_at_seq(inode_id, source_revision, base_seq)
        .ok_or(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id,
                source_revision,
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
