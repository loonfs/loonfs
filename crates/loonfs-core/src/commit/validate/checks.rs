//! The single op-validation loop and its precondition checks.
//!
//! Every check is written once against [`CommitValidationView`]; checks that
//! only differ by error vocabulary take error-constructor closures so each
//! call site keeps its exact wire-visible variant.

use super::super::{
    CommitOp, CommitPrecondition, CommitValidationError, PlannedOp, ResolvedBinding, ValidatedOp,
};
use super::view::CommitValidationView;
use crate::metadata::{BindingIdentity, InodeRecord, RevisionRecord, SubtreeTombstoneRecord};
use loonfs_api::{ChangeSeq, DisplayName, InodeId, InodeKind, NameKey, RevisionNo};

/// Running counters shared by every validation pass over one commit's
/// operations, so a pass that validates the operations in slices numbers
/// delta positions exactly as a pass over the whole list does.
pub(crate) struct OpValidationCursor {
    op_index: u32,
    next_delta_index: u32,
}

impl OpValidationCursor {
    pub(crate) fn new() -> Self {
        Self {
            op_index: 0,
            next_delta_index: 0,
        }
    }
}

/// Validates `ops` in order against `metadata_state`, folding each accepted
/// operation into it so the next one observes what the previous would
/// persist.
pub(crate) async fn validate_ops<V: CommitValidationView>(
    ops: &[PlannedOp],
    metadata_state: &mut V,
    cursor: &mut OpValidationCursor,
    committed_seq: ChangeSeq,
    committed_at_ms: u64,
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
) -> Result<Vec<ValidatedOp>, V::Error> {
    let mut validated_ops = Vec::with_capacity(ops.len());
    let next_delta_index = &mut cursor.next_delta_index;

    for planned in ops {
        let op_index = cursor.op_index;
        cursor.op_index = op_index
            .checked_add(1)
            .ok_or(CommitValidationError::OpIndexOverflow)?;
        // Race checks belong to the operation that carries them and are
        // evaluated where it runs: an operation's checks describe the state
        // its own planning observed, which includes everything the earlier
        // operations of the same commit did.
        validate_explicit_preconditions(&planned.preconditions, metadata_state).await?;
        let validated_op = match &planned.op {
            CommitOp::CreateDirectory {
                parent_inode_id,
                display_name,
            } => {
                let name_key =
                    validate_child_name_absent(metadata_state, *parent_inode_id, display_name)
                        .await?;
                validate_create_parent_not_covered(metadata_state, *parent_inode_id).await?;
                ValidatedOp::CreateDir {
                    op_index,
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode_id: next_allocated_inode(
                        allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    create_inode_delta_index: reserve_delta_index(next_delta_index)?,
                    bind_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::CreateFile {
                parent_inode_id,
                display_name,
                content_ref,
            } => {
                let name_key =
                    validate_child_name_absent(metadata_state, *parent_inode_id, display_name)
                        .await?;
                validate_create_parent_not_covered(metadata_state, *parent_inode_id).await?;
                ValidatedOp::CreateFile {
                    op_index,
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode_id: next_allocated_inode(
                        allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    content_ref: content_ref.clone(),
                    create_inode_delta_index: reserve_delta_index(next_delta_index)?,
                    bind_delta_index: reserve_delta_index(next_delta_index)?,
                    revision_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                validate_inode_revision_is(metadata_state, *inode_id, *base_revision_no).await?;
                let revision_no = next_revision_no(
                    *inode_id,
                    *base_revision_no,
                    |inode_id, base_revision_no| {
                        CommitValidationError::ReplaceFileRevisionOverflow {
                            inode_id,
                            base_revision_no,
                        }
                    },
                )?;
                validate_replace_target_not_covered(metadata_state, *inode_id).await?;
                ValidatedOp::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                    revision_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                validate_restore_target(metadata_state, *inode_id, *base_revision_no).await?;
                let source_revision = validate_restore_source_revision(
                    metadata_state,
                    *inode_id,
                    *source_revision_no,
                )
                .await?;
                let revision_no = next_revision_no(
                    *inode_id,
                    *base_revision_no,
                    |inode_id, base_revision_no| CommitValidationError::RestoreRevisionOverflow {
                        inode_id,
                        base_revision_no,
                    },
                )?;
                validate_not_covered_by_tombstone(metadata_state, *inode_id, |tombstone| {
                    CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
                        inode_id: *inode_id,
                        root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    }
                })
                .await?;
                ValidatedOp::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision_no,
                    revision_no,
                    content_ref: source_revision.content_ref,
                    revision_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::DeleteFile { inode_id } => {
                let source_binding =
                    resolve_current_binding_for_mutation(metadata_state, *inode_id).await?;
                validate_inode_kind(
                    metadata_state,
                    *inode_id,
                    InodeKind::File,
                    || CommitValidationError::DeleteFileInodeMissing {
                        inode_id: *inode_id,
                    },
                    |actual_kind| CommitValidationError::DeleteFileInodeNotFile {
                        inode_id: *inode_id,
                        actual_kind,
                    },
                )
                .await?;
                validate_not_covered_by_tombstone(metadata_state, *inode_id, |tombstone| {
                    CommitValidationError::DeleteFileCoveredByTombstone {
                        inode_id: *inode_id,
                        covering_root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    }
                })
                .await?;
                ValidatedOp::DeleteFile {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::Rename {
                inode_id,
                new_parent_inode_id,
                new_display_name,
            } => {
                let source_binding =
                    resolve_current_binding_for_mutation(metadata_state, *inode_id).await?;
                validate_rename_source(metadata_state, *inode_id).await?;
                let new_name_key = validate_rename_target_name_absent(
                    metadata_state,
                    *inode_id,
                    *new_parent_inode_id,
                    new_display_name,
                )
                .await?;
                validate_rename_does_not_cycle(metadata_state, *inode_id, *new_parent_inode_id)
                    .await?;
                validate_not_covered_by_tombstone(metadata_state, *inode_id, |tombstone| {
                    CommitValidationError::RenameInodeUnderSubtreeTombstone {
                        inode_id: *inode_id,
                        root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    }
                })
                .await?;
                validate_not_covered_by_tombstone(
                    metadata_state,
                    *new_parent_inode_id,
                    |tombstone| CommitValidationError::RenameTargetParentUnderSubtreeTombstone {
                        parent_inode_id: *new_parent_inode_id,
                        root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    },
                )
                .await?;
                ValidatedOp::Rename {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    new_parent_inode_id: *new_parent_inode_id,
                    new_display_name: new_display_name.clone(),
                    new_name_key,
                    unbind_delta_index: reserve_delta_index(next_delta_index)?,
                    bind_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::DeleteSubtree { root_inode_id } => {
                let source_binding =
                    resolve_current_binding_for_mutation(metadata_state, *root_inode_id).await?;
                validate_inode_kind(
                    metadata_state,
                    *root_inode_id,
                    InodeKind::Directory,
                    || CommitValidationError::DeleteSubtreeRootMissing {
                        root_inode_id: *root_inode_id,
                    },
                    |actual_kind| CommitValidationError::DeleteSubtreeRootNotDirectory {
                        root_inode_id: *root_inode_id,
                        actual_kind,
                    },
                )
                .await?;
                validate_not_covered_by_tombstone(metadata_state, *root_inode_id, |tombstone| {
                    CommitValidationError::DeleteSubtreeRootCoveredByTombstone {
                        root_inode_id: *root_inode_id,
                        covering_root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    }
                })
                .await?;
                ValidatedOp::DeleteSubtree {
                    op_index,
                    root_inode_id: *root_inode_id,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::Undelete {
                inode_id,
                deleted_at_seq,
                parent_inode_id,
                display_name,
            } => {
                // The target must exist and be the root of the newest,
                // still-live deletion. A child of a deleted directory is
                // covered by its ancestor's tombstone, not its own —
                // recover the directory, not the child.
                if metadata_state.inode_at_seq(*inode_id).await?.is_none() {
                    return Err(CommitValidationError::UndeleteInodeMissing {
                        inode_id: *inode_id,
                    }
                    .into());
                }
                // Only a deletion from a strictly earlier commit is
                // recoverable. Assigned sequences are guessable (head + 1),
                // so without this bound one multi-op commit could delete,
                // undelete, and re-delete an inode — minting two deletion
                // generations that share a sequence and making the public
                // `(inode, deleted_at_seq)` handle ambiguous. With it, two
                // live deletions of one root can never share a sequence.
                if *deleted_at_seq >= committed_seq {
                    return Err(CommitValidationError::UndeleteTargetsCurrentCommit {
                        inode_id: *inode_id,
                        requested_seq: *deleted_at_seq,
                    }
                    .into());
                }
                let Some(active) = metadata_state.active_subtree_tombstone(*inode_id).await? else {
                    return Err(CommitValidationError::UndeleteTargetNotDeleted {
                        inode_id: *inode_id,
                    }
                    .into());
                };
                // Recovery is scoped to the deletion the caller observed,
                // never "whatever is active now": a stale handle must not
                // cancel a later delete of the same inode. The rule
                // re-applies unchanged on every stale-head revalidation
                // because the requested generation rides in the op.
                if active.generation.seq != *deleted_at_seq {
                    return Err(CommitValidationError::UndeleteGenerationMismatch {
                        inode_id: *inode_id,
                        requested_seq: *deleted_at_seq,
                        active_seq: active.generation.seq,
                    }
                    .into());
                }
                // The new home mirrors create validation: an existing,
                // visible directory parent (visibility rules out a parent
                // inside the recovered subtree, so the bind cannot cycle),
                // a free name, and no covering tombstone over the parent.
                let name_key =
                    validate_child_name_absent(metadata_state, *parent_inode_id, display_name)
                        .await?;
                validate_create_parent_not_covered(metadata_state, *parent_inode_id).await?;
                ValidatedOp::Undelete {
                    op_index,
                    inode_id: *inode_id,
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    name_key,
                    target: active.generation,
                    revoke_tombstone_delta_index: reserve_delta_index(next_delta_index)?,
                    bind_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
        };
        metadata_state.apply_validated_op_mut(committed_seq, committed_at_ms, &validated_op);
        validated_ops.push(validated_op);
    }

    Ok(validated_ops)
}

async fn validate_explicit_preconditions<V: CommitValidationView>(
    preconditions: &[CommitPrecondition],
    metadata_state: &V,
) -> Result<(), V::Error> {
    for precondition in preconditions {
        match precondition {
            CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => {
                validate_inode_revision_is(metadata_state, *inode_id, *revision_no).await?;
            }
            CommitPrecondition::AncestorsNotSubtreeDeleted { inode_id } => {
                validate_replace_target_not_covered(metadata_state, *inode_id).await?;
            }
            CommitPrecondition::ChildNameAbsent {
                parent_inode_id,
                name_key,
            } => {
                validate_child_name_absent_precondition(metadata_state, *parent_inode_id, name_key)
                    .await?;
            }
            CommitPrecondition::BindingIs {
                parent_inode_id,
                name_key,
                child_inode_id,
                bind_seq,
                bind_delta_index,
            } => {
                validate_binding_is_precondition(
                    metadata_state,
                    *parent_inode_id,
                    name_key,
                    *child_inode_id,
                    *bind_seq,
                    *bind_delta_index,
                )
                .await?;
            }
            CommitPrecondition::DirectoryEmpty { inode_id } => {
                validate_directory_empty_precondition(metadata_state, *inode_id).await?;
            }
        }
    }

    Ok(())
}

fn next_allocated_inode(
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
    error: CommitValidationError,
) -> Result<InodeId, CommitValidationError> {
    allocated_inode_ids.next().ok_or(error)
}

fn reserve_delta_index(next_delta_index: &mut u32) -> Result<u32, CommitValidationError> {
    let delta_index = *next_delta_index;
    *next_delta_index = next_delta_index
        .checked_add(1)
        .ok_or(CommitValidationError::DeltaIndexOverflow)?;
    Ok(delta_index)
}

fn next_revision_no(
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    overflow: impl FnOnce(InodeId, RevisionNo) -> CommitValidationError,
) -> Result<RevisionNo, CommitValidationError> {
    base_revision_no
        .0
        .checked_add(1)
        .map(RevisionNo)
        .ok_or_else(|| overflow(inode_id, base_revision_no))
}

/// Requires the inode to exist and to have `expected_kind`, with the error
/// vocabulary supplied by the call site.
async fn validate_inode_kind<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    expected_kind: InodeKind,
    missing: impl FnOnce() -> CommitValidationError,
    wrong_kind: impl FnOnce(InodeKind) -> CommitValidationError,
) -> Result<InodeRecord, V::Error> {
    let inode = metadata_state
        .inode_at_seq(inode_id)
        .await?
        .ok_or_else(missing)?;
    if inode.inode_kind != expected_kind {
        return Err(wrong_kind(inode.inode_kind).into());
    }

    Ok(inode)
}

/// Requires no covering subtree tombstone over `inode_id`, with the covered
/// error supplied by the call site.
async fn validate_not_covered_by_tombstone<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    covered: impl FnOnce(&SubtreeTombstoneRecord) -> CommitValidationError,
) -> Result<(), V::Error> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id).await? {
        return Err(covered(&tombstone).into());
    }

    Ok(())
}

async fn validate_create_parent_not_covered<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
) -> Result<(), V::Error> {
    validate_not_covered_by_tombstone(metadata_state, parent_inode_id, |tombstone| {
        CommitValidationError::CreateUnderSubtreeTombstone {
            parent_inode_id,
            root_inode_id: tombstone.root_inode_id,
            tombstone_seq: tombstone.generation.seq,
        }
    })
    .await
}

/// Shared by the `ReplaceFile` op and the `AncestorsNotSubtreeDeleted`
/// explicit precondition, both of which report the replace-flavored error.
async fn validate_replace_target_not_covered<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
) -> Result<(), V::Error> {
    validate_not_covered_by_tombstone(metadata_state, inode_id, |tombstone| {
        CommitValidationError::ReplaceFileUnderSubtreeTombstone {
            inode_id,
            root_inode_id: tombstone.root_inode_id,
            tombstone_seq: tombstone.generation.seq,
        }
    })
    .await
}

/// Requires `display_name` to be valid and unbound under `parent_inode_id`,
/// which must be an existing directory; returns the derived name key. The
/// error vocabulary (create vs rename) is supplied by the call site.
#[allow(clippy::too_many_arguments)]
async fn validate_name_absent<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    rebinding_inode_id: Option<InodeId>,
    parent_missing: impl FnOnce() -> CommitValidationError,
    parent_not_directory: impl FnOnce(InodeKind) -> CommitValidationError,
    collision: impl FnOnce(NameKey, InodeId) -> CommitValidationError,
) -> Result<NameKey, V::Error> {
    validate_inode_kind(
        metadata_state,
        parent_inode_id,
        InodeKind::Directory,
        parent_missing,
        parent_not_directory,
    )
    .await?;

    let name_key = NameKey::for_display_name(display_name);
    if let Some(existing) = metadata_state
        .visible_child(parent_inode_id, &name_key)
        .await?
    {
        // A binding already held by the inode being rebound is the same
        // directory slot getting a respelled display name, not a collision.
        if rebinding_inode_id != Some(existing.child_inode_id) {
            return Err(collision(name_key, existing.child_inode_id).into());
        }
    }

    Ok(name_key)
}

async fn validate_child_name_absent<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<NameKey, V::Error> {
    validate_name_absent(
        metadata_state,
        parent_inode_id,
        display_name,
        None,
        || CommitValidationError::CreateParentMissing { parent_inode_id },
        |actual_kind| CommitValidationError::CreateParentNotDirectory {
            parent_inode_id,
            actual_kind,
        },
        |name_key, child_inode_id| CommitValidationError::CreateChildNameCollision {
            parent_inode_id,
            name_key,
            child_inode_id,
        },
    )
    .await
}

async fn validate_rename_target_name_absent<V: CommitValidationView>(
    metadata_state: &V,
    renaming_inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<NameKey, V::Error> {
    validate_name_absent(
        metadata_state,
        parent_inode_id,
        display_name,
        Some(renaming_inode_id),
        || CommitValidationError::RenameTargetParentMissing { parent_inode_id },
        |actual_kind| CommitValidationError::RenameTargetParentNotDirectory {
            parent_inode_id,
            actual_kind,
        },
        |name_key, child_inode_id| CommitValidationError::RenameTargetNameCollision {
            parent_inode_id,
            name_key,
            child_inode_id,
        },
    )
    .await
}

async fn validate_child_name_absent_precondition<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<(), V::Error> {
    validate_name_precondition_parent(metadata_state, parent_inode_id).await?;

    if let Some(existing) = metadata_state
        .visible_child(parent_inode_id, name_key)
        .await?
    {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode_id,
            name_key: name_key.clone(),
            child_inode_id: existing.child_inode_id,
        }
        .into());
    }

    Ok(())
}

async fn validate_binding_is_precondition<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
    name_key: &NameKey,
    child_inode_id: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
) -> Result<(), V::Error> {
    validate_name_precondition_parent(metadata_state, parent_inode_id).await?;

    let Some(existing) = metadata_state
        .visible_child(parent_inode_id, name_key)
        .await?
    else {
        return Err(CommitValidationError::BindingPreconditionMissing {
            parent_inode_id,
            name_key: name_key.clone(),
        }
        .into());
    };
    // Same parent and name by construction: `existing` was looked up under
    // `(parent_inode_id, name_key)`, so identity equality reduces to the
    // remaining binding-identity fields.
    let expected = BindingIdentity {
        parent_inode_id,
        name_key,
        child_inode_id,
        bind_seq,
        bind_delta_index,
    };
    if BindingIdentity::from(&existing) != expected {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            parent_inode_id,
            name_key: name_key.clone(),
            expected_child_inode_id: child_inode_id,
            actual_child_inode_id: existing.child_inode_id,
        }
        .into());
    }

    Ok(())
}

async fn validate_name_precondition_parent<V: CommitValidationView>(
    metadata_state: &V,
    parent_inode_id: InodeId,
) -> Result<(), V::Error> {
    validate_inode_kind(
        metadata_state,
        parent_inode_id,
        InodeKind::Directory,
        || CommitValidationError::NamePreconditionParentMissing { parent_inode_id },
        |actual_kind| CommitValidationError::NamePreconditionParentNotDirectory {
            parent_inode_id,
            actual_kind,
        },
    )
    .await
    .map(|_| ())
}

async fn validate_directory_empty_precondition<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
) -> Result<(), V::Error> {
    let inode = metadata_state
        .visible_inode(inode_id)
        .await?
        .ok_or(CommitValidationError::DirectoryEmptyPreconditionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Directory {
        return Err(
            CommitValidationError::DirectoryEmptyPreconditionInodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            }
            .into(),
        );
    }

    if metadata_state.has_visible_children(inode_id).await? {
        return Err(CommitValidationError::DirectoryEmptyPreconditionNotEmpty { inode_id }.into());
    }

    Ok(())
}

async fn resolve_current_binding_for_mutation<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
) -> Result<ResolvedBinding, V::Error> {
    let binding = metadata_state
        .current_parent_binding_for_child(inode_id)
        .await?
        .ok_or(CommitValidationError::SourceBindingMissing { inode_id })?;
    Ok(ResolvedBinding {
        parent_inode_id: binding.parent_inode_id,
        name_key: binding.name_key,
        display_name: binding.display_name,
        child_inode_id: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

/// Requires the file to exist at exactly `expected_revision_no`, with the
/// error vocabulary (replace vs restore) supplied by the call site.
async fn validate_file_base_revision_is<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    missing: impl FnOnce() -> CommitValidationError,
    not_file: impl FnOnce(InodeKind) -> CommitValidationError,
    revision_mismatch: impl FnOnce(Option<RevisionNo>) -> CommitValidationError,
) -> Result<(), V::Error> {
    validate_inode_kind(metadata_state, inode_id, InodeKind::File, missing, not_file).await?;

    let actual_revision_no = metadata_state
        .latest_revision_record(inode_id)
        .await?
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(revision_mismatch(actual_revision_no).into());
    }

    Ok(())
}

/// Shared by the `ReplaceFile` op and the `InodeRevisionIs` explicit
/// precondition, both of which report the replace-flavored error.
async fn validate_inode_revision_is<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
) -> Result<(), V::Error> {
    validate_file_base_revision_is(
        metadata_state,
        inode_id,
        expected_revision_no,
        || CommitValidationError::ReplaceFileInodeMissing { inode_id },
        |actual_kind| CommitValidationError::ReplaceFileInodeNotFile {
            inode_id,
            actual_kind,
        },
        |actual| CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual,
        },
    )
    .await
}

async fn validate_restore_target<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
) -> Result<(), V::Error> {
    validate_file_base_revision_is(
        metadata_state,
        inode_id,
        expected_revision_no,
        || CommitValidationError::RestoreRevisionInodeMissing { inode_id },
        |actual_kind| CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id,
            actual_kind,
        },
        |actual| CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual,
        },
    )
    .await
}

async fn validate_restore_source_revision<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
) -> Result<RevisionRecord, V::Error> {
    Ok(metadata_state
        .revision_at_head(inode_id, source_revision_no)
        .await?
        .ok_or(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id,
                source_revision_no,
            },
        )?)
}

async fn validate_rename_source<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
) -> Result<(), V::Error> {
    metadata_state
        .inode_at_seq(inode_id)
        .await?
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if metadata_state
        .current_parent_binding_for_child(inode_id)
        .await?
        .is_none()
    {
        return Err(CommitValidationError::RenameSourceBindingMissing { inode_id }.into());
    }

    Ok(())
}

async fn validate_rename_does_not_cycle<V: CommitValidationView>(
    metadata_state: &V,
    inode_id: InodeId,
    new_parent_inode_id: InodeId,
) -> Result<(), V::Error> {
    let inode = metadata_state
        .inode_at_seq(inode_id)
        .await?
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Directory {
        return Ok(());
    }
    if metadata_state
        .would_create_directory_cycle(inode_id, new_parent_inode_id)
        .await?
    {
        return Err(CommitValidationError::RenameWouldCycleDirectory {
            inode_id,
            new_parent_inode_id,
        }
        .into());
    }

    Ok(())
}
