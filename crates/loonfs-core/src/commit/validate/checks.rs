//! The single op-validation loop and its precondition checks.
//!
//! Checks that only differ by error vocabulary take error-constructor
//! closures so each call site keeps its exact wire-visible variant.

use super::super::{
    CommitOp, CommitPrecondition, CommitValidationError, PlannedOp, ResolvedBinding, ValidatedOp,
};
use super::view::PublishValidationView;
use crate::error::CoreError;
use crate::metadata::{BindingIdentity, InodeRecord, RevisionRecord, SubtreeTombstoneRecord};
use loonfs_api::{
    next_public_ordinal, ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, DisplayName,
    InodeId, InodeKind, NameKey, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

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
pub(crate) async fn validate_ops<S: ObjectStore + ?Sized>(
    ops: &[PlannedOp],
    metadata_state: &mut PublishValidationView<'_, S>,
    cursor: &mut OpValidationCursor,
    committed_seq: ChangeSeq,
    actor: &ActorRef,
    committed_at_ms: u64,
) -> Result<Vec<ValidatedOp>, CoreError> {
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
                child_inode_id,
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
                    child_inode_id: *child_inode_id,
                    create_inode_delta_index: reserve_delta_index(next_delta_index)?,
                    bind_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
            CommitOp::CreateFile {
                child_inode_id,
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
                    child_inode_id: *child_inode_id,
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
                let inode = metadata_state.inode_at_seq(*inode_id).await?.ok_or(
                    CommitValidationError::RenameInodeMissing {
                        inode_id: *inode_id,
                    },
                )?;
                let new_name_key = validate_rename_target_name_absent(
                    metadata_state,
                    *inode_id,
                    *new_parent_inode_id,
                    new_display_name,
                )
                .await?;
                validate_rename_does_not_cycle(metadata_state, &inode, *new_parent_inode_id)
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
                deletion_seq,
                parent_inode_id,
                display_name,
            } => {
                let active = validate_undelete_target(
                    metadata_state,
                    *inode_id,
                    *deletion_seq,
                    committed_seq,
                )
                .await?;
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
            CommitOp::UpdateAttributes {
                inode_id,
                base_attributes_revision_no,
                attributes,
            } => {
                validate_attributes_target_visible(metadata_state, *inode_id).await?;
                validate_inode_attributes_revision_is(
                    metadata_state,
                    *inode_id,
                    *base_attributes_revision_no,
                )
                .await?;
                // The base is what the guard above just confirmed is current,
                // so the revision this update publishes is one past it.
                let attributes_revision_no =
                    next_attributes_revision_no(*inode_id, *base_attributes_revision_no)?;
                validate_not_covered_by_tombstone(metadata_state, *inode_id, |tombstone| {
                    CommitValidationError::UpdateAttributesUnderSubtreeTombstone {
                        inode_id: *inode_id,
                        root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.generation.seq,
                    }
                })
                .await?;
                ValidatedOp::UpdateAttributes {
                    op_index,
                    inode_id: *inode_id,
                    attributes_revision_no,
                    attributes: attributes.clone(),
                    attributes_delta_index: reserve_delta_index(next_delta_index)?,
                }
            }
        };
        metadata_state.apply_validated_op_mut(committed_seq, actor, committed_at_ms, &validated_op);
        validated_ops.push(validated_op);
    }

    Ok(validated_ops)
}

async fn validate_explicit_preconditions<S: ObjectStore + ?Sized>(
    preconditions: &[CommitPrecondition],
    metadata_state: &PublishValidationView<'_, S>,
) -> Result<(), CoreError> {
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

/// Requires the target to be the root of the requested live deletion.
async fn validate_undelete_target<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    deleted_at_seq: ChangeSeq,
    committed_seq: ChangeSeq,
) -> Result<SubtreeTombstoneRecord, CoreError> {
    // A child of a deleted directory is covered by its ancestor's tombstone,
    // not its own — recover the directory, not the child.
    if metadata_state.inode_at_seq(inode_id).await?.is_none() {
        return Err(CommitValidationError::UndeleteInodeMissing { inode_id }.into());
    }
    // Only a deletion from a strictly earlier commit is recoverable.
    // Assigned sequences are guessable (head + 1), so this prevents one
    // multi-op commit from minting ambiguous deletion generations.
    if deleted_at_seq >= committed_seq {
        return Err(CommitValidationError::UndeleteTargetsCurrentCommit {
            inode_id,
            requested_seq: deleted_at_seq,
        }
        .into());
    }
    let Some(active) = metadata_state.active_subtree_tombstone(inode_id).await? else {
        return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
    };
    // Recovery is scoped to the deletion the caller observed, never whatever
    // deletion is active now, and revalidation applies the same generation.
    if active.generation.seq != deleted_at_seq {
        return Err(CommitValidationError::UndeleteGenerationMismatch {
            inode_id,
            requested_seq: deleted_at_seq,
            active_seq: active.generation.seq,
        }
        .into());
    }

    Ok(active)
}

/// Requires an attribute target to be visible, whether file or directory.
async fn validate_attributes_target_visible<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
) -> Result<(), CoreError> {
    if metadata_state.visible_inode(inode_id).await?.is_none() {
        return Err(CommitValidationError::UpdateAttributesInodeMissing { inode_id }.into());
    }

    Ok(())
}

/// The base-revision guard every `UpdateAttributes` op carries, whether or
/// not the caller stated an expectation of its own.
///
/// The inode's own attribute revision is the whole check: an inode that has
/// never had attributes written is at revision 0, so a first write states
/// zero and a concurrent first write of the same inode conflicts.
async fn validate_inode_attributes_revision_is<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected: AttributeRevisionNo,
) -> Result<Attributes, CoreError> {
    let (actual, attributes) = metadata_state.attributes_at_seq(inode_id).await?;
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

/// Requires the inode to exist and to have `expected_kind`, with the error
/// vocabulary supplied by the call site.
async fn validate_inode_kind<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected_kind: InodeKind,
    missing: impl FnOnce() -> CommitValidationError,
    wrong_kind: impl FnOnce(InodeKind) -> CommitValidationError,
) -> Result<InodeRecord, CoreError> {
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
async fn validate_not_covered_by_tombstone<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    covered: impl FnOnce(&SubtreeTombstoneRecord) -> CommitValidationError,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id).await? {
        return Err(covered(&tombstone).into());
    }

    Ok(())
}

async fn validate_create_parent_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
) -> Result<(), CoreError> {
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
async fn validate_replace_target_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
) -> Result<(), CoreError> {
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
async fn validate_name_absent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    rebinding_inode_id: Option<InodeId>,
    parent_missing: impl FnOnce() -> CommitValidationError,
    parent_not_directory: impl FnOnce(InodeKind) -> CommitValidationError,
    collision: impl FnOnce(NameKey, InodeId) -> CommitValidationError,
) -> Result<NameKey, CoreError> {
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

async fn validate_child_name_absent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<NameKey, CoreError> {
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

async fn validate_rename_target_name_absent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    renaming_inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<NameKey, CoreError> {
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

async fn validate_child_name_absent_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<(), CoreError> {
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

async fn validate_binding_is_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    name_key: &NameKey,
    child_inode_id: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
) -> Result<(), CoreError> {
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

async fn validate_name_precondition_parent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
) -> Result<(), CoreError> {
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

async fn validate_directory_empty_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
) -> Result<(), CoreError> {
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

async fn resolve_current_binding_for_mutation<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
) -> Result<ResolvedBinding, CoreError> {
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
async fn validate_file_base_revision_is<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    missing: impl FnOnce() -> CommitValidationError,
    not_file: impl FnOnce(InodeKind) -> CommitValidationError,
    revision_mismatch: impl FnOnce(Option<RevisionNo>) -> CommitValidationError,
) -> Result<(), CoreError> {
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
async fn validate_inode_revision_is<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
) -> Result<(), CoreError> {
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

async fn validate_restore_target<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
) -> Result<(), CoreError> {
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

async fn validate_restore_source_revision<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
) -> Result<RevisionRecord, CoreError> {
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

async fn validate_rename_does_not_cycle<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    inode: &InodeRecord,
    new_parent_inode_id: InodeId,
) -> Result<(), CoreError> {
    if inode.inode_kind != InodeKind::Directory {
        return Ok(());
    }
    if metadata_state
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
