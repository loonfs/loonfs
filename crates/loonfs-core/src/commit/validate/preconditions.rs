//! Explicit commit preconditions checked immediately before their operation.

use super::super::{CommitPrecondition, CommitValidationError};
use super::checks::{
    validate_inode_kind, validate_inode_revision_is, validate_replace_target_not_covered,
};
use super::view::PublishValidationView;
use crate::error::CoreError;
use crate::metadata::BindingIdentity;
use loonfs_api::{ChangeSeq, InodeId, InodeKind, NameKey};
use loonfs_objectstore::ObjectStore;

pub(super) async fn validate_explicit_preconditions<S: ObjectStore + ?Sized>(
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

async fn validate_child_name_absent_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishValidationView<'_, S>,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<(), CoreError> {
    validate_name_precondition_parent(metadata_state, parent_inode_id).await?;

    if let Some(existing) = metadata_state
        .view()
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
        .view()
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
        .view()
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

    if metadata_state.view().has_visible_children(inode_id).await? {
        return Err(CommitValidationError::DirectoryEmptyPreconditionNotEmpty { inode_id }.into());
    }

    Ok(())
}
