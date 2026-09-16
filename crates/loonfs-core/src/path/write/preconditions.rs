//! Request preconditions against the candidate pre-state.

use super::publish_path_planning::{
    check_binding_generation, is_missing_visible_path, resolve_parent_directory,
    resolve_visible_inode, PublishPathPlanningView,
};
use crate::authorize::{Absence, Authorizer};
use crate::commit::CommitValidationError;
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataView, VisiblePathError};
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{
    AbsolutePath, AccessRevisionNo, AccessRight, AccessRights, BindingGeneration,
    CommitPrecondition, InodeId, Subject,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn evaluate_preconditions<S: ObjectStore + ?Sized>(
    preconditions: &[CommitPrecondition],
    subject: Option<&Subject>,
    head: &NamespaceReadState,
    pre_state: &MetadataView<'_, '_, S>,
) -> Result<()> {
    let authorizer = Authorizer::for_request(&head.namespace_id, &head.access, subject)?;
    let view = PublishPathPlanningView {
        namespace_id: &head.namespace_id,
        access: &head.access,
        authorizer: &authorizer,
        view: pre_state,
    };
    for (index, precondition) in preconditions.iter().enumerate() {
        let precondition_index = u32::try_from(index).ok();
        match precondition {
            CommitPrecondition::NamespaceHead { expected_head_seq } => {
                if *expected_head_seq != head.seq {
                    return Err(CoreError::StaleHeadPrecondition {
                        expected: *expected_head_seq,
                        actual: head.seq,
                        precondition_index,
                    });
                }
            }
            CommitPrecondition::FileRevision {
                inode_id,
                expected_revision_no,
            } => {
                view.authorize(
                    *inode_id,
                    AccessRights::from_iter([AccessRight::Read]),
                    Absence::Inode,
                )
                .await?;
                let actual = if inode_is_visible(&view, *inode_id).await? {
                    pre_state
                        .latest_revision_head(*inode_id)
                        .await?
                        .map(|revision| revision.revision_no)
                } else {
                    None
                };
                if actual != Some(*expected_revision_no) {
                    return Err(CommitValidationError::BaseRevisionMismatch {
                        inode_id: *inode_id,
                        expected: *expected_revision_no,
                        actual,
                        precondition_index,
                    }
                    .into());
                }
            }
            CommitPrecondition::PathBinding {
                path,
                expected_inode_id,
                expected_binding_generation,
            } => {
                evaluate_binding(
                    &view,
                    path,
                    Some(*expected_inode_id),
                    expected_binding_generation.as_ref(),
                    precondition_index,
                )
                .await?;
            }
            CommitPrecondition::PathAbsence { path } => {
                match resolve_parent_directory(&view, path).await {
                    Ok(parent) => {
                        view.authorize(
                            parent,
                            AccessRights::from_iter([AccessRight::Read]),
                            Absence::Path(path.as_str()),
                        )
                        .await?;
                    }
                    Err(error) if is_missing_visible_path(&error) => continue,
                    Err(
                        CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                            ..
                        })
                        | CoreError::ExpectedDirectory { .. },
                    ) => continue,
                    Err(error) => return Err(error),
                }
                evaluate_binding(&view, path, None, None, precondition_index).await?;
            }
            CommitPrecondition::AttributesRevision {
                inode_id,
                expected_attributes_revision_no,
            } => {
                view.authorize(
                    *inode_id,
                    AccessRights::from_iter([AccessRight::Read]),
                    Absence::Inode,
                )
                .await?;
                let actual = if inode_is_visible(&view, *inode_id).await? {
                    Some(pre_state.attributes_at_visible_seq(*inode_id).await?.0)
                } else {
                    None
                };
                if actual != Some(*expected_attributes_revision_no) {
                    return Err(
                        CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                            inode_id: *inode_id,
                            expected: *expected_attributes_revision_no,
                            actual,
                            precondition_index,
                        }
                        .into(),
                    );
                }
            }
            CommitPrecondition::AccessRevision {
                inode_id,
                expected_access_revision_no,
            } => {
                view.authorize(
                    *inode_id,
                    AccessRights::from_iter([AccessRight::Read]),
                    Absence::Inode,
                )
                .await?;
                let actual = if inode_is_visible(&view, *inode_id).await? {
                    Some(
                        pre_state
                            .latest_access_revision(*inode_id)
                            .await?
                            .map_or(AccessRevisionNo(0), |revision| revision.access_revision_no),
                    )
                } else {
                    None
                };
                if actual != Some(*expected_access_revision_no) {
                    return Err(CommitValidationError::UpdateAccessBaseRevisionMismatch {
                        inode_id: *inode_id,
                        expected: *expected_access_revision_no,
                        actual,
                        precondition_index,
                    }
                    .into());
                }
            }
        }
    }
    Ok(())
}

async fn inode_is_visible<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    inode_id: InodeId,
) -> Result<bool> {
    match resolve_visible_inode(view, inode_id).await {
        Ok(_) => Ok(true),
        Err(CoreError::InodeNotFound(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn evaluate_binding<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    path: &AbsolutePath,
    expected_inode_id: Option<InodeId>,
    expected_binding_generation: Option<&BindingGeneration>,
    precondition_index: Option<u32>,
) -> Result<()> {
    let actual = match view.view.resolve_visible_path(path).await {
        Ok(binding) => {
            if expected_inode_id.is_some() {
                view.authorize(
                    binding.inode_id,
                    AccessRights::from_iter([AccessRight::Read]),
                    Absence::Path(path.as_str()),
                )
                .await?;
            }
            Some(binding)
        }
        Err(error) if is_missing_visible_path(&error) => None,
        Err(CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory { .. })) => None,
        Err(error) => return Err(error),
    };
    let actual_inode_id = actual.as_ref().map(|binding| binding.inode_id);
    if actual_inode_id != expected_inode_id {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            target: format!("path `{path}`"),
            expected_inode_id,
            actual_inode_id,
            precondition_index,
        }
        .into());
    }
    if let (Some(binding), Some(expected)) = (actual, expected_binding_generation) {
        check_binding_generation(view, &binding, expected).map_err(|error| match error {
            CoreError::BindingGenerationMismatch {
                inode_id,
                expected_binding_generation,
                actual_binding_generation,
                ..
            } => CoreError::BindingGenerationMismatch {
                inode_id,
                expected_binding_generation,
                actual_binding_generation,
                precondition_index,
            },
            CoreError::RootMutationForbidden => CoreError::BindingGenerationMismatch {
                inode_id: binding.inode_id,
                expected_binding_generation: expected.clone(),
                actual_binding_generation: None,
                precondition_index,
            },
            CoreError::InvalidCommitField { field, message, .. } => CoreError::InvalidCommitField {
                field,
                message,
                precondition_index,
            },
            error => error,
        })?;
    }
    Ok(())
}
