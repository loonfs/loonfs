//! Request assertions against the candidate pre-state.

use super::publish_path_planning::{
    check_binding_generation, is_missing_visible_path, resolve_visible_inode,
    PublishPathPlanningView,
};
use crate::commit::CommitValidationError;
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataView, VisiblePathError};
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{AbsolutePath, BindingGeneration, CommitAssertion, InodeId};
use loonfs_objectstore::ObjectStore;

pub(super) async fn evaluate_assertions<S: ObjectStore + ?Sized>(
    assertions: &[CommitAssertion],
    head: &NamespaceReadState,
    pre_state: &MetadataView<'_, '_, S>,
) -> Result<()> {
    let view = PublishPathPlanningView {
        namespace_id: &head.namespace_id,
        view: pre_state,
    };
    for (index, assertion) in assertions.iter().enumerate() {
        let assertion_index = u32::try_from(index).ok();
        match assertion {
            CommitAssertion::NamespaceHead { expected_head_seq } => {
                if *expected_head_seq != head.seq {
                    return Err(CoreError::StaleHeadPrecondition {
                        expected: *expected_head_seq,
                        actual: head.seq,
                        assertion_index,
                    });
                }
            }
            CommitAssertion::FileRevision {
                inode_id,
                expected_revision_no,
            } => {
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
                        assertion_index,
                    }
                    .into());
                }
            }
            CommitAssertion::Binding {
                path,
                expected_inode_id,
                expected_binding_generation,
            } => {
                evaluate_binding(
                    &view,
                    path,
                    *expected_inode_id,
                    expected_binding_generation.as_ref(),
                    assertion_index,
                )
                .await?;
            }
            CommitAssertion::Attributes {
                inode_id,
                expected_attributes_revision_no,
            } => {
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
                            assertion_index,
                        }
                        .into(),
                    );
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
    assertion_index: Option<u32>,
) -> Result<()> {
    if expected_binding_generation.is_some() && expected_inode_id.is_none() {
        return Err(CoreError::InvalidCommitRequest(
            "expected_binding_generation requires expected_inode_id".to_owned(),
        ));
    }
    let actual = match view.view.resolve_visible_path(path).await {
        Ok(binding) => Some(binding),
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
            assertion_index,
        }
        .into());
    }
    if let (Some(binding), Some(expected)) = (actual, expected_binding_generation) {
        check_binding_generation(view, &binding, expected).map_err(|error| match error {
            CoreError::BindingGenerationMismatch { inode_id, .. } => {
                CoreError::BindingGenerationMismatch {
                    inode_id,
                    assertion_index,
                }
            }
            CoreError::RootMutationForbidden => CoreError::BindingGenerationMismatch {
                inode_id: binding.inode_id,
                assertion_index,
            },
            error => error,
        })?;
    }
    Ok(())
}
