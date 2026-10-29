//! Request preconditions against the candidate pre-state.

use super::publish_path_planning::{
    ensure_binding_version, hidden_inode_binding_error, is_missing_visible_path,
    resolve_visible_inode, PublishPathPlanningView,
};
use super::session::PublishPlanningSession;
use crate::authorize::{Absence, Authorizer, CommitAuthority};
use crate::commit::CommitValidationError;
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataView, ResolvedVisiblePath, VisiblePathError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AbsolutePath, AccessRevisionNo, AccessRight, AttributesRevisionNo, BindingVersion,
    CommitPrecondition, DestinationBehavior, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
    ROOT_INODE_ID,
};

pub(super) async fn evaluate_preconditions<S: ObjectStore + ?Sized>(
    preconditions: &[CommitPrecondition],
    authority: CommitAuthority<'_>,
    session: &PublishPlanningSession<'_, S>,
    pre_state: &MetadataView<'_, '_, S>,
) -> Result<()> {
    let head = session.head();
    let authorizer = Authorizer::for_request(&head.namespace_id, &head.access, authority)?;
    let view = PublishPathPlanningView {
        namespace_id: &head.namespace_id,
        access: &head.access,
        authorizer: &authorizer,
        view: pre_state,
        store: session.store(),
        content_writes: session.content_writes(),
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
                view.authorize(*inode_id, AccessRight::Read.into(), Absence::Inode)
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
                expected_binding_version,
            } => {
                evaluate_binding(
                    &view,
                    path,
                    Some(*expected_inode_id),
                    expected_binding_version.as_ref(),
                    precondition_index,
                )
                .await?;
            }
            CommitPrecondition::InodeBinding {
                inode_id,
                expected_binding_version,
            } => {
                view.authorize(*inode_id, AccessRight::Read.into(), Absence::Inode)
                    .await?;
                let checked = match resolve_visible_inode(&view, *inode_id).await {
                    Ok(resolved) => {
                        ensure_binding_version(&view, &resolved, expected_binding_version)
                    }
                    Err(CoreError::InodeNotFound(_)) => Err(hidden_inode_binding_error(
                        &view,
                        *inode_id,
                        expected_binding_version,
                    )),
                    Err(error) => return Err(error),
                };
                checked.map_err(|error| at_precondition(error, precondition_index))?;
            }
            CommitPrecondition::PathAbsence { path } => {
                let parent_path = path.parent().unwrap_or_else(AbsolutePath::root);
                match view.view.resolve_visible_path(&parent_path).await {
                    Ok(parent) => {
                        view.authorize(
                            parent.inode_id,
                            AccessRight::Read.into(),
                            Absence::Path(path.as_str()),
                        )
                        .await?;
                        if parent.inode_kind != InodeKind::Directory {
                            continue;
                        }
                    }
                    Err(error) if is_missing_visible_path(&error) => continue,
                    Err(CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                        inode_id,
                        ..
                    })) => {
                        view.authorize(
                            inode_id,
                            AccessRight::Read.into(),
                            Absence::Path(path.as_str()),
                        )
                        .await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                evaluate_binding(&view, path, None, None, precondition_index).await?;
            }
            CommitPrecondition::NameAbsence {
                parent_inode_id,
                display_name,
            } => {
                match resolve_visible_inode(&view, *parent_inode_id).await {
                    Ok(parent) => {
                        view.authorize(*parent_inode_id, AccessRight::Read.into(), Absence::Inode)
                            .await?;
                        if parent.inode_kind != InodeKind::Directory {
                            continue;
                        }
                    }
                    Err(CoreError::InodeNotFound(_)) => continue,
                    Err(error) => return Err(error),
                }
                let name_key = NameKey::for_display_name(display_name);
                if let Some(binding) = view.view.visible_child(*parent_inode_id, &name_key).await? {
                    return Err(CommitValidationError::BindingPreconditionMismatch {
                        target: format!("name `{name_key}` under parent inode `{parent_inode_id}`"),
                        expected_inode_id: None,
                        actual_inode_id: Some(binding.child_inode_id),
                        precondition_index,
                    }
                    .into());
                }
            }
            CommitPrecondition::AttributesRevision {
                inode_id,
                expected_attributes_revision_no,
            } => {
                view.authorize(*inode_id, AccessRight::Read.into(), Absence::Inode)
                    .await?;
                let actual = if inode_is_visible(&view, *inode_id).await? {
                    Some(
                        pre_state
                            .attributes(*inode_id)
                            .await?
                            .attributes_revision_no,
                    )
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
                view.authorize(*inode_id, AccessRight::Read.into(), Absence::Inode)
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
    expected_binding_version: Option<&BindingVersion>,
    precondition_index: Option<u32>,
) -> Result<()> {
    let actual = match view.view.resolve_visible_path(path).await {
        Ok(binding) => {
            if expected_inode_id.is_some() {
                view.authorize(
                    binding.inode_id,
                    AccessRight::Read.into(),
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
    if let (Some(binding), Some(expected)) = (actual, expected_binding_version) {
        ensure_binding_version(view, &binding, expected)
            .map_err(|error| at_precondition(error, precondition_index))?;
    }
    Ok(())
}

fn at_precondition(error: CoreError, precondition_index: Option<u32>) -> CoreError {
    match error {
        CoreError::BindingVersionMismatch {
            inode_id,
            expected_binding_version,
            actual_binding_version,
            ..
        } => CoreError::BindingVersionMismatch {
            inode_id,
            expected_binding_version,
            actual_binding_version,
            precondition_index,
        },
        CoreError::InvalidCommitField { field, message, .. } => CoreError::InvalidCommitField {
            field,
            message,
            precondition_index,
        },
        error => error,
    }
}

pub(super) fn ensure_expected_inode(
    resolved: &ResolvedVisiblePath,
    expected: Option<InodeId>,
    name: &DisplayName,
) -> Result<()> {
    if let Some(expected) = expected {
        if resolved.inode_id != expected {
            return Err(
                crate::commit::CommitValidationError::BindingPreconditionMismatch {
                    target: format!(
                        "name `{}` under parent inode `{}`",
                        NameKey::for_display_name(name),
                        resolved.parent_inode_id.unwrap_or(ROOT_INODE_ID)
                    ),
                    expected_inode_id: Some(expected),
                    actual_inode_id: Some(resolved.inode_id),
                    precondition_index: None,
                }
                .into(),
            );
        }
    }
    Ok(())
}

/// Field-name family used when validating replacement preconditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreconditionFields {
    /// Fields on a file put operation.
    Put,
    /// Destination fields on a move or copy operation.
    Destination,
}

impl PreconditionFields {
    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Put => ("expected_revision_no", "expected_inode_id"),
            Self::Destination => (
                "expected_destination_revision_no",
                "expected_destination_inode_id",
            ),
        }
    }
}

/// Validated file state required before replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExpectedFileState {
    /// Destination inode required by the request.
    pub(super) inode_id: InodeId,
    /// Destination content revision required by the request.
    pub(super) revision_no: Option<RevisionNo>,
}

/// Why a replacement precondition is not a valid request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(super) enum DestinationPreconditionError {
    /// A create-only operation supplied a replacement precondition.
    #[error("destination preconditions require replace behavior")]
    PreconditionsRequireReplace {
        /// First supplied expectation field, with inode before revision.
        field: &'static str,
    },
    /// A revision precondition did not name the inode whose revision it checks.
    #[error(
        "`{revision_field}` names the revision of one inode; pair it with `{inode_field}` so the precondition names which inode"
    )]
    RevisionRequiresInode {
        /// Revision field supplied by the request.
        revision_field: &'static str,
        /// Inode field required beside the revision.
        inode_field: &'static str,
    },
}

/// Validates a replacement precondition and returns the required file state.
pub(super) fn validate_expected_file_state(
    behavior: DestinationBehavior,
    expected_inode_id: Option<InodeId>,
    expected_revision_no: Option<RevisionNo>,
    fields: PreconditionFields,
) -> std::result::Result<Option<ExpectedFileState>, DestinationPreconditionError> {
    let (revision_field, inode_field) = fields.names();
    if behavior == DestinationBehavior::NoReplace
        && (expected_inode_id.is_some() || expected_revision_no.is_some())
    {
        return Err(DestinationPreconditionError::PreconditionsRequireReplace {
            field: if expected_inode_id.is_some() {
                inode_field
            } else {
                revision_field
            },
        });
    }
    let Some(inode_id) = expected_inode_id else {
        if expected_revision_no.is_some() {
            return Err(DestinationPreconditionError::RevisionRequiresInode {
                revision_field,
                inode_field,
            });
        }
        return Ok(None);
    };
    Ok(Some(ExpectedFileState {
        inode_id,
        revision_no: expected_revision_no,
    }))
}

/// Rejects an attribute revision precondition without an inode precondition.
pub(super) fn validate_attributes_precondition(
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributesRevisionNo>,
) -> std::result::Result<(), DestinationPreconditionError> {
    validate_revision_precondition(
        expected_inode_id,
        expected_attributes_revision_no.is_some(),
        "expected_attributes_revision_no",
    )
}

/// Rejects an access revision precondition without an inode precondition.
pub(super) fn validate_access_precondition(
    expected_inode_id: Option<InodeId>,
    expected_access_revision_no: Option<AccessRevisionNo>,
) -> std::result::Result<(), DestinationPreconditionError> {
    validate_revision_precondition(
        expected_inode_id,
        expected_access_revision_no.is_some(),
        "expected_access_revision_no",
    )
}

fn validate_revision_precondition(
    expected_inode_id: Option<InodeId>,
    has_revision: bool,
    revision_field: &'static str,
) -> std::result::Result<(), DestinationPreconditionError> {
    if has_revision && expected_inode_id.is_none() {
        return Err(DestinationPreconditionError::RevisionRequiresInode {
            revision_field,
            inode_field: "expected_inode_id",
        });
    }
    Ok(())
}

impl From<DestinationPreconditionError> for CoreError {
    fn from(error: DestinationPreconditionError) -> Self {
        let field = match error {
            DestinationPreconditionError::PreconditionsRequireReplace { field } => field,
            DestinationPreconditionError::RevisionRequiresInode { revision_field, .. } => {
                revision_field
            }
        };
        Self::InvalidCommitField {
            field,
            message: error.to_string(),
            precondition_index: None,
        }
    }
}
