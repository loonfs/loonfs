//! Request preconditions against the candidate pre-state.

use super::publish_path_planning::{is_missing_visible_path, PlanningView};
use crate::authorize::Absence;
use crate::commit::CommitValidationError;
use crate::error::{CoreError, Result};
use crate::metadata::{ResolvedVisiblePath, VisiblePathError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AbsolutePath, AccessRevisionNo, AccessRight, AttributesRevisionNo, BindingVersion, ChangeSeq,
    CommitPrecondition, DestinationBehavior, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
    ROOT_INODE_ID,
};
use std::future::Future;

impl<S: ObjectStore + ?Sized> PlanningView<'_, S> {
    /// Checks the request's preconditions in order against the view, which
    /// is the state at `head_seq` before the commit.
    ///
    /// A precondition that doesn't hold fails with its index. A failure to
    /// evaluate one, such as a missing right, fails without the index.
    pub(super) async fn evaluate_preconditions(
        &self,
        preconditions: &[CommitPrecondition],
        head_seq: ChangeSeq,
    ) -> Result<()> {
        for (index, precondition) in preconditions.iter().enumerate() {
            if let Some(mismatch) = self
                .find_precondition_mismatch(precondition, head_seq)
                .await?
            {
                return Err(mismatch.at_precondition(index));
            }
        }
        Ok(())
    }

    async fn find_precondition_mismatch(
        &self,
        precondition: &CommitPrecondition,
        head_seq: ChangeSeq,
    ) -> Result<Option<CoreError>> {
        match precondition {
            CommitPrecondition::NamespaceHead { expected_head_seq } => Ok((*expected_head_seq
                != head_seq)
                .then_some(CoreError::StaleHeadPrecondition {
                    expected: *expected_head_seq,
                    actual: head_seq,
                })),
            CommitPrecondition::FileRevision {
                inode_id,
                expected_revision_no,
            } => {
                self.find_file_revision_mismatch(*inode_id, *expected_revision_no)
                    .await
            }
            CommitPrecondition::PathBinding {
                path,
                expected_inode_id,
                expected_binding_version,
            } => {
                self.find_binding_mismatch(
                    path,
                    Some(*expected_inode_id),
                    expected_binding_version.as_ref(),
                )
                .await
            }
            CommitPrecondition::InodeBinding {
                inode_id,
                expected_binding_version,
            } => {
                self.find_inode_binding_mismatch(*inode_id, expected_binding_version)
                    .await
            }
            CommitPrecondition::PathAbsence { path } => self.find_path_absence_mismatch(path).await,
            CommitPrecondition::NameAbsence {
                parent_inode_id,
                display_name,
            } => {
                self.find_name_absence_mismatch(*parent_inode_id, display_name)
                    .await
            }
            CommitPrecondition::AttributesRevision {
                inode_id,
                expected_attributes_revision_no,
            } => {
                self.find_attributes_revision_mismatch(*inode_id, *expected_attributes_revision_no)
                    .await
            }
            CommitPrecondition::AccessRevision {
                inode_id,
                expected_access_revision_no,
            } => {
                self.find_access_revision_mismatch(*inode_id, *expected_access_revision_no)
                    .await
            }
        }
    }

    async fn find_file_revision_mismatch(
        &self,
        inode_id: InodeId,
        expected: RevisionNo,
    ) -> Result<Option<CoreError>> {
        self.authorize(inode_id, AccessRight::Read.into(), Absence::Inode)
            .await?;
        let actual = self
            .visible_value(inode_id, self.metadata.latest_revision_head(inode_id))
            .await?
            .flatten()
            .map(|revision| revision.revision_no);
        Ok((actual != Some(expected)).then(|| {
            CommitValidationError::BaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            }
            .into()
        }))
    }

    async fn find_attributes_revision_mismatch(
        &self,
        inode_id: InodeId,
        expected: AttributesRevisionNo,
    ) -> Result<Option<CoreError>> {
        self.authorize(inode_id, AccessRight::Read.into(), Absence::Inode)
            .await?;
        let actual = self
            .visible_value(inode_id, async {
                Ok(self
                    .metadata
                    .attributes(inode_id)
                    .await?
                    .attributes_revision_no)
            })
            .await?;
        Ok((actual != Some(expected)).then(|| {
            CommitValidationError::UpdateAttributesBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            }
            .into()
        }))
    }

    async fn find_access_revision_mismatch(
        &self,
        inode_id: InodeId,
        expected: AccessRevisionNo,
    ) -> Result<Option<CoreError>> {
        self.authorize(inode_id, AccessRight::Read.into(), Absence::Inode)
            .await?;
        let actual = self
            .visible_value(inode_id, async {
                Ok(self
                    .metadata
                    .latest_access_revision(inode_id)
                    .await?
                    .map_or(AccessRevisionNo(0), |revision| revision.access_revision_no))
            })
            .await?;
        Ok((actual != Some(expected)).then(|| {
            CommitValidationError::UpdateAccessBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            }
            .into()
        }))
    }

    async fn find_inode_binding_mismatch(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
    ) -> Result<Option<CoreError>> {
        self.authorize(inode_id, AccessRight::Read.into(), Absence::Inode)
            .await?;
        match self.resolve_visible_inode(inode_id).await {
            Ok(resolved) => self.find_binding_version_mismatch(&resolved, expected_binding_version),
            Err(CoreError::InodeNotFound(_)) => Ok(Some(
                self.hidden_inode_binding_error(inode_id, expected_binding_version),
            )),
            Err(error) => Err(error),
        }
    }

    async fn find_path_absence_mismatch(&self, path: &AbsolutePath) -> Result<Option<CoreError>> {
        let parent_path = path.parent().unwrap_or_else(AbsolutePath::root);
        let absence = Absence::Path(path.as_str());
        match self.metadata.resolve_visible_path(&parent_path).await {
            Ok(parent) => {
                self.authorize(parent.inode_id, AccessRight::Read.into(), absence)
                    .await?;
                if parent.inode_kind != InodeKind::Directory {
                    return Ok(None);
                }
            }
            Err(error) if is_missing_visible_path(&error) => return Ok(None),
            Err(CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                inode_id,
                ..
            })) => {
                self.authorize(inode_id, AccessRight::Read.into(), absence)
                    .await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        self.find_binding_mismatch(path, None, None).await
    }

    async fn find_name_absence_mismatch(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Option<CoreError>> {
        match self.resolve_visible_inode(parent_inode_id).await {
            Ok(parent) => {
                self.authorize(parent_inode_id, AccessRight::Read.into(), Absence::Inode)
                    .await?;
                if parent.inode_kind != InodeKind::Directory {
                    return Ok(None);
                }
            }
            Err(CoreError::InodeNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        }
        let name_key = NameKey::for_display_name(display_name);
        Ok(self
            .metadata
            .visible_child(parent_inode_id, &name_key)
            .await?
            .map(|binding| {
                CommitValidationError::BindingPreconditionMismatch {
                    target: format!("name `{name_key}` under parent inode `{parent_inode_id}`"),
                    expected_inode_id: None,
                    actual_inode_id: Some(binding.child_inode_id),
                }
                .into()
            }))
    }

    async fn find_binding_mismatch(
        &self,
        path: &AbsolutePath,
        expected_inode_id: Option<InodeId>,
        expected_binding_version: Option<&BindingVersion>,
    ) -> Result<Option<CoreError>> {
        let actual = match self.metadata.resolve_visible_path(path).await {
            Ok(binding) => {
                if expected_inode_id.is_some() {
                    self.authorize(
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
            return Ok(Some(
                CommitValidationError::BindingPreconditionMismatch {
                    target: format!("path `{path}`"),
                    expected_inode_id,
                    actual_inode_id,
                }
                .into(),
            ));
        }
        match (actual, expected_binding_version) {
            (Some(binding), Some(expected)) => {
                self.find_binding_version_mismatch(&binding, expected)
            }
            _ => Ok(None),
        }
    }

    /// Returns the binding version mismatch of `resolved`, if any.
    ///
    /// The root has no binding version, and its refusal is a failure to
    /// evaluate the precondition rather than a mismatch of it.
    fn find_binding_version_mismatch(
        &self,
        resolved: &ResolvedVisiblePath,
        expected_binding_version: &BindingVersion,
    ) -> Result<Option<CoreError>> {
        match self.ensure_binding_version(resolved, expected_binding_version) {
            Ok(()) => Ok(None),
            Err(CoreError::RootMutationForbidden) => Err(CoreError::RootMutationForbidden),
            Err(mismatch) => Ok(Some(mismatch)),
        }
    }

    /// Returns what `read` returns when `inode_id` is visible, and `None`
    /// when it isn't.
    async fn visible_value<T>(
        &self,
        inode_id: InodeId,
        read: impl Future<Output = Result<T>>,
    ) -> Result<Option<T>> {
        match self.resolve_visible_inode(inode_id).await {
            Ok(_) => read.await.map(Some),
            Err(CoreError::InodeNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
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
        }
    }
}
