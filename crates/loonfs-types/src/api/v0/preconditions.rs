//! Replacement and revision preconditions on v0 HTTP API operations, and their validation.

use crate::{AccessRevisionNo, AttributesRevisionNo, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};

/// What a path put, move, or copy does when the destination already exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationBehavior {
    /// An operation that fails if the destination path already exists.
    #[default]
    NoReplace,
    /// An operation that replaces the current destination file.
    Replace,
}

/// The requirements for replacing the destination of a move or copy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DestinationPrecondition {
    /// Whether the operation can replace an existing destination file.
    #[serde(default)]
    pub behavior: DestinationBehavior,
    /// The inode that the destination path must contain. It applies only with `replace` behavior.
    #[serde(
        rename = "expected_destination_inode_id",
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_inode_id: Option<InodeId>,
    /// The content revision that must be the destination file's current revision. It applies only
    /// with `replace` behavior and requires `expected_destination_inode_id`.
    #[serde(
        rename = "expected_destination_revision_no",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_revision_no: Option<RevisionNo>,
}

/// Field-name family used when validating replacement preconditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionFields {
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
pub struct ExpectedFileState {
    /// Destination inode required by the request.
    pub inode_id: InodeId,
    /// Destination content revision required by the request.
    pub revision_no: Option<RevisionNo>,
}

/// Why a destination precondition is not a valid request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DestinationPreconditionError {
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

impl DestinationPrecondition {
    /// Validates the precondition and returns the required destination state.
    pub fn resolve(
        &self,
        fields: PreconditionFields,
    ) -> Result<Option<ExpectedFileState>, DestinationPreconditionError> {
        if self.behavior == DestinationBehavior::NoReplace
            && !matches!(
                (self.expected_inode_id, self.expected_revision_no),
                (None, None)
            )
        {
            let (revision_field, inode_field) = fields.names();
            return Err(DestinationPreconditionError::PreconditionsRequireReplace {
                field: if self.expected_inode_id.is_some() {
                    inode_field
                } else {
                    revision_field
                },
            });
        }
        let Some(inode_id) = self.expected_inode_id else {
            if self.expected_revision_no.is_some() {
                let (revision_field, inode_field) = fields.names();
                return Err(DestinationPreconditionError::RevisionRequiresInode {
                    revision_field,
                    inode_field,
                });
            }
            return Ok(None);
        };
        Ok(Some(ExpectedFileState {
            inode_id,
            revision_no: self.expected_revision_no,
        }))
    }
}

/// Rejects an attribute revision precondition without an inode precondition.
pub fn validate_attributes_precondition(
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributesRevisionNo>,
) -> Result<(), DestinationPreconditionError> {
    validate_revision_precondition(
        expected_inode_id,
        expected_attributes_revision_no.is_some(),
        "expected_attributes_revision_no",
    )
}

/// Rejects an access revision precondition without an inode precondition.
pub fn validate_access_precondition(
    expected_inode_id: Option<InodeId>,
    expected_access_revision_no: Option<AccessRevisionNo>,
) -> Result<(), DestinationPreconditionError> {
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
) -> Result<(), DestinationPreconditionError> {
    if has_revision && expected_inode_id.is_none() {
        return Err(DestinationPreconditionError::RevisionRequiresInode {
            revision_field,
            inode_field: "expected_inode_id",
        });
    }
    Ok(())
}
