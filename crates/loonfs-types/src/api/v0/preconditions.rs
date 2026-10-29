//! Replacement preconditions on v0 HTTP API operations.

use crate::{InodeId, RevisionNo};
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
