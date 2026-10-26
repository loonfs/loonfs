//! Request-level preconditions, which are checked in order against the state before the commit
//! (api.md, "Request-level preconditions").

use super::planner::stale_revision;
use crate::error::{MountSimError, Result};
use crate::model_view::ModelView;
use crate::tokens::decode_binding_version;
use loonfs_model::metadata::DeltaPosition;
use loonfs_model::visibility::PathLookup;
use loonfs_types::api::v0::CommitPrecondition;
use loonfs_types::{
    AbsolutePath, AccessRevisionNo, BindingVersion, ErrorCode, InodeId, InodeKind, ROOT_INODE_ID,
};

/// Checks each precondition in order, and returns the first failure with its
/// `precondition_index`.
pub(super) fn evaluate(view: &ModelView<'_>, preconditions: &[CommitPrecondition]) -> Result<()> {
    for (index, precondition) in preconditions.iter().enumerate() {
        evaluate_one(view, precondition).map_err(|error| error.at_precondition(index))?;
    }
    Ok(())
}

fn evaluate_one(view: &ModelView<'_>, precondition: &CommitPrecondition) -> Result<()> {
    match precondition {
        CommitPrecondition::NamespaceHead { expected_head_seq } => {
            if *expected_head_seq == view.seq {
                return Ok(());
            }
            Err(MountSimError::new(
                ErrorCode::StaleHead,
                format!(
                    "the head is {}, not the expected {expected_head_seq}",
                    view.seq
                ),
            )
            .with_details(|details| {
                details.expected_head_seq = Some(*expected_head_seq);
                details.actual_head_seq = Some(view.seq);
            }))
        }
        CommitPrecondition::FileRevision {
            inode_id,
            expected_revision_no,
        } => {
            let actual = view
                .visible_inode(*inode_id)
                .and_then(|_| view.current_revision(*inode_id))
                .map(|revision| revision.revision_no);
            if actual == Some(*expected_revision_no) {
                return Ok(());
            }
            Err(stale_revision(*inode_id, *expected_revision_no, actual))
        }
        CommitPrecondition::PathBinding {
            path,
            expected_inode_id,
            expected_binding_version,
        } => path_binding(
            view,
            path,
            *expected_inode_id,
            expected_binding_version.as_ref(),
        ),
        CommitPrecondition::InodeBinding {
            inode_id,
            expected_binding_version,
        } => {
            if *inode_id == ROOT_INODE_ID {
                return Err(
                    MountSimError::invalid_request("the root has no binding version")
                        .with_param("inode_id"),
                );
            }
            ensure_binding(view, *inode_id, expected_binding_version)
        }
        CommitPrecondition::PathAbsence { path } => match view.resolve_path(path) {
            PathLookup::Found { inode, .. } => Err(bound(path.as_str(), inode.inode_id)),
            PathLookup::NotFound { .. } | PathLookup::NotADirectory { .. } => Ok(()),
        },
        CommitPrecondition::NameAbsence {
            parent_inode_id,
            display_name,
        } => {
            let is_directory = view
                .visible_inode(*parent_inode_id)
                .is_some_and(|parent| parent.inode_kind == InodeKind::Directory);
            match view.visible_child(*parent_inode_id, display_name) {
                Some(child) if is_directory => {
                    Err(bound(display_name.as_str(), child.child.inode_id))
                }
                _ => Ok(()),
            }
        }
        CommitPrecondition::AttributesRevision {
            inode_id,
            expected_attributes_revision_no,
        } => {
            let actual = view
                .visible_inode(*inode_id)
                .map(|_| view.attributes(*inode_id).revision_no);
            if actual == Some(*expected_attributes_revision_no) {
                return Ok(());
            }
            Err(MountSimError::new(
                ErrorCode::StaleAttributes,
                "the inode's attribute revision isn't the expected revision",
            )
            .with_details(|details| {
                details.inode_id = Some(*inode_id);
                details.expected_attributes_revision_no = Some(*expected_attributes_revision_no);
                details.actual_attributes_revision_no = actual;
            }))
        }
        // An unrestricted namespace has no access rows, so every visible inode is at access
        // revision 0.
        CommitPrecondition::AccessRevision {
            inode_id,
            expected_access_revision_no,
        } => {
            let actual = view.visible_inode(*inode_id).map(|_| AccessRevisionNo(0));
            if actual == Some(*expected_access_revision_no) {
                return Ok(());
            }
            Err(MountSimError::new(
                ErrorCode::StaleAccess,
                "the inode's access revision isn't the expected revision",
            )
            .with_details(|details| {
                details.inode_id = Some(*inode_id);
                details.expected_access_revision_no = Some(*expected_access_revision_no);
                details.actual_access_revision_no = actual;
            }))
        }
    }
}

fn path_binding(
    view: &ModelView<'_>,
    path: &AbsolutePath,
    expected_inode_id: InodeId,
    expected_binding_version: Option<&BindingVersion>,
) -> Result<()> {
    let actual = match view.resolve_path(path) {
        PathLookup::Found { inode, .. } => Some(inode.inode_id),
        PathLookup::NotFound { .. } | PathLookup::NotADirectory { .. } => None,
    };
    if actual != Some(expected_inode_id) {
        return Err(MountSimError::new(
            ErrorCode::PathConflict,
            format!("`{path}` doesn't resolve to the expected inode"),
        )
        .with_details(|details| {
            details.expected_inode_id = Some(expected_inode_id);
            details.actual_inode_id = actual;
        }));
    }
    match expected_binding_version {
        Some(_) if path.is_root() => Err(MountSimError::invalid_request(
            "the root has no binding version",
        )
        .with_param("expected_binding_version")),
        Some(expected) => ensure_binding(view, expected_inode_id, expected),
        None => Ok(()),
    }
}

/// Checks that a visible inode is still bound at the version the caller read.
fn ensure_binding(
    view: &ModelView<'_>,
    inode_id: InodeId,
    expected: &BindingVersion,
) -> Result<()> {
    let Some(expected_position) = decode_binding_version(view.namespace_id, expected) else {
        return Err(MountSimError::invalid_request(
            "`expected_binding_version` is malformed or belongs to another namespace",
        )
        .with_param("expected_binding_version"));
    };
    let binding = view
        .visible_inode(inode_id)
        .and_then(|_| view.current_binding(inode_id));
    let actual = binding.map(|binding| DeltaPosition {
        seq: binding.committed_seq,
        delta_index: binding.delta_index,
    });
    if actual == Some(expected_position) {
        return Ok(());
    }
    Err(MountSimError::new(
        ErrorCode::BindingVersionMismatch,
        "the entry's binding changed since the caller read it",
    )
    .with_details(|details| {
        details.inode_id = Some(inode_id);
        details.expected_binding_version = Some(expected.clone());
        details.actual_binding_version = binding.map(|binding| view.binding_version(binding));
    }))
}

fn bound(name: &str, actual_inode_id: InodeId) -> MountSimError {
    MountSimError::new(ErrorCode::PathConflict, format!("`{name}` is bound"))
        .with_details(|details| details.actual_inode_id = Some(actual_inode_id))
}
