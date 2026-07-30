//! The engine's inode-level planning IR: the operations a filesystem
//! mutation compiles into, and the race checks evaluated beside them.
//!
//! This vocabulary is internal. Callers speak
//! [`MutationRequest`](crate::path::write::MutationRequest), whose
//! path-oriented operations the planner compiles into the ops below; nothing
//! outside this crate constructs them.

use loonfs_api::{ChangeSeq, ContentRef, DisplayName, InodeId, NameKey, RevisionNo};
use serde::{Deserialize, Serialize};

/// One planned inode-level operation together with the race checks that must
/// hold immediately before it runs.
///
/// Checks are scoped to their operation rather than to the whole commit
/// because a commit's operations observe each other: a check written against
/// what operation `k` saw is only meaningful once operations `0..k` have been
/// applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PlannedOp {
    /// Race checks evaluated immediately before [`Self::op`].
    pub(crate) preconditions: Vec<CommitPrecondition>,
    /// The operation itself.
    pub(crate) op: CommitOp,
}

impl PlannedOp {
    /// A planned operation with no race checks of its own.
    #[cfg(test)]
    pub(crate) fn unchecked(op: CommitOp) -> Self {
        Self {
            preconditions: Vec::new(),
            op,
        }
    }
}

/// Semantic operation inside a planned commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CommitOp {
    /// Create a directory under a parent inode.
    CreateDirectory {
        /// Visible directory that will own the new binding.
        parent_inode_id: InodeId,
        /// Requested child spelling, whose derived name key must be absent.
        display_name: DisplayName,
    },
    /// Create a file under a parent inode.
    CreateFile {
        /// Visible directory that will own the new binding.
        parent_inode_id: InodeId,
        /// Requested child spelling, whose derived name key must be absent.
        display_name: DisplayName,
        /// Immutable initial bytes, which must have valid preparation proof
        /// before publication.
        content_ref: ContentRef,
    },
    /// Append a new revision to an existing file.
    ReplaceFile {
        /// Visible file inode receiving a new revision.
        inode_id: InodeId,
        /// Revision the caller observed; the operation conflicts if it is no
        /// longer current.
        base_revision_no: RevisionNo,
        /// Immutable replacement bytes, which must have valid preparation
        /// proof before publication.
        content_ref: ContentRef,
    },
    /// Restore a prior revision as a new current revision.
    RestoreRevision {
        /// Visible file inode receiving the restored content as a new
        /// revision.
        inode_id: InodeId,
        /// Existing historical revision whose content is copied forward.
        source_revision_no: RevisionNo,
        /// Current revision the caller observed; concurrent advancement
        /// causes a conflict.
        base_revision_no: RevisionNo,
    },
    /// Delete a file inode.
    DeleteFile {
        /// Visible file whose exact parent binding and subtree visibility are
        /// validated.
        inode_id: InodeId,
    },
    /// Rename or move an inode.
    Rename {
        /// Visible inode whose current binding will be replaced.
        inode_id: InodeId,
        /// Visible destination directory, which may equal the current parent.
        new_parent_inode_id: InodeId,
        /// Destination spelling, whose derived key must not name another
        /// child.
        new_display_name: DisplayName,
    },
    /// Delete a directory subtree.
    DeleteSubtree {
        /// Visible non-root directory whose entire reachable subtree becomes
        /// tombstoned.
        root_inode_id: InodeId,
    },
    /// Recover a deleted file or subtree: revoke the deletion recorded at
    /// `deleted_at_seq` (the delete's committed sequence, reported by the
    /// delete and by the change feed) and re-bind the inode under a visible
    /// parent directory. Scoping recovery to the observed generation keeps
    /// a stale request from cancelling a later deletion of the same inode.
    Undelete {
        /// Deleted inode to make reachable again.
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer
        /// tombstone generation.
        deleted_at_seq: ChangeSeq,
        /// Visible directory that will own the recovered binding.
        parent_inode_id: InodeId,
        /// Recovered child spelling, whose derived key must be absent.
        display_name: DisplayName,
    },
}

/// Race check evaluated immediately before the operation that carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CommitPrecondition {
    /// File inode is still at this revision.
    InodeRevisionIs {
        /// File inode whose visible revision is tested.
        inode_id: InodeId,
        /// Exact revision required at commit evaluation time.
        revision_no: RevisionNo,
    },
    /// Inode ancestors have not been subtree-deleted.
    AncestorsNotSubtreeDeleted {
        /// Inode whose ancestor chain must contain no active tombstone.
        inode_id: InodeId,
    },
    /// Directory child name is still absent.
    ChildNameAbsent {
        /// Visible directory in which absence is tested.
        parent_inode_id: InodeId,
        /// Canonical lookup key that must not have an active binding.
        name_key: NameKey,
    },
    /// Directory binding is still exactly the binding the planner saw.
    BindingIs {
        /// Directory expected to own the observed binding.
        parent_inode_id: InodeId,
        /// Canonical name key of the observed binding.
        name_key: NameKey,
        /// Child identity expected at that name.
        child_inode_id: InodeId,
        /// Sequence that created the exact observed binding generation.
        bind_seq: ChangeSeq,
        /// Delta position that disambiguates the generation within `bind_seq`.
        bind_delta_index: u32,
    },
    /// Directory is still empty.
    DirectoryEmpty {
        /// Visible directory that must have no active child bindings.
        inode_id: InodeId,
    },
}

#[cfg(test)]
mod tests {
    use super::{CommitOp, CommitPrecondition};
    use loonfs_api::{InodeId, NameKey};

    #[test]
    fn commit_precondition_name_key_serializes_as_plain_string() {
        let precondition = CommitPrecondition::ChildNameAbsent {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
        };

        assert_eq!(
            serde_json::to_string(&precondition).expect("serialize precondition"),
            r#"{"kind":"child_name_absent","parent_inode_id":1,"name_key":"report.txt"}"#
        );
    }

    #[test]
    fn commit_precondition_rejects_invalid_name_key() {
        let encoded = r#"{
            "kind":"child_name_absent",
            "parent_inode_id":1,
            "name_key":"invalid/name"
        }"#;

        assert!(serde_json::from_str::<CommitPrecondition>(encoded).is_err());
    }

    #[test]
    fn commit_create_directory_uses_directory_wire_name() {
        let op = CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        };

        assert_eq!(
            serde_json::to_value(&op).expect("create directory op json"),
            serde_json::json!({
                "kind": "create_directory",
                "parent_inode_id": 1,
                "display_name": "docs"
            })
        );
    }
}
