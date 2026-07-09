//! Explicit-commit and change-feed shapes for the v0 HTTP API: commit
//! requests with preconditions and semantic operations, the materialized
//! deltas those commits produce, and the ordered change feed that exposes
//! them. Path-oriented convenience operations live in [`super::operations`].

use crate::{
    ChangeSeq, CommitId, ContentRef, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Move behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MoveBehavior {
    /// Move only if the destination name is absent.
    #[default]
    NoReplace,
}

/// Explicit semantic commit request.
///
/// Use this lower-level shape when you need one commit id, optional
/// preconditions, and multiple ordered operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitRequest {
    /// Client idempotency key for this logical commit.
    pub commit_id: CommitId,
    /// Optional race checks evaluated before mutation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    /// Ordered semantic operations.
    pub ops: Vec<CommitOp>,
    /// Optional human-readable note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response for a committed explicit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitResponse {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub committed_seq: ChangeSeq,
}

/// Semantic operation inside a commit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitOp {
    /// Create a directory under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpCreateDirectory"))]
    CreateDirectory {
        parent_inode_id: InodeId,
        display_name: String,
    },
    /// Create a file under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpCreateFile"))]
    CreateFile {
        parent_inode_id: InodeId,
        display_name: String,
        content_ref: ContentRef,
    },
    /// Append a new revision to an existing file.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpReplaceFile"))]
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    /// Restore a prior revision as a new current revision.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRestoreRevision"))]
    RestoreRevision {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
    },
    /// Delete a file inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteFile"))]
    DeleteFile { inode_id: InodeId },
    /// Rename or move an inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRename"))]
    Rename {
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        new_display_name: String,
        #[serde(default)]
        behavior: MoveBehavior,
    },
    /// Delete a directory subtree.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteSubtree"))]
    DeleteSubtree { root_inode_id: InodeId },
}

/// Race check evaluated before a commit is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitPrecondition {
    /// File inode is still at this revision.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionInodeRevisionIs")
    )]
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    /// Inode ancestors have not been subtree-deleted.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAncestorsNotSubtreeDeleted")
    )]
    AncestorsNotSubtreeDeleted { inode_id: InodeId },
    /// Directory child name is still absent.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionChildNameAbsent")
    )]
    ChildNameAbsent {
        parent_inode_id: InodeId,
        name_key: NameKey,
    },
    /// Directory binding is still exactly the binding the caller saw.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionBindingIs"))]
    BindingIs {
        parent_inode_id: InodeId,
        name_key: NameKey,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    /// Directory is still empty.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionDirectoryEmpty")
    )]
    DirectoryEmpty { inode_id: InodeId },
}

/// Durable metadata fact exposed through the change feed.
///
/// Sync and projection clients can apply deltas directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitDelta {
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaCreateInode"))]
    CreateInode {
        semantic_op_index: u32,
        delta_index: u32,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaBindDirentry"))]
    BindDirentry {
        semantic_op_index: u32,
        delta_index: u32,
        parent_inode_id: InodeId,
        name_key: NameKey,
        display_name: String,
        child_inode_id: InodeId,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaUnbindDirentry"))]
    UnbindDirentry {
        semantic_op_index: u32,
        delta_index: u32,
        parent_inode_id: InodeId,
        name_key: NameKey,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaAppendFileRevision"))]
    AppendFileRevision {
        semantic_op_index: u32,
        delta_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaTombstoneSubtree"))]
    TombstoneSubtree {
        semantic_op_index: u32,
        delta_index: u32,
        root_inode_id: InodeId,
    },
}

/// One committed change in namespace order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommittedChange {
    /// Namespace sequence for this logical commit.
    pub seq: ChangeSeq,
    /// Client idempotency key for this logical commit.
    pub commit_id: CommitId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Materialized metadata deltas.
    pub deltas: Vec<CommitDelta>,
}

/// Change-feed response after a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChangesResponse {
    pub namespace_id: NamespaceId,
    pub after_seq: ChangeSeq,
    pub through_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<ChangeSeq>,
    pub changes: Vec<CommittedChange>,
}

#[cfg(test)]
mod tests {
    use super::{CommitDelta, CommitOp, CommitPrecondition, MoveBehavior};
    use crate::{ChangeSeq, InodeId, InodeKind, NameKey};

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
    fn commit_delta_name_key_serializes_as_plain_string() {
        let delta = CommitDelta::BindDirentry {
            semantic_op_index: 0,
            delta_index: 1,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: "Report.txt".to_owned(),
            child_inode_id: InodeId(2),
        };

        assert_eq!(
            serde_json::to_string(&delta).expect("serialize delta"),
            r#"{"kind":"bind_direntry","semantic_op_index":0,"delta_index":1,"parent_inode_id":1,"name_key":"report.txt","display_name":"Report.txt","child_inode_id":2}"#
        );

        let unbind = CommitDelta::UnbindDirentry {
            semantic_op_index: 0,
            delta_index: 2,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(7),
            bind_delta_index: 1,
        };
        assert_eq!(
            serde_json::to_string(&unbind).expect("serialize unbind delta"),
            r#"{"kind":"unbind_direntry","semantic_op_index":0,"delta_index":2,"parent_inode_id":1,"name_key":"report.txt","child_inode_id":2,"bind_seq":7,"bind_delta_index":1}"#
        );

        let create_inode = CommitDelta::CreateInode {
            semantic_op_index: 0,
            delta_index: 0,
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
        };
        assert_eq!(
            serde_json::to_string(&create_inode).expect("serialize create inode"),
            r#"{"kind":"create_inode","semantic_op_index":0,"delta_index":0,"inode_id":2,"inode_kind":"file"}"#
        );
    }

    #[test]
    fn commit_rename_defaults_omitted_behavior_to_no_replace() {
        assert_eq!(MoveBehavior::default(), MoveBehavior::NoReplace);

        let op: CommitOp = serde_json::from_value(serde_json::json!({
            "kind": "rename",
            "inode_id": 2,
            "new_parent_inode_id": 1,
            "new_display_name": "renamed.txt"
        }))
        .expect("rename defaults behavior");

        assert_eq!(
            op,
            CommitOp::Rename {
                inode_id: InodeId(2),
                new_parent_inode_id: InodeId(1),
                new_display_name: "renamed.txt".to_owned(),
                behavior: MoveBehavior::NoReplace,
            }
        );
    }

    #[test]
    fn commit_create_directory_uses_directory_wire_name() {
        let op = CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: "docs".to_owned(),
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
