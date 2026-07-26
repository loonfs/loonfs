//! Explicit-commit and change-feed shapes for the v0 HTTP API: commit
//! requests with preconditions and semantic operations, the materialized
//! deltas those commits produce, and the ordered change feed that exposes
//! them. Path-oriented convenience operations live in [`super::operations`].

use super::ValidatedContentToken;
use crate::{
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo,
};
use serde::{Deserialize, Serialize};

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

/// Transport wrapper for submitting an explicit semantic commit.
///
/// The flattened commit fields preserve the original bare request shape,
/// while `content_tokens` carries transport-only preparation proofs that do
/// not participate in the semantic commit fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitSubmissionRequest {
    /// Semantic commit whose fields remain at the top level on the wire.
    #[serde(flatten)]
    pub commit: CommitRequest,
    /// Proofs for new external content refs introduced by the commit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ValidatedContentToken>,
}

/// Result of one committed mutation.
///
/// Every mutation resolves to this envelope — path-oriented operations and
/// explicit commits, embedded or remote. The commit id is the caller's
/// reconciliation handle: resubmitting the same request with the same id
/// replays this result instead of committing twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitResponse {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Idempotency key the mutation committed under: caller-supplied, or
    /// generated on the caller's behalf when the request carried none.
    pub commit_id: CommitId,
    /// Sequence number where the mutation became visible.
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
        /// Visible directory that will own the new binding.
        parent_inode_id: InodeId,
        /// Requested child spelling, whose derived name key must be absent.
        display_name: DisplayName,
    },
    /// Create a file under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpCreateFile"))]
    CreateFile {
        /// Visible directory that will own the new binding.
        parent_inode_id: InodeId,
        /// Requested child spelling, whose derived name key must be absent.
        display_name: DisplayName,
        /// Immutable initial bytes, which must have valid preparation proof before publication.
        content_ref: ContentRef,
    },
    /// Append a new revision to an existing file.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpReplaceFile"))]
    ReplaceFile {
        /// Visible file inode receiving a new revision.
        inode_id: InodeId,
        /// Revision the caller observed; the operation conflicts if it is no longer current.
        base_revision_no: RevisionNo,
        /// Immutable replacement bytes, which must have valid preparation proof before publication.
        content_ref: ContentRef,
    },
    /// Restore a prior revision as a new current revision.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRestoreRevision"))]
    RestoreRevision {
        /// Visible file inode receiving the restored content as a new revision.
        inode_id: InodeId,
        /// Existing historical revision whose content is copied forward.
        source_revision_no: RevisionNo,
        /// Current revision the caller observed; concurrent advancement causes a conflict.
        base_revision_no: RevisionNo,
    },
    /// Delete a file inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteFile"))]
    DeleteFile {
        /// Visible file whose exact parent binding and subtree visibility are validated.
        inode_id: InodeId,
    },
    /// Rename or move an inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRename"))]
    Rename {
        /// Visible inode whose current binding will be replaced.
        inode_id: InodeId,
        /// Visible destination directory, which may equal the current parent.
        new_parent_inode_id: InodeId,
        /// Destination spelling, whose derived key must not name another child.
        new_display_name: DisplayName,
    },
    /// Delete a directory subtree.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteSubtree"))]
    DeleteSubtree {
        /// Visible non-root directory whose entire reachable subtree becomes tombstoned.
        root_inode_id: InodeId,
    },
    /// Recover a deleted file or subtree: revoke the deletion recorded at
    /// `deleted_at_seq` (the delete's committed sequence, reported by the
    /// delete and by the change feed) and re-bind the inode under a visible
    /// parent directory. Scoping recovery to the observed generation keeps
    /// a stale request from cancelling a later deletion of the same inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpUndelete"))]
    Undelete {
        /// Deleted inode to make reachable again.
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer tombstone generation.
        deleted_at_seq: ChangeSeq,
        /// Visible directory that will own the recovered binding.
        parent_inode_id: InodeId,
        /// Recovered child spelling, whose derived key must be absent.
        display_name: DisplayName,
    },
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
        /// File inode whose visible revision is tested.
        inode_id: InodeId,
        /// Exact revision required at commit evaluation time.
        revision_no: RevisionNo,
    },
    /// Inode ancestors have not been subtree-deleted.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAncestorsNotSubtreeDeleted")
    )]
    AncestorsNotSubtreeDeleted {
        /// Inode whose ancestor chain must contain no active tombstone.
        inode_id: InodeId,
    },
    /// Directory child name is still absent.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionChildNameAbsent")
    )]
    ChildNameAbsent {
        /// Visible directory in which absence is tested.
        parent_inode_id: InodeId,
        /// Canonical lookup key that must not have an active binding.
        name_key: NameKey,
    },
    /// Directory binding is still exactly the binding the caller saw.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionBindingIs"))]
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
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionDirectoryEmpty")
    )]
    DirectoryEmpty {
        /// Visible directory that must have no active child bindings.
        inode_id: InodeId,
    },
}

/// Durable metadata fact exposed through the change feed.
///
/// Sync and projection clients can apply deltas directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitDelta {
    /// Announces a newly allocated inode and its immutable kind.
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaCreateInode"))]
    CreateInode {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position of this fact within the commit.
        delta_index: u32,
        /// Newly allocated namespace-scoped inode identity.
        inode_id: InodeId,
        /// File-or-directory classification fixed at creation.
        inode_kind: InodeKind,
    },
    /// Announces a newly visible generation of a directory binding.
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaBindDirentry"))]
    BindDirentry {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position and binding identity within the commit.
        delta_index: u32,
        /// Directory receiving the binding.
        parent_inode_id: InodeId,
        /// Policy-derived key used for uniqueness and lookup.
        name_key: NameKey,
        /// User-facing spelling retained for directory responses.
        display_name: DisplayName,
        /// Inode made reachable by the binding.
        child_inode_id: InodeId,
    },
    /// Announces retirement of one exact historical directory binding.
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaUnbindDirentry"))]
    UnbindDirentry {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position of this unbind within the commit.
        delta_index: u32,
        /// Directory from which the binding was removed.
        parent_inode_id: InodeId,
        /// Canonical lookup key of the retired binding.
        name_key: NameKey,
        /// User-facing spelling the retired binding carried, so a feed
        /// projection renders the deleted name without a second lookup.
        display_name: DisplayName,
        /// Child identity on the retired binding.
        child_inode_id: InodeId,
        /// Sequence that created the exact retired binding generation.
        bind_seq: ChangeSeq,
        /// Delta position that disambiguates the generation within `bind_seq`.
        bind_delta_index: u32,
    },
    /// Announces publication of the next immutable content revision of a file.
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaAppendFileRevision"))]
    AppendFileRevision {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position of this revision within the commit.
        delta_index: u32,
        /// File inode whose history advanced.
        inode_id: InodeId,
        /// New monotonic position in that file's revision history.
        revision_no: RevisionNo,
        /// Immutable content published by the revision.
        content_ref: ContentRef,
    },
    /// Announces a tombstone that hides one rooted subtree.
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaTombstoneSubtree"))]
    TombstoneSubtree {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position and tombstone identity within the commit.
        delta_index: u32,
        /// Inode at the root of the newly hidden subtree.
        root_inode_id: InodeId,
        /// Directory that held the deleted binding, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_inode_id: Option<InodeId>,
        /// Canonical key of the deleted binding, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name_key: Option<NameKey>,
        /// User-facing spelling of the deleted binding, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name: Option<DisplayName>,
    },
    /// Announces a compensating event that revokes one exact subtree tombstone.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitDeltaRevokeSubtreeTombstone")
    )]
    RevokeSubtreeTombstone {
        /// Zero-based request-operation position that produced this fact.
        semantic_op_index: u32,
        /// Stable ordering position of this revoke within the commit.
        delta_index: u32,
        /// Root inode governed by the targeted tombstone.
        root_inode_id: InodeId,
        /// The exact deletion generation this revoke cancels. Projections
        /// must reduce with the target, never "whatever is newest".
        target_seq: ChangeSeq,
        /// Delta position that identifies the targeted deletion within `target_seq`.
        target_delta_index: u32,
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
    /// Wall-clock stamp of the commit, in Unix milliseconds.
    /// Observational: `seq` is the order.
    pub committed_at_ms: u64,
    /// Caller annotation, omitted when absent and carrying no filesystem semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Writer label of the session that published this commit.
    /// Observational, like `committed_at_ms`.
    pub writer_id: String,
    /// Session id of the publishing session, disambiguating processes that
    /// share one writer label. Observational.
    pub writer_session_id: String,
    /// Materialized metadata deltas.
    pub deltas: Vec<CommitDelta>,
}

/// Change-feed response after a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChangesResponse {
    /// Namespace whose ordered commit stream was read.
    pub namespace_id: NamespaceId,
    /// Exclusive cursor supplied by the caller, or the endpoint's initial position.
    pub after_seq: ChangeSeq,
    /// Snapshot head through which this page was evaluated.
    pub through_seq: ChangeSeq,
    /// Cursor to request when another page remains, or `None` at `through_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<ChangeSeq>,
    /// Logical commits after `after_seq`, ordered by ascending namespace sequence.
    pub changes: Vec<CommittedChange>,
}

#[cfg(test)]
mod tests {
    use super::{CommitDelta, CommitOp, CommitPrecondition, CommitSubmissionRequest};
    use crate::{ChangeSeq, CommitId, InodeId, InodeKind, NameKey};

    #[test]
    fn bare_commit_body_without_content_tokens_parses_as_submission() {
        let body = br#"{
            "commit_id":"commit-a",
            "preconditions":[],
            "ops":[{
                "kind":"create_directory",
                "parent_inode_id":1,
                "display_name":"docs"
            }],
            "message":"existing body"
        }"#;

        let submission: CommitSubmissionRequest =
            serde_json::from_slice(body).expect("parse pre-token commit body");

        assert_eq!(
            submission.commit.commit_id,
            CommitId::parse("commit-a").expect("valid commit id")
        );
        assert_eq!(submission.commit.ops.len(), 1);
        assert!(submission.content_tokens.is_empty());
    }

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
    fn commit_delta_name_key_serializes_as_plain_string() {
        let delta = CommitDelta::BindDirentry {
            semantic_op_index: 0,
            delta_index: 1,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: crate::DisplayName::parse("Report.txt").expect("valid display name"),
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
            display_name: crate::DisplayName::parse("Report.txt").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(7),
            bind_delta_index: 1,
        };
        assert_eq!(
            serde_json::to_string(&unbind).expect("serialize unbind delta"),
            r#"{"kind":"unbind_direntry","semantic_op_index":0,"delta_index":2,"parent_inode_id":1,"name_key":"report.txt","display_name":"Report.txt","child_inode_id":2,"bind_seq":7,"bind_delta_index":1}"#
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
    fn commit_ops_tolerate_unknown_fields() {
        // Tolerant wire: decoders ignore unknown fields so additive
        // evolution never breaks an older reader.
        let op: CommitOp = serde_json::from_value(serde_json::json!({
            "kind": "rename",
            "inode_id": 2,
            "new_parent_inode_id": 1,
            "new_display_name": "renamed.txt",
            "unknown_future_field": true
        }))
        .expect("unknown fields are ignored");

        assert_eq!(
            op,
            CommitOp::Rename {
                inode_id: InodeId(2),
                new_parent_inode_id: InodeId(1),
                new_display_name: crate::DisplayName::parse("renamed.txt")
                    .expect("valid display name"),
            }
        );
    }

    #[test]
    fn commit_create_directory_uses_directory_wire_name() {
        let op = CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("docs").expect("valid display name"),
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
