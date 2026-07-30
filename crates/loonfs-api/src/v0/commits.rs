//! The engine's commit vocabulary and the change-feed shapes for the v0
//! HTTP API: commit requests with preconditions and semantic operations
//! (the internal IR path operations compile into), and the ordered feed of
//! semantic filesystem events those commits produce. Path-oriented
//! operations live in [`super::operations`].

use crate::{
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Explicit semantic commit request: one commit id, optional preconditions,
/// and multiple ordered operations.
///
/// This is the engine's internal mutation vocabulary — path operations
/// compile into it during planning. It is not a stable wire surface.
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

/// One semantic filesystem change inside a committed mutation.
///
/// Each event corresponds to one operation of the committed request, in
/// request order. Events name inodes and their parent-directory bindings
/// rather than full paths; a consumer that needs paths can stat the inode
/// or maintain its own binding projection from this feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilesystemChange {
    /// A file or directory was created.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeCreated"))]
    Created {
        /// Newly allocated namespace-scoped inode identity.
        inode_id: InodeId,
        /// File-or-directory classification fixed at creation.
        inode_kind: InodeKind,
        /// Directory the new entry was bound under.
        parent_inode_id: InodeId,
        /// User-facing spelling of the new entry.
        name: DisplayName,
        /// First revision number, for file creations.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision_no: Option<RevisionNo>,
        /// Content of the first revision, for file creations.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_ref: Option<ContentRef>,
    },
    /// A file received a new current revision — a put over an existing
    /// file, or a revision restore (one durable fact for both).
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeContentChanged"))]
    ContentChanged {
        /// File inode whose history advanced.
        inode_id: InodeId,
        /// New monotonic position in that file's revision history.
        revision_no: RevisionNo,
        /// Immutable content published by the revision.
        content_ref: ContentRef,
    },
    /// An inode moved to a new parent directory or name.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeMoved"))]
    Moved {
        /// Inode whose binding changed.
        inode_id: InodeId,
        /// Directory that held the old binding.
        from_parent_inode_id: InodeId,
        /// Spelling of the old binding.
        from_name: DisplayName,
        /// Directory holding the new binding.
        to_parent_inode_id: InodeId,
        /// Spelling of the new binding.
        to_name: DisplayName,
    },
    /// A file or directory subtree was deleted. The enclosing change's
    /// `seq` is the deletion generation an undelete request passes as
    /// `deleted_at_seq`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeDeleted"))]
    Deleted {
        /// Inode at the root of the deleted subtree.
        inode_id: InodeId,
        /// Directory that held the deleted binding, when the delete
        /// recorded one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_inode_id: Option<InodeId>,
        /// Spelling of the deleted binding, when the delete recorded one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<DisplayName>,
    },
    /// A deleted inode was recovered and re-bound.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeUndeleted"))]
    Undeleted {
        /// Recovered inode.
        inode_id: InodeId,
        /// Directory the recovered entry was bound under.
        parent_inode_id: InodeId,
        /// Spelling of the recovered binding.
        name: DisplayName,
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
    /// Semantic filesystem events, one per committed operation, in
    /// request-operation order.
    pub events: Vec<FilesystemChange>,
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
    use super::{CommitOp, CommitPrecondition, FilesystemChange};
    use crate::{InodeId, InodeKind, NameKey};

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
    fn filesystem_change_events_use_snake_case_kind_tags() {
        let sample_content_ref = crate::ContentRef {
            kind: crate::ContentRefKind::WholeFileV0,
            digest: "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                .to_owned(),
            size_bytes: 5,
        };

        let created = FilesystemChange::Created {
            inode_id: InodeId(2),
            inode_kind: InodeKind::Directory,
            parent_inode_id: InodeId(1),
            name: crate::DisplayName::parse("Docs").expect("valid display name"),
            revision_no: None,
            content_ref: None,
        };
        assert_eq!(
            serde_json::to_string(&created).expect("serialize created event"),
            r#"{"kind":"created","inode_id":2,"inode_kind":"dir","parent_inode_id":1,"name":"Docs"}"#
        );

        let created_file = FilesystemChange::Created {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(1),
            name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            revision_no: Some(crate::RevisionNo(1)),
            content_ref: Some(sample_content_ref.clone()),
        };
        assert_eq!(
            serde_json::to_string(&created_file).expect("serialize created file event"),
            r#"{"kind":"created","inode_id":2,"inode_kind":"file","parent_inode_id":1,"name":"a.txt","revision_no":1,"content_ref":{"kind":"whole_file_v0","digest":"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","size_bytes":5}}"#
        );

        let content_changed = FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: crate::RevisionNo(3),
            content_ref: sample_content_ref,
        };
        assert_eq!(
            serde_json::to_string(&content_changed).expect("serialize content changed event"),
            r#"{"kind":"content_changed","inode_id":2,"revision_no":3,"content_ref":{"kind":"whole_file_v0","digest":"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","size_bytes":5}}"#
        );

        let moved = FilesystemChange::Moved {
            inode_id: InodeId(2),
            from_parent_inode_id: InodeId(1),
            from_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            to_parent_inode_id: InodeId(3),
            to_name: crate::DisplayName::parse("b.txt").expect("valid display name"),
        };
        assert_eq!(
            serde_json::to_string(&moved).expect("serialize moved event"),
            r#"{"kind":"moved","inode_id":2,"from_parent_inode_id":1,"from_name":"a.txt","to_parent_inode_id":3,"to_name":"b.txt"}"#
        );

        let deleted = FilesystemChange::Deleted {
            inode_id: InodeId(2),
            parent_inode_id: Some(InodeId(1)),
            name: Some(crate::DisplayName::parse("a.txt").expect("valid display name")),
        };
        assert_eq!(
            serde_json::to_string(&deleted).expect("serialize deleted event"),
            r#"{"kind":"deleted","inode_id":2,"parent_inode_id":1,"name":"a.txt"}"#
        );

        let undeleted = FilesystemChange::Undeleted {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            name: crate::DisplayName::parse("a.txt").expect("valid display name"),
        };
        assert_eq!(
            serde_json::to_string(&undeleted).expect("serialize undeleted event"),
            r#"{"kind":"undeleted","inode_id":2,"parent_inode_id":1,"name":"a.txt"}"#
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
