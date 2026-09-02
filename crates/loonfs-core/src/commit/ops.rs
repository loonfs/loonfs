//! Inode-level operations produced by filesystem mutation planning, along
//! with their concurrency checks.
//!
//! This vocabulary is internal. Callers speak
//! [`CommitRequest`](crate::path::write::CommitRequest), whose
//! path-oriented operations the planner compiles into the ops below; nothing
//! outside this crate constructs them.

use loonfs_api::{
    AttributeRevisionNo, Attributes, ChangeSeq, ContentRef, DisplayName, InodeId, RevisionNo,
};

use super::ResolvedBinding;

/// Semantic operation inside a planned commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommitOp {
    /// Create a directory under a parent inode.
    CreateDirectory {
        /// Inode identity assigned once by the candidate allocator during
        /// planning and carried unchanged through validation.
        child_inode_id: InodeId,
        /// Visible directory that will own the new binding.
        parent_inode_id: InodeId,
        /// Requested child spelling, whose derived name key must be absent.
        display_name: DisplayName,
    },
    /// Create a file under a parent inode.
    CreateFile {
        /// Inode identity assigned once by the candidate allocator during
        /// planning and carried unchanged through validation.
        child_inode_id: InodeId,
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
        /// Exact parent binding observed during planning.
        source_binding: ResolvedBinding,
    },
    /// Rename or move an inode.
    Rename {
        /// Visible inode whose current binding will be replaced.
        inode_id: InodeId,
        /// Exact parent binding observed during planning.
        source_binding: ResolvedBinding,
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
        /// Exact parent binding observed during planning.
        source_binding: ResolvedBinding,
        /// Whether the root must have no visible children.
        require_empty: bool,
    },
    /// Recover a deleted file or subtree: revoke the deletion recorded at
    /// `deletion_seq` (the delete's committed sequence, reported by the
    /// delete and by the change feed) and re-bind the inode under a visible
    /// parent directory. Scoping recovery to the observed generation keeps
    /// a stale request from cancelling a later deletion of the same inode.
    Undelete {
        /// Deleted inode to make reachable again.
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer
        /// tombstone generation.
        deletion_seq: ChangeSeq,
        /// Visible directory that will own the recovered binding.
        parent_inode_id: InodeId,
        /// Recovered child spelling, whose derived key must be absent.
        display_name: DisplayName,
    },
    /// Replace an inode's attributes with a new complete map.
    ///
    /// The planner has already applied the caller's writes and removals to
    /// what it read, so the op carries the result rather than the request.
    /// It does not carry the revision it publishes: validation derives that
    /// from the base.
    UpdateAttributes {
        /// Visible file or directory whose attributes are replaced.
        inode_id: InodeId,
        /// Attribute revision the planner resolved against; the operation
        /// conflicts if it is no longer current.
        base_attributes_revision_no: AttributeRevisionNo,
        /// The inode's complete attribute map after the update.
        attributes: Attributes,
    },
}
