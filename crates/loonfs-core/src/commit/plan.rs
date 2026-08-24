//! [`CommitPlan`]: one normalized, validated commit, ready to materialize
//! into WAL deltas.

use super::CommitFingerprint;

use loonfs_api::wire::manifest::TombstoneGeneration;
use loonfs_api::{
    ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName,
    InodeId, NameKey, NamespaceId, RevisionNo, WriterEpoch,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub actor: ActorRef,
    pub writer_epoch: WriterEpoch,
    pub message: Option<String>,
    pub semantic_identity: CommitFingerprint,
    pub apply_after_seq: ChangeSeq,
    pub assigned_seq: ChangeSeq,
    pub(crate) validated_ops: Vec<ValidatedOp>,
    pub resulting_next_inode_id: InodeId,
}

/// Validation output before the candidate-local inode allocation is accepted.
///
/// The request's identity moves in here the moment validation succeeds, so
/// there is never an independently identified copy that later needs an
/// equality check; only the accepted allocation's resulting position is
/// still missing, and [`Self::finish`] adds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedCommitPlan {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) commit_id: CommitId,
    pub(crate) actor: ActorRef,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) message: Option<String>,
    pub(crate) semantic_identity: CommitFingerprint,
    pub(crate) apply_after_seq: ChangeSeq,
    pub(crate) assigned_seq: ChangeSeq,
    pub(crate) validated_ops: Vec<ValidatedOp>,
}

impl ValidatedCommitPlan {
    /// Adds the result of accepting the candidate allocation, completing the
    /// one prepared representation.
    pub(crate) fn finish(self, resulting_next_inode_id: InodeId) -> CommitPlan {
        let Self {
            namespace_id,
            commit_id,
            actor,
            writer_epoch,
            message,
            semantic_identity,
            apply_after_seq,
            assigned_seq,
            validated_ops,
        } = self;
        CommitPlan {
            namespace_id,
            commit_id,
            actor,
            writer_epoch,
            message,
            semantic_identity,
            apply_after_seq,
            assigned_seq,
            validated_ops,
            resulting_next_inode_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ValidatedOp {
    CreateDir {
        op_index: u32,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        child_inode_id: InodeId,
        create_inode_delta_index: u32,
        bind_delta_index: u32,
    },
    CreateFile {
        op_index: u32,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        child_inode_id: InodeId,
        content_ref: ContentRef,
        create_inode_delta_index: u32,
        bind_delta_index: u32,
        revision_delta_index: u32,
    },
    ReplaceFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
        revision_delta_index: u32,
    },
    RestoreRevision {
        op_index: u32,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        revision_no: RevisionNo,
        content_ref: ContentRef,
        revision_delta_index: u32,
    },
    DeleteFile {
        op_index: u32,
        inode_id: InodeId,
        source_binding: ResolvedBinding,
        unbind_delta_index: u32,
        tombstone_delta_index: u32,
    },
    Rename {
        op_index: u32,
        inode_id: InodeId,
        source_binding: ResolvedBinding,
        new_parent_inode_id: InodeId,
        new_display_name: DisplayName,
        new_name_key: NameKey,
        unbind_delta_index: u32,
        bind_delta_index: u32,
    },
    DeleteSubtree {
        op_index: u32,
        root_inode_id: InodeId,
        source_binding: ResolvedBinding,
        unbind_delta_index: u32,
        tombstone_delta_index: u32,
    },
    Undelete {
        op_index: u32,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        /// The exact deletion generation validation resolved and pinned:
        /// the active tombstone's own event coordinates.
        target: TombstoneGeneration,
        revoke_tombstone_delta_index: u32,
        bind_delta_index: u32,
    },
    UpdateAttributes {
        op_index: u32,
        inode_id: InodeId,
        attributes_revision_no: AttributeRevisionNo,
        attributes: Attributes,
        attributes_delta_index: u32,
    },
}
