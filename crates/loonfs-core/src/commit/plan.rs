//! [`CommitPlan`]: one normalized, validated commit, ready to materialize
//! into WAL deltas.

use super::{CommitFingerprint, CommitIr};

use loonfs_api::wire::manifest::TombstoneGeneration;
use loonfs_api::{
    AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId,
    NameKey, NamespaceId, RevisionNo, WriterEpoch,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
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
/// Identity stays on the request until acceptance, so this value cannot carry
/// an independently identified copy that later needs an equality check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedCommitPlan {
    apply_after_seq: ChangeSeq,
    assigned_seq: ChangeSeq,
    validated_ops: Vec<ValidatedOp>,
}

impl ValidatedCommitPlan {
    pub(crate) fn new(
        apply_after_seq: ChangeSeq,
        assigned_seq: ChangeSeq,
        validated_ops: Vec<ValidatedOp>,
    ) -> Self {
        Self {
            apply_after_seq,
            assigned_seq,
            validated_ops,
        }
    }

    /// Adds the result of accepting the candidate allocation and consumes the
    /// request into the one prepared representation.
    pub(crate) fn prepare(
        self,
        request: CommitIr,
        semantic_identity: CommitFingerprint,
        resulting_next_inode_id: InodeId,
    ) -> CommitPlan {
        let CommitIr {
            namespace_id,
            commit_id,
            writer_epoch,
            ops: _,
            message,
        } = request;
        CommitPlan {
            namespace_id,
            commit_id,
            writer_epoch,
            message,
            semantic_identity,
            apply_after_seq: self.apply_after_seq,
            assigned_seq: self.assigned_seq,
            validated_ops: self.validated_ops,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, PlannedOp};

    #[test]
    fn preparing_validated_plan_moves_identity_into_one_owner() {
        let request = CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_epoch: WriterEpoch(7),
            ops: vec![PlannedOp::unchecked(CommitOp::CreateDirectory {
                child_inode_id: InodeId(2),
                parent_inode_id: InodeId(1),
                display_name: DisplayName::parse("docs").expect("valid display name"),
            })],
            message: Some("create docs".to_owned()),
        };
        let semantic_identity = CommitFingerprint::new_unchecked("v0:sha256:test".to_owned());
        let plan = ValidatedCommitPlan::new(ChangeSeq(3), ChangeSeq(4), Vec::new()).prepare(
            request,
            semantic_identity.clone(),
            InodeId(3),
        );

        assert_eq!(plan.namespace_id.as_str(), "demo");
        assert_eq!(plan.commit_id.as_str(), "commit-a");
        assert_eq!(plan.writer_epoch, WriterEpoch(7));
        assert_eq!(plan.message.as_deref(), Some("create docs"));
        assert_eq!(plan.semantic_identity, semantic_identity);
        assert_eq!(plan.resulting_next_inode_id, InodeId(3));
    }
}
