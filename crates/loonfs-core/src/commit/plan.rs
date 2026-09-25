//! Validated commit deltas and the metadata needed for publication.

use super::CommitFingerprint;
use crate::storage::inline_content::InlineContent;

use loonfs_api::wire::manifest::DeltaPosition;
use loonfs_api::wire::wal::WalCommitDelta;
use loonfs_api::{
    ActorId, ChangeSeq, CommitId, DisplayName, InodeId, NameKey, NamespaceId, WriterEpoch,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub actor_id: ActorId,
    pub writer_epoch: WriterEpoch,
    pub message: Option<String>,
    pub semantic_identity: CommitFingerprint,
    pub apply_after_seq: ChangeSeq,
    pub assigned_seq: ChangeSeq,
    pub(crate) deltas: Vec<WalCommitDelta>,
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
    pub(crate) actor_id: ActorId,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) message: Option<String>,
    pub(crate) semantic_identity: CommitFingerprint,
    pub(crate) apply_after_seq: ChangeSeq,
    pub(crate) assigned_seq: ChangeSeq,
    pub(crate) deltas: Vec<WalCommitDelta>,
}

impl ValidatedCommitPlan {
    /// Adds the result of accepting the candidate allocation, completing the
    /// one prepared representation.
    pub(crate) fn finish(self, resulting_next_inode_id: InodeId) -> CommitPlan {
        let Self {
            namespace_id,
            commit_id,
            actor_id,
            writer_epoch,
            message,
            semantic_identity,
            apply_after_seq,
            assigned_seq,
            deltas,
        } = self;
        CommitPlan {
            namespace_id,
            commit_id,
            actor_id,
            writer_epoch,
            message,
            semantic_identity,
            apply_after_seq,
            assigned_seq,
            deltas,
            resulting_next_inode_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub position: DeltaPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedCommit {
    pub commit: CommitPlan,
    pub committed_at_ms: u64,
    pub inline_content: Vec<InlineContent>,
}
