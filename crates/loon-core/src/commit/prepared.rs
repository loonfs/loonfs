use super::{
    semantic_commit_fingerprint_sha256, CommitFingerprintError, CommitPlan, CommitRequest,
};
use loon_api::{FenceToken, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionContext {
    pub namespace_id: NamespaceId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommit {
    pub request: CommitRequest,
    pub plan: CommitPlan,
    pub semantic_commit_fingerprint_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommit {
    pub prepared: PreparedCommit,
    pub results: Vec<loon_api::v0::CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitPrepareError {
    #[error("prepared commit namespace mismatch: request={request:?}, plan={plan:?}")]
    NamespaceMismatch {
        request: NamespaceId,
        plan: NamespaceId,
    },
    #[error("prepared commit id mismatch")]
    CommitIdMismatch,
    #[error(transparent)]
    Fingerprint(#[from] CommitFingerprintError),
}

impl PreparedCommit {
    pub fn new(request: CommitRequest, plan: CommitPlan) -> Result<Self, CommitPrepareError> {
        if request.namespace_id != plan.namespace_id {
            return Err(CommitPrepareError::NamespaceMismatch {
                request: request.namespace_id.clone(),
                plan: plan.namespace_id.clone(),
            });
        }
        if request.commit_id != plan.commit_id {
            return Err(CommitPrepareError::CommitIdMismatch);
        }
        let semantic_commit_fingerprint_sha256 = semantic_commit_fingerprint_sha256(&request)?;

        Ok(Self {
            request,
            plan,
            semantic_commit_fingerprint_sha256,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitOp, Precondition};
    use loon_api::{ChangeSeq, CommitId, InodeId};

    fn request() -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::from("demo"),
            commit_id: CommitId::from("commit-a"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(0),
            semantic_commit_fingerprint_sha256: None,
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(0))],
            message: None,
            annotations: None,
        }
    }

    fn plan() -> CommitPlan {
        CommitPlan {
            namespace_id: NamespaceId::from("demo"),
            commit_id: CommitId::from("commit-a"),
            base_head_seq: ChangeSeq(0),
            next_seq: ChangeSeq(1),
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resulting_next_inode_id: InodeId(3),
            durable_content_required: false,
            wal_object_must_be_written: true,
            head_cas_must_succeed: true,
            metadata_preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(0))],
            checked_invariants: Vec::new(),
        }
    }

    #[test]
    fn prepared_commit_rejects_namespace_mismatch() {
        let mut plan = plan();
        plan.namespace_id = NamespaceId::from("other");

        assert!(matches!(
            PreparedCommit::new(request(), plan),
            Err(CommitPrepareError::NamespaceMismatch { .. })
        ));
    }

    #[test]
    fn prepared_commit_rejects_commit_id_mismatch() {
        let mut plan = plan();
        plan.commit_id = CommitId::from("commit-b");

        assert!(matches!(
            PreparedCommit::new(request(), plan),
            Err(CommitPrepareError::CommitIdMismatch)
        ));
    }

    #[test]
    fn prepared_commit_allows_ephemeral_batch_base_seq() {
        let mut plan = plan();
        plan.base_head_seq = ChangeSeq(9);

        PreparedCommit::new(request(), plan).expect("prepare commit");
    }
}
