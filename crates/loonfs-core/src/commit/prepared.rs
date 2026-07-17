//! [`PreparedCommit`]: a validated request paired with its plan and
//! semantic identity, ready for publication.

use super::{
    core_commit_fingerprint, CommitFingerprintError, CommitPlan, CommitRequest,
    PathIntentFingerprint, SemanticMutationIdentity,
};
use loonfs_api::{NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionContext {
    pub namespace_id: NamespaceId,
    pub writer_id: String,
    pub writer_session_id: String,
    pub writer_epoch: WriterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommitIdentitySource {
    CoreCommitRequest,
    PathIntent(PathIntentFingerprint),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommit {
    pub request: CommitRequest,
    pub plan: CommitPlan,
    pub semantic_identity: SemanticMutationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitPrepareError {
    #[error("prepared commit namespace mismatch: request `{request}`, plan `{plan}`")]
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
        Self::prepare(request, plan, CommitIdentitySource::CoreCommitRequest)
    }

    pub(crate) fn prepare(
        request: CommitRequest,
        plan: CommitPlan,
        identity_source: CommitIdentitySource,
    ) -> Result<Self, CommitPrepareError> {
        if request.namespace_id != plan.namespace_id {
            return Err(CommitPrepareError::NamespaceMismatch {
                request: request.namespace_id.clone(),
                plan: plan.namespace_id.clone(),
            });
        }
        if request.commit_id != plan.commit_id {
            return Err(CommitPrepareError::CommitIdMismatch);
        }
        let semantic_identity = match identity_source {
            CommitIdentitySource::CoreCommitRequest => {
                SemanticMutationIdentity::CoreCommit(core_commit_fingerprint(&request)?)
            }
            CommitIdentitySource::PathIntent(fingerprint) => {
                SemanticMutationIdentity::PathIntent(fingerprint)
            }
        };

        Ok(Self {
            request,
            plan,
            semantic_identity,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{materialize_commit, CommitOp, CommitOpResult, ValidatedOp};
    use loonfs_api::wire::wal::WalDelta;
    use loonfs_api::{ChangeSeq, CommitId, InodeId};

    fn request() -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: None,
        }
    }

    fn plan() -> CommitPlan {
        CommitPlan {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            validated_ops: vec![ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode_id: InodeId(1),
                display_name: "docs".to_owned(),
                name_key: "docs".to_owned(),
                child_inode_id: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
            checked_invariants: Vec::new(),
        }
    }

    #[test]
    fn prepared_commit_rejects_namespace_mismatch() {
        let mut plan = plan();
        plan.namespace_id = NamespaceId::parse("other").expect("valid namespace id");

        assert!(matches!(
            PreparedCommit::new(request(), plan),
            Err(CommitPrepareError::NamespaceMismatch { .. })
        ));
    }

    #[test]
    fn prepared_commit_rejects_commit_id_mismatch() {
        let mut plan = plan();
        plan.commit_id = CommitId::parse("commit-b").expect("valid commit id");

        assert!(matches!(
            PreparedCommit::new(request(), plan),
            Err(CommitPrepareError::CommitIdMismatch)
        ));
    }

    #[test]
    fn prepared_commit_allows_ephemeral_batch_apply_after_seq() {
        let mut plan = plan();
        plan.apply_after_seq = ChangeSeq(9);

        PreparedCommit::new(request(), plan).expect("prepare commit");
    }

    #[test]
    fn prepared_commit_uses_path_intent_identity() {
        let fingerprint = PathIntentFingerprint::new_unchecked("path-intent".to_owned());
        let prepared = PreparedCommit::prepare(
            request(),
            plan(),
            CommitIdentitySource::PathIntent(fingerprint.clone()),
        )
        .expect("prepare commit");

        assert_eq!(
            prepared.semantic_identity,
            SemanticMutationIdentity::PathIntent(fingerprint)
        );
    }

    #[test]
    fn materialize_commit_outputs_wal_ops_and_results_once() {
        let materialized = materialize_commit(
            PreparedCommit::new(request(), plan()).expect("prepare commit"),
            4_200,
        );

        assert_eq!(materialized.deltas.len(), 2);
        assert!(matches!(
            materialized.deltas[0].wal_delta,
            WalDelta::CreateInode { .. }
        ));
        assert!(matches!(
            materialized.deltas[1].wal_delta,
            WalDelta::BindDirentry { .. }
        ));
        assert!(matches!(
            materialized.results[0],
            CommitOpResult::CreateDirectory { .. }
        ));
    }
}
