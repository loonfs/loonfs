//! [`PreparedCommit`]: a validated request paired with its plan and
//! semantic identity, ready for publication.

use super::{CommitFingerprint, CommitIr, CommitPlan};
use loonfs_api::NamespaceId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedCommit {
    pub(crate) request: CommitIr,
    pub(crate) plan: CommitPlan,
    pub(crate) semantic_identity: CommitFingerprint,
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
}

impl PreparedCommit {
    pub(crate) fn new(
        request: CommitIr,
        plan: CommitPlan,
        semantic_identity: CommitFingerprint,
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
    use crate::commit::{materialize_commit, PlannedOp, ValidatedOp};
    use crate::commit::{CommitFingerprint, CommitOp};
    use loonfs_api::wire::wal::WalDelta;
    use loonfs_api::NameKey;
    use loonfs_api::{ChangeSeq, CommitId, InodeId, WriterEpoch};

    fn fingerprint() -> CommitFingerprint {
        CommitFingerprint::new_unchecked("v0:sha256:test".to_owned())
    }

    fn request() -> CommitIr {
        CommitIr {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            writer_epoch: WriterEpoch(1),
            ops: vec![PlannedOp::unchecked(CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            })],
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
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
                name_key: NameKey::parse("docs").expect("valid name key"),
                child_inode_id: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
        }
    }

    #[test]
    fn prepared_commit_rejects_namespace_mismatch() {
        let mut plan = plan();
        plan.namespace_id = NamespaceId::parse("other").expect("valid namespace id");

        assert!(matches!(
            PreparedCommit::new(request(), plan, fingerprint()),
            Err(CommitPrepareError::NamespaceMismatch { .. })
        ));
    }

    #[test]
    fn prepared_commit_rejects_commit_id_mismatch() {
        let mut plan = plan();
        plan.commit_id = CommitId::parse("commit-b").expect("valid commit id");

        assert!(matches!(
            PreparedCommit::new(request(), plan, fingerprint()),
            Err(CommitPrepareError::CommitIdMismatch)
        ));
    }

    #[test]
    fn prepared_commit_allows_ephemeral_batch_apply_after_seq() {
        let mut plan = plan();
        plan.apply_after_seq = ChangeSeq(9);

        PreparedCommit::new(request(), plan, fingerprint()).expect("prepare commit");
    }

    #[test]
    fn prepared_commit_carries_the_request_fingerprint() {
        let prepared =
            PreparedCommit::new(request(), plan(), fingerprint()).expect("prepare commit");

        assert_eq!(prepared.semantic_identity, fingerprint());
    }

    #[test]
    fn materialize_commit_outputs_wal_deltas_once() {
        let materialized = materialize_commit(
            PreparedCommit::new(request(), plan(), fingerprint()).expect("prepare commit"),
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
    }
}
