use super::{
    semantic_commit_fingerprint, CommitFingerprintError, CommitOp, CommitPlan, CommitRequest,
    SemanticCommitFingerprint,
};
use loon_api::{
    v0::CommitOpResult, ContentRef, FenceToken, InodeId, NamespaceId, RevisionNo, WalOp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionContext {
    pub namespace_id: NamespaceId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommitFingerprintSource {
    ComputeFromRequest,
    TrustedPrecomputed(SemanticCommitFingerprint),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommit {
    pub request: CommitRequest,
    pub plan: CommitPlan,
    pub semantic_commit_fingerprint: SemanticCommitFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommitOp {
    pub op_index: u32,
    pub wal_op: WalOp,
    pub result: CommitOpResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommit {
    pub prepared: PreparedCommit,
    pub ops: Vec<MaterializedCommitOp>,
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitMaterializationError {
    #[error("commit op index overflow")]
    OpIndexOverflow,
    #[error(
        "allocated inode count mismatch: request_create_ops={request_create_ops}, plan_allocated_count={plan_allocated_count}"
    )]
    AllocatedInodeCountMismatch {
        request_create_ops: usize,
        plan_allocated_count: usize,
    },
    #[error("missing allocated inode for op index {op_index}")]
    MissingAllocatedInode { op_index: u32 },
    #[error("missing resolved restore content ref for op index {op_index}")]
    MissingResolvedRestoreContentRef { op_index: u32 },
    #[error("replace_file revision overflow for inode {inode_id:?} at base {base_revision_no:?}")]
    ReplaceRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error("restore_revision overflow for inode {inode_id:?} at base {base_revision_no:?}")]
    RestoreRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
}

impl PreparedCommit {
    pub fn new(request: CommitRequest, plan: CommitPlan) -> Result<Self, CommitPrepareError> {
        Self::prepare(request, plan, CommitFingerprintSource::ComputeFromRequest)
    }

    pub(crate) fn prepare(
        request: CommitRequest,
        plan: CommitPlan,
        fingerprint_source: CommitFingerprintSource,
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
        let semantic_commit_fingerprint = match fingerprint_source {
            CommitFingerprintSource::ComputeFromRequest => semantic_commit_fingerprint(&request)?,
            CommitFingerprintSource::TrustedPrecomputed(fingerprint) => fingerprint,
        };

        Ok(Self {
            request,
            plan,
            semantic_commit_fingerprint,
        })
    }
}

pub fn materialize_commit(
    prepared: PreparedCommit,
) -> Result<MaterializedCommit, CommitMaterializationError> {
    let request_create_ops = prepared
        .request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    if request_create_ops != prepared.plan.allocated_inode_ids.len() {
        return Err(CommitMaterializationError::AllocatedInodeCountMismatch {
            request_create_ops,
            plan_allocated_count: prepared.plan.allocated_inode_ids.len(),
        });
    }

    let mut allocated_inode_ids = prepared.plan.allocated_inode_ids.iter().copied();
    let mut ops = Vec::with_capacity(prepared.request.ops.len());
    for (op_index, op) in prepared.request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitMaterializationError::OpIndexOverflow)?;
        let resolved_restore_content_ref = prepared
            .plan
            .resolved_restore_content_refs
            .get(op_index as usize)
            .and_then(|content_ref| content_ref.as_ref());
        ops.push(materialize_commit_op(
            op,
            op_index,
            resolved_restore_content_ref,
            &mut allocated_inode_ids,
        )?);
    }

    Ok(MaterializedCommit { prepared, ops })
}

pub(super) fn materialize_commit_op(
    op: &CommitOp,
    op_index: u32,
    resolved_restore_content_ref: Option<&ContentRef>,
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
) -> Result<MaterializedCommitOp, CommitMaterializationError> {
    let materialized = match op {
        CommitOp::CreateDir {
            parent_inode,
            display_name,
        } => {
            let inode_id = allocated_inode_ids
                .next()
                .ok_or(CommitMaterializationError::MissingAllocatedInode { op_index })?;
            MaterializedCommitOp {
                op_index,
                wal_op: WalOp::CreateDir {
                    op_index,
                    inode_id,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                },
                result: CommitOpResult::CreateDir { op_index, inode_id },
            }
        }
        CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        } => {
            let inode_id = allocated_inode_ids
                .next()
                .ok_or(CommitMaterializationError::MissingAllocatedInode { op_index })?;
            MaterializedCommitOp {
                op_index,
                wal_op: WalOp::CreateFile {
                    op_index,
                    inode_id,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                    content_ref: content_ref.clone(),
                },
                result: CommitOpResult::CreateFile {
                    op_index,
                    inode_id,
                    revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
            }
        }
        CommitOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_ref,
        } => {
            let revision_no = base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                CommitMaterializationError::ReplaceRevisionOverflow {
                    inode_id: *inode_id,
                    base_revision_no: *base_revision_no,
                },
            )?;
            MaterializedCommitOp {
                op_index,
                wal_op: WalOp::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    base_revision_no: *base_revision_no,
                    content_ref: content_ref.clone(),
                },
                result: CommitOpResult::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                },
            }
        }
        CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no,
        } => {
            let content_ref = resolved_restore_content_ref
                .ok_or(CommitMaterializationError::MissingResolvedRestoreContentRef { op_index })?
                .clone();
            let revision_no = base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                CommitMaterializationError::RestoreRevisionOverflow {
                    inode_id: *inode_id,
                    base_revision_no: *base_revision_no,
                },
            )?;
            MaterializedCommitOp {
                op_index,
                wal_op: WalOp::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision_no,
                    base_revision_no: *base_revision_no,
                    content_ref: content_ref.clone(),
                },
                result: CommitOpResult::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision_no,
                    revision_no,
                    content_ref,
                },
            }
        }
        CommitOp::DeleteFile { inode_id } => MaterializedCommitOp {
            op_index,
            wal_op: WalOp::DeleteFile {
                op_index,
                inode_id: *inode_id,
            },
            result: CommitOpResult::DeleteFile {
                op_index,
                inode_id: *inode_id,
            },
        },
        CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        } => MaterializedCommitOp {
            op_index,
            wal_op: WalOp::Rename {
                op_index,
                inode_id: *inode_id,
                new_parent_inode: *new_parent_inode,
                new_display_name: new_display_name.clone(),
            },
            result: CommitOpResult::Rename {
                op_index,
                inode_id: *inode_id,
            },
        },
        CommitOp::DeleteSubtree { root_inode } => MaterializedCommitOp {
            op_index,
            wal_op: WalOp::DeleteSubtree {
                op_index,
                root_inode: *root_inode,
            },
            result: CommitOpResult::DeleteSubtree {
                op_index,
                root_inode: *root_inode,
            },
        },
    };

    Ok(materialized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitOp;
    use loon_api::{ChangeSeq, CommitId};

    fn request() -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::from("demo"),
            commit_id: CommitId::from("commit-a"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        }
    }

    fn plan() -> CommitPlan {
        CommitPlan {
            namespace_id: NamespaceId::from("demo"),
            commit_id: CommitId::from("commit-a"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resulting_next_inode_id: InodeId(3),
            metadata_preconditions: Vec::new(),
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
    fn prepared_commit_allows_ephemeral_batch_apply_after_seq() {
        let mut plan = plan();
        plan.apply_after_seq = ChangeSeq(9);

        PreparedCommit::new(request(), plan).expect("prepare commit");
    }

    #[test]
    fn prepared_commit_uses_trusted_precomputed_fingerprint() {
        let fingerprint = SemanticCommitFingerprint::new_unchecked("trusted".to_owned());
        let prepared = PreparedCommit::prepare(
            request(),
            plan(),
            CommitFingerprintSource::TrustedPrecomputed(fingerprint.clone()),
        )
        .expect("prepare commit");

        assert_eq!(prepared.semantic_commit_fingerprint, fingerprint);
    }

    #[test]
    fn materialize_commit_outputs_wal_ops_and_results_once() {
        let materialized =
            materialize_commit(PreparedCommit::new(request(), plan()).expect("prepare commit"))
                .expect("materialize commit");

        assert_eq!(materialized.ops.len(), 1);
        assert!(matches!(
            materialized.ops[0].wal_op,
            WalOp::CreateDir { .. }
        ));
        assert!(matches!(
            materialized.ops[0].result,
            CommitOpResult::CreateDir { .. }
        ));
    }
}
