use super::{
    core_commit_fingerprint, CommitFingerprintError, CommitPlan, CommitRequest,
    PathIntentFingerprint, ResolvedBinding, SemanticMutationIdentity, ValidatedOp,
};
use loon_api::wire::wal::WalDelta;
use loon_api::{v0::CommitOpResult, FenceToken, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionContext {
    pub namespace_id: NamespaceId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommitDelta {
    pub semantic_op_index: u32,
    pub delta_index: u32,
    pub wal_delta: WalDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommit {
    pub prepared: PreparedCommit,
    pub deltas: Vec<MaterializedCommitDelta>,
    pub results: Vec<CommitOpResult>,
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
    #[error("commit delta index overflow")]
    DeltaIndexOverflow,
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

pub fn materialize_commit(
    prepared: PreparedCommit,
) -> Result<MaterializedCommit, CommitMaterializationError> {
    let mut deltas = Vec::new();
    let mut results = Vec::with_capacity(prepared.plan.validated_ops.len());
    for op in &prepared.plan.validated_ops {
        let (mut op_deltas, result) = materialize_validated_op(op);
        deltas.append(&mut op_deltas);
        results.push(result);
    }

    Ok(MaterializedCommit {
        prepared,
        deltas,
        results,
    })
}

fn materialize_validated_op(op: &ValidatedOp) -> (Vec<MaterializedCommitDelta>, CommitOpResult) {
    let mut deltas = Vec::new();
    let result = match op {
        ValidatedOp::CreateDir {
            op_index,
            parent_inode,
            display_name,
            name_key,
            child_inode,
            create_inode_delta_index,
            bind_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::CreateInode {
                    delta_index: *create_inode_delta_index,
                    inode_id: *child_inode,
                    inode_kind: InodeKind::Dir,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode: *parent_inode,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode: *child_inode,
                },
            );
            CommitOpResult::CreateDir {
                op_index: *op_index,
                inode_id: *child_inode,
            }
        }
        ValidatedOp::CreateFile {
            op_index,
            parent_inode,
            display_name,
            name_key,
            child_inode,
            content_ref,
            create_inode_delta_index,
            bind_delta_index,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::CreateInode {
                    delta_index: *create_inode_delta_index,
                    inode_id: *child_inode,
                    inode_kind: InodeKind::File,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode: *parent_inode,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode: *child_inode,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *child_inode,
                    revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::CreateFile {
                op_index: *op_index,
                inode_id: *child_inode,
                revision_no: RevisionNo(1),
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::ReplaceFile {
            op_index,
            inode_id,
            revision_no,
            content_ref,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *inode_id,
                    revision_no: *revision_no,
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::ReplaceFile {
                op_index: *op_index,
                inode_id: *inode_id,
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::RestoreRevision {
            op_index,
            inode_id,
            source_revision_no,
            revision_no,
            content_ref,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *inode_id,
                    revision_no: *revision_no,
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::RestoreRevision {
                op_index: *op_index,
                inode_id: *inode_id,
                source_revision_no: *source_revision_no,
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::DeleteFile {
            op_index,
            inode_id,
            source_binding,
            unbind_delta_index,
            tombstone_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::TombstoneSubtree {
                    delta_index: *tombstone_delta_index,
                    root_inode: *inode_id,
                },
            );
            CommitOpResult::DeleteFile {
                op_index: *op_index,
                inode_id: *inode_id,
            }
        }
        ValidatedOp::Rename {
            op_index,
            inode_id,
            new_parent_inode,
            new_display_name,
            new_name_key,
            source_binding,
            unbind_delta_index,
            bind_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode: *new_parent_inode,
                    name_key: new_name_key.clone(),
                    display_name: new_display_name.clone(),
                    child_inode: *inode_id,
                },
            );
            CommitOpResult::Rename {
                op_index: *op_index,
                inode_id: *inode_id,
            }
        }
        ValidatedOp::DeleteSubtree {
            op_index,
            root_inode,
            source_binding,
            unbind_delta_index,
            tombstone_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::TombstoneSubtree {
                    delta_index: *tombstone_delta_index,
                    root_inode: *root_inode,
                },
            );
            CommitOpResult::DeleteSubtree {
                op_index: *op_index,
                root_inode: *root_inode,
            }
        }
    };

    (deltas, result)
}

fn push_unbind_delta(
    deltas: &mut Vec<MaterializedCommitDelta>,
    semantic_op_index: u32,
    delta_index: u32,
    binding: &ResolvedBinding,
) {
    push_delta(
        deltas,
        semantic_op_index,
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode: binding.parent_inode,
            name_key: binding.name_key.clone(),
            child_inode: binding.child_inode,
            bind_seq: binding.bind_seq,
            bind_delta_index: binding.bind_delta_index,
        },
    )
}

fn push_delta(
    deltas: &mut Vec<MaterializedCommitDelta>,
    semantic_op_index: u32,
    wal_delta: WalDelta,
) {
    let delta_index = wal_delta_index(&wal_delta);
    deltas.push(MaterializedCommitDelta {
        semantic_op_index,
        delta_index,
        wal_delta,
    });
}

fn wal_delta_index(wal_delta: &WalDelta) -> u32 {
    match wal_delta {
        WalDelta::CreateInode { delta_index, .. }
        | WalDelta::BindDirentry { delta_index, .. }
        | WalDelta::UnbindDirentry { delta_index, .. }
        | WalDelta::AppendFileRevision { delta_index, .. }
        | WalDelta::TombstoneSubtree { delta_index, .. } => *delta_index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitOp;
    use loon_api::{ChangeSeq, CommitId, InodeId};

    fn request() -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
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
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit-a").expect("valid commit id"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            validated_ops: vec![ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
                name_key: "docs".to_owned(),
                child_inode: InodeId(2),
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
        let materialized =
            materialize_commit(PreparedCommit::new(request(), plan()).expect("prepare commit"))
                .expect("materialize commit");

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
            CommitOpResult::CreateDir { .. }
        ));
    }
}
