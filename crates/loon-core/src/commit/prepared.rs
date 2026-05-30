use super::{
    core_commit_fingerprint, CommitFingerprintError, CommitOp, CommitPlan, CommitRequest,
    PathIntentFingerprint, ResolvedBinding, SemanticMutationIdentity,
};
use loon_api::wire::wal::WalDelta;
use loon_api::{
    name_key_for_display_name, v0::CommitOpResult, ContentRef, FenceToken, InodeId, InodeKind,
    NamePolicy, NamespaceId, RevisionNo,
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
    #[error("commit op index overflow")]
    OpIndexOverflow,
    #[error("commit delta index overflow")]
    DeltaIndexOverflow,
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
    #[error("missing resolved source binding for op index {op_index}")]
    MissingResolvedSourceBinding { op_index: u32 },
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
    let mut deltas = Vec::with_capacity(prepared.request.ops.len());
    let mut results = Vec::with_capacity(prepared.request.ops.len());
    let mut next_delta_index = 0u32;
    for (op_index, op) in prepared.request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitMaterializationError::OpIndexOverflow)?;
        let resolved_restore_content_ref = prepared
            .plan
            .resolved_restore_content_refs
            .get(op_index as usize)
            .and_then(|content_ref| content_ref.as_ref());
        let resolved_source_binding = prepared
            .plan
            .resolved_source_bindings
            .get(op_index as usize)
            .and_then(|binding| binding.as_ref());
        let (mut op_deltas, result) = materialize_commit_op(
            op,
            op_index,
            prepared.plan.name_policy,
            resolved_restore_content_ref,
            resolved_source_binding,
            &mut allocated_inode_ids,
            &mut next_delta_index,
        )?;
        deltas.append(&mut op_deltas);
        results.push(result);
    }

    Ok(MaterializedCommit {
        prepared,
        deltas,
        results,
    })
}

pub(super) fn materialize_commit_op(
    op: &CommitOp,
    op_index: u32,
    name_policy: NamePolicy,
    resolved_restore_content_ref: Option<&ContentRef>,
    resolved_source_binding: Option<&ResolvedBinding>,
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
    next_delta_index: &mut u32,
) -> Result<(Vec<MaterializedCommitDelta>, CommitOpResult), CommitMaterializationError> {
    let mut deltas = Vec::new();
    let result = match op {
        CommitOp::CreateDir {
            parent_inode,
            display_name,
        } => {
            let inode_id = allocated_inode_ids
                .next()
                .ok_or(CommitMaterializationError::MissingAllocatedInode { op_index })?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::CreateInode {
                    delta_index: 0,
                    inode_id,
                    inode_kind: InodeKind::Dir,
                },
            )?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode: *parent_inode,
                    name_key: name_key_for_display_name(name_policy, display_name),
                    display_name: display_name.clone(),
                    child_inode: inode_id,
                },
            )?;
            CommitOpResult::CreateDir { op_index, inode_id }
        }
        CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        } => {
            let inode_id = allocated_inode_ids
                .next()
                .ok_or(CommitMaterializationError::MissingAllocatedInode { op_index })?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::CreateInode {
                    delta_index: 0,
                    inode_id,
                    inode_kind: InodeKind::File,
                },
            )?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode: *parent_inode,
                    name_key: name_key_for_display_name(name_policy, display_name),
                    display_name: display_name.clone(),
                    child_inode: inode_id,
                },
            )?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::AppendFileRevision {
                    delta_index: 0,
                    inode_id,
                    revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
            )?;
            CommitOpResult::CreateFile {
                op_index,
                inode_id,
                revision_no: RevisionNo(1),
                content_ref: content_ref.clone(),
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
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::AppendFileRevision {
                    delta_index: 0,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                },
            )?;
            CommitOpResult::ReplaceFile {
                op_index,
                inode_id: *inode_id,
                revision_no,
                content_ref: content_ref.clone(),
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
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::AppendFileRevision {
                    delta_index: 0,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                },
            )?;
            CommitOpResult::RestoreRevision {
                op_index,
                inode_id: *inode_id,
                source_revision_no: *source_revision_no,
                revision_no,
                content_ref,
            }
        }
        CommitOp::DeleteFile { inode_id } => {
            let binding = resolved_source_binding
                .ok_or(CommitMaterializationError::MissingResolvedSourceBinding { op_index })?;
            push_unbind_delta(&mut deltas, op_index, next_delta_index, binding)?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::TombstoneSubtree {
                    delta_index: 0,
                    root_inode: *inode_id,
                },
            )?;
            CommitOpResult::DeleteFile {
                op_index,
                inode_id: *inode_id,
            }
        }
        CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
            ..
        } => {
            let binding = resolved_source_binding
                .ok_or(CommitMaterializationError::MissingResolvedSourceBinding { op_index })?;
            push_unbind_delta(&mut deltas, op_index, next_delta_index, binding)?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode: *new_parent_inode,
                    name_key: name_key_for_display_name(name_policy, new_display_name),
                    display_name: new_display_name.clone(),
                    child_inode: *inode_id,
                },
            )?;
            CommitOpResult::Rename {
                op_index,
                inode_id: *inode_id,
            }
        }
        CommitOp::DeleteSubtree { root_inode } => {
            let binding = resolved_source_binding
                .ok_or(CommitMaterializationError::MissingResolvedSourceBinding { op_index })?;
            push_unbind_delta(&mut deltas, op_index, next_delta_index, binding)?;
            push_delta(
                &mut deltas,
                op_index,
                next_delta_index,
                WalDelta::TombstoneSubtree {
                    delta_index: 0,
                    root_inode: *root_inode,
                },
            )?;
            CommitOpResult::DeleteSubtree {
                op_index,
                root_inode: *root_inode,
            }
        }
    };

    Ok((deltas, result))
}

fn push_unbind_delta(
    deltas: &mut Vec<MaterializedCommitDelta>,
    semantic_op_index: u32,
    next_delta_index: &mut u32,
    binding: &ResolvedBinding,
) -> Result<(), CommitMaterializationError> {
    push_delta(
        deltas,
        semantic_op_index,
        next_delta_index,
        WalDelta::UnbindDirentry {
            delta_index: 0,
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
    next_delta_index: &mut u32,
    mut wal_delta: WalDelta,
) -> Result<(), CommitMaterializationError> {
    let delta_index = *next_delta_index;
    *next_delta_index = next_delta_index
        .checked_add(1)
        .ok_or(CommitMaterializationError::DeltaIndexOverflow)?;
    set_wal_delta_index(&mut wal_delta, delta_index);
    deltas.push(MaterializedCommitDelta {
        semantic_op_index,
        delta_index,
        wal_delta,
    });
    Ok(())
}

fn set_wal_delta_index(wal_delta: &mut WalDelta, delta_index: u32) {
    match wal_delta {
        WalDelta::CreateInode {
            delta_index: slot, ..
        }
        | WalDelta::BindDirentry {
            delta_index: slot, ..
        }
        | WalDelta::UnbindDirentry {
            delta_index: slot, ..
        }
        | WalDelta::AppendFileRevision {
            delta_index: slot, ..
        }
        | WalDelta::TombstoneSubtree {
            delta_index: slot, ..
        } => *slot = delta_index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitOp;
    use loon_api::{ChangeSeq, CommitId};

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
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resolved_source_bindings: vec![None],
            resulting_next_inode_id: InodeId(3),
            name_policy: loon_api::NamePolicy::default(),
            metadata_preconditions: Vec::new(),
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
