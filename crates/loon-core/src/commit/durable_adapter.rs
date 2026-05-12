use super::{CommitOp, MaterializedCommit, Precondition};
use crate::wal::WalBuildError;
use loon_api::{WalCommitPayload, WalOp, WalPrecondition};

pub fn wal_payload_from_materialized_commit(
    commit: &MaterializedCommit,
) -> Result<WalCommitPayload, WalBuildError> {
    let prepared = &commit.prepared;
    if !prepared.plan.wal_object_must_be_written {
        return Err(WalBuildError::WalWriteNotRequired);
    }
    if prepared.request.namespace_id != prepared.plan.namespace_id {
        return Err(WalBuildError::NamespaceMismatch {
            request: prepared.request.namespace_id.clone(),
            plan: prepared.plan.namespace_id.clone(),
        });
    }

    Ok(WalCommitPayload {
        namespace_id: prepared.plan.namespace_id.clone(),
        seq: prepared.plan.next_seq,
        base_head_seq: prepared.plan.base_head_seq,
        commit_id: prepared.plan.commit_id.clone(),
        semantic_commit_fingerprint_sha256: prepared.semantic_commit_fingerprint_sha256.clone(),
        writer_id: prepared.request.writer_id.clone(),
        writer_fence_token: prepared.request.writer_fence_token,
        message: prepared.request.message.clone(),
        annotations: prepared.request.annotations.clone(),
        ops: build_wal_ops(prepared)?,
        preconditions: prepared
            .request
            .preconditions
            .iter()
            .map(WalPrecondition::from)
            .collect(),
        results: commit.results.clone(),
    })
}

fn build_wal_ops(prepared: &super::PreparedCommit) -> Result<Vec<WalOp>, WalBuildError> {
    let request_create_ops = prepared
        .request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    if request_create_ops != prepared.plan.allocated_inode_ids.len() {
        return Err(WalBuildError::AllocatedInodeCountMismatch {
            request_create_ops,
            plan_allocated_count: prepared.plan.allocated_inode_ids.len(),
        });
    }

    let mut allocated_inode_ids = prepared.plan.allocated_inode_ids.iter().copied();
    let mut wal_ops = Vec::with_capacity(prepared.request.ops.len());

    for (op_index, op) in prepared.request.ops.iter().enumerate() {
        let op_index = u32::try_from(op_index)
            .map_err(|_| WalBuildError::Codec("op index overflow".to_owned()))?;
        let resolved_restore_content_ref = prepared
            .plan
            .resolved_restore_content_refs
            .get(op_index as usize)
            .and_then(|content_ref| content_ref.as_ref());
        let wal_op = match op {
            CommitOp::CreateDir {
                parent_inode,
                display_name,
            } => WalOp::CreateDir {
                op_index,
                inode_id: allocated_inode_ids.next().ok_or(
                    WalBuildError::AllocatedInodeCountMismatch {
                        request_create_ops,
                        plan_allocated_count: prepared.plan.allocated_inode_ids.len(),
                    },
                )?,
                parent_inode: *parent_inode,
                display_name: display_name.clone(),
            },
            CommitOp::CreateFile {
                parent_inode,
                display_name,
                content_ref,
            } => WalOp::CreateFile {
                op_index,
                inode_id: allocated_inode_ids.next().ok_or(
                    WalBuildError::AllocatedInodeCountMismatch {
                        request_create_ops,
                        plan_allocated_count: prepared.plan.allocated_inode_ids.len(),
                    },
                )?,
                parent_inode: *parent_inode,
                display_name: display_name.clone(),
                content_ref: content_ref.clone(),
            },
            CommitOp::ReplaceFile {
                inode_id,
                base_revision,
                content_ref,
            } => WalOp::ReplaceFile {
                op_index,
                inode_id: *inode_id,
                base_revision: *base_revision,
                content_ref: content_ref.clone(),
            },
            CommitOp::RestoreRevision {
                inode_id,
                source_revision,
                base_revision,
            } => WalOp::RestoreRevision {
                op_index,
                inode_id: *inode_id,
                source_revision_no: *source_revision,
                base_revision: *base_revision,
                content_ref: resolved_restore_content_ref
                    .ok_or_else(|| {
                        WalBuildError::Codec(format!(
                            "missing resolved restore content ref for op index {op_index}"
                        ))
                    })?
                    .clone(),
            },
            CommitOp::DeleteFile { inode_id } => WalOp::DeleteFile {
                op_index,
                inode_id: *inode_id,
            },
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
            } => WalOp::Rename {
                op_index,
                inode_id: *inode_id,
                new_parent_inode: *new_parent_inode,
                new_display_name: new_display_name.clone(),
            },
            CommitOp::DeleteSubtree { root_inode } => WalOp::DeleteSubtree {
                op_index,
                root_inode: *root_inode,
            },
        };
        wal_ops.push(wal_op);
    }

    Ok(wal_ops)
}

impl From<&Precondition> for WalPrecondition {
    fn from(value: &Precondition) -> Self {
        match value {
            Precondition::HeadSeqIs(seq) => Self::HeadSeqIs(*seq),
            Precondition::InodeRevisionIs { inode_id, revision } => Self::InodeRevisionIs {
                inode_id: *inode_id,
                revision: *revision,
            },
            Precondition::AncestorsNotSubtreeDeleted { inode_id } => {
                Self::AncestorsNotSubtreeDeleted {
                    inode_id: *inode_id,
                }
            }
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key,
            } => Self::ChildNameAbsent {
                parent_inode: *parent_inode,
                name_key: name_key.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitPlan, CommitRequest, PreparedCommit};
    use loon_api::{v0::CommitOpResult, ChangeSeq, CommitId, FenceToken, InodeId, NamespaceId};

    #[test]
    fn durable_adapter_builds_expected_wal_payload() {
        let namespace_id = NamespaceId::from("demo");
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from("c_wal_payload"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            planned_head_seq: ChangeSeq(0),
            semantic_commit_fingerprint_sha256: None,
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(0))],
            message: Some("create docs".to_owned()),
            annotations: None,
        };
        let plan = CommitPlan {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from("c_wal_payload"),
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
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        let materialized = MaterializedCommit {
            prepared,
            results: vec![CommitOpResult::CreateDir {
                op_index: 0,
                inode_id: InodeId(2),
            }],
        };

        let payload =
            wal_payload_from_materialized_commit(&materialized).expect("build wal payload");

        assert_eq!(payload.namespace_id, namespace_id);
        assert_eq!(payload.seq, ChangeSeq(1));
        assert_eq!(payload.ops.len(), 1);
        assert_eq!(payload.results.len(), 1);
    }
}
