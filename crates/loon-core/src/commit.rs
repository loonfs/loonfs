use crate::invariants::INVARIANTS;
use crate::namespace::head_and_lease_fence_tokens_agree;
use loon_types::{ChangeSeq, FenceToken, HeadState, InodeId, LeaseState, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub request_id: String,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub planned_head_seq: ChangeSeq,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOp {
    ReplaceFile {
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
    RestoreRevision {
        inode_id: InodeId,
        restore_from_revision: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    HeadSeqIs(ChangeSeq),
    InodeRevisionIs {
        inode_id: InodeId,
        revision: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: String,
    pub base_head_seq: ChangeSeq,
    pub next_seq: ChangeSeq,
    pub durable_content_required: bool,
    pub wal_object_must_be_written: bool,
    pub head_cas_must_succeed: bool,
    pub metadata_preconditions: Vec<Precondition>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitValidationContext {
    pub head: HeadState,
    pub lease: LeaseState,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitValidationError {
    EmptyCommit,
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    MissingHeadSeqPrecondition {
        expected: ChangeSeq,
    },
    ConflictingHeadSeqPrecondition {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    StaleWriterFenceToken {
        active: FenceToken,
        requested: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
}

pub fn build_commit_plan(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitPlan, CommitValidationError> {
    if request.ops.is_empty() {
        return Err(CommitValidationError::EmptyCommit);
    }

    if request.namespace_id != context.head.namespace_id
        || request.namespace_id != context.lease.namespace_id
    {
        return Err(CommitValidationError::NamespaceMismatch);
    }

    if context.head.namespace_id != context.lease.namespace_id {
        return Err(CommitValidationError::HeadLeaseNamespaceMismatch);
    }

    if !head_and_lease_fence_tokens_agree(&context.head, &context.lease) {
        return Err(CommitValidationError::HeadLeaseFenceMismatch {
            head: context.head.active_fence_token,
            lease: context.lease.fence_token,
        });
    }

    if request.planned_head_seq != context.head.seq {
        return Err(CommitValidationError::PlannedHeadSeqMismatch {
            expected: context.head.seq,
            actual: request.planned_head_seq,
        });
    }

    validate_head_seq_preconditions(&request.preconditions, request.planned_head_seq)?;

    if request.writer_fence_token != context.head.active_fence_token {
        return Err(CommitValidationError::StaleWriterFenceToken {
            active: context.head.active_fence_token,
            requested: request.writer_fence_token,
        });
    }

    if request.writer_id != context.lease.holder_id {
        return Err(CommitValidationError::LeaseHolderMismatch {
            expected: context.lease.holder_id.clone(),
            actual: request.writer_id.clone(),
        });
    }

    if !context.lease.is_valid_at(context.now_ms) {
        return Err(CommitValidationError::LeaseExpired {
            lease_expires_at_ms: context.lease.lease_expires_at_ms,
            now_ms: context.now_ms,
        });
    }

    let next_seq = context
        .head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.request_id.clone(),
        base_head_seq: request.planned_head_seq,
        next_seq,
        durable_content_required: request
            .ops
            .iter()
            .any(|op| matches!(op, CommitOp::ReplaceFile { .. })),
        wal_object_must_be_written: true,
        head_cas_must_succeed: true,
        metadata_preconditions: request.preconditions.clone(),
        checked_invariants: INVARIANTS
            .iter()
            .copied()
            .filter(|name| {
                matches!(
                    *name,
                    "stale_writer_cannot_publish"
                        | "head_and_lease_fence_tokens_agree"
                        | "next_inode_id_is_monotonic"
                )
            })
            .map(str::to_owned)
            .collect(),
    })
}

fn validate_head_seq_preconditions(
    preconditions: &[Precondition],
    planned_head_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let mut saw_head_seq_precondition = false;

    for precondition in preconditions {
        if let Precondition::HeadSeqIs(actual) = precondition {
            saw_head_seq_precondition = true;
            if *actual != planned_head_seq {
                return Err(CommitValidationError::ConflictingHeadSeqPrecondition {
                    expected: planned_head_seq,
                    actual: *actual,
                });
            }
        }
    }

    if !saw_head_seq_precondition {
        return Err(CommitValidationError::MissingHeadSeqPrecondition {
            expected: planned_head_seq,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
        Precondition,
    };
    use loon_types::{
        ChangeSeq, FenceToken, HeadState, InodeId, LeaseState, NamespaceId, RevisionNo,
    };

    #[test]
    fn build_commit_plan_accepts_active_writer() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::Rename {
                inode_id: InodeId(42),
                new_parent_inode: InodeId(2),
                new_display_name: "renamed.txt".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert_eq!(plan.next_seq, ChangeSeq(42));
        assert!(!plan.durable_content_required);
        assert!(plan
            .checked_invariants
            .contains(&"stale_writer_cannot_publish".to_owned()));
    }

    #[test]
    fn build_commit_plan_requires_durable_content_for_replace_file() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-2".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let plan = build_commit_plan(&request, &validation_context(999)).expect("valid plan");

        assert!(plan.durable_content_required);
    }

    #[test]
    fn build_commit_plan_rejects_stale_writer_token() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-3".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(7),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            CommitValidationError::StaleWriterFenceToken {
                active: FenceToken(8),
                requested: FenceToken(7),
            }
        );
    }

    #[test]
    fn build_commit_plan_rejects_expired_lease() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-4".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![Precondition::HeadSeqIs(ChangeSeq(41))],
        };

        let error = build_commit_plan(&request, &validation_context(1_001))
            .expect_err("expired lease should be rejected");

        assert_eq!(
            error,
            CommitValidationError::LeaseExpired {
                lease_expires_at_ms: 1_000,
                now_ms: 1_001,
            }
        );
    }

    #[test]
    fn build_commit_plan_requires_explicit_head_seq_precondition() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-5".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: Vec::new(),
        };

        let error = build_commit_plan(&request, &validation_context(1_000))
            .expect_err("missing head precondition should be rejected");

        assert_eq!(
            error,
            CommitValidationError::MissingHeadSeqPrecondition {
                expected: ChangeSeq(41),
            }
        );
    }

    fn validation_context(now_ms: u64) -> CommitValidationContext {
        CommitValidationContext {
            head: HeadState {
                namespace_id: NamespaceId::from("ns-1"),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            lease: LeaseState {
                namespace_id: NamespaceId::from("ns-1"),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
            now_ms,
        }
    }
}
