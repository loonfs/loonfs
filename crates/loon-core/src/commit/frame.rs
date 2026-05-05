use super::{
    CommitRequest, CommitValidationContext, CommitValidationError, IndexedCommitValidationContext,
    Precondition,
};
use crate::namespace::head_and_lease_fence_tokens_agree;
use loon_api::{ChangeSeq, HeadState, LeaseState};

pub(super) trait CommitFrameContext {
    fn head(&self) -> &HeadState;
    fn lease(&self) -> &LeaseState;
    fn now_ms(&self) -> u64;
}

impl CommitFrameContext for CommitValidationContext {
    fn head(&self) -> &HeadState {
        &self.head
    }

    fn lease(&self) -> &LeaseState {
        &self.lease
    }

    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}

impl CommitFrameContext for IndexedCommitValidationContext<'_> {
    fn head(&self) -> &HeadState {
        self.head
    }

    fn lease(&self) -> &LeaseState {
        self.lease
    }

    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}

pub(super) fn validate_commit_request_frame<C: CommitFrameContext>(
    request: &CommitRequest,
    context: &C,
) -> Result<(), CommitValidationError> {
    let head = context.head();
    let lease = context.lease();

    if request.ops.is_empty() {
        return Err(CommitValidationError::EmptyCommit);
    }

    if request.namespace_id != head.namespace_id || request.namespace_id != lease.namespace_id {
        return Err(CommitValidationError::NamespaceMismatch);
    }

    if head.namespace_id != lease.namespace_id {
        return Err(CommitValidationError::HeadLeaseNamespaceMismatch);
    }

    if !head_and_lease_fence_tokens_agree(head, lease) {
        return Err(CommitValidationError::HeadLeaseFenceMismatch {
            head: head.active_fence_token,
            lease: lease.fence_token,
        });
    }

    if request.planned_head_seq != head.seq {
        return Err(CommitValidationError::PlannedHeadSeqMismatch {
            expected: head.seq,
            actual: request.planned_head_seq,
        });
    }

    validate_head_seq_preconditions(&request.preconditions, request.planned_head_seq)?;

    if request.writer_fence_token != head.active_fence_token {
        return Err(CommitValidationError::StaleWriterFenceToken {
            active: head.active_fence_token,
            requested: request.writer_fence_token,
        });
    }

    if request.writer_id != lease.holder_id {
        return Err(CommitValidationError::LeaseHolderMismatch {
            expected: lease.holder_id.clone(),
            actual: request.writer_id.clone(),
        });
    }

    if !lease.is_valid_at(context.now_ms()) {
        return Err(CommitValidationError::LeaseExpired {
            lease_expires_at_ms: lease.lease_expires_at_ms,
            now_ms: context.now_ms(),
        });
    }

    Ok(())
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
