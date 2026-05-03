use super::{CommitRequest, CommitValidationContext, CommitValidationError, Precondition};
use crate::namespace::head_and_lease_fence_tokens_agree;
use loon_api::ChangeSeq;

pub(super) fn validate_commit_request_frame(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<(), CommitValidationError> {
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
