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

    validate_head_seq_preconditions(&request.preconditions, context.head.seq)?;

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
    current_head_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    for precondition in preconditions {
        if let Precondition::HeadSeqIs(actual) = precondition {
            if *actual != current_head_seq {
                return Err(CommitValidationError::ConflictingHeadSeqPrecondition {
                    expected: current_head_seq,
                    actual: *actual,
                });
            }
        }
    }

    Ok(())
}
