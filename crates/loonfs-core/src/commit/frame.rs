use super::{CommitRequest, CommitValidationContext, CommitValidationError};
use crate::namespace::head_and_lease_fence_tokens_agree;
use loonfs_api::wire::control::{HeadState, LeaseState};

pub(super) fn validate_commit_request_frame(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<(), CommitValidationError> {
    validate_commit_request_frame_parts(request, &context.head, &context.lease, context.now_ms)
}

pub(super) fn validate_commit_request_frame_parts(
    request: &CommitRequest,
    head: &HeadState,
    lease: &LeaseState,
    now_ms: u64,
) -> Result<(), CommitValidationError> {
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

    if !lease.is_valid_at(now_ms) {
        return Err(CommitValidationError::LeaseExpired {
            lease_expires_at_ms: lease.lease_expires_at_ms,
            now_ms,
        });
    }

    Ok(())
}
