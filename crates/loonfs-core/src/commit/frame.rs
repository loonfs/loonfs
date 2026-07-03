use super::{CommitRequest, CommitValidationContext, CommitValidationError};
use loonfs_api::wire::control::HeadState;

pub(super) fn validate_commit_request_frame(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<(), CommitValidationError> {
    validate_commit_request_frame_parts(request, &context.head, context.now_ms)
}

pub(super) fn validate_commit_request_frame_parts(
    request: &CommitRequest,
    head: &HeadState,
    now_ms: u64,
) -> Result<(), CommitValidationError> {
    if request.ops.is_empty() {
        return Err(CommitValidationError::EmptyCommit);
    }

    if request.namespace_id != head.namespace_id {
        return Err(CommitValidationError::NamespaceMismatch);
    }

    if request.writer_epoch != head.writer_epoch {
        return Err(CommitValidationError::StaleWriterEpoch {
            active: head.writer_epoch,
            requested: request.writer_epoch,
        });
    }

    let Some(lease) = &head.writer_lease else {
        return Err(CommitValidationError::MissingWriterLease);
    };

    if request.writer_id != lease.writer_id {
        return Err(CommitValidationError::WriterLeaseHolderMismatch {
            expected: lease.writer_id.clone(),
            actual: request.writer_id.clone(),
        });
    }

    if request.writer_session_id != lease.writer_session_id {
        return Err(CommitValidationError::WriterLeaseSessionMismatch {
            expected: lease.writer_session_id.clone(),
            actual: request.writer_session_id.clone(),
        });
    }

    if !lease.is_valid_at(now_ms) {
        return Err(CommitValidationError::WriterLeaseExpired {
            lease_expires_at_ms: lease.lease_expires_at_ms,
            now_ms,
        });
    }

    Ok(())
}
