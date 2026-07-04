use super::{CommitRequest, CommitValidationError};
use loonfs_api::wire::control::HeadState;

pub(super) fn validate_commit_request_frame(
    request: &CommitRequest,
    head: &HeadState,
) -> Result<(), CommitValidationError> {
    if request.ops.is_empty() {
        return Err(CommitValidationError::EmptyCommit);
    }

    if request.namespace_id != head.namespace_id {
        return Err(CommitValidationError::NamespaceMismatch);
    }

    // Fencing authority is the epoch plus the head CAS; the head's `writer`
    // block is observability and must not be consulted here.
    if request.writer_epoch != head.writer_epoch {
        return Err(CommitValidationError::StaleWriterEpoch {
            active: head.writer_epoch,
            requested: request.writer_epoch,
        });
    }

    Ok(())
}
