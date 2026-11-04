//! How the merge loop acts on an error that a merge or a rebase returned.

use crate::{PendingCommit, Result};
use loonfs_client::ClientError;
use loonfs_types::{u64_from_usize, ChangeSeq, ErrorCode, ErrorKind, ErrorResponse};

/// The attempts that one merge or rebase request gets when it fails with a
/// transient error: the first attempt and three more.
///
/// A transient error is `commit_queue_full`, `server_busy`, or
/// `shutting_down`, which the client has already retried with a backoff, or
/// an error from a commit that failed because the namespace changed after
/// the plan (see [`ErrorCode::is_stale_plan`]), which the server returns
/// after it planned the merge four times. So a session that reaches this
/// limit is merging into a namespace that doesn't stop changing.
pub const MAX_MERGE_ATTEMPTS: usize = 4;

/// How the loop acts on an error that a merge or a rebase returned.
pub(crate) enum Refusal {
    /// The loop sends another request for the range or for a piece of it.
    Next(NextRequest),
    /// The merge conflicted.
    Conflict,
    /// The loop can't act on the error, or the request had its last attempt.
    Fail,
}

/// The request that the loop sends after a refusal that it can act on.
pub(crate) enum NextRequest {
    /// The request was too large, and the next request goes through the
    /// error's split hint.
    Split(ChangeSeq),
    /// The request failed with a transient error, and the same request goes
    /// again.
    Retry,
}

/// Returns the server's error response for a merge or a rebase that it
/// refused, or returns `error` when the request's outcome isn't known (a
/// transport failure or an outcome-unknown error), so that the step leaves
/// the state as it was and the next step sends the same request.
pub(crate) fn refused(error: ClientError) -> Result<ErrorResponse> {
    let outcome_unknown = error
        .code()
        .is_some_and(|code| code.kind() == ErrorKind::OutcomeUnknown);
    match error {
        ClientError::Api { response, .. } if !outcome_unknown => Ok(*response),
        error => Err(error.into()),
    }
}

/// Returns how the loop acts on `error`, which the server returned for
/// `request`.
pub(crate) fn refusal(request: &PendingCommit, error: &ErrorResponse) -> Refusal {
    let Some(code) = ErrorCode::parse(&error.code) else {
        return Refusal::Fail;
    };
    match code {
        ErrorCode::MergeTooLarge => match error.details.as_ref().and_then(|d| d.split_at_seq) {
            // A hint at or above the refused request's end wouldn't shorten
            // the range, and the loop would repeat the request without end.
            Some(split_at_seq) if split_at_seq < request.through_seq => {
                Refusal::Next(NextRequest::Split(split_at_seq))
            }
            _ => Refusal::Fail,
        },
        ErrorCode::MergeConflict => Refusal::Conflict,
        code if code.is_stale_plan() || code.retryable_without_operator_action() => {
            if request.failed_attempts + 1 < u64_from_usize(MAX_MERGE_ATTEMPTS) {
                Refusal::Next(NextRequest::Retry)
            } else {
                Refusal::Fail
            }
        }
        _ => Refusal::Fail,
    }
}

/// Returns whether `error` says that the namespace it's about doesn't exist
/// or was deleted.
pub(crate) fn is_gone(error: &ClientError) -> bool {
    matches!(
        error.code(),
        Some(ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted)
    )
}
