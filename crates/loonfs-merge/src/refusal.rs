//! How the merge loop acts on an error that a merge or a rebase returned.

use crate::{PendingCommit, Result};
use loonfs_client::ClientError;
use loonfs_types::{CommitSeq, ErrorCode, ErrorKind, ErrorResponse};

/// How the loop acts on an error that a merge or a rebase returned.
pub(crate) enum Refusal {
    /// The request was too large, and the next request goes through the
    /// error's split hint.
    Split(CommitSeq),
    /// The merge conflicted.
    Conflict,
    /// The loop can't act on the error.
    Fail,
}

/// Returns the server's error response for a merge or a rebase that it
/// refused, or returns `error` when the request's outcome isn't known (a
/// transport failure or an outcome-unknown error) or the server refused the
/// request with a transient error, so that the step leaves the state as it
/// was and the next step sends the same request.
///
/// A transient error is `commit_queue_full`, `server_busy`, or
/// `shutting_down` (see [`ErrorCode::retryable_without_operator_action`]),
/// which the client has already retried with a backoff.
pub(crate) fn refused(error: ClientError) -> Result<ErrorResponse> {
    let send_again = error.code().is_some_and(|code| {
        code.kind() == ErrorKind::OutcomeUnknown || code.retryable_without_operator_action()
    });
    match error {
        ClientError::Api { response, .. } if !send_again => Ok(*response),
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
                Refusal::Split(split_at_seq)
            }
            _ => Refusal::Fail,
        },
        ErrorCode::MergeConflict => Refusal::Conflict,
        _ => Refusal::Fail,
    }
}

/// Returns whether `error` says that the namespace it's about doesn't exist
/// or was deleted.
pub(crate) fn is_gone(error: &ClientError) -> bool {
    error.code().is_some_and(ErrorCode::is_namespace_gone)
}
