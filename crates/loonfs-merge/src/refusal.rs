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

/// Returns whether sending the request again can get past `error`. That's
/// the case when the request's outcome isn't known (an `outcome_unknown`
/// error), or when the client would retry the error for a request that's
/// safe to send twice (see
/// [`ClientError::retryable_without_operator_action`]).
pub(crate) fn sends_again(error: &ClientError) -> bool {
    error
        .code()
        .is_some_and(|code| code.kind() == ErrorKind::OutcomeUnknown)
        || error.retryable_without_operator_action()
}

/// Returns the server's error response for a merge or a rebase that it
/// refused, or returns `error` when sending the request again can get past
/// it (see [`sends_again`]), so that the step leaves the state as it was and
/// the next step sends the same request.
pub(crate) fn refused(error: ClientError) -> Result<ErrorResponse> {
    let sends_again = sends_again(&error);
    match error {
        ClientError::Api { response, .. } if !sends_again => Ok(*response),
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

#[cfg(test)]
mod tests {
    use super::sends_again;
    use loonfs_client::ClientError;
    use loonfs_types::ErrorResponse;

    #[test]
    fn sends_again_on_an_unknown_outcome_and_on_what_the_client_retries() {
        let api = |code: &str| ClientError::Api {
            status: 503,
            response: Box::new(ErrorResponse {
                code: code.to_owned(),
                feature: None,
                message: String::new(),
                param: None,
                request_id: None,
                details: None,
            }),
        };
        let status = |status: u16| ClientError::HttpStatus {
            status,
            provider_code: None,
            detail: "not the error envelope".to_owned(),
        };
        assert!(sends_again(&api("outcome_unknown")));
        assert!(sends_again(&api("server_busy")));
        assert!(sends_again(&ClientError::Http("reset".to_owned())));
        for retried in [408, 429, 502] {
            assert!(sends_again(&status(retried)), "{retried}");
        }

        for non_retryable in [
            api("store_contention"),
            ClientError::ConfigIo("missing".to_owned()),
            ClientError::ConfigDecode("bad toml".to_owned()),
            ClientError::InvalidNamespacePath("/bad".to_owned()),
            ClientError::UploadTooLarge {
                size_bytes: 100,
                reason: "too big".to_owned(),
            },
            ClientError::Io("eof".to_owned()),
            ClientError::Json("invalid json".to_owned()),
            ClientError::Protocol("contract violation".to_owned()),
            status(400),
            status(501),
            status(505),
        ] {
            assert!(
                !sends_again(&non_retryable),
                "must not retry {non_retryable:?}"
            );
        }
    }
}
