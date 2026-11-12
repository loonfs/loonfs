//! Defines [`MergeError`], returned by a merge session step that didn't
//! complete.

use crate::MAX_NEW_FORKS;
use loonfs_client::ClientError;
use loonfs_types::NamespaceId;
use thiserror::Error;

/// Error returned by a merge session step that didn't complete, which leaves
/// the session's state as it was before the step.
///
/// An error that a merge or a rebase returned and that the loop can't act on
/// doesn't end up here: the session records it and becomes
/// [`failed`](crate::MergeSessionStatus::Failed).
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum MergeError {
    /// A request failed before its outcome was known (the transport failed,
    /// the server returned an outcome-unknown error, or a read that the step
    /// needs failed), or the server refused a merge or a rebase with a
    /// transient error. Running the step again sends the same request.
    #[error("merge session request failed: {0}")]
    Client(#[from] ClientError),
    /// The caller abandoned a session that isn't `failed`.
    #[error("only a failed merge session can be abandoned, and this session is `{status}`")]
    NotFailed {
        /// The session's status.
        status: &'static str,
    },
    /// Recovery found more than one new fork that a session could have
    /// continued with after a rebase of `fork_id`.
    #[error(
        "cannot recover the merge session: forks {new_fork_ids:?} were each rebased from \
         `{fork_id}` and then merged or rebased"
    )]
    AmbiguousRecovery {
        /// The fork that the new forks were rebased from.
        fork_id: NamespaceId,
        /// The new forks that a session could have continued with.
        new_fork_ids: Vec<NamespaceId>,
    },
    /// Recovery found more new forks than one session creates.
    #[error(
        "cannot recover the merge session: more than {MAX_NEW_FORKS} forks were rebased from \
         `{original_fork_id}` and the forks rebased from it"
    )]
    TooManyNewForks {
        /// The fork that the session started with.
        original_fork_id: NamespaceId,
    },
}

/// Result type returned by the merge loop.
pub type Result<T> = std::result::Result<T, MergeError>;
