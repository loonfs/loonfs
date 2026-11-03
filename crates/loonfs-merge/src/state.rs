//! The state of a merge session, which the caller saves after every step.

use loonfs_types::options::MergeForkOptions;
use loonfs_types::{ActorId, ChangeSeq, CommitId, ErrorResponse, MergeConflict, NamespaceId};
use serde::{Deserialize, Serialize};

/// The state of one fork's merge session: the namespaces that the session
/// uses, the new forks that its rebases created, and its status.
///
/// This is the session record that the design note describes. A caller saves
/// it after every step, and [`MergeSession::resume`](crate::MergeSession::resume)
/// continues from it.
///
/// Warning: keep the state until the session has deleted all of its new
/// forks. [`MergeSession::recover`](crate::MergeSession::recover) finds a new
/// fork through the fork that it was rebased from, so if the state is lost
/// after a `merged` session deleted some of its new forks, recovery can't
/// connect the remaining new forks to the original fork, and the session
/// merges the original fork again. A change that a new fork already merged
/// is then left out where the namespace has the same version, and conflicts
/// where it doesn't, so the namespace keeps what the new forks merged, but
/// the session can end `failed`, with new forks of its own to abandon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeSessionState {
    /// The namespace that the session merges into.
    pub namespace_id: NamespaceId,
    /// The fork that the session started with.
    pub original_fork_id: NamespaceId,
    /// The actor that every merge and replay commit of the session commits
    /// as. A request that the session sends again has to repeat it, because
    /// the actor is part of a commit's identity.
    pub actor_id: ActorId,
    /// The new forks that the session's rebases created, oldest first, and at
    /// most [`MAX_NEW_FORKS`](crate::MAX_NEW_FORKS). The session deletes them
    /// once it's `merged` or `abandoned`, and the caller deletes the original
    /// fork.
    pub new_fork_ids: Vec<NamespaceId>,
    /// Where the session is.
    pub status: MergeSessionStatus,
}

/// Where a merge session is in the loop.
///
/// ```text
///                  merge_too_large with split_at_seq:
///                  merge through split_at_seq
///                  +---------+
///                  v         |
///               +-----------------+   merge committed   +--------+
///    start ---> |     merging     | ------------------> | merged |
///               +-----------------+                     +--------+
///                  |           ^
///  merge_conflict: |           | resolver returns: merge
///  rebase into a   |           | the new fork through its head
///  new fork        v           |
///             +----------+   +-----------+
///             | rebasing |-->| resolving |
///             +----------+   +-----------+
///          replay committed
///          through the end
///
/// An error that the loop can't act on, while merging or rebasing, or an
/// error from the resolver:
///
///             +--------+   caller abandons   +-----------+
///             | failed | ------------------> | abandoned |
///             +--------+                     +-----------+
/// ```
///
/// When a rebase returns `merge_too_large` with `split_at_seq`, the session
/// stays `rebasing` and replays through the hint. A merged or abandoned
/// session deletes its new forks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeSessionStatus {
    /// The current fork is ready to merge, and the next step sends `merge`.
    Merging {
        /// The fork that the session merges: the original fork, or the newest
        /// new fork.
        fork_id: NamespaceId,
        /// The fork sequence that the session merges the fork through.
        through_seq: ChangeSeq,
        /// The merge request that the next step sends. It goes through
        /// `through_seq`, or through a split hint below it.
        merge: PendingCommit,
    },
    /// A merge of the current fork conflicted, and the session replays the
    /// fork's unmerged commits into a new fork, in pieces when a replay
    /// commit is too large.
    Rebasing {
        /// The fork whose unmerged commits the session replays.
        fork_id: NamespaceId,
        /// The fork sequence that the session replays the fork's commits
        /// through.
        through_seq: ChangeSeq,
        /// The new fork that the replay commits go into.
        new_fork_id: NamespaceId,
        /// The rebase request that the next step sends.
        replay: PendingCommit,
        /// The conflict reports of the replay commits so far.
        reports: Vec<ConflictReport>,
    },
    /// The new fork has every replay commit, and the next step hands it to
    /// the resolver.
    Resolving {
        /// The new fork that the resolver writes resolutions into.
        new_fork_id: NamespaceId,
        /// The conflict report of every replay commit into the new fork.
        reports: Vec<ConflictReport>,
    },
    /// The session's last merge committed. The next steps delete the new
    /// forks.
    Merged,
    /// A merge, a rebase, or the resolver failed in a way that the loop can't
    /// act on. The caller chooses the next step, and the new forks stay until
    /// the caller abandons the session.
    Failed {
        /// The fork to inspect: the fork that the failed merge or rebase
        /// read, or the new fork that the resolver didn't resolve.
        fork_id: NamespaceId,
        /// What failed.
        failure: MergeFailure,
        /// The conflict reports of the new fork's replay commits, when a
        /// rebase or the resolver failed.
        reports: Vec<ConflictReport>,
    },
    /// The caller abandoned the session. The next steps delete the new forks.
    Abandoned,
}

impl MergeSessionStatus {
    /// Returns the status's name, as the state's `status` field stores it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Merging { .. } => "merging",
            Self::Rebasing { .. } => "rebasing",
            Self::Resolving { .. } => "resolving",
            Self::Merged => "merged",
            Self::Failed { .. } => "failed",
            Self::Abandoned => "abandoned",
        }
    }

    /// Returns whether the caller can abandon a session in this status,
    /// which it can do only after the session failed.
    pub fn can_abandon(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

/// A merge or rebase request that the session sends, and sends again with the
/// same commit ID until the request's outcome is known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingCommit {
    /// The commit ID of the merge or the replay commit. A request sent again
    /// after its commit was committed returns the original commit.
    pub commit_id: CommitId,
    /// The fork sequence that the request goes through.
    pub through_seq: ChangeSeq,
    /// The number of times that the request failed with a transient error,
    /// which is less than [`MAX_MERGE_ATTEMPTS`](crate::MAX_MERGE_ATTEMPTS).
    pub failed_attempts: u32,
}

impl PendingCommit {
    /// Builds a request through `through_seq` under a new commit ID.
    pub(crate) fn new(through_seq: ChangeSeq) -> Self {
        Self {
            commit_id: CommitId::generate(),
            through_seq,
            failed_attempts: 0,
        }
    }

    /// Returns the same request after one more transient error.
    pub(crate) fn retried(&self) -> Self {
        Self {
            failed_attempts: self.failed_attempts + 1,
            ..self.clone()
        }
    }

    /// Returns the request's options, with its commit ID and its sequence.
    pub(crate) fn options(&self) -> MergeForkOptions {
        MergeForkOptions {
            commit_id: Some(self.commit_id.clone()),
            message: None,
            through_seq: Some(self.through_seq),
        }
    }
}

/// The conflict report of one replay commit: the changes that it left out.
///
/// A replay commit's report has at most one entry for each inode that the
/// fork's commits in its range touched, so the reports of one rebase have at
/// most one entry for each inode that the rebased range touched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictReport {
    /// The fork sequence that the replay commit replayed the fork's commits
    /// through.
    pub through_seq: ChangeSeq,
    /// One entry for each inode whose change the replay commit left out.
    pub conflicts: Vec<MergeConflict>,
}

/// Why a merge session failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeFailure {
    /// A merge or a rebase returned an error that the loop can't act on, or
    /// a transient error on its last attempt (see
    /// [`MAX_MERGE_ATTEMPTS`](crate::MAX_MERGE_ATTEMPTS)).
    Refused {
        /// The error that the server returned.
        error: ErrorResponse,
    },
    /// The resolver returned an error instead of making the new fork ready
    /// to merge.
    Unresolved {
        /// The resolver's error.
        message: String,
    },
    /// The session needed another new fork after its rebases had created
    /// [`MAX_NEW_FORKS`](crate::MAX_NEW_FORKS) new forks.
    NewForkLimit,
}
