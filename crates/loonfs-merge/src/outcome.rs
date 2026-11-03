//! How one step of a merge session ended.

use loonfs_types::{ChangeSeq, CommitId, ErrorResponse, NamespaceId};
use serde::Serialize;

/// How one step of a merge session ended.
///
/// A caller reports it, so it serializes, with the variant's name in
/// `outcome`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MergeSessionOutcome {
    /// A merge of `fork_id` through `through_seq` committed.
    MergeCommitted {
        /// The fork that the merge applied.
        fork_id: NamespaceId,
        /// The merge's commit ID.
        commit_id: CommitId,
        /// The sequence that the merge committed at.
        committed_seq: ChangeSeq,
        /// The fork sequence that the merge applied the fork's commits
        /// through.
        through_seq: ChangeSeq,
    },
    /// A merge or a replay commit was too large, and the next step sends it
    /// through `split_at_seq`.
    Split {
        /// The `merge_too_large` error.
        error: ErrorResponse,
        /// The split hint that the next request goes through.
        split_at_seq: ChangeSeq,
    },
    /// A merge conflicted, and the next step rebases the fork into
    /// `new_fork_id`.
    Conflicted {
        /// The `merge_conflict` error, which counts the conflicts and lists
        /// the first one.
        error: ErrorResponse,
        /// The new fork that the rebase creates.
        new_fork_id: NamespaceId,
    },
    /// A replay commit into `new_fork_id` through `through_seq` committed.
    ReplayCommitted {
        /// The new fork.
        new_fork_id: NamespaceId,
        /// The fork sequence that the replay commit replayed the fork's
        /// commits through.
        through_seq: ChangeSeq,
        /// The changes that the replay commit left out.
        conflict_count: usize,
    },
    /// A rebase returned no conflict report, because an earlier request had
    /// committed its replay commit, so the session deleted the new fork and
    /// replays the fork's unmerged commits again into another one.
    ReportLost {
        /// The new fork whose report was lost.
        deleted_fork_id: NamespaceId,
        /// The new fork that the next step rebases into.
        new_fork_id: NamespaceId,
    },
    /// The resolver returned, and the session merges `fork_id`, the new
    /// fork, through `through_seq`, its head.
    Resolved {
        /// The new fork, which is now the current fork.
        fork_id: NamespaceId,
        /// The new fork's head, which the session merges it through.
        through_seq: ChangeSeq,
    },
    /// A merge or a rebase failed with a transient error, and the next step
    /// sends the same request again.
    Retrying {
        /// The error.
        error: ErrorResponse,
        /// The times that the request has failed so far.
        failed_attempts: u32,
    },
    /// The session failed, and its status says why.
    Failed,
    /// The session deleted one of its new forks.
    Deleted {
        /// The deleted fork.
        namespace_id: NamespaceId,
    },
}
