//! Merging a fork into a namespace, and rebasing a fork: the requests, where
//! a fork's next merge starts, the conflict that a refused merge reports, and
//! a rebase's conflict report.

use crate::{
    AbsolutePath, CommitId, CommitSeq, ContentRef, InodeId, Merge, NamespaceId, NamespaceMetadata,
    RevisionNo,
};
use serde::{Deserialize, Serialize};

/// A request to merge a fork's commits into a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MergeForkRequest {
    /// The fork whose commits the merge applies. It has to be a fork of the namespace that the
    /// request merges into.
    pub source_namespace_id: NamespaceId,
    /// The fork sequence that the merge applies the fork's commits through. Without it, the merge
    /// applies them through the fork's head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub through_seq: Option<CommitSeq>,
    /// The caller's idempotency key for the merge.
    pub commit_id: CommitId,
    /// An annotation from the caller. The message is part of the merge's identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
}

/// A request to rebase a fork: to replay its unmerged commits into a new fork of the namespace
/// that it was forked from, as one replay commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct RebaseForkRequest {
    /// The fork whose unmerged commits the rebase replays. It has to be a fork of the namespace
    /// that the request names in its path.
    pub source_namespace_id: NamespaceId,
    /// The new fork that the replay commit commits into. The first rebase of the fork creates it
    /// from the namespace's head, and a later rebase continues into it.
    pub new_namespace_id: NamespaceId,
    /// The fork sequence that the replay commit replays the fork's commits through. Without it,
    /// the replay commit replays them through the fork's head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub through_seq: Option<CommitSeq>,
    /// The caller's idempotency key for the replay commit.
    pub commit_id: CommitId,
    /// An annotation from the caller. The message is part of the replay commit's identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
}

/// The result of a rebase: the new fork, and the changes that the replay commit left out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RebaseForkResponse {
    /// The new fork after the replay commit.
    pub namespace: NamespaceMetadata,
    /// The replay commit's conflict report, with one entry for each inode whose change the replay
    /// commit didn't apply. It's absent when the request's commit ID identifies a replay commit
    /// that an earlier request committed, because only the response that committed the replay
    /// commit has its report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub conflicts: Option<Vec<MergeConflict>>,
}

/// Where a fork's next merge into a namespace starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NextMergeRangeResponse {
    /// The sequence that the fork was made at, of the namespace or, for a rebase's new fork, of
    /// the namespace that the rebase forked.
    pub fork_point_seq: CommitSeq,
    /// The fork's head sequence when the server read it.
    pub source_head_seq: CommitSeq,
    /// Whether the retention floors allow the fork's next merge, and where that merge starts.
    #[serde(flatten)]
    pub outcome: NextMergeRangeOutcome,
}

/// Whether the retention floors allow a fork's next merge into a namespace, and where that merge
/// starts.
///
/// A newer server can return an outcome that isn't listed here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum NextMergeRangeOutcome {
    /// The floors allow the next merge.
    Open {
        /// The fork's newest merge into the namespace, which the next merge continues. It's
        /// absent before the fork's first merge. In a rebase's new fork, it's the fork's newest
        /// replay commit, never a merge that the new fork inherited.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        merge: Option<Merge>,
        /// The fork sequence that the next merge starts after: the newest merge's
        /// `source_through_seq`, or the fork point before the fork's first merge. In a rebase's
        /// new fork, it's the new fork's `resume_after_seq` before the first replay commit.
        next_after_seq: CommitSeq,
    },
    /// The range starts below the fork's own retention floor, so a merge of the fork returns
    /// `rebootstrap_required`, because the fork no longer keeps the history that the merge's net
    /// change needs.
    RangeStartBelowFloor {
        /// The fork's newest merge into the namespace, as for `open`. It's absent before the
        /// fork's first merge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        merge: Option<Merge>,
        /// The fork sequence that the next merge would start after, as for `open`.
        next_after_seq: CommitSeq,
        /// The fork's retention floor, which is above `next_after_seq`.
        retention_floor_seq: CommitSeq,
    },
    /// The fork point is below the namespace's retention floor, so a merge of the fork returns
    /// `rebootstrap_required`. The outcome doesn't say where the next merge would start, because
    /// a compaction might have removed the fork's newest merge from the namespace's merge index.
    ForkPointBelowFloor {
        /// The namespace's retention floor, which is above the fork point (for a rebase's new
        /// fork, above the new fork's own fork point).
        retention_floor_seq: CommitSeq,
    },
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

/// One inode whose change a merge can't apply, because the namespace being merged into changed
/// the inode after the fork point, hides it, or has the name that the change needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MergeConflict {
    /// The inode. It has the same ID in the fork and in the namespace being merged into.
    #[serde(with = "crate::inode_id::public")]
    pub inode_id: InodeId,
    /// Which check failed.
    pub kind: MergeConflictKind,
    /// The version that the fork started from, which is the merged-into namespace's version at
    /// the fork point. It's absent for an inode that the fork created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub base: Option<MergeConflictVersion>,
    /// The fork's version at the end of the merge's range. It's absent for an inode that the fork
    /// deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub ours: Option<MergeConflictVersion>,
    /// The merged-into namespace's current version. It's absent when that namespace deleted or
    /// hides the inode. For a `name` conflict, it's the inode that has the name there. For a
    /// `dependency` conflict, it's the fork's version of the inode whose change was skipped (the
    /// directory, or the moved inode), so its path is the `ours` path of that inode's own entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub theirs: Option<MergeConflictVersion>,
}

/// Which check of the conflict rule a merge's change failed.
///
/// A newer server can return a kind that isn't listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MergeConflictKind {
    /// The file's current revision was committed after the fork point by a commit that isn't a
    /// merge of this fork.
    Content,
    /// The inode's current binding was committed after the fork point by a commit that isn't a
    /// merge of this fork.
    Binding,
    /// The inode's current attribute row was committed after the fork point by a commit that
    /// isn't a merge of this fork.
    Attributes,
    /// The inode's current access row was committed after the fork point by a commit that isn't a
    /// merge of this fork.
    Access,
    /// A deletion hides the inode or one of its ancestors, or the parent that an inode that the
    /// fork created or moved needs.
    Visibility,
    /// Another inode has the name that the fork's create or move needs.
    Name,
    /// Something below a directory that the fork deleted or moved changed after the fork point.
    Subtree,
    /// The change depends on a change that a rebase's replay commit skipped: a create, an
    /// restore, or a move into a directory whose create or restore was skipped, or the delete of
    /// a directory that still contains an inode whose move out of it was skipped.
    Dependency,
    /// A kind from a newer server. It serializes as `unknown`.
    #[serde(other)]
    Unknown,
}

/// One version of an inode in a merge conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MergeConflictVersion {
    /// The inode's path in that version, built from the stored display names.
    #[serde(deserialize_with = "crate::path::deserialize_derived_path")]
    pub path: AbsolutePath,
    /// The file's revision in that version. A directory has none.
    #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub revision: Option<MergeConflictRevision>,
}

/// A file's revision in one version of a merge conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MergeConflictRevision {
    /// The file's revision number in that version. A directory has none.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub revision_no: RevisionNo,
    /// The file's content in that version. This field is present exactly when `revision_no` is
    /// present.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub content_ref: ContentRef,
}

/// The bound that a merge's size exceeded, which the details of a `merge_too_large` error report.
///
/// A newer server can return a bound that isn't listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MergeBound {
    /// One WAL object, which stores the merge's commit alone.
    WalObject,
    /// The server's merge allowance, which limits the estimated peak memory of the merges that
    /// the server plans and publishes at the same time.
    MergeAllowance,
    /// The number of inodes that one WAL object could contain operations for. The commits in the
    /// merge's range touched more inodes than that.
    TouchedInodes,
    /// A bound from a newer server. It serializes as `unknown`.
    #[serde(other)]
    Unknown,
}

impl MergeBound {
    /// Returns the bound in words, for an error message.
    pub fn describe(self) -> &'static str {
        match self {
            Self::WalObject => "one WAL object",
            Self::MergeAllowance => "the merge allowance",
            Self::TouchedInodes => "the inodes that one WAL object can change",
            Self::Unknown => "a bound of the server",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that each outcome of the next-range query is tagged with `outcome` beside the
    /// fields that every response has, and that each response round-trips.
    #[test]
    fn a_next_range_outcome_is_tagged_beside_the_common_fields() {
        let merge = Merge {
            source_namespace_id: NamespaceId::parse("demo-branch").expect("namespace id"),
            source_after_seq: CommitSeq(418),
            source_through_seq: CommitSeq(425),
        };
        let merge_json = serde_json::json!({
            "source_namespace_id": "demo-branch",
            "source_after_seq": 418,
            "source_through_seq": 425
        });
        let cases = [
            (
                NextMergeRangeOutcome::Open {
                    merge: Some(merge.clone()),
                    next_after_seq: CommitSeq(425),
                },
                serde_json::json!({"outcome": "open", "merge": merge_json, "next_after_seq": 425}),
            ),
            (
                NextMergeRangeOutcome::Open {
                    merge: None,
                    next_after_seq: CommitSeq(418),
                },
                serde_json::json!({"outcome": "open", "next_after_seq": 418}),
            ),
            (
                NextMergeRangeOutcome::RangeStartBelowFloor {
                    merge: Some(merge),
                    next_after_seq: CommitSeq(425),
                    retention_floor_seq: CommitSeq(430),
                },
                serde_json::json!({
                    "outcome": "range_start_below_floor",
                    "merge": merge_json,
                    "next_after_seq": 425,
                    "retention_floor_seq": 430
                }),
            ),
            (
                NextMergeRangeOutcome::ForkPointBelowFloor {
                    retention_floor_seq: CommitSeq(450),
                },
                serde_json::json!({"outcome": "fork_point_below_floor", "retention_floor_seq": 450}),
            ),
        ];
        for (outcome, mut expected) in cases {
            let response = NextMergeRangeResponse {
                fork_point_seq: CommitSeq(418),
                source_head_seq: CommitSeq(440),
                outcome,
            };
            expected["fork_point_seq"] = serde_json::json!(418);
            expected["source_head_seq"] = serde_json::json!(440);
            let encoded = serde_json::to_value(&response).expect("serialize the next range");
            assert_eq!(encoded, expected);
            let decoded: NextMergeRangeResponse =
                serde_json::from_value(encoded).expect("decode the next range");
            assert_eq!(decoded, response);
        }
    }

    /// Checks that a client decodes an outcome from a newer server as `Unknown`, with the fields
    /// that every response has.
    #[test]
    fn a_next_range_outcome_from_a_newer_server_decodes_as_unknown() {
        let response: NextMergeRangeResponse = serde_json::from_value(serde_json::json!({
            "fork_point_seq": 418,
            "source_head_seq": 440,
            "outcome": "paused",
            "until_seq": 450
        }))
        .expect("a newer outcome decodes");
        assert_eq!(response.outcome, NextMergeRangeOutcome::Unknown);
        assert_eq!(response.source_head_seq, CommitSeq(440));
    }
}
