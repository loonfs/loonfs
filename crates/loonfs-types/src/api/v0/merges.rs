//! Merging a fork into a namespace, and rebasing a fork: the requests, the
//! conflict that a refused merge reports, and a rebase's conflict report.

use crate::{
    AbsolutePath, ChangeSeq, CommitId, ContentRef, InodeId, NamespaceId, NamespaceMetadata,
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
    pub through_seq: Option<ChangeSeq>,
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
    pub through_seq: Option<ChangeSeq>,
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
    /// undelete, or a move into a directory whose create or undelete was skipped, or the delete of
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
    /// The server's merge allowance, which limits the estimated peak memory of the one merge
    /// that the server admits at a time.
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
