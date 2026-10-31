//! The plan of one commit: its validated deltas and what its WAL record
//! needs for publication.

use super::CommitFingerprint;
use crate::storage::inline_content::InlineContent;

use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::format::wal::{WalCommitDelta, WalInlineContent};
use loonfs_types::{
    ActorId, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, NameKey, NamespaceId,
    WriterEpoch,
};

/// The bytes one append adds, the reference its revision names, and what
/// the commit records about that reference's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendedContent {
    pub(crate) content_ref: ContentRef,
    pub(crate) layout: Option<loonfs_types::ContentLayout>,
    pub(crate) pieces: Vec<WalInlineContent>,
}

/// One planned and validated commit, ready for its WAL record.
///
/// The request's identity moves in here when validation succeeds, so there
/// is no second copy that would need an equality check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitPlan {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) commit_id: CommitId,
    pub(crate) actor_id: ActorId,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) message: Option<String>,
    pub(crate) semantic_identity: CommitFingerprint,
    pub(crate) apply_after_seq: ChangeSeq,
    pub(crate) assigned_seq: ChangeSeq,
    pub(crate) committed_at_ms: u64,
    pub(crate) deltas: Vec<WalCommitDelta>,
    pub(crate) appended: Vec<WalInlineContent>,
    pub(crate) inline_content: Vec<InlineContent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub position: DeltaPosition,
}
