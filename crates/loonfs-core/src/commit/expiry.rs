//! [`DeletionExpiry`]: the commit in which a namespace's writer expires
//! deletions, so they can no longer be undeleted.

use super::{CommitFingerprint, CommitOperation};
use loonfs_types::{ActorId, CommitId, ExpiredDeletion, NamespaceId};

/// The deletions that one of the writer's expiry commits expires, in order
/// ([File and subtree deletion](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
///
/// A client can't send this commit: `FilesystemOperation` has no expire
/// operation, so `POST /commits` and the embedded handles can't express it,
/// and only this crate builds one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeletionExpiry {
    commit_id: CommitId,
    deletions: Vec<ExpiredDeletion>,
}

impl DeletionExpiry {
    pub(crate) fn new(commit_id: CommitId, deletions: Vec<ExpiredDeletion>) -> Self {
        Self {
            commit_id,
            deletions,
        }
    }

    /// The actor every expiry commit is attributed to, the one the root
    /// inode is attributed to.
    pub(crate) fn actor_id() -> ActorId {
        ActorId::loonfs()
    }

    pub(crate) fn commit_id(&self) -> &CommitId {
        &self.commit_id
    }

    pub(crate) fn deletions(&self) -> &[ExpiredDeletion] {
        &self.deletions
    }

    pub(crate) fn semantic_identity(&self, namespace_id: &NamespaceId) -> CommitFingerprint {
        loonfs_types::expiry_commit_fingerprint(namespace_id, &Self::actor_id(), &self.deletions)
    }

    /// One operation for each deletion, in order, so each expire event is
    /// its own semantic operation and its own change-feed event.
    pub(crate) fn operations(&self) -> impl Iterator<Item = CommitOperation> + '_ {
        self.deletions
            .iter()
            .map(|deletion| CommitOperation::ExpireDeletion {
                inode_id: deletion.inode_id,
                deletion_seq: deletion.deletion_seq,
            })
    }
}
