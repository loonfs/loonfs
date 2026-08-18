//! The model's genesis state: the root inode a fresh namespace starts
//! with, mirroring core's bootstrap.

use crate::metadata::{InodeRecord, MetadataState};
use loonfs_api::{ActorRef, ChangeSeq, InodeKind, ROOT_INODE_ID};

/// Builds the metadata state of a fresh namespace.
pub fn bootstrap_metadata_state(created_at_ms: u64) -> MetadataState {
    MetadataState {
        inodes: vec![InodeRecord {
            inode_id: ROOT_INODE_ID,
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
            commit_id: loonfs_api::wire::control::genesis_commit_id(),
            created_by: ActorRef::loonfs_system(),
            created_at_ms,
        }],
        ..MetadataState::default()
    }
}
