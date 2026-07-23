//! The model's genesis state: the root inode a fresh namespace starts
//! with, mirroring core's bootstrap.

use crate::metadata::{InodeRecord, MetadataState};
use loonfs_api::{ChangeSeq, InodeId, InodeKind};

/// Builds the metadata state of a fresh namespace.
pub fn bootstrap_metadata_state() -> MetadataState {
    MetadataState {
        inodes: vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
        }],
        direntry_binds: Vec::new(),
        direntry_unbinds: Vec::new(),
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
    }
}
