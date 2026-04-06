use crate::metadata::{InodeRecord, MetadataState};
use loon_api::{ChangeSeq, InodeId, InodeKind};

pub fn bootstrap_basis_metadata_state() -> MetadataState {
    MetadataState {
        inodes: vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        direntries: Vec::new(),
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
    }
}
