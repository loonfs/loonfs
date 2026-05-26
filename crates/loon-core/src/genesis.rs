use crate::metadata::{InodeRecord, MetadataState};
use loon_api::{ChangeSeq, InodeId, InodeKind};

pub(crate) fn bootstrap_basis_metadata_state() -> MetadataState {
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}
