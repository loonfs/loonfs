use loon_api::{ChangeSeq, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
    BindingIs {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    DirectoryEmpty {
        inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    pub parent_inode: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}
