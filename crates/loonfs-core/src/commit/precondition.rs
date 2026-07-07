use loonfs_api::{ChangeSeq, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};

/// Core form of a commit precondition.
///
/// Part of the commit fingerprint preimage (format spec, "Commit identity
/// fingerprints"): its serde encoding is durable contract, pinned by
/// `identity::tests`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Precondition {
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode_id: InodeId,
        name_key: String,
    },
    BindingIs {
        parent_inode_id: InodeId,
        name_key: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    DirectoryEmpty {
        inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}
