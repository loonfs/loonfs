//! [`Precondition`]: the core form of one commit precondition.

use loonfs_api::{ChangeSeq, InodeId, NameKey, RevisionNo};
use serde::{Deserialize, Serialize};

/// Core form of a commit precondition.
///
/// The serde representation deliberately mirrors the v0 wire encoding of
/// [`loonfs_api::v0::CommitPrecondition`]: this type is part of the commit
/// fingerprint preimage (format spec, "Commit identity fingerprints"), so its
/// serialization is durable
/// contract, not an implementation detail. `identity::tests` pins the
/// encoding.
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
    pub name_key: NameKey,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}
