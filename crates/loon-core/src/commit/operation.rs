use loon_api::v0::RenameMode;
use loon_api::{ContentRef, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};

/// Core form of a semantic commit operation.
///
/// The serde representation deliberately mirrors the v0 wire encoding of
/// [`loon_api::v0::CommitOp`]: this type is part of the commit fingerprint
/// preimage (spec 050 §3.1), so its serialization is durable contract, not an
/// implementation detail. `identity::tests` pins the encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommitOp {
    CreateDir {
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        parent_inode: InodeId,
        display_name: String,
        content_ref: ContentRef,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    RestoreRevision {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
    },
    DeleteFile {
        inode_id: InodeId,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
        mode: RenameMode,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
}
