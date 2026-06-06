use loon_api::v0::RenameMode;
use loon_api::{ContentRef, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
