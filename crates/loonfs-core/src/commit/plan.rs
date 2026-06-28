use super::ResolvedBinding;
use crate::invariants::InvariantId;
use crate::metadata::MetadataState;
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, CommitId, ContentRef, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub apply_after_seq: ChangeSeq,
    pub assigned_seq: ChangeSeq,
    pub(crate) validated_ops: Vec<ValidatedOp>,
    pub resulting_next_inode_id: InodeId,
    pub checked_invariants: Vec<InvariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ValidatedOp {
    CreateDir {
        op_index: u32,
        parent_inode: InodeId,
        display_name: String,
        name_key: String,
        child_inode: InodeId,
        create_inode_delta_index: u32,
        bind_delta_index: u32,
    },
    CreateFile {
        op_index: u32,
        parent_inode: InodeId,
        display_name: String,
        name_key: String,
        child_inode: InodeId,
        content_ref: ContentRef,
        create_inode_delta_index: u32,
        bind_delta_index: u32,
        revision_delta_index: u32,
    },
    ReplaceFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
        revision_delta_index: u32,
    },
    RestoreRevision {
        op_index: u32,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        revision_no: RevisionNo,
        content_ref: ContentRef,
        revision_delta_index: u32,
    },
    DeleteFile {
        op_index: u32,
        inode_id: InodeId,
        source_binding: ResolvedBinding,
        unbind_delta_index: u32,
        tombstone_delta_index: u32,
    },
    Rename {
        op_index: u32,
        inode_id: InodeId,
        source_binding: ResolvedBinding,
        new_parent_inode: InodeId,
        new_display_name: String,
        new_name_key: String,
        unbind_delta_index: u32,
        bind_delta_index: u32,
    },
    DeleteSubtree {
        op_index: u32,
        root_inode: InodeId,
        source_binding: ResolvedBinding,
        unbind_delta_index: u32,
        tombstone_delta_index: u32,
    },
}

#[derive(Debug, Clone)]
pub struct CommitValidationContext<'a> {
    pub head: HeadState,
    pub now_ms: u64,
    pub metadata_state: &'a MetadataState,
}
