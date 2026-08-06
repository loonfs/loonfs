//! [`CommitPlan`]: the output of validation — every op checked and
//! resolved, ready to materialize into WAL deltas.

#[cfg(test)]
use crate::metadata::MetadataState;
#[cfg(test)]
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::TombstoneGeneration;
use loonfs_api::{
    AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId,
    NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub apply_after_seq: ChangeSeq,
    pub assigned_seq: ChangeSeq,
    pub(crate) validated_ops: Vec<ValidatedOp>,
    pub resulting_next_inode_id: InodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ValidatedOp {
    CreateDir {
        op_index: u32,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        child_inode_id: InodeId,
        create_inode_delta_index: u32,
        bind_delta_index: u32,
    },
    CreateFile {
        op_index: u32,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        child_inode_id: InodeId,
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
        new_parent_inode_id: InodeId,
        new_display_name: DisplayName,
        new_name_key: NameKey,
        unbind_delta_index: u32,
        bind_delta_index: u32,
    },
    DeleteSubtree {
        op_index: u32,
        root_inode_id: InodeId,
        source_binding: ResolvedBinding,
        unbind_delta_index: u32,
        tombstone_delta_index: u32,
    },
    Undelete {
        op_index: u32,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: DisplayName,
        name_key: NameKey,
        /// The exact deletion generation validation resolved and pinned:
        /// the active tombstone's own event coordinates.
        target: TombstoneGeneration,
        revoke_tombstone_delta_index: u32,
        bind_delta_index: u32,
    },
    UpdateAttributes {
        op_index: u32,
        inode_id: InodeId,
        attributes_revision_no: AttributeRevisionNo,
        attributes: Attributes,
        attributes_delta_index: u32,
    },
}

/// The base state a store-free validation pass runs against.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct CommitValidationContext<'a> {
    pub head: HeadState,
    pub metadata_state: &'a MetadataState,
}
