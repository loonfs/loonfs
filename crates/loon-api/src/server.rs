use crate::{ChangeSeq, ContentRef, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativePathEntry {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub authoritative_head_seq: ChangeSeq,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub revision_no: Option<RevisionNo>,
    pub size_bytes: Option<u64>,
    pub content_ref: Option<ContentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileBytes {
    pub entry: AuthoritativePathEntry,
    pub bytes: Vec<u8>,
}
