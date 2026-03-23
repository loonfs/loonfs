use crate::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedRemoteInode {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub observed_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMutationRequest {
    pub namespace_id: NamespaceId,
    pub client_request_id: String,
    pub op: ClientMutationOp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMutationResponse {
    pub namespace_id: NamespaceId,
    pub client_request_id: String,
    pub committed_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_inode: Option<CreatedRemoteInode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaced_file: Option<ReplacedRemoteFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedRemoteInode {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub revision_no: RevisionNo,
    pub parent_inode_id: InodeId,
    pub display_name: String,
    pub content_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacedRemoteFile {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub revision_no: RevisionNo,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientMutationOp {
    CreateDir {
        parent_inode_id: InodeId,
        display_name: String,
    },
    CreateFile {
        parent_inode_id: InodeId,
        display_name: String,
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_manifest_digest: String,
    },
}
