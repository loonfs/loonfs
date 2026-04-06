use crate::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type CommitAnnotations = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    ServiceProxied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub block_size_bytes: u64,
    pub mode: UploadMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadBlockResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub block_index: u32,
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteUploadRequest {
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub request_id: String,
    pub planned_head_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    pub ops: Vec<CommitOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResponse {
    pub namespace_id: NamespaceId,
    pub commit_id: String,
    pub committed_seq: ChangeSeq,
    pub results: Vec<CommitOpResult>,
}

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
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_manifest_digest: String,
    },
    DeleteFile {
        inode_id: InodeId,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommitPrecondition {
    HeadSeqIs {
        expected_seq: ChangeSeq,
    },
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommitOpResult {
    CreateDir {
        op_index: u32,
        inode_id: InodeId,
    },
    CreateFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_manifest_digest: String,
    },
    ReplaceFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_manifest_digest: String,
    },
    DeleteFile {
        op_index: u32,
        inode_id: InodeId,
    },
    Rename {
        op_index: u32,
        inode_id: InodeId,
    },
    DeleteSubtree {
        op_index: u32,
        root_inode: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedChange {
    pub seq: ChangeSeq,
    pub commit_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
    pub ops: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesResponse {
    pub namespace_id: NamespaceId,
    pub from_exclusive_seq: ChangeSeq,
    pub through_seq: ChangeSeq,
    pub changes: Vec<CommittedChange>,
}
