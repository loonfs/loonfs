use crate::{v0::CommitAnnotations, ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
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
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileBytes {
    pub entry: AuthoritativePathEntry,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileRevision {
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    pub content_manifest_digest: String,
    pub size_bytes: u64,
    pub content_digest: String,
    pub commit_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileRevisionList {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub authoritative_head_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_absolute_path: Option<String>,
    pub currently_visible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_before_revision_no: Option<RevisionNo>,
    pub revisions: Vec<AuthoritativeFileRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileRevisionBytes {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_absolute_path: Option<String>,
    pub currently_visible: bool,
    pub authoritative_head_seq: ChangeSeq,
    pub revision: AuthoritativeFileRevision,
    pub bytes: Vec<u8>,
}
