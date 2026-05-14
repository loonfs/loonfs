use crate::{ChangeSeq, CommitId, ContentRef, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkNamespaceRequest {
    pub new_namespace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSummary {
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationResult {
    pub namespace_id: NamespaceId,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemPutBehavior {
    CreateOnly,
    ReplaceExisting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FilesystemOperation {
    PutFile {
        path: String,
        content_ref: ContentRef,
        behavior: FilesystemPutBehavior,
    },
    DeletePath {
        path: String,
    },
    MovePath {
        from_path: String,
        to_path: String,
    },
    CopyPath {
        from_path: String,
        to_path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemOperationRequest {
    pub commit_id: CommitId,
    pub operation: FilesystemOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemOperationResponse {
    pub namespace_id: NamespaceId,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCheckpointResponse {
    pub namespace_id: NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub checkpoint_hint_seq: Option<ChangeSeq>,
    pub checkpoint_hint_points_at_checkpoint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceRetentionResponse {
    pub namespace_id: NamespaceId,
    pub retention_floor_seq: ChangeSeq,
}

impl From<MutationResult> for FilesystemOperationResponse {
    fn from(value: MutationResult) -> Self {
        Self {
            namespace_id: value.namespace_id,
            committed_seq: value.committed_seq,
        }
    }
}

impl From<FilesystemOperationResponse> for MutationResult {
    fn from(value: FilesystemOperationResponse) -> Self {
        Self {
            namespace_id: value.namespace_id,
            committed_seq: value.committed_seq,
        }
    }
}
