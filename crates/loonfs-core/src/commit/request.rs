use super::{CommitOp, Precondition};
use loonfs_api::v0::CommitAnnotations;
use loonfs_api::{CommitId, FenceToken, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
}
