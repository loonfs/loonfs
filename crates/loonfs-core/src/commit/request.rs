//! [`CommitRequest`]: one logical commit's ops, preconditions, and fencing
//! epoch.

use super::CommitExecutionContext;
use loonfs_api::v0::{CommitOp, CommitPrecondition, CommitRequest as ApiCommitRequest};
use loonfs_api::{CommitId, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub writer_epoch: WriterEpoch,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<CommitPrecondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl CommitRequest {
    pub(crate) fn from_v0(context: CommitExecutionContext, request: ApiCommitRequest) -> Self {
        Self {
            namespace_id: context.namespace_id,
            commit_id: request.commit_id,
            writer_epoch: context.writer_epoch,
            ops: request.ops,
            preconditions: request.preconditions,
            message: request.message,
        }
    }
}
