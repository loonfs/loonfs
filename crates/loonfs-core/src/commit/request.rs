//! [`CommitRequest`]: one logical commit's ops, preconditions, and writer
//! identity.

use super::{CommitOp, Precondition};
use loonfs_api::{CommitId, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub writer_id: String,
    pub writer_session_id: String,
    pub writer_epoch: WriterEpoch,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
