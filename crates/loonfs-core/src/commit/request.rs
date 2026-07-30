//! [`CommitRequest`]: one planned commit's operations and fencing epoch.

use super::PlannedOp;
use loonfs_api::{CommitId, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};

/// The planner's output: every operation the mutation compiles into, in
/// order, under one commit id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommitRequest {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) commit_id: CommitId,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) ops: Vec<PlannedOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
}
