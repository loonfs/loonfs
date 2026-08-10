//! [`CommitIr`]: one planned commit's operations and fencing epoch.

use super::PlannedOp;
use loonfs_api::{CommitId, NamespaceId, WriterEpoch};

/// The planner's output: every operation the mutation compiles into, in
/// order, under one commit id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitIr {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) commit_id: CommitId,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) ops: Vec<PlannedOp>,
    pub(crate) message: Option<String>,
}
