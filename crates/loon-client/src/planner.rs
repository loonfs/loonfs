use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlannerDecision {
    UploadLocalEdit,
    DownloadRemoteEdit,
    CreateConflictCopy,
    NoOp,
}
