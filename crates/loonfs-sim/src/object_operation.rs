//! Object-store operation vocabulary used by fault schedules and traces.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectOperationKind {
    Head,
    Get,
    Put,
    PutIfAbsent,
    CompareAndSwap,
    Delete,
    ListPrefix,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectOperation {
    pub step: u64,
    pub kind: ObjectOperationKind,
    pub key: String,
}
