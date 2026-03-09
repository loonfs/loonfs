use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    ReorderResponses,
    CrashAndRestart,
    FsErrorOnce { op: String },
    NetworkErrorOnce { rpc: String },
}
