use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    ReorderResponses,
    CrashAndRestart,
    FsErrorOnce { op: String },
    NetworkErrorOnce { rpc: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FaultPlan {
    #[serde(default)]
    pub client_faults: Vec<InjectedClientFault>,
}

impl FaultPlan {
    pub fn new(client_faults: Vec<InjectedClientFault>) -> Self {
        Self { client_faults }
    }

    pub fn is_empty(&self) -> bool {
        self.client_faults.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InjectedClientFault {
    CrashAfterStepOnce {
        checkpoint_id: String,
    },
    StoreErrorOnce {
        operation: InjectedStoreOperation,
        key_contains: String,
        error_kind: InjectedStoreErrorKind,
    },
    DispatchErrorOnce {
        checkpoint_id: String,
        message: String,
    },
    LocalApplyErrorOnce {
        operation: String,
        path_contains: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectedStoreOperation {
    Head,
    Get,
    Put,
    Delete,
    ListPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectedStoreErrorKind {
    NotFound,
    PreconditionFailed,
    Conflict,
    InvalidRange,
    Transport,
}
