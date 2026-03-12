use loon_types::{ChangeSeq, ControlObjectEnvelope, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkClass {
    BuildSnapshot,
    BuildChangeIndex,
    BuildListingIndex,
    BuildRevisionIndex,
    VerifyNamespaceIntegrity,
    PlanGc,
    SweepGcPlan,
    AdvanceRetentionFloor,
}

impl WorkClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BuildSnapshot => "BuildSnapshot",
            Self::BuildChangeIndex => "BuildChangeIndex",
            Self::BuildListingIndex => "BuildListingIndex",
            Self::BuildRevisionIndex => "BuildRevisionIndex",
            Self::VerifyNamespaceIntegrity => "VerifyNamespaceIntegrity",
            Self::PlanGc => "PlanGc",
            Self::SweepGcPlan => "SweepGcPlan",
            Self::AdvanceRetentionFloor => "AdvanceRetentionFloor",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueBroker {
    pub broker_id: String,
    pub epoch: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueClaim {
    pub worker_id: String,
    pub claim_token: String,
    pub heartbeat_at_ms: u64,
    pub timeout_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeqScopedPayload {
    pub namespace_id: NamespaceId,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Ready,
    Claimed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJob {
    pub job_id: String,
    pub dedupe_key: String,
    pub state: JobState,
    pub payload: SeqScopedPayload,
    pub follow_up: Option<SeqScopedPayload>,
    pub claim: Option<QueueClaim>,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueShardState {
    pub work_class: WorkClass,
    pub shard_id: u32,
    pub broker: Option<QueueBroker>,
    pub jobs: Vec<QueueJob>,
}

pub type QueueShardEnvelope = ControlObjectEnvelope<QueueShardState>;
