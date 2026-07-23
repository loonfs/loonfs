//! Serializable object-store fault schedules.

use crate::object_operation::{ObjectOperation, ObjectOperationKind};
use crate::rng::SimSeed;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreFault {
    PutReturnsTransientError,
    PutSucceedsButResponseLost,
    CompareAndSwapStale,
    GetReturnsStaleValue,
    ListOmitsRecentObject,
    DeleteReturnsTransientError,
    CorruptObjectBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledFault {
    pub step: u64,
    pub op_kind: Option<ObjectOperationKind>,
    pub key_contains: Option<String>,
    pub fault: ObjectStoreFault,
}

impl ScheduledFault {
    pub fn matches(&self, op: &ObjectOperation) -> bool {
        self.step == op.step
            && self.op_kind.is_none_or(|kind| kind == op.kind)
            && self
                .key_contains
                .as_deref()
                .is_none_or(|needle| op.key.contains(needle))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultSchedule {
    pub seed: SimSeed,
    pub faults: Vec<ScheduledFault>,
}

impl FaultSchedule {
    pub fn empty(seed: SimSeed) -> Self {
        Self {
            seed,
            faults: Vec::new(),
        }
    }

    pub fn fault_for(&self, op: &ObjectOperation) -> Option<&ScheduledFault> {
        self.faults.iter().find(|fault| fault.matches(op))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_schedule_round_trips_json() {
        let schedule = FaultSchedule {
            seed: SimSeed(11),
            faults: vec![ScheduledFault {
                step: 3,
                op_kind: Some(ObjectOperationKind::PutIfAbsent),
                key_contains: Some("head".to_owned()),
                fault: ObjectStoreFault::PutSucceedsButResponseLost,
            }],
        };

        let json = serde_json::to_string(&schedule).expect("serialize schedule");
        let decoded: FaultSchedule = serde_json::from_str(&json).expect("decode schedule");
        assert_eq!(decoded, schedule);
    }

    #[test]
    fn fault_schedule_matches_expected_step() {
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            faults: vec![ScheduledFault {
                step: 2,
                op_kind: Some(ObjectOperationKind::Get),
                key_contains: Some("wal".to_owned()),
                fault: ObjectStoreFault::GetReturnsStaleValue,
            }],
        };

        let matching = ObjectOperation {
            step: 2,
            kind: ObjectOperationKind::Get,
            key: "namespaces/ns/wal/1".to_owned(),
        };
        let wrong_kind = ObjectOperation {
            step: 2,
            kind: ObjectOperationKind::Head,
            key: "namespaces/ns/wal/1".to_owned(),
        };

        assert!(schedule.fault_for(&matching).is_some());
        assert!(schedule.fault_for(&wrong_kind).is_none());
    }
}
