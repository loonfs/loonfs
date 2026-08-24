//! Serializable regression seeds and replay command formatting.

use crate::fault::FaultSchedule;
use crate::rng::SimSeed;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySeed {
    pub schema_version: u16,
    pub scenario: String,
    pub seed: SimSeed,
    pub max_steps: usize,
    pub writers: usize,
    pub failing_step: Option<u64>,
    pub fault_schedule: FaultSchedule,
}

impl ReplaySeed {
    pub fn to_json_pretty(&self) -> Result<String, ReplayError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn from_json_str(input: &str) -> Result<Self, ReplayError> {
        Ok(serde_json::from_str(input)?)
    }

    pub fn replay_command(&self) -> String {
        format!(
            "cargo run -p loonfs-sim-scenarios -- replay --seed-file seeds/regressions/{}-{:016x}.json",
            self.scenario, self.seed.0
        )
    }
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::FaultSchedule;

    #[test]
    fn replay_seed_formats_command() {
        let replay = ReplaySeed {
            schema_version: 1,
            scenario: "scenario".to_owned(),
            seed: SimSeed(0x2a),
            max_steps: 100,
            writers: 1,
            failing_step: None,
            fault_schedule: FaultSchedule::empty(SimSeed(0x2a)),
        };

        assert_eq!(
            replay.replay_command(),
            "cargo run -p loonfs-sim-scenarios -- replay --seed-file seeds/regressions/scenario-000000000000002a.json"
        );
    }
}
