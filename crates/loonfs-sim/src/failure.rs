use crate::replay::ReplaySeed;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimFailure {
    #[error("scenario failed: {message}")]
    Scenario {
        message: String,
        replay: Box<ReplaySeed>,
        trace_path: Option<PathBuf>,
    },
    #[error("model mismatch at step {step}: {message}")]
    ModelMismatch {
        step: u64,
        message: String,
        replay: Box<ReplaySeed>,
    },
    #[error("unexpected engine error at step {step}: {source}")]
    Engine {
        step: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        replay: Box<ReplaySeed>,
    },
}

impl SimFailure {
    pub fn replay(&self) -> &ReplaySeed {
        match self {
            Self::Scenario { replay, .. }
            | Self::ModelMismatch { replay, .. }
            | Self::Engine { replay, .. } => replay,
        }
    }

    pub fn format_summary(&self) -> String {
        let replay = self.replay();
        format!(
            "Simulation failed\n\nscenario: {}\nseed:     0x{:016x}\n\nRe-run:\n{}",
            replay.scenario,
            replay.seed.0,
            replay.replay_command()
        )
    }
}
