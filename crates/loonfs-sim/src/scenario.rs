//! Simulation scenario configuration and execution contract.

use crate::failure::SimFailure;
use crate::rng::SimSeed;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimConfig {
    pub seed: SimSeed,
    pub max_steps: usize,
    pub writers: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: SimSeed(0xC0FFEE),
            max_steps: 10_000,
            writers: 3,
        }
    }
}

pub trait SimScenario {
    fn name(&self) -> &'static str;
    fn run(&self, config: SimConfig) -> Result<(), SimFailure>;
}
