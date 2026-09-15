//! Simulation scenario configuration.

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
