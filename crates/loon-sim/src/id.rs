use crate::rng::{DeterministicRng, SimRng, SimSeed};

#[derive(Debug, Clone)]
pub struct SimIdGenerator {
    rng: DeterministicRng,
}

impl SimIdGenerator {
    pub fn new(seed: SimSeed) -> Self {
        Self {
            rng: DeterministicRng::new(seed),
        }
    }

    pub fn from_rng(rng: DeterministicRng) -> Self {
        Self { rng }
    }

    pub fn next_id(&mut self, prefix: &'static str) -> String {
        format!(
            "{}_{:016x}{:016x}",
            prefix,
            self.rng.next_u64(),
            self.rng.next_u64()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_replayable() {
        let mut left = SimIdGenerator::new(SimSeed(7));
        let mut right = SimIdGenerator::new(SimSeed(7));

        assert_eq!(left.next_id("c"), right.next_id("c"));
    }
}
