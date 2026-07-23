//! Seeded deterministic randomness for replayable simulations.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimSeed(pub u64);

pub trait SimRng {
    fn next_u64(&mut self) -> u64;
    fn fill_bytes(&mut self, dest: &mut [u8]);
    fn fork(&mut self, label: &'static str) -> DeterministicRng;
}

#[derive(Debug, Clone)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub fn new(seed: SimSeed) -> Self {
        Self { state: seed.0 }
    }
}

impl SimRng for DeterministicRng {
    fn next_u64(&mut self) -> u64 {
        // SplitMix64 is small, stable across platforms, and sufficient for simulation choices.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    fn fork(&mut self, label: &'static str) -> DeterministicRng {
        let mut seed = self.next_u64();
        for byte in label.as_bytes() {
            seed ^= u64::from(*byte);
            seed = seed.wrapping_mul(0x100_0000_01B3);
        }
        DeterministicRng::new(SimSeed(seed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_rng_repeats_for_same_seed() {
        let mut left = DeterministicRng::new(SimSeed(42));
        let mut right = DeterministicRng::new(SimSeed(42));

        for _ in 0..16 {
            assert_eq!(left.next_u64(), right.next_u64());
        }
    }

    #[test]
    fn forked_rng_depends_on_label() {
        let mut root = DeterministicRng::new(SimSeed(42));
        let mut left = root.fork("left");
        let mut root = DeterministicRng::new(SimSeed(42));
        let mut right = root.fork("right");

        assert_ne!(left.next_u64(), right.next_u64());
    }
}
