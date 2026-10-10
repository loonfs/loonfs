//! Bounded approximate membership for byte strings.

use crate::limits::MAX_BLOOM_FILTER_BYTES;
use xxhash_rust::xxh64::xxh64;

pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    seed: u64,
}

impl BloomFilter {
    pub(crate) fn new(expected_entries: u64, seed: u64) -> Option<Self> {
        let bytes = expected_entries.checked_mul(10)?.div_ceil(8).max(1);
        if bytes > MAX_BLOOM_FILTER_BYTES as u64 {
            return None;
        }
        Some(Self {
            bits: vec![0; bytes as usize],
            seed,
        })
    }

    fn positions(&self, bytes: &[u8]) -> [usize; 7] {
        let first = xxh64(bytes, self.seed);
        let step = xxh64(bytes, self.seed.wrapping_add(1));
        std::array::from_fn(|index| {
            (first.wrapping_add((index as u64).wrapping_mul(step)) % (self.bits.len() as u64 * 8))
                as usize
        })
    }

    pub(crate) fn insert(&mut self, bytes: &[u8]) {
        for position in self.positions(bytes) {
            self.bits[position / 8] |= 1 << (position % 8);
        }
    }

    pub(crate) fn may_contain(&self, bytes: &[u8]) -> bool {
        self.positions(bytes)
            .into_iter()
            .all(|position| self.bits[position / 8] & (1 << (position % 8)) != 0)
    }
}
