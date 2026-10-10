//! Bounded approximate membership for byte strings.

use crate::limits::MAX_BLOOM_FILTER_BYTES;
use xxhash_rust::xxh64::xxh64;

pub(crate) struct BloomFilter {
    levels: Vec<BloomLevel>,
    expected_entries: u64,
    entries: u64,
    remaining_bytes: usize,
}

struct BloomLevel {
    bits: Vec<u8>,
    seed: u64,
}

impl BloomFilter {
    pub(crate) fn new(expected_entries: u64, seed: u64) -> Option<Self> {
        Self::with_byte_limit(expected_entries, seed, MAX_BLOOM_FILTER_BYTES)
    }

    pub(crate) fn with_byte_limit(
        expected_entries: u64,
        seed: u64,
        byte_limit: usize,
    ) -> Option<Self> {
        let expected_entries = expected_entries.max(1);
        let level = BloomLevel::new(expected_entries, seed, byte_limit)?;
        Some(Self {
            remaining_bytes: byte_limit - level.bits.len(),
            levels: vec![level],
            expected_entries,
            entries: 0,
        })
    }

    pub(crate) fn insert(&mut self, bytes: &[u8]) -> Option<()> {
        if self.entries == self.expected_entries {
            let expected_entries = self.expected_entries.checked_mul(2)?;
            let seed = xxh64(
                &(self.levels.len() as u64).to_le_bytes(),
                self.levels[0].seed,
            );
            let level = BloomLevel::new(expected_entries, seed, self.remaining_bytes)?;
            self.remaining_bytes -= level.bits.len();
            self.levels.push(level);
            self.expected_entries = expected_entries;
            self.entries = 0;
        }
        self.levels
            .last_mut()
            .expect("a filter should have at least one level")
            .insert(bytes);
        self.entries += 1;
        Some(())
    }

    pub(crate) fn may_contain(&self, bytes: &[u8]) -> bool {
        self.levels.iter().any(|level| level.may_contain(bytes))
    }
}

impl BloomLevel {
    fn new(expected_entries: u64, seed: u64, byte_limit: usize) -> Option<Self> {
        let bytes = expected_entries.checked_mul(10)?.div_ceil(8).max(1);
        if bytes > byte_limit as u64 {
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

    fn insert(&mut self, bytes: &[u8]) {
        for position in self.positions(bytes) {
            self.bits[position / 8] |= 1 << (position % 8);
        }
    }

    fn may_contain(&self, bytes: &[u8]) -> bool {
        self.positions(bytes)
            .into_iter()
            .all(|position| self.bits[position / 8] & (1 << (position % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_preserves_every_inserted_key() {
        for expected_entries in [0, 2_048] {
            let mut filter = BloomFilter::new(expected_entries, 42).expect("filter");
            for key in 0..expected_entries.max(1) {
                filter.insert(&key.to_le_bytes()).expect("insert");
            }
            assert_eq!(filter.levels.len(), 1);
            let entries = expected_entries.max(1) * 8;
            for key in expected_entries.max(1)..entries {
                filter.insert(&key.to_le_bytes()).expect("insert");
            }
            assert_eq!(filter.levels.len(), 4);
            for key in 0..entries {
                assert!(filter.may_contain(&key.to_le_bytes()), "key {key}");
            }
        }
    }

    #[test]
    fn the_byte_limit_counts_all_levels_and_refuses_growth_without_losing_keys() {
        let mut filter = BloomFilter::with_byte_limit(2, 42, 30).expect("filter");
        for key in 0_u64..14 {
            filter.insert(&key.to_le_bytes()).expect("insert");
        }
        assert!(filter.insert(&14_u64.to_le_bytes()).is_none());
        assert!(filter.insert(&15_u64.to_le_bytes()).is_none());
        assert_eq!(filter.levels.len(), 3);
        assert_eq!(filter.remaining_bytes, 12);
        for key in 0_u64..14 {
            assert!(filter.may_contain(&key.to_le_bytes()), "key {key}");
        }
        assert!(BloomFilter::new(u64::MAX, 42).is_none());
    }
}
