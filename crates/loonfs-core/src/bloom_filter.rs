//! Bounded approximate membership for byte strings.

use crate::limits::MAX_BLOOM_FILTER_BYTES;
use std::f64::consts::LN_2;
use xxhash_rust::xxh64::xxh64;

/// Bits that a level has for each expected entry when its byte limit allows.
const BITS_PER_ENTRY: u64 = 10;

/// Most positions that a level sets for one entry, which is the count that
/// ten bits per entry calls for.
const MAX_HASH_POSITIONS: u8 = 7;

pub(crate) struct BloomFilter {
    levels: Vec<BloomLevel>,
    expected_entries: u64,
    entries: u64,
    remaining_bytes: usize,
}

struct BloomLevel {
    bits: Vec<u8>,
    seed: u64,
    /// How many positions an insert sets and a lookup tests.
    hash_positions: u64,
}

impl BloomFilter {
    /// Returns a filter for `expected_entries` whose levels take at most
    /// `MAX_BLOOM_FILTER_BYTES`.
    pub(crate) fn new(expected_entries: u64, seed: u64) -> Self {
        Self::with_max_bytes(expected_entries, seed, MAX_BLOOM_FILTER_BYTES)
    }

    /// Returns a filter for `expected_entries` whose levels take at most
    /// `max_bytes`.
    ///
    /// The first level has ten bits for each expected entry, or all of
    /// `max_bytes` when that's smaller (see `BloomLevel::new`). A smaller
    /// first level reports more false positives, and it leaves no bytes for
    /// `insert` to add a level.
    pub(crate) fn with_max_bytes(expected_entries: u64, seed: u64, max_bytes: usize) -> Self {
        let expected_entries = expected_entries.max(1);
        let level = BloomLevel::new(expected_entries, seed, max_bytes);
        Self {
            remaining_bytes: max_bytes - level.bits.len(),
            levels: vec![level],
            expected_entries,
            entries: 0,
        }
    }

    /// Whether `max_bytes` fits a level with ten bits for each of
    /// `expected_entries`, which is when `with_max_bytes` doesn't shrink the
    /// first level.
    pub(crate) fn fits_at_full_size(expected_entries: u64, max_bytes: usize) -> bool {
        full_level_bytes(expected_entries.max(1)).is_some_and(|bytes| bytes <= max_bytes)
    }

    /// Inserts `bytes`, or returns `None` when the filter has its expected
    /// entries and its remaining bytes don't fit a level with ten bits for
    /// each of twice as many.
    pub(crate) fn insert(&mut self, bytes: &[u8]) -> Option<()> {
        if self.entries == self.expected_entries {
            let expected_entries = self.expected_entries.checked_mul(2)?;
            if !Self::fits_at_full_size(expected_entries, self.remaining_bytes) {
                return None;
            }
            let seed = xxh64(
                &(self.levels.len() as u64).to_le_bytes(),
                self.levels[0].seed,
            );
            let level = BloomLevel::new(expected_entries, seed, self.remaining_bytes);
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

/// Bytes of a level with ten bits for each of `expected_entries`, rounded up
/// to whole bytes, or `None` when that doesn't fit in `usize`.
fn full_level_bytes(expected_entries: u64) -> Option<usize> {
    let bytes = expected_entries
        .checked_mul(BITS_PER_ENTRY)?
        .div_ceil(8)
        .max(1);
    usize::try_from(bytes).ok()
}

impl BloomLevel {
    /// Returns a level for `expected_entries` that takes at most `max_bytes`.
    ///
    /// The level has ten bits for each expected entry, rounded up to whole
    /// bytes, or `max_bytes` when that's smaller. Each entry sets
    /// `clamp(round(bits_per_entry * ln 2), 1, 7)` positions, the whole count
    /// nearest the one with the fewest false positives at the level's bits per
    /// entry. That count is 7 at ten bits per entry, and 2 for 200 million
    /// entries in 64 MiB (about 2.7 bits per entry).
    fn new(expected_entries: u64, seed: u64, max_bytes: usize) -> Self {
        assert!(
            max_bytes > 0,
            "a Bloom filter level should have a positive byte limit"
        );
        let bytes =
            full_level_bytes(expected_entries).map_or(max_bytes, |bytes| bytes.min(max_bytes));
        let bits_per_entry = bytes as f64 * 8.0 / expected_entries as f64;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the clamp keeps the count from 1 to 7, and no `try_from` exists for a float"
        )]
        let hash_positions = (bits_per_entry * LN_2)
            .round()
            .clamp(1.0, f64::from(MAX_HASH_POSITIONS)) as u64;
        Self {
            bits: vec![0; bytes],
            seed,
            hash_positions,
        }
    }

    fn positions(&self, bytes: &[u8]) -> impl Iterator<Item = usize> {
        let first = xxh64(bytes, self.seed);
        let step = xxh64(bytes, self.seed.wrapping_add(1));
        let bit_len = self.bits.len() as u64 * 8;
        (0..self.hash_positions).map(move |index| {
            loonfs_types::usize_from_u64(first.wrapping_add(index.wrapping_mul(step)) % bit_len)
        })
    }

    fn insert(&mut self, bytes: &[u8]) {
        for position in self.positions(bytes) {
            self.bits[position / 8] |= 1 << (position % 8);
        }
    }

    fn may_contain(&self, bytes: &[u8]) -> bool {
        self.positions(bytes)
            .all(|position| self.bits[position / 8] & (1 << (position % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_preserves_every_inserted_key() {
        for expected_entries in [0, 2_048] {
            let mut filter = BloomFilter::new(expected_entries, 42);
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
        let mut filter = BloomFilter::with_max_bytes(2, 42, 30);
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
    }

    /// Checks that a level has ten bits per entry and seven hash positions up
    /// to the byte limit, the same bytes as when a filter over the limit
    /// wasn't built, and that past the limit it takes the whole limit with
    /// fewer hash positions.
    #[test]
    fn a_first_level_takes_ten_bits_per_entry_up_to_the_byte_limit_and_the_whole_limit_past_it() {
        // The most entries that fit in 64 MiB at ten bits each.
        let largest_full_size = 53_687_091;
        for (expected_entries, bytes, hash_positions) in [
            (0, 2, 7),
            (1, 2, 7),
            (1_000, 1_250, 7),
            (largest_full_size, MAX_BLOOM_FILTER_BYTES, 7),
            (largest_full_size + 1, MAX_BLOOM_FILTER_BYTES, 7),
            (100_000_000, MAX_BLOOM_FILTER_BYTES, 4),
            (200_000_000, MAX_BLOOM_FILTER_BYTES, 2),
            (u64::MAX, MAX_BLOOM_FILTER_BYTES, 1),
        ] {
            let filter = BloomFilter::new(expected_entries, 42);
            assert_eq!(filter.levels.len(), 1);
            let level = &filter.levels[0];
            assert_eq!(level.bits.len(), bytes, "{expected_entries} entries");
            assert_eq!(
                level.hash_positions, hash_positions,
                "{expected_entries} entries"
            );
            assert_eq!(filter.remaining_bytes, MAX_BLOOM_FILTER_BYTES - bytes);
            assert_eq!(
                BloomFilter::fits_at_full_size(expected_entries, MAX_BLOOM_FILTER_BYTES),
                expected_entries <= largest_full_size,
                "{expected_entries} entries"
            );
        }
    }

    /// Checks that a filter over its byte limit matches every inserted key,
    /// rejects a share of the keys it never saw, and can't grow.
    #[test]
    fn a_filter_over_its_byte_limit_keeps_every_key_and_still_rejects_others() {
        let mut capped = BloomFilter::new(200_000_000, 42);
        for key in 0_u64..10_000 {
            capped.insert(&key.to_le_bytes()).expect("insert");
        }
        for key in 0_u64..10_000 {
            assert!(capped.may_contain(&key.to_le_bytes()), "key {key}");
        }
        // One bit and one hash position per entry.
        let entries = 8_000_u64;
        let mut filter = BloomFilter::with_max_bytes(entries, 42, 1_000);
        assert_eq!(filter.levels[0].hash_positions, 1);
        for key in 0..entries {
            filter.insert(&key.to_le_bytes()).expect("insert");
        }
        assert!(filter.insert(&entries.to_le_bytes()).is_none());
        for key in 0..entries {
            assert!(filter.may_contain(&key.to_le_bytes()), "key {key}");
        }
        let false_positives = (entries..2 * entries)
            .filter(|key| filter.may_contain(&key.to_le_bytes()))
            .count() as u64;
        assert!(
            false_positives < entries * 9 / 10,
            "{false_positives} of {entries} fresh keys matched"
        );
    }
}
