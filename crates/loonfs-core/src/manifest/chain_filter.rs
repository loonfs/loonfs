//! Content ids named by a compaction's revision rows.

use crate::limits::MAX_CHAIN_FILTER_BYTES;
use loonfs_types::{ContentId, RunNo};
use xxhash_rust::xxh64::xxh64;

pub(super) struct ChainFilter {
    bits: Vec<u8>,
    seed: u64,
}

impl ChainFilter {
    pub(super) fn new(expected_ids: u64, output_run_no: RunNo) -> Option<Self> {
        let bytes = expected_ids.checked_mul(10)?.div_ceil(8).max(1);
        if bytes > MAX_CHAIN_FILTER_BYTES as u64 {
            return None;
        }
        Some(Self {
            bits: vec![0; bytes as usize],
            seed: output_run_no.0,
        })
    }

    fn positions(&self, content_id: &ContentId) -> [usize; 7] {
        let bytes = content_id.as_str().as_bytes();
        let first = xxh64(bytes, self.seed);
        let step = xxh64(bytes, self.seed.wrapping_add(1));
        std::array::from_fn(|index| {
            (first.wrapping_add((index as u64).wrapping_mul(step)) % (self.bits.len() as u64 * 8))
                as usize
        })
    }

    pub(super) fn insert(&mut self, content_id: &ContentId) {
        for position in self.positions(content_id) {
            self.bits[position / 8] |= 1 << (position % 8);
        }
    }

    pub(super) fn may_contain(&self, content_id: &ContentId) -> bool {
        self.positions(content_id)
            .into_iter()
            .all(|position| self.bits[position / 8] & (1 << (position % 8)) != 0)
    }
}
