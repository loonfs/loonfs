//! [`Recency`]: the least-recently-used order the runtime's stamped caches
//! evict by.

use std::collections::VecDeque;

/// Shortest queue worth compacting, so a nearly empty cache does not compact
/// on every touch.
const MIN_COMPACTION_POSITIONS: usize = 16;

/// A recency queue with lazy deletion.
///
/// A hit appends the key with a fresh stamp in constant time and leaves its
/// old position behind as a ghost. The caller stores the returned stamp on
/// its own entry and answers `is_live` from it, so this holds no second copy
/// of the entry table and never duplicates whatever hangs off a key.
/// Eviction skips ghosts as it pops them, and ghosts are dropped in bulk
/// once they outnumber the live entries they shadow — amortized constant,
/// like a vector reallocation. Nothing is scheduled.
#[derive(Debug)]
pub struct Recency<K> {
    order: VecDeque<(K, u64)>,
    counter: u64,
}

impl<K> Default for Recency<K> {
    fn default() -> Self {
        Self {
            order: VecDeque::new(),
            counter: 0,
        }
    }
}

impl<K: Clone> Recency<K> {
    /// Records an access and returns the stamp the caller must store on its
    /// entry before calling anything else here: until the entry carries it,
    /// the position just appended reads as a ghost.
    ///
    /// Stamps start at 1, so an entry that has never been touched can carry
    /// 0 and read as a ghost.
    pub fn touch(&mut self, key: &K) -> u64 {
        self.counter = self.counter.saturating_add(1);
        self.order.push_back((key.clone(), self.counter));
        self.counter
    }

    /// Removes and returns the least recently used live key.
    ///
    /// `None` means the queue holds no live position at all, so an eviction
    /// loop must stop: popping again cannot make progress.
    pub fn pop_oldest(&mut self, mut is_live: impl FnMut(&K, u64) -> bool) -> Option<K> {
        while let Some((key, stamp)) = self.order.pop_front() {
            if is_live(&key, stamp) {
                return Some(key);
            }
        }
        None
    }

    /// Drops ghost positions in place once they outnumber the `live_len`
    /// entries they shadow. Cheap enough to call on every touch.
    pub fn compact(&mut self, live_len: usize, mut is_live: impl FnMut(&K, u64) -> bool) {
        let positions_before_compacting = live_len.saturating_mul(2).max(MIN_COMPACTION_POSITIONS);
        if self.order.len() <= positions_before_compacting {
            return;
        }
        self.order.retain(|(key, stamp)| is_live(key, *stamp));
    }

    /// Queue positions held, live and ghost alike.
    pub fn positions(&self) -> usize {
        self.order.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Recency;
    use std::collections::HashMap;

    /// A stand-in for a cache's entry table: keys mapped to the stamp the
    /// queue last handed out for them.
    #[derive(Default)]
    struct Entries(HashMap<&'static str, u64>);

    impl Entries {
        fn is_live(&self, key: &&'static str, stamp: u64) -> bool {
            self.0.get(key).is_some_and(|live| *live == stamp)
        }
    }

    fn touch(recency: &mut Recency<&'static str>, entries: &mut Entries, key: &'static str) {
        let stamp = recency.touch(&key);
        entries.0.insert(key, stamp);
        recency.compact(entries.0.len(), |key, stamp| entries.is_live(key, stamp));
    }

    #[test]
    fn eviction_returns_keys_least_recently_touched_first() {
        let mut recency = Recency::default();
        let mut entries = Entries::default();
        for key in ["a", "b", "c"] {
            touch(&mut recency, &mut entries, key);
        }
        touch(&mut recency, &mut entries, "a");

        let evicted = recency
            .pop_oldest(|key, stamp| entries.is_live(key, stamp))
            .expect("a live position");
        assert_eq!(evicted, "b");
    }

    #[test]
    fn eviction_skips_the_ghosts_a_re_touch_left_behind() {
        let mut recency = Recency::default();
        let mut entries = Entries::default();
        touch(&mut recency, &mut entries, "a");
        touch(&mut recency, &mut entries, "a");
        touch(&mut recency, &mut entries, "b");

        let evicted = recency
            .pop_oldest(|key, stamp| entries.is_live(key, stamp))
            .expect("a live position");
        assert_eq!(evicted, "a", "the stale position for a must not evict b");
    }

    #[test]
    fn eviction_reports_a_queue_of_ghosts_as_empty() {
        let mut recency = Recency::default();
        let mut entries = Entries::default();
        touch(&mut recency, &mut entries, "a");
        entries.0.clear();

        assert!(recency
            .pop_oldest(|key, stamp| entries.is_live(key, stamp))
            .is_none());
    }

    #[test]
    fn repeated_hits_on_one_key_keep_the_queue_bounded() {
        let mut recency = Recency::default();
        let mut entries = Entries::default();
        for key in ["a", "b", "c"] {
            touch(&mut recency, &mut entries, key);
        }
        for _ in 0..10_000 {
            touch(&mut recency, &mut entries, "a");
        }

        assert_eq!(entries.0.len(), 3);
        assert!(
            recency.positions() <= (entries.0.len() * 2).max(16),
            "compaction must bound the queue, positions = {}",
            recency.positions()
        );
    }

    #[test]
    fn compaction_holds_off_until_ghosts_outnumber_live_entries() {
        let mut recency = Recency::default();
        let mut entries = Entries::default();
        touch(&mut recency, &mut entries, "a");
        assert_eq!(recency.positions(), 1);

        let mut previous = recency.positions();
        let mut compacted = false;
        for _ in 0..1_024 {
            touch(&mut recency, &mut entries, "a");
            let positions = recency.positions();
            if positions < previous {
                assert_eq!(positions, 1, "crossing the floor drops every ghost");
                compacted = true;
                break;
            }
            assert_eq!(positions, previous + 1, "a ghost stays until compaction");
            previous = positions;
        }
        assert!(compacted, "the queue must compact within the bound");
    }
}
