//! A set of byte ranges, which records the bytes of a partial cache file
//! that are present or being fetched.

use std::collections::BTreeMap;
use std::ops::Range;

/// A set of disjoint, non-adjacent half-open byte ranges.
///
/// Its size is bounded by the fetches of one file, because each insert merges
/// with the ranges it touches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RangeSet {
    /// Each range's end, keyed by its start.
    ranges: BTreeMap<u64, u64>,
}

impl RangeSet {
    /// Adds a range, merging it with the ranges it overlaps or touches.
    pub(crate) fn insert(&mut self, range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        let mut start = range.start;
        let mut end = range.end;
        if let Some((&before_start, &before_end)) = self.ranges.range(..=start).next_back() {
            if before_end >= start {
                start = before_start;
                end = end.max(before_end);
            }
        }
        let overlapping: Vec<u64> = self
            .ranges
            .range(start..=end)
            .map(|(&overlapping_start, _)| overlapping_start)
            .collect();
        for overlapping_start in overlapping {
            let overlapping_end = self
                .ranges
                .remove(&overlapping_start)
                .expect("an overlapping range should be in the set");
            end = end.max(overlapping_end);
        }
        self.ranges.insert(start, end);
    }

    /// Removes a range.
    pub(crate) fn remove(&mut self, range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        let affected: Vec<(u64, u64)> = self
            .ranges
            .range(..range.end)
            .filter(|(_, &end)| end > range.start)
            .map(|(&start, &end)| (start, end))
            .collect();
        for (start, end) in affected {
            self.ranges.remove(&start);
            if start < range.start {
                self.ranges.insert(start, range.start);
            }
            if end > range.end {
                self.ranges.insert(range.end, end);
            }
        }
    }

    /// Returns whether every byte of `range` is in the set.
    pub(crate) fn contains(&self, range: &Range<u64>) -> bool {
        if range.is_empty() {
            return true;
        }
        self.ranges
            .range(..=range.start)
            .next_back()
            .is_some_and(|(_, &end)| end >= range.end)
    }

    /// Returns the parts of `range` that aren't in the set, in order.
    pub(crate) fn missing(&self, range: Range<u64>) -> Vec<Range<u64>> {
        let mut missing = Vec::new();
        let mut cursor = range.start;
        let first = self
            .ranges
            .range(..=range.start)
            .next_back()
            .map(|(&start, &end)| (start, end));
        let rest = self
            .ranges
            .range(range.start.saturating_add(1)..range.end)
            .map(|(&start, &end)| (start, end));
        for (start, end) in first.into_iter().chain(rest) {
            if end <= cursor {
                continue;
            }
            if start > cursor {
                missing.push(cursor..start.min(range.end));
            }
            cursor = cursor.max(end);
            if cursor >= range.end {
                break;
            }
        }
        if cursor < range.end {
            missing.push(cursor..range.end);
        }
        missing
    }

    /// Returns the end of the range that starts at zero, or zero when the set
    /// has no such range.
    pub(crate) fn prefix_end(&self) -> u64 {
        self.ranges.get(&0).copied().unwrap_or(0)
    }

    /// Returns the number of bytes in the set.
    pub(crate) fn len_bytes(&self) -> u64 {
        self.ranges.iter().map(|(start, end)| end - start).sum()
    }

    /// Returns whether the set is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_merge_and_missing_ranges_fill_the_gaps() {
        let mut set = RangeSet::default();
        set.insert(10..20);
        set.insert(30..40);
        set.insert(20..25);
        assert_eq!(set.missing(0..50), vec![0..10, 25..30, 40..50]);
        assert!(set.contains(&(12..25)));
        assert!(!set.contains(&(12..26)));
        assert_eq!(set.prefix_end(), 0);
        set.insert(0..35);
        assert_eq!(set.prefix_end(), 40);
        assert_eq!(set.len_bytes(), 40);
        set.remove(5..15);
        assert_eq!(set.missing(0..40), vec![5..15]);
        assert_eq!(set.missing(16..18), Vec::<Range<u64>>::new());
        assert!(!set.is_empty());
    }
}
