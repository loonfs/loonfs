//! [`ManifestTailMerge`]: merges a paged scan of manifest rows with the rows
//! that the WAL tail contains in the same range, in row-key order.

use crate::error::Result;
use loonfs_types::format::manifest::lookup_keys;
use std::collections::VecDeque;
use std::future::Future;

/// Which side a merge takes first when both contain a row with the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TieOrder {
    TailFirst,
    ManifestFirst,
}

/// One page of a manifest scan, in row-key order.
pub(super) struct ManifestPage<T> {
    pub(super) rows: Vec<(String, T)>,
    /// Whether the scanned range can contain rows after this page.
    pub(super) more: bool,
}

/// A merge of manifest pages, read on demand, with a sorted set of tail
/// rows.
///
/// The merge buffers one manifest page at a time, so it keeps at most one
/// page of manifest rows beside the tail rows.
pub(super) struct ManifestTailMerge<T> {
    position: ScanPosition,
    manifest: VecDeque<(String, T)>,
    tail: VecDeque<(String, T)>,
    ties: TieOrder,
}

impl<T> ManifestTailMerge<T> {
    /// Starts a merge of the manifest's rows from `lower_bound` with `tail`,
    /// whose rows are in row-key order and at or after `lower_bound`.
    pub(super) fn new(lower_bound: String, tail: Vec<(String, T)>, ties: TieOrder) -> Self {
        Self {
            position: ScanPosition::From(lower_bound),
            manifest: VecDeque::new(),
            tail: VecDeque::from(tail),
            ties,
        }
    }

    /// Returns the next row of either side in row-key order, or `None` when
    /// both sides are spent.
    ///
    /// When no manifest row is buffered, the merge first reads the next
    /// manifest page with `refill`, which takes the key that the page starts
    /// at.
    pub(super) async fn next<F, Fut>(&mut self, mut refill: F) -> Result<Option<(String, T)>>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<ManifestPage<T>>>,
    {
        loop {
            if self.manifest.is_empty() {
                if let ScanPosition::From(lower_bound) =
                    std::mem::replace(&mut self.position, ScanPosition::Exhausted)
                {
                    let page = refill(lower_bound).await?;
                    if let Some((row_key, _)) = page.rows.last().filter(|_| page.more) {
                        self.position = ScanPosition::From(lookup_keys::after_row_key(row_key));
                    }
                    self.manifest.extend(page.rows);
                    continue;
                }
            }
            let take_tail = match (self.manifest.front(), self.tail.front()) {
                (Some((manifest_key, _)), Some((tail_key, _))) => match self.ties {
                    TieOrder::TailFirst => tail_key <= manifest_key,
                    TieOrder::ManifestFirst => tail_key < manifest_key,
                },
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => return Ok(None),
            };
            return Ok(if take_tail {
                self.tail.pop_front()
            } else {
                self.manifest.pop_front()
            });
        }
    }

    /// Skips every row before `lower_bound` on both sides. When that empties
    /// the buffered manifest page, the next page starts at `lower_bound`, so
    /// the rows in between are never read.
    pub(super) fn seek(&mut self, lower_bound: &str) {
        while self
            .tail
            .front()
            .is_some_and(|(row_key, _)| row_key.as_str() < lower_bound)
        {
            self.tail.pop_front();
        }
        while self
            .manifest
            .front()
            .is_some_and(|(row_key, _)| row_key.as_str() < lower_bound)
        {
            self.manifest.pop_front();
        }
        if let ScanPosition::From(next) = &mut self.position {
            if self.manifest.is_empty() && next.as_str() < lower_bound {
                lower_bound.clone_into(next);
            }
        }
    }
}

/// Where a paged scan of manifest rows resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanPosition {
    /// The next page starts at this key.
    From(String),
    /// No rows remain.
    Exhausted,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that a seek skips the buffered rows before its bound on both
    /// sides, and starts the next manifest page at the bound once the
    /// buffered page is spent.
    #[tokio::test]
    async fn a_seek_skips_both_sides_and_starts_the_next_page_at_its_bound() {
        let manifest: Vec<String> = ["a1", "a2", "b1", "c1", "c2", "c3", "d1"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let starts = std::cell::RefCell::new(Vec::new());
        let refill = |lower_bound: String| {
            starts.borrow_mut().push(lower_bound.clone());
            let rows: Vec<(String, ())> = manifest
                .iter()
                .filter(|key| key.as_str() >= lower_bound.as_str())
                .take(2)
                .map(|key| (key.clone(), ()))
                .collect();
            async move {
                Ok(ManifestPage {
                    more: !rows.is_empty(),
                    rows,
                })
            }
        };
        let tail = vec![("a3".to_owned(), ()), ("c4".to_owned(), ())];
        let mut merge = ManifestTailMerge::new("a".to_owned(), tail, TieOrder::ManifestFirst);
        let mut read = Vec::new();
        while let Some((row_key, ())) = merge.next(refill).await.expect("next row") {
            // Read the first row of each letter, then skip to the next one.
            let letter = row_key[..1].to_owned();
            read.push(row_key);
            merge.seek(&format!("{letter}\u{ff}"));
        }
        assert_eq!(read, ["a1", "b1", "c1", "d1"]);
        // The pages after a spent page start at the seek's bound, so `c2` and
        // `c3` are never read.
        assert_eq!(*starts.borrow(), ["a", "a\u{ff}", "c\u{ff}", "d\u{ff}"]);
    }
}
