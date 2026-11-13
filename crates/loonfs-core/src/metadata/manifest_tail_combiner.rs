//! [`ManifestTailCombiner`]: combines a paged scan of manifest rows with the rows
//! that the WAL tail contains in the same range, in row-key order.

use super::tail_rows::{TailFamily, TailRows};
use crate::error::Result;
use loonfs_types::format::manifest::lookup_keys;
use std::collections::VecDeque;
use std::future::Future;

/// Which side a combiner takes first when both contain a row with the same key.
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

/// A combination of manifest pages, read on demand, with a merge of the tail's
/// rows.
///
/// A row that both sides hold is one row, so the combiner returns each row key
/// once.
///
/// The combiner buffers one manifest page at a time, so it keeps at most one
/// page of manifest rows.
pub(super) struct ManifestTailCombiner<'a, T: TailFamily> {
    position: ScanPosition,
    manifest: VecDeque<(String, T)>,
    tail: TailRows<'a, T>,
    ties: TieOrder,
    last_row_key: Option<String>,
}

impl<'a, T: TailFamily> ManifestTailCombiner<'a, T> {
    /// Starts a combination of the manifest's rows from `lower_bound` with `tail`,
    /// whose rows are at or after `lower_bound`.
    pub(super) fn new(lower_bound: String, tail: TailRows<'a, T>, ties: TieOrder) -> Self {
        Self {
            position: ScanPosition::From(lower_bound),
            manifest: VecDeque::new(),
            tail,
            ties,
            last_row_key: None,
        }
    }

    /// Returns the next row of either side in row-key order, or `None` when
    /// both sides are spent.
    ///
    /// When no manifest row is buffered, the combiner first reads the next
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
            let take_tail = match (self.manifest.front(), self.tail.peek_key()) {
                (Some((manifest_key, _)), Some(tail_key)) => match self.ties {
                    TieOrder::TailFirst => tail_key <= manifest_key.as_str(),
                    TieOrder::ManifestFirst => tail_key < manifest_key.as_str(),
                },
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => return Ok(None),
            };
            let (row_key, row) = if take_tail {
                self.tail.next()
            } else {
                self.manifest.pop_front()
            }
            .expect("the chosen side holds a row");
            if self.last_row_key.as_ref() == Some(&row_key) {
                continue;
            }
            self.last_row_key = Some(row_key.clone());
            return Ok(Some((row_key, row)));
        }
    }

    /// Skips every row before `lower_bound` on both sides. When that empties
    /// the buffered manifest page, the next page starts at `lower_bound`, so
    /// the rows in between are never read.
    pub(super) fn seek(&mut self, lower_bound: &str) {
        self.tail.seek(lower_bound);
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
    use super::super::tail_rows::tests::{inode, state};
    use super::*;
    use crate::metadata::InodeRecord;
    use loonfs_types::{CommitSeq, InodeId};

    fn key(inode_id: u128) -> String {
        lookup_keys::inode_key(InodeId(inode_id))
    }

    /// Returns a refill that reads pages of at most `page_rows` of the inodes
    /// `ids`, and records the key that each page starts at.
    fn refill_of<'a>(
        ids: &'a [u128],
        page_rows: usize,
        starts: &'a std::cell::RefCell<Vec<String>>,
    ) -> impl FnMut(String) -> std::future::Ready<Result<ManifestPage<InodeRecord>>> + 'a {
        move |lower_bound: String| {
            starts.borrow_mut().push(lower_bound.clone());
            let rows: Vec<(String, InodeRecord)> = ids
                .iter()
                .map(|id| (key(*id), inode(*id, 1)))
                .filter(|(row_key, _)| *row_key >= lower_bound)
                .take(page_rows)
                .collect();
            std::future::ready(Ok(ManifestPage {
                more: !rows.is_empty(),
                rows,
            }))
        }
    }

    /// Checks that a seek skips the buffered rows before its bound on both
    /// sides, and starts the next manifest page at the bound once the
    /// buffered page is spent.
    #[tokio::test]
    async fn a_seek_skips_both_sides_and_starts_the_next_page_at_its_bound() {
        // An inode's group is its ID without the low four bits.
        let manifest = [0x11, 0x12, 0x21, 0x31, 0x32, 0x33, 0x41];
        let starts = std::cell::RefCell::new(Vec::new());
        let mut refill = refill_of(&manifest, 2, &starts);
        let tail_state = state(&[inode(0x34, 2), inode(0x13, 2)]);
        let tail = TailRows::new([(&tail_state, None)], &key(0x10), None, CommitSeq(2));
        let mut combiner = ManifestTailCombiner::new(key(0x10), tail, TieOrder::ManifestFirst);
        let mut read = Vec::new();
        while let Some((_, row)) = combiner.next(&mut refill).await.expect("next row") {
            // Read the first row of each group, then skip to the next one.
            let group = row.inode_id.0 >> 4;
            read.push(row.inode_id.0);
            combiner.seek(&key((group + 1) << 4));
        }
        assert_eq!(read, [0x11, 0x21, 0x31, 0x41]);
        // The pages after a spent page start at the seek's bound, so 0x32 and
        // 0x33 are never read.
        assert_eq!(
            *starts.borrow(),
            [key(0x10), key(0x20), key(0x40), key(0x50)]
        );
    }

    /// Checks that a row both sides hold comes back once.
    #[tokio::test]
    async fn a_row_on_both_sides_comes_back_once() {
        let starts = std::cell::RefCell::new(Vec::new());
        let mut refill = refill_of(&[1, 2, 3], usize::MAX, &starts);
        let tail_state = state(&[inode(2, 1)]);
        let tail = TailRows::new([(&tail_state, None)], &key(1), None, CommitSeq(1));
        let mut combiner = ManifestTailCombiner::new(key(1), tail, TieOrder::TailFirst);
        let mut read = Vec::new();
        while let Some((_, row)) = combiner.next(&mut refill).await.expect("next row") {
            read.push(row.inode_id.0);
        }
        assert_eq!(read, [1, 2, 3]);
    }
}
