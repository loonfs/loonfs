//! [`ManifestTailMerge`]: merges a paged scan of manifest rows with the rows
//! that the WAL tail contains in the same range, in row-key order.

use crate::error::Result;
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
    /// Starts a merge of the manifest's rows with `tail`, whose rows are in
    /// row-key order.
    pub(super) fn new(tail: Vec<(String, T)>, ties: TieOrder) -> Self {
        Self {
            position: ScanPosition::Start,
            manifest: VecDeque::new(),
            tail: VecDeque::from(tail),
            ties,
        }
    }

    /// Returns the next row of either side in row-key order, or `None` when
    /// both sides are spent.
    ///
    /// When no manifest row is buffered, the merge first reads the next
    /// manifest page with `refill`, which takes the row key that the page
    /// starts after, or `None` for the first page.
    pub(super) async fn next<F, Fut>(&mut self, mut refill: F) -> Result<Option<(String, T)>>
    where
        F: FnMut(Option<String>) -> Fut,
        Fut: Future<Output = Result<ManifestPage<T>>>,
    {
        loop {
            if self.manifest.is_empty() && self.position != ScanPosition::Exhausted {
                let after = match &self.position {
                    ScanPosition::After(row_key) => Some(row_key.clone()),
                    ScanPosition::Start | ScanPosition::Exhausted => None,
                };
                let page = refill(after).await?;
                self.position = match page.rows.last() {
                    Some((row_key, _)) if page.more => ScanPosition::After(row_key.clone()),
                    _ => ScanPosition::Exhausted,
                };
                self.manifest.extend(page.rows);
                continue;
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
}

/// Where a paged scan of manifest rows resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanPosition {
    /// No page read yet.
    Start,
    /// Resumes after this row key.
    After(String),
    /// No rows remain.
    Exhausted,
}
