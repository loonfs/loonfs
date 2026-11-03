//! The rows of one grep step's output, which it collects by family and
//! writes in key order.

use crate::codec::{ContentValue, Gram, IndexRow};
use std::collections::BTreeMap;

const GRAM_POSTING_BATCH_TARGET: usize = 256;

/// The rows of one grep step's output, by family, before they're written in
/// key order.
#[derive(Default)]
pub(super) struct IndexRows {
    /// `content_revisions` rows by row key.
    pub(super) content_revisions: BTreeMap<String, IndexRow>,
    /// The ordinals, in the output run's value table, of the content values
    /// whose bytes contain each gram, in any order.
    pub(super) postings: BTreeMap<Gram, Vec<u64>>,
    /// Each examined value's eligibility.
    pub(super) texts: BTreeMap<ContentValue, bool>,
}

impl IndexRows {
    pub(super) fn add_content_revision(&mut self, row: IndexRow) {
        self.content_revisions.insert(row.row_key(), row);
    }

    /// Returns the rows in key order: the `content_revisions` family, the
    /// postings, and then the `texts` family, as their key prefixes sort.
    /// The run's filter and table rows aren't included.
    pub(super) fn into_rows(self) -> Vec<IndexRow> {
        let mut rows: Vec<IndexRow> = self.content_revisions.into_values().collect();
        for (gram, mut ordinals) in self.postings {
            ordinals.sort_unstable();
            ordinals.dedup();
            for batch in ordinals.chunks(GRAM_POSTING_BATCH_TARGET) {
                rows.push(
                    IndexRow::gram_postings(gram, batch)
                        .expect("a non-empty gram postings batch should encode"),
                );
            }
        }
        rows.extend(
            self.texts
                .iter()
                .map(|(value, eligible)| IndexRow::text(value, *eligible)),
        );
        rows
    }
}
