//! The value table of a build step's run: the content values that the step
//! has a `texts` row for, which the run's postings refer to by ordinal. A
//! reorganization reads its runs' tables a block at a time instead (see
//! `worker::tables`).

use crate::codec::{ContentValue, IndexRow};
use loonfs_types::u64_from_usize;
use std::collections::BTreeSet;

/// The content values of one run, in content order, where a value's ordinal
/// is its position.
#[derive(Debug)]
pub(crate) struct ValueTable {
    values: Vec<ContentValue>,
}

impl ValueTable {
    /// The table of `values`.
    pub(crate) fn new(values: BTreeSet<ContentValue>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Returns the ordinal of `value`, or `None` when the table doesn't list
    /// it.
    pub(crate) fn ordinal(&self, value: &ContentValue) -> Option<u64> {
        self.values.binary_search(value).ok().map(u64_from_usize)
    }

    /// Returns the table's rows, in key order.
    pub(crate) fn rows(&self) -> impl Iterator<Item = IndexRow> + '_ {
        self.values
            .iter()
            .enumerate()
            .map(|(position, value)| IndexRow::value(u64_from_usize(position), value))
    }
}
