//! The value tables that a reorganize step reads. One read of the input
//! runs' tables, in content order, decides the output run's table in the
//! reorganization's first step, or reads it back in a later step, and builds
//! each input run's translation to the output run's ordinals. The read takes
//! a block of each table at a time, so a step keeps only the translations.

use super::pruning::{filter_from_rows, RetainedReferences};
use super::GREP_WORKER_REQUESTS_IN_FLIGHT;
use crate::codec::{lookup, ContentValue, IndexRow};
use crate::segments::{GrepSegments, RunWriter};
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs::engine::{refill_iterators, SegmentRowIterator};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::format::segment_blocks::{
    key_range_may_intersect, string_prefix_upper_bound, SegmentFilter, DEFAULT_TARGET_BLOCK_BYTES,
};
use loonfs_types::{u64_from_usize, RunNo};
use std::collections::{BTreeMap, BTreeSet};

/// The translation entry of an input value that the output run's table
/// doesn't list.
pub(super) const DROPPED: u32 = u32::MAX;

/// One input run's segments, and the output ordinal of each of the run's
/// ordinals, or [`DROPPED`]. An entry takes four bytes, so the
/// translations are what a step keeps of the tables.
pub(super) struct Translation {
    pub(super) segments: Vec<ExtensionSegmentRef>,
    pub(super) ordinals: Vec<u32>,
}

impl Translation {
    /// The output ordinal of `ordinal`, `Some(None)` for a dropped value, or
    /// `None` past the input run's table.
    pub(super) fn output(&self, ordinal: u64) -> Option<Option<u64>> {
        let entry = *self.ordinals.get(usize::try_from(ordinal).ok()?)?;
        Some((entry != DROPPED).then_some(u64::from(entry)))
    }
}

/// Decoded bytes of one value table that a reader keeps, about one data
/// block, so a step that reads many tables at once keeps a block of each.
const MAX_TABLE_READ_DECODED_BYTES: usize = DEFAULT_TARGET_BLOCK_BYTES.get();

/// Reads one run's value table in ordinal order from a starting ordinal, and
/// checks that the ordinals arrive in order.
pub(super) struct TableReader {
    run_no: RunNo,
    rows: SegmentRowIterator<IndexRow, ExtensionSegmentRef, ()>,
    next_ordinal: u64,
    previous: Option<ContentValue>,
}

impl TableReader {
    pub(super) fn new(run_no: RunNo, run: &[ExtensionSegmentRef], ordinal: u64) -> Self {
        let lower = lookup::value_row_key(ordinal);
        // The run's segments whose key ranges reach the value table, in key
        // order.
        let mut segments: Vec<ExtensionSegmentRef> = run
            .iter()
            .filter(|segment| {
                key_range_may_intersect(
                    &segment.min_row_key,
                    &segment.max_row_key,
                    segment.row_count,
                    &lower,
                    None,
                )
            })
            .cloned()
            .collect();
        segments.sort_by(|left, right| left.min_row_key.cmp(&right.min_row_key));
        Self {
            run_no,
            rows: SegmentRowIterator::new((), segments, Some(lower)),
            next_ordinal: ordinal,
            previous: None,
        }
    }

    /// Returns the next ordinal and its value, or `None` after the table's
    /// last value.
    pub(super) async fn next<S: ObjectStore + ?Sized>(
        &mut self,
        segments: &GrepSegments<'_, S>,
    ) -> Result<Option<(u64, ContentValue)>> {
        refill_iterators(
            segments,
            std::slice::from_mut(&mut self.rows),
            MAX_TABLE_READ_DECODED_BYTES,
        )
        .await?;
        if self.rows.head().is_none() {
            return Ok(None);
        }
        // A run's value table ends the run's keys, so every row from the
        // table's first key on is a value row.
        let IndexRow::Value(row) = self.rows.take_head() else {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "grep run {} has a row of another family under a value table key",
                    self.run_no
                ),
            });
        };
        let value = row.value();
        if row.ordinal != self.next_ordinal
            || self.previous.is_some_and(|previous| previous >= value)
        {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "the value table of grep run {} has ordinal {} out of place",
                    self.run_no, row.ordinal
                ),
            });
        }
        self.next_ordinal += 1;
        self.previous = Some(value);
        Ok(Some((row.ordinal, value)))
    }
}

/// Returns the ordinal of `value` in the value table of `run`, whose table
/// has `len` values, or `None` when the table doesn't list it, by a binary
/// search over single reads of the table.
pub(super) async fn table_ordinal<S: ObjectStore + ?Sized>(
    segments: &GrepSegments<'_, S>,
    run: &[ExtensionSegmentRef],
    len: usize,
    value: &ContentValue,
) -> Result<Option<u64>> {
    let (mut low, mut high) = (0, u64_from_usize(len));
    while low < high {
        let middle = low + (high - low) / 2;
        let found = segments
            .table_values(run, &BTreeSet::from([middle]))
            .await?[&middle];
        match found.cmp(value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

/// The input runs' value tables, read together in content order.
struct InputTables<'r> {
    readers: Vec<(RunNo, &'r [ExtensionSegmentRef], TableReader)>,
    heads: Vec<Option<ContentValue>>,
    ordinals: Vec<Vec<u32>>,
}

impl<'r> InputTables<'r> {
    async fn start<S: ObjectStore + ?Sized>(
        segments: &GrepSegments<'_, S>,
        inputs: &'r BTreeMap<RunNo, Vec<ExtensionSegmentRef>>,
    ) -> Result<Self> {
        let mut readers: Vec<_> = inputs
            .iter()
            .map(|(run_no, run)| (*run_no, run.as_slice(), TableReader::new(*run_no, run, 0)))
            .collect();
        // The first reads open the input runs' segments, so they overlap.
        let first_reads: Vec<_> = readers
            .iter_mut()
            .map(|(_, _, reader)| reader.next(segments))
            .collect();
        let heads: Vec<Option<(u64, ContentValue)>> = stream::iter(first_reads)
            .buffered(GREP_WORKER_REQUESTS_IN_FLIGHT)
            .try_collect()
            .await?;
        let heads = heads
            .into_iter()
            .map(|head| head.map(|(_, value)| value))
            .collect();
        Ok(Self {
            ordinals: vec![Vec::new(); readers.len()],
            readers,
            heads,
        })
    }

    /// Returns the least value at the head of any input table.
    fn least(&self) -> Option<ContentValue> {
        self.heads.iter().flatten().min().copied()
    }

    /// Records `output` as the output ordinal of `value` in every input
    /// table that lists it, and moves those tables past it.
    async fn translate<S: ObjectStore + ?Sized>(
        &mut self,
        segments: &GrepSegments<'_, S>,
        value: ContentValue,
        output: u32,
    ) -> Result<()> {
        for (position, head) in self.heads.iter_mut().enumerate() {
            if *head == Some(value) {
                self.ordinals[position].push(output);
                *head = self.readers[position]
                    .2
                    .next(segments)
                    .await?
                    .map(|(_, value)| value);
            }
        }
        Ok(())
    }

    fn into_translations(self) -> BTreeMap<RunNo, Translation> {
        self.readers
            .into_iter()
            .zip(self.ordinals)
            .map(|((run_no, run, _), ordinals)| {
                (
                    run_no,
                    Translation {
                        segments: run.to_vec(),
                        ordinals,
                    },
                )
            })
            .collect()
    }
}

/// Decides the output run's value table in a reorganization's first step:
/// every input value, less the values that `filters` doesn't keep. Writes
/// the table's rows to `writer` as it decides them, and returns each input
/// run's translation.
pub(super) async fn decide_output_table<S: ObjectStore + ?Sized>(
    segments: &GrepSegments<'_, S>,
    inputs: &BTreeMap<RunNo, Vec<ExtensionSegmentRef>>,
    filters: Option<&RetainedReferences>,
    writer: &mut RunWriter<'_, '_, S>,
) -> Result<BTreeMap<RunNo, Translation>> {
    let mut tables = writer.run(InputTables::start(segments, inputs)).await?;
    let mut next_output = 0_u32;
    while let Some(value) = tables.least() {
        let output = if filters.is_none_or(|filters| filters.keeps_value(&value)) {
            let output = next_output;
            if output == DROPPED {
                return Err(GrepError::IndexCorrupt {
                    message: "a grep reorganization's output has more values than a run holds"
                        .to_owned(),
                });
            }
            writer
                .push(&IndexRow::value(u64::from(output), &value))
                .await?;
            next_output += 1;
            output
        } else {
            DROPPED
        };
        writer
            .run(tables.translate(segments, value, output))
            .await?;
    }
    Ok(tables.into_translations())
}

/// Reads the output run's value table, which the reorganization's first step
/// wrote in `output_run`, back beside the input runs' tables, and returns
/// each input run's translation.
pub(super) async fn read_translations<S: ObjectStore + ?Sized>(
    segments: &GrepSegments<'_, S>,
    inputs: &BTreeMap<RunNo, Vec<ExtensionSegmentRef>>,
    output_run_no: RunNo,
    output_run: &[ExtensionSegmentRef],
) -> Result<BTreeMap<RunNo, Translation>> {
    let mut tables = InputTables::start(segments, inputs).await?;
    let mut output_table = TableReader::new(output_run_no, output_run, 0);
    let mut output_head = output_table.next(segments).await?;
    while let Some(value) = tables.least() {
        while output_head.is_some_and(|(_, output)| output < value) {
            output_head = output_table.next(segments).await?;
        }
        let output = match output_head {
            Some((ordinal, output)) if output == value => u32::try_from(ordinal)
                .ok()
                .filter(|ordinal| *ordinal != DROPPED)
                .ok_or_else(|| GrepError::IndexCorrupt {
                    message: format!("grep run {output_run_no} has more values than a run holds"),
                })?,
            _ => DROPPED,
        };
        tables.translate(segments, value, output).await?;
    }
    Ok(tables.into_translations())
}

/// Reads the revision filter of the run whose segments are `run`, or `None`
/// when the run has no filter rows.
pub(super) async fn run_filter<S: ObjectStore + ?Sized>(
    segments: &GrepSegments<'_, S>,
    run: &[ExtensionSegmentRef],
) -> Result<Option<SegmentFilter>> {
    let upper_bound = string_prefix_upper_bound(lookup::FILTER_ROW_PREFIX);
    let object_key = run
        .first()
        .map(|segment| segments.object_key(segment))
        .unwrap_or_default();
    let mut chunks = Vec::new();
    for segment in run {
        let range = segments
            .rows_in_range(
                segment,
                None,
                lookup::FILTER_ROW_PREFIX,
                upper_bound.as_deref(),
                usize::MAX,
            )
            .await?;
        for (_, row) in range.rows {
            let IndexRow::Filter(row) = row else {
                return Err(GrepError::IndexCorrupt {
                    message: format!(
                        "the run of `{object_key}` has a row of another family under a filter key"
                    ),
                });
            };
            chunks.push((row.chunk, row.payload.into_vec()));
        }
    }
    filter_from_rows(&object_key, chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::GrepBlockCache;
    use loonfs::engine::DecodedBlockCacheConfig;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::ContentId;
    use std::num::NonZeroUsize;

    fn value(content: u128) -> ContentValue {
        ContentValue::new(&ContentId::from_be_bytes(content.to_be_bytes()), 1)
    }

    /// Writes a run whose value table lists `contents`, two rows to a
    /// segment, so the table spans segments.
    async fn table_run(
        segments: &GrepSegments<'_, LocalFsStore>,
        contents: &[u128],
    ) -> Vec<ExtensionSegmentRef> {
        let rows = contents
            .iter()
            .zip(0..)
            .map(|(content, ordinal)| IndexRow::value(ordinal, &value(*content)))
            .collect();
        segments
            .write_run(rows, NonZeroUsize::new(2).expect("two"))
            .await
            .expect("write the run")
    }

    #[tokio::test]
    async fn a_first_step_and_a_later_step_translate_the_inputs_alike() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("tables");
        let cache = GrepBlockCache::new(DecodedBlockCacheConfig::new(1 << 20));
        let segments = GrepSegments {
            store: &store,
            namespace_id: &namespace_id,
            block_cache: Some(&cache),
        };
        let inputs = BTreeMap::from([
            (RunNo(1), table_run(&segments, &[1, 3, 5, 7]).await),
            (RunNo(2), table_run(&segments, &[2, 3, 8]).await),
        ]);

        let mut writer = RunWriter::new(&segments, NonZeroUsize::new(2).expect("two"));
        let first = decide_output_table(&segments, &inputs, None, &mut writer)
            .await
            .expect("decide");
        let output = writer.finish().await.expect("finish");
        assert_eq!(first[&RunNo(1)].ordinals, [0, 2, 3, 4]);
        assert_eq!(first[&RunNo(2)].ordinals, [1, 2, 5]);

        let later = read_translations(&segments, &inputs, RunNo(3), &output)
            .await
            .expect("read back");
        for run_no in [RunNo(1), RunNo(2)] {
            assert_eq!(later[&run_no].ordinals, first[&run_no].ordinals);
        }
        let mut reader = TableReader::new(RunNo(3), &output, 3);
        assert_eq!(
            reader.next(&segments).await.expect("read"),
            Some((3, value(5)))
        );
        assert_eq!(
            table_ordinal(&segments, &output, 6, &value(7))
                .await
                .expect("search"),
            Some(4)
        );
        assert_eq!(
            table_ordinal(&segments, &output, 6, &value(4))
                .await
                .expect("search"),
            None
        );
    }
}
