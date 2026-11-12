//! Grep index reorganization: bounded rewrites of index runs into a run at the
//! next level.
//!
//! Reorganization publishes bounded rewrites like metadata compaction.
//! Grep uses delta, mid, and base levels because each file contributes to
//! many gram posting lists. The mid level absorbs frequent delta reorganizations
//! without rewriting most of the base on every step.

use super::index_rows::IndexRows;
use super::pruning::{first_step_filters, re_reference_check_fails, RetainedReferences};
use super::tables::{
    decide_output_table, read_translations, run_filter, table_ordinal, TableReader, Translation,
};
use super::{GramIndexBuildPolicy, GrepReorganizeOutcome, GrepSteps};
use crate::codec::{lookup, IndexRow, TextRow};
use crate::entry::{GrepEntry, GrepReorganizeState};
use crate::reads::NamespaceReads;
use crate::segments::{GrepSegments, RunWriter};
use crate::{GrepError, Result};
use loonfs::engine::{
    refill_iterators, select_next_iterator, LoadedBlocks, SegmentBlockLoader, SegmentRowIterator,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys::after_row_key;
use loonfs_types::format::manifest::{ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::format::segment_blocks::{SegmentIndexEntry, DEFAULT_MAX_COMPACTION_INPUT_BYTES};
use loonfs_types::{IndexSegmentId, RunNo};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(super) const INDEX_GRAMS_DELTA_LEVEL: u32 = 0;
const INDEX_GRAMS_MID_LEVEL: u32 = 1;
pub(super) const INDEX_GRAMS_BASE_LEVEL: u32 = 2;

/// The reorganization one step continues or starts, or the run counts that
/// call for none.
enum ReorganizePlan {
    Continue(GrepReorganizeState),
    /// A new reorganization, whose run number the step takes from the
    /// entry.
    Start(GrepReorganizeState),
    NotNeeded {
        delta_runs: usize,
        mid_runs: usize,
    },
}

impl<S: ObjectStore + Clone> GrepSteps<'_, S> {
    /// Runs one partitioned delta-to-mid or mid-plus-base reorganize step,
    /// from the entry that the run's earlier steps left pending.
    ///
    /// A step that has runs to reorganize waits for a step permit before it reads
    /// index segments, and keeps it until it writes its output. A step with
    /// nothing to reorganize takes no permit. The step ends by publishing the
    /// run's pending change when it's due.
    ///
    /// Each step translates the inputs' posting ordinals to the output run's
    /// ordinals, through one read of the input runs' value tables beside
    /// the output run's, a block of each at a time, so it keeps four bytes
    /// for each input value. The first step decides the output run's table
    /// and writes it as it decides it.
    ///
    /// A reorganization into the base level drops the content values and the
    /// `content_revisions` rows that no retained revision refers to. Its
    /// first step scans the namespace's revisions once and writes the
    /// revision filter in the output run, and its last step abandons the
    /// reorganization when a commit since the filters' sequence brings back
    /// a value that it drops.
    pub async fn reorganize_step(
        &mut self,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepReorganizeOutcome> {
        let pending = self.pending().await?;
        let Some(entry) = pending.enabled_entry().cloned() else {
            return Ok(GrepReorganizeOutcome::NotEnabled);
        };
        let mut reorganize = match plan_reorganize(&entry, policy) {
            ReorganizePlan::Continue(reorganize) => reorganize,
            ReorganizePlan::Start(start) => GrepReorganizeState {
                run_no: entry.next_run_no()?,
                ..start
            },
            ReorganizePlan::NotNeeded {
                delta_runs,
                mid_runs,
            } => {
                return self
                    .finish_step(
                        GrepReorganizeOutcome::NotNeeded {
                            delta_runs,
                            mid_runs,
                        },
                        GrepReorganizeOutcome::Superseded,
                    )
                    .await
            }
        };
        let input_runs = input_runs(&entry, &reorganize);
        let _permit = self.worker.step_budget.permit().await;
        let segments = GrepSegments {
            store: &self.worker.store,
            namespace_id: &self.namespace_id,
            block_cache: Some(&self.worker.blocks),
        };
        let reads = self.worker.reads(&self.namespace_id);
        let (translations, filters, mut new_segments) = step_tables(
            &segments,
            &reads,
            &entry,
            &mut reorganize,
            &input_runs,
            policy.max_rows_per_segment,
        )
        .await?;
        let combined = segments
            .combine_range(
                &input_runs,
                &reorganize.row_key_cursor,
                policy.max_decoded_input_rows_per_step.get(),
                &CombineTables {
                    translations: &translations,
                    revisions: filters.as_ref(),
                },
            )
            .await?;
        let completed = combined.exhausted;
        if completed
            && re_reference_check_fails(&reads, &reorganize, &segments, &translations).await?
        {
            self.abandon_reorganization(&entry).await?;
            return self
                .finish_step(
                    GrepReorganizeOutcome::Abandoned,
                    GrepReorganizeOutcome::Superseded,
                )
                .await;
        }
        new_segments.extend(
            segments
                .write_run(combined.rows.into_rows(), policy.max_rows_per_segment)
                .await?,
        );
        let segments_written = new_segments.len() as u64;
        reorganize.output_segment_ids.extend(
            new_segments
                .iter()
                .map(|segment| segment.segment_id.clone()),
        );
        let mut runs = entry.runs().to_vec();
        match runs.iter_mut().find(|run| run.run_no == reorganize.run_no) {
            Some(output) => output.segments.extend(new_segments),
            None if new_segments.is_empty() => {}
            None => runs.push(ExtensionRunRef {
                run_no: reorganize.run_no,
                level: reorganize.output_level,
                segments: new_segments,
            }),
        }
        let reorganize = if completed {
            let input_ids: BTreeSet<&IndexSegmentId> =
                reorganize.input_segment_ids.iter().collect();
            without_segments(&mut runs, &input_ids);
            None
        } else {
            reorganize.row_key_cursor = combined.next_cursor;
            Some(reorganize)
        };
        self.pending().await?.set_entry(
            GrepEntry::new(entry.status().clone(), reorganize, runs)
                .expect("a reorganize step should keep a valid entry valid"),
        );
        self.finish_step(
            GrepReorganizeOutcome::UnitWritten {
                combined_rows: combined.rows_read,
                segments_written,
                completed,
            },
            GrepReorganizeOutcome::Superseded,
        )
        .await
    }

    /// Drops the reorganization from the pending entry, with the outputs that
    /// its earlier steps listed. The inputs stay listed, so no value that the
    /// reorganization would drop leaves the index, and the outputs are left
    /// to the collector.
    async fn abandon_reorganization(&mut self, entry: &GrepEntry) -> Result<()> {
        let Some(reorganize) = entry.reorganize() else {
            return Ok(());
        };
        let outputs: BTreeSet<&IndexSegmentId> = reorganize.output_segment_ids.iter().collect();
        let mut runs = entry.runs().to_vec();
        without_segments(&mut runs, &outputs);
        self.pending().await?.set_entry(
            GrepEntry::new(entry.status().clone(), None, runs)
                .expect("dropping a reorganization should keep a valid entry valid"),
        );
        Ok(())
    }
}

/// Returns the posting translations and revision filters that one reorganize
/// step merges with, and the segments that the step writes for them.
///
/// The first step decides the output run's value table, and for a base
/// reorganization its revision filter. A later step reads the translations
/// and the filter back from the outputs that the first step wrote, and
/// writes no segments.
async fn step_tables<S: ObjectStore>(
    segments: &GrepSegments<'_, S>,
    reads: &NamespaceReads,
    entry: &GrepEntry,
    reorganize: &mut GrepReorganizeState,
    input_runs: &BTreeMap<RunNo, Vec<ExtensionSegmentRef>>,
    max_rows_per_segment: NonZeroUsize,
) -> Result<(
    BTreeMap<RunNo, Translation>,
    Option<RetainedReferences>,
    Vec<ExtensionSegmentRef>,
)> {
    let inputs: Vec<ExtensionSegmentRef> = input_runs.values().flatten().cloned().collect();
    let base = reorganize.output_level == INDEX_GRAMS_BASE_LEVEL;
    let first_step = reorganize.row_key_cursor.is_empty();
    if first_step {
        let filters = if base {
            Some(first_step_filters(reads, reorganize, &inputs).await?)
        } else {
            None
        };
        // The first step writes the output run's filter and value table
        // in segments of their own as it decides them, so it keeps
        // neither table.
        let mut writer = RunWriter::new(segments, max_rows_per_segment);
        for row in filters
            .as_ref()
            .map(RetainedReferences::filter_rows)
            .unwrap_or_default()
        {
            writer.push(&row).await?;
        }
        let translations =
            decide_output_table(segments, input_runs, filters.as_ref(), &mut writer).await?;
        let new_segments = writer.finish().await?;
        Ok((translations, filters, new_segments))
    } else {
        let outputs: Vec<ExtensionSegmentRef> = entry
            .segments()
            .filter(|segment| reorganize.output_segment_ids.contains(&segment.segment_id))
            .cloned()
            .collect();
        let translations =
            read_translations(segments, input_runs, reorganize.run_no, &outputs).await?;
        let filters = if base {
            let revisions =
                run_filter(segments, &outputs)
                    .await?
                    .ok_or_else(|| GrepError::IndexCorrupt {
                        message: format!(
                            "the outputs of grep reorganization run {} have no revision filter",
                            reorganize.run_no
                        ),
                    })?;
            Some(RetainedReferences::written(revisions))
        } else {
            None
        };
        Ok((translations, filters, Vec::new()))
    }
}

/// Returns whether the next step over `entry` is a reorganize step, which
/// it is when the entry records a reorganization or a level has reached its
/// run limit under `policy`.
pub(super) fn reorganize_due(entry: &GrepEntry, policy: GramIndexBuildPolicy) -> bool {
    !matches!(
        plan_reorganize(entry, policy),
        ReorganizePlan::NotNeeded { .. }
    )
}

/// Continues the reorganization that `entry` records, or picks the inputs
/// of a new one when a level has reached its run limit.
fn plan_reorganize(entry: &GrepEntry, policy: GramIndexBuildPolicy) -> ReorganizePlan {
    if let Some(reorganize) = entry.reorganize() {
        return ReorganizePlan::Continue(reorganize.clone());
    }
    let delta_runs = runs_at_level(entry.runs(), INDEX_GRAMS_DELTA_LEVEL);
    let mid_runs = runs_at_level(entry.runs(), INDEX_GRAMS_MID_LEVEL);
    let (is_input, output_level): (fn(&ExtensionRunRef) -> bool, u32) =
        if delta_runs >= policy.max_delta_runs.get() {
            (
                |run| run.level == INDEX_GRAMS_DELTA_LEVEL,
                INDEX_GRAMS_MID_LEVEL,
            )
        } else if mid_runs >= policy.max_mid_runs.get() {
            (
                |run| run.level != INDEX_GRAMS_DELTA_LEVEL,
                INDEX_GRAMS_BASE_LEVEL,
            )
        } else {
            return ReorganizePlan::NotNeeded {
                delta_runs,
                mid_runs,
            };
        };
    let input_segment_ids = entry
        .runs()
        .iter()
        .filter(|run| is_input(run))
        .flat_map(|run| &run.segments)
        .map(|segment| segment.segment_id.clone())
        .collect();
    ReorganizePlan::Start(GrepReorganizeState {
        input_segment_ids,
        output_segment_ids: Vec::new(),
        row_key_cursor: String::new(),
        output_level,
        // The step takes the run's number from the entry.
        run_no: RunNo(0),
        pruning: None,
    })
}

fn runs_at_level(runs: &[ExtensionRunRef], level: u32) -> usize {
    runs.iter().filter(|run| run.level == level).count()
}

/// Returns the segments of `reorganize`'s inputs that `entry` lists, by the
/// number of the run that each belongs to.
fn input_runs(
    entry: &GrepEntry,
    reorganize: &GrepReorganizeState,
) -> BTreeMap<RunNo, Vec<ExtensionSegmentRef>> {
    let input_ids: BTreeSet<&IndexSegmentId> = reorganize.input_segment_ids.iter().collect();
    let input_runs: BTreeMap<RunNo, Vec<ExtensionSegmentRef>> = entry
        .runs()
        .iter()
        .map(|run| {
            let inputs = run
                .segments
                .iter()
                .filter(|segment| input_ids.contains(&segment.segment_id))
                .cloned()
                .collect::<Vec<_>>();
            (run.run_no, inputs)
        })
        .filter(|(_, inputs)| !inputs.is_empty())
        .collect();
    assert_eq!(
        input_runs.values().map(Vec::len).sum::<usize>(),
        input_ids.len(),
        "a valid grep entry should list its reorganization's inputs"
    );
    input_runs
}

/// Removes the segments in `segment_ids` from `runs`, and the runs that
/// have no segments left.
fn without_segments(runs: &mut Vec<ExtensionRunRef>, segment_ids: &BTreeSet<&IndexSegmentId>) {
    runs.retain_mut(|run| {
        run.segments
            .retain(|segment| !segment_ids.contains(&segment.segment_id));
        !run.segments.is_empty()
    });
}

/// The translations and the revision filter that one reorganize step merges
/// its input rows by.
struct CombineTables<'a> {
    /// Each input run's translation to the output run's ordinals.
    translations: &'a BTreeMap<RunNo, Translation>,
    /// The revisions whose `content_revisions` rows the step keeps, or
    /// `None` to keep every row.
    revisions: Option<&'a RetainedReferences>,
}

struct CombinedRange {
    rows: IndexRows,
    next_cursor: String,
    exhausted: bool,
    rows_read: u64,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Combines the rows of `inputs` from `cursor` on, stopping after the key
    /// that brings the combination to `max_rows` rows, and keeps what `tables`
    /// keep. The combination ends at the inputs' value tables, which the output's
    /// table replaces.
    async fn combine_range(
        &self,
        inputs: &BTreeMap<RunNo, Vec<ExtensionSegmentRef>>,
        cursor: &str,
        max_rows: usize,
        tables: &CombineTables<'_>,
    ) -> Result<CombinedRange> {
        let loader = CombineInputs(self);
        let start = (!cursor.is_empty()).then(|| cursor.to_owned());
        let start = &start;
        // A reorganization's first step writes its output's filter and value
        // table rows before rows with lower keys, so a run's list order isn't
        // key order, and each segment has an iterator of its own. An
        // iterator's sort key is its segment's run number.
        let mut readers: Vec<SegmentRowIterator<IndexRow, ExtensionSegmentRef, RunNo>> = inputs
            .iter()
            .flat_map(|(run_no, run)| {
                run.iter().map(move |segment| {
                    SegmentRowIterator::new(*run_no, vec![segment.clone()], start.clone())
                })
            })
            .collect();
        let mut combined = CombinedRange {
            rows: IndexRows::default(),
            next_cursor: String::new(),
            exhausted: false,
            rows_read: 0,
        };
        let mut text_tables = BTreeMap::new();
        let mut last_key = String::new();
        while combined.rows_read < max_rows as u64 {
            refill_iterators(
                &loader,
                &mut readers,
                DEFAULT_MAX_COMPACTION_INPUT_BYTES.get(),
            )
            .await?;
            let Some(position) = select_next_iterator(&readers, |_, row_key| row_key) else {
                combined.exhausted = true;
                return Ok(combined);
            };
            let key = readers[position]
                .head()
                .expect("the selected iterator should have a row")
                .0
                .to_owned();
            if key.as_str() >= lookup::VALUE_ROW_PREFIX {
                combined.exhausted = true;
                return Ok(combined);
            }
            let (object_key, run_no) = current_segment(&readers[position], self);
            let row = readers[position].take_head();
            self.combine_row(
                &mut combined,
                row,
                &object_key,
                run_no,
                tables,
                &mut text_tables,
            )
            .await?;
            loop {
                refill_iterators(
                    &loader,
                    &mut readers,
                    DEFAULT_MAX_COMPACTION_INPUT_BYTES.get(),
                )
                .await?;
                let mut found = false;
                for reader in &mut readers {
                    while reader.head().is_some_and(|(row_key, _)| row_key == key) {
                        let (object_key, run_no) = current_segment(reader, self);
                        let duplicate = reader.take_head();
                        self.combine_row(
                            &mut combined,
                            duplicate,
                            &object_key,
                            run_no,
                            tables,
                            &mut text_tables,
                        )
                        .await?;
                        found = true;
                    }
                }
                if !found {
                    break;
                }
            }
            last_key = key;
        }
        refill_iterators(
            &loader,
            &mut readers,
            DEFAULT_MAX_COMPACTION_INPUT_BYTES.get(),
        )
        .await?;
        if readers.iter().any(|reader| {
            reader
                .head()
                .is_some_and(|(row_key, _)| row_key < lookup::VALUE_ROW_PREFIX)
        }) {
            combined.next_cursor = after_row_key(&last_key);
        } else {
            combined.exhausted = true;
        }
        Ok(combined)
    }

    /// Adds one input row to the combination: a gram's postings join the gram's
    /// other batches as ordinals in the output run's table, and two rows of
    /// one `texts` or `content_revisions` key combine into one row.
    ///
    /// A run's `texts` rows list the values of its table in the same order,
    /// so the `n`th `texts` row of a run has ordinal `n`. The step finds the
    /// ordinal of a run's first `texts` row with a binary search of the run's
    /// table, and reads the table on from there, a block at a time, beside
    /// the run's later `texts` rows in `text_tables`.
    async fn combine_row(
        &self,
        combined: &mut CombinedRange,
        row: IndexRow,
        object_key: &str,
        run_no: RunNo,
        tables: &CombineTables<'_>,
        text_tables: &mut BTreeMap<RunNo, TableReader>,
    ) -> Result<()> {
        combined.rows_read += 1;
        let translation = tables
            .translations
            .get(&run_no)
            .expect("every input run should have a translation");
        match row {
            IndexRow::GramPostings(row) => {
                let ordinals = row.ordinals().map_err(|error| GrepError::IndexCorrupt {
                    message: format!(
                        "index segment `{object_key}` carries an unreadable posting batch: {error}"
                    ),
                })?;
                let postings = combined.rows.postings.entry(row.gram).or_default();
                for ordinal in ordinals {
                    let output =
                        translation
                            .output(ordinal)
                            .ok_or_else(|| GrepError::IndexCorrupt {
                                message: format!(
                                    "a posting in index segment `{object_key}` refers to \
                                     ordinal {ordinal}, which its run's value table doesn't list"
                                ),
                            })?;
                    postings.extend(output);
                }
            }
            IndexRow::Text(row) => {
                let value = row.value();
                let table = match text_tables.entry(run_no) {
                    Entry::Occupied(table) => table.into_mut(),
                    Entry::Vacant(entry) => {
                        let ordinal = table_ordinal(
                            self,
                            &translation.segments,
                            translation.ordinals.len(),
                            &value,
                        )
                        .await?
                        .ok_or_else(|| text_without_table(object_key, &row))?;
                        entry.insert(TableReader::new(run_no, &translation.segments, ordinal))
                    }
                };
                let Some((ordinal, listed)) = table.next(self).await? else {
                    return Err(text_without_table(object_key, &row));
                };
                if listed != value {
                    return Err(text_without_table(object_key, &row));
                }
                let kept = translation
                    .output(ordinal)
                    .ok_or_else(|| text_without_table(object_key, &row))?
                    .is_some();
                if kept && combined.rows.texts.insert(value, row.eligible) == Some(!row.eligible) {
                    return Err(GrepError::IndexCorrupt {
                        message: format!(
                            "index segment `{object_key}` records content `{}` of {} bytes as \
                             both eligible and ineligible",
                            row.content_id, row.size_bytes
                        ),
                    });
                }
            }
            IndexRow::ContentRevision(row) => {
                if tables
                    .revisions
                    .is_none_or(|revisions| revisions.keeps_revision(row.inode_id, row.revision_no))
                {
                    combined
                        .rows
                        .add_content_revision(IndexRow::ContentRevision(row));
                }
            }
            // An input's revision filter describes the reorganization that
            // wrote the input, and the output has a filter of its own if it
            // needs one.
            IndexRow::Filter(_) => {}
            // The combination stops at the first value table key, so a value row
            // here sits under another family's key.
            IndexRow::Value(_) => {
                return Err(GrepError::IndexCorrupt {
                    message: format!(
                        "index segment `{object_key}` has a value row under another family's key"
                    ),
                });
            }
        }
        Ok(())
    }
}

/// The error of a `texts` row whose value its run's table doesn't list in
/// the same place.
fn text_without_table(object_key: &str, row: &TextRow) -> GrepError {
    GrepError::IndexCorrupt {
        message: format!(
            "index segment `{object_key}` has a `texts` row for content `{}` of {} bytes that \
             its run's value table doesn't list in the same place",
            row.content_id, row.size_bytes
        ),
    }
}

/// Loads a combination's input blocks: index blocks through the step's cache, and
/// data blocks around it, so a combination of up to 64 MiB doesn't evict the value
/// table blocks that every step reads again.
struct CombineInputs<'s, 'a, S: ?Sized>(&'s GrepSegments<'a, S>);

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<IndexRow, ExtensionSegmentRef>
    for CombineInputs<'_, '_, S>
{
    type Error = GrepError;

    async fn load_index(
        &self,
        segment: ExtensionSegmentRef,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        self.0.load_index(segment).await
    }

    async fn load_data_blocks(
        &self,
        segment: ExtensionSegmentRef,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<LoadedBlocks<IndexRow>> {
        let uncached = GrepSegments {
            store: self.0.store,
            namespace_id: self.0.namespace_id,
            block_cache: None,
        };
        uncached.load_data_blocks(segment, entries).await
    }
}

/// Returns the object key and the run number of the segment that an
/// iterator with a row reads.
fn current_segment<S: ObjectStore + ?Sized>(
    iterator: &SegmentRowIterator<IndexRow, ExtensionSegmentRef, RunNo>,
    segments: &GrepSegments<'_, S>,
) -> (String, RunNo) {
    let segment = iterator
        .current_segment()
        .expect("an iterator with a row should have a current segment");
    (segments.object_key(segment), *iterator.sort_key())
}
