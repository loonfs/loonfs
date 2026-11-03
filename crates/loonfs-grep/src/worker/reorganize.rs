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
use super::{next_index_run_no, next_manifest_no, GramIndexBuildPolicy, GrepReorganizeOutcome};
use crate::codec::{lookup, IndexRow, TextRow};
use crate::manifest::{
    publish_grep_manifest, GrepIndexState, GrepManifestState, GrepReorganizeState, GrepSegmentRef,
    LoadedGrepManifest,
};
use crate::segments::{GrepSegments, RunWriter};
use crate::{GrepError, GrepWorker, Result};
use loonfs::engine::{
    refill_iterators, select_next_iterator, OperationClock, SegmentBlockLoader, SegmentRowIterator,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys::after_row_key;
use loonfs_types::format::segment_blocks::{
    DecodedDataBlock, SegmentIndexEntry, DEFAULT_MAX_COMPACTION_INPUT_BYTES,
};
use loonfs_types::{IndexSegmentId, NamespaceId, RunNo};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) const INDEX_GRAMS_DELTA_LEVEL: u32 = 0;
const INDEX_GRAMS_MID_LEVEL: u32 = 1;
pub(super) const INDEX_GRAMS_BASE_LEVEL: u32 = 2;

/// The reorganization one step continues or starts, or the run counts that
/// call for none.
enum ReorganizePlan {
    Run {
        reorganize: GrepReorganizeState,
        next_run_no: RunNo,
    },
    NotNeeded {
        delta_runs: usize,
        mid_runs: usize,
    },
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    /// Runs one partitioned delta-to-mid or mid-plus-base reorganize step.
    ///
    /// A step that has runs to reorganize waits for a step permit before it reads
    /// index segments, and keeps it until it publishes. A step with nothing
    /// to reorganize takes no permit.
    ///
    /// Each step translates the inputs' posting ordinals to the output run's
    /// ordinals, through one pass over the input runs' value tables beside
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
        &self,
        namespace_id: &NamespaceId,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepReorganizeOutcome> {
        let deadline = OperationClock::start(self.now_ms(), self.timer());
        let Some(current) = self.load_enabled(namespace_id, &deadline).await? else {
            return Ok(GrepReorganizeOutcome::NotEnabled);
        };
        let (mut reorganize, next_run_no) = match plan_reorganize(current.manifest_state(), policy)?
        {
            ReorganizePlan::Run {
                reorganize,
                next_run_no,
            } => (reorganize, next_run_no),
            ReorganizePlan::NotNeeded {
                delta_runs,
                mid_runs,
            } => {
                return Ok(GrepReorganizeOutcome::NotNeeded {
                    delta_runs,
                    mid_runs,
                })
            }
        };
        let inputs = reorganize
            .input_segment_ids
            .iter()
            .map(|segment_id| {
                current
                    .manifest_state()
                    .segments()
                    .iter()
                    .find(|segment| &segment.segment_id == segment_id)
                    .cloned()
                    .ok_or_else(|| GrepError::IndexCorrupt {
                        message: format!(
                            "grep reorganization input segment `{segment_id}` is missing from the manifest"
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let _permit = self.step_budget.permit().await;
        let segments = GrepSegments {
            store: &self.store,
            namespace_id,
            block_cache: Some(&self.reorganize_blocks),
        };
        let mut input_runs: BTreeMap<RunNo, Vec<GrepSegmentRef>> = BTreeMap::new();
        for segment in &inputs {
            input_runs
                .entry(segment.run_no)
                .or_default()
                .push(segment.clone());
        }
        let reads = self.reads(namespace_id);
        let base = reorganize.output_level == INDEX_GRAMS_BASE_LEVEL;
        let first_step = reorganize.row_key_cursor.is_empty();
        let mut new_segments = Vec::new();
        let (translations, filters) = if first_step {
            let filters = if base {
                Some(first_step_filters(&reads, &mut reorganize, &inputs).await?)
            } else {
                None
            };
            // The first step writes the output run's filter and value table
            // in segments of their own as it decides them, so it keeps
            // neither table.
            let mut writer = RunWriter::new(
                &segments,
                reorganize.run_no,
                reorganize.output_level,
                policy.max_rows_per_segment,
            );
            for row in filters
                .as_ref()
                .map(RetainedReferences::filter_rows)
                .unwrap_or_default()
            {
                writer.push(&row).await?;
            }
            let translations =
                decide_output_table(&segments, &input_runs, filters.as_ref(), &mut writer).await?;
            new_segments = writer.finish().await?;
            (translations, filters)
        } else {
            let outputs: Vec<GrepSegmentRef> = current
                .manifest_state()
                .segments()
                .iter()
                .filter(|segment| reorganize.output_segment_ids.contains(&segment.segment_id))
                .cloned()
                .collect();
            let translations =
                read_translations(&segments, &input_runs, reorganize.run_no, &outputs).await?;
            let filters = if base {
                let revisions = run_filter(&segments, &outputs).await?.ok_or_else(|| {
                    GrepError::IndexCorrupt {
                        message: format!(
                            "the outputs of grep reorganization run {} have no revision filter",
                            reorganize.run_no
                        ),
                    }
                })?;
                Some(RetainedReferences::written(revisions))
            } else {
                None
            };
            (translations, filters)
        };
        let merged = segments
            .merge_range(
                &inputs,
                &reorganize.row_key_cursor,
                policy.max_decoded_input_rows_per_step.get(),
                &MergeTables {
                    translations: &translations,
                    revisions: filters.as_ref(),
                },
            )
            .await?;
        let completed = merged.exhausted;
        if completed
            && re_reference_check_fails(&reads, &reorganize, &segments, &translations).await?
        {
            return self
                .abandon_reorganization(namespace_id, &current, &deadline)
                .await;
        }
        let manifest_no = next_manifest_no(Some(&current))?;
        new_segments.extend(
            segments
                .write_run(
                    reorganize.run_no,
                    reorganize.output_level,
                    merged.rows.into_rows(),
                    policy.max_rows_per_segment,
                )
                .await?,
        );
        let segments_written = new_segments.len() as u64;
        let mut manifest_segments = current.manifest_state().segments().to_vec();
        reorganize.output_segment_ids.extend(
            new_segments
                .iter()
                .map(|segment| segment.segment_id.clone()),
        );
        manifest_segments.extend(new_segments);
        let reorganize = if completed {
            let input_ids: BTreeSet<&IndexSegmentId> =
                reorganize.input_segment_ids.iter().collect();
            manifest_segments.retain(|segment| !input_ids.contains(&segment.segment_id));
            None
        } else {
            reorganize.row_key_cursor = merged.next_cursor;
            Some(reorganize)
        };
        let next = GrepManifestState::new(
            namespace_id.clone(),
            manifest_no,
            current.manifest_state().status().clone(),
            GrepIndexState {
                reorganize,
                next_run_no,
            },
            manifest_segments,
        )
        .expect("a successor of a grep manifest that decoding validated should be valid");
        match publish_grep_manifest(&self.store, Some(&current), &next, &deadline).await {
            Ok(_) => Ok(GrepReorganizeOutcome::UnitPublished {
                merged_rows: merged.rows_read,
                segments_written,
                completed,
            }),
            Err(GrepError::PublicationConflict { .. }) => Ok(GrepReorganizeOutcome::Superseded),
            Err(error) => Err(error),
        }
    }

    /// Publishes `current` without its reorganization, and without the
    /// outputs that the reorganization's earlier steps listed. The inputs
    /// stay listed, so no value that the reorganization would drop leaves the
    /// index, and the outputs are left to the collector. A reorganization
    /// that no step has published yet needs no publication.
    async fn abandon_reorganization(
        &self,
        namespace_id: &NamespaceId,
        current: &LoadedGrepManifest,
        deadline: &OperationClock,
    ) -> Result<GrepReorganizeOutcome> {
        let state = current.manifest_state();
        let Some(reorganize) = &state.index().reorganize else {
            return Ok(GrepReorganizeOutcome::Abandoned);
        };
        let outputs: BTreeSet<&IndexSegmentId> = reorganize.output_segment_ids.iter().collect();
        let next = GrepManifestState::new(
            namespace_id.clone(),
            next_manifest_no(Some(current))?,
            state.status().clone(),
            GrepIndexState {
                reorganize: None,
                next_run_no: state.index().next_run_no,
            },
            state
                .segments()
                .iter()
                .filter(|segment| !outputs.contains(&segment.segment_id))
                .cloned()
                .collect(),
        )
        .expect("a successor of a grep manifest that decoding validated should be valid");
        match publish_grep_manifest(&self.store, Some(current), &next, deadline).await {
            Ok(_) => Ok(GrepReorganizeOutcome::Abandoned),
            Err(GrepError::PublicationConflict { .. }) => Ok(GrepReorganizeOutcome::Superseded),
            Err(error) => Err(error),
        }
    }
}

/// Continues the reorganization that `current` records, or picks the inputs
/// of a new one when a level has reached its run limit.
fn plan_reorganize(
    current: &GrepManifestState,
    policy: GramIndexBuildPolicy,
) -> Result<ReorganizePlan> {
    let current_run_no = current.index().next_run_no;
    if let Some(reorganize) = current.index().reorganize.clone() {
        return Ok(ReorganizePlan::Run {
            reorganize,
            next_run_no: current_run_no,
        });
    }
    let delta_runs = distinct_run_nos_at_level(current.segments(), INDEX_GRAMS_DELTA_LEVEL);
    let mid_runs = distinct_run_nos_at_level(current.segments(), INDEX_GRAMS_MID_LEVEL);
    let (is_input, output_level): (fn(&GrepSegmentRef) -> bool, u32) =
        if delta_runs >= policy.max_delta_runs.get() {
            (
                |segment| segment.level == INDEX_GRAMS_DELTA_LEVEL,
                INDEX_GRAMS_MID_LEVEL,
            )
        } else if mid_runs >= policy.max_mid_runs.get() {
            (
                |segment| segment.level != INDEX_GRAMS_DELTA_LEVEL,
                INDEX_GRAMS_BASE_LEVEL,
            )
        } else {
            return Ok(ReorganizePlan::NotNeeded {
                delta_runs,
                mid_runs,
            });
        };
    let input_segment_ids = current
        .segments()
        .iter()
        .filter(|segment| is_input(segment))
        .map(|segment| segment.segment_id.clone())
        .collect();
    Ok(ReorganizePlan::Run {
        reorganize: GrepReorganizeState {
            input_segment_ids,
            output_segment_ids: Vec::new(),
            row_key_cursor: String::new(),
            output_level,
            run_no: current_run_no,
            pruning: None,
        },
        next_run_no: next_index_run_no(current_run_no)?,
    })
}

fn distinct_run_nos_at_level(segments: &[GrepSegmentRef], level: u32) -> usize {
    segments
        .iter()
        .filter(|segment| segment.level == level)
        .map(|segment| segment.run_no)
        .collect::<BTreeSet<_>>()
        .len()
}

/// The translations and the revision filter that one reorganize step merges
/// its input rows by.
struct MergeTables<'a> {
    /// Each input run's translation to the output run's ordinals.
    translations: &'a BTreeMap<RunNo, Translation>,
    /// The revisions whose `content_revisions` rows the step keeps, or
    /// `None` to keep every row.
    revisions: Option<&'a RetainedReferences>,
}

struct MergedRange {
    rows: IndexRows,
    next_cursor: String,
    exhausted: bool,
    rows_read: u64,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Merges the rows of `inputs` from `cursor` on, stopping after the key
    /// that brings the merge to `max_rows` rows, and keeps what `tables`
    /// keep. The merge ends at the inputs' value tables, which the output's
    /// table replaces.
    async fn merge_range(
        &self,
        inputs: &[GrepSegmentRef],
        cursor: &str,
        max_rows: usize,
        tables: &MergeTables<'_>,
    ) -> Result<MergedRange> {
        let loader = MergeInputs(self);
        let start = (!cursor.is_empty()).then(|| cursor.to_owned());
        let mut readers: Vec<SegmentRowIterator<IndexRow, GrepSegmentRef, ()>> = inputs
            .iter()
            .map(|segment| SegmentRowIterator::new((), vec![segment.clone()], start.clone()))
            .collect();
        let mut merged = MergedRange {
            rows: IndexRows::default(),
            next_cursor: String::new(),
            exhausted: false,
            rows_read: 0,
        };
        let mut text_tables = BTreeMap::new();
        let mut last_key = String::new();
        while merged.rows_read < max_rows as u64 {
            refill_iterators(
                &loader,
                &mut readers,
                DEFAULT_MAX_COMPACTION_INPUT_BYTES.get(),
            )
            .await?;
            let Some(position) = select_next_iterator(&readers, |_, row_key| row_key) else {
                merged.exhausted = true;
                return Ok(merged);
            };
            let key = readers[position]
                .head()
                .expect("the selected iterator should have a row")
                .0
                .to_owned();
            if key.as_str() >= lookup::VALUE_ROW_PREFIX {
                merged.exhausted = true;
                return Ok(merged);
            }
            let (object_key, run_no) = current_segment(&readers[position], self);
            let row = readers[position].take_head();
            self.merge_row(
                &mut merged,
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
                        self.merge_row(
                            &mut merged,
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
            merged.next_cursor = after_row_key(&last_key);
        } else {
            merged.exhausted = true;
        }
        Ok(merged)
    }

    /// Adds one input row to the merge: a gram's postings join the gram's
    /// other batches as ordinals in the output run's table, and two rows of
    /// one `texts` or `content_revisions` key merge into one row.
    ///
    /// A run's `texts` rows list the values of its table in the same order,
    /// so the `n`th `texts` row of a run has ordinal `n`. The step finds the
    /// ordinal of a run's first `texts` row with a binary search of the run's
    /// table, and reads the table on from there, a block at a time, beside
    /// the run's later `texts` rows in `text_tables`.
    async fn merge_row(
        &self,
        merged: &mut MergedRange,
        row: IndexRow,
        object_key: &str,
        run_no: RunNo,
        tables: &MergeTables<'_>,
        text_tables: &mut BTreeMap<RunNo, TableReader>,
    ) -> Result<()> {
        merged.rows_read += 1;
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
                let postings = merged.rows.postings.entry(row.gram).or_default();
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
                if kept && merged.rows.texts.insert(value, row.eligible) == Some(!row.eligible) {
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
                    merged
                        .rows
                        .add_content_revision(IndexRow::ContentRevision(row));
                }
            }
            // An input's revision filter describes the reorganization that
            // wrote the input, and the output has a filter of its own if it
            // needs one.
            IndexRow::Filter(_) => {}
            // The merge stops at the first value table key, so a value row
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

/// Loads a merge's input blocks: index blocks through the step's cache, and
/// data blocks around it, so a merge of up to 64 MiB doesn't evict the value
/// table blocks that every step reads again.
struct MergeInputs<'s, 'a, S: ?Sized>(&'s GrepSegments<'a, S>);

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<IndexRow, GrepSegmentRef>
    for MergeInputs<'_, '_, S>
{
    type Error = GrepError;

    async fn load_index(&self, segment: GrepSegmentRef) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        self.0.load_index(segment).await
    }

    async fn load_data_blocks(
        &self,
        segment: GrepSegmentRef,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<Vec<Arc<DecodedDataBlock<IndexRow>>>> {
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
    iterator: &SegmentRowIterator<IndexRow, GrepSegmentRef, ()>,
    segments: &GrepSegments<'_, S>,
) -> (String, RunNo) {
    let segment = iterator
        .current_segment()
        .expect("an iterator with a row should have a current segment");
    (segments.object_key(&segment.segment_id), segment.run_no)
}
