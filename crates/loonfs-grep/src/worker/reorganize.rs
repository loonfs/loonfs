//! Grep index reorganization: bounded merges of index runs into a run at the
//! next level.
//!
//! Reorganization publishes bounded merges like metadata reorganization.
//! Grep uses delta, mid, and base levels because each file contributes to
//! many gram posting lists. The mid level absorbs frequent delta merges
//! without rewriting most of the base on every step.

use super::build::gram_postings_rows;
use super::{next_index_run_no, next_manifest_no, GramIndexBuildPolicy, GrepReorganizeOutcome};
use crate::codec::{lookup::GRAM_ROW_PREFIX, Gram, GramPosting, IndexRow};
use crate::manifest::{
    publish_grep_manifest, GrepIndexState, GrepManifestState, GrepReorganizeState, GrepSegmentRef,
};
use crate::segments::GrepSegments;
use crate::{GrepError, GrepWorker, Result};
use loonfs::engine::{refill_iterators, select_next_iterator, OperationClock, SegmentRowIterator};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::segment_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES;
use loonfs_types::{IndexSegmentId, NamespaceId, RunNo};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const INDEX_GRAMS_DELTA_LEVEL: u32 = 0;
const INDEX_GRAMS_MID_LEVEL: u32 = 1;
const INDEX_GRAMS_BASE_LEVEL: u32 = 2;

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
    /// A step that has runs to merge waits for a step permit before it reads
    /// index segments, and holds it until it publishes. A step with nothing
    /// to merge takes no permit.
    pub async fn reorganize_step(
        &self,
        namespace_id: &NamespaceId,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepReorganizeOutcome> {
        let deadline = OperationClock::start(self.now_ms(), self.timer());
        let Some(current) = self.load_enabled(namespace_id, &deadline).await? else {
            return Ok(GrepReorganizeOutcome::NotEnabled);
        };
        let (reorganize, next_run_no) = match plan_reorganize(current.manifest_state(), policy)? {
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
            block_cache: None,
        };
        let merged = segments
            .merge_range(
                &inputs,
                &reorganize.row_key_cursor,
                policy.max_decoded_input_rows_per_step.get(),
            )
            .await?;
        let rows = gram_postings_rows(merged.postings);
        let manifest_no = next_manifest_no(Some(&current))?;
        let new_segments = segments
            .write_run(
                reorganize.run_no,
                reorganize.output_level,
                rows,
                policy.max_rows_per_segment,
            )
            .await?;
        let segments_written = new_segments.len() as u64;
        let mut manifest_segments = current.manifest_state().segments().to_vec();
        let mut next_reorganize = reorganize.clone();
        next_reorganize.output_segment_ids.extend(
            new_segments
                .iter()
                .map(|segment| segment.segment_id.clone()),
        );
        manifest_segments.extend(new_segments);
        let completed = merged.exhausted;
        let reorganize = if completed {
            let input_ids: BTreeSet<&IndexSegmentId> =
                next_reorganize.input_segment_ids.iter().collect();
            manifest_segments.retain(|segment| !input_ids.contains(&segment.segment_id));
            None
        } else {
            next_reorganize.row_key_cursor = merged.next_cursor;
            Some(next_reorganize)
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
                merged_rows: merged.rows,
                segments_written,
                completed,
            }),
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

struct MergedRange {
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    next_cursor: String,
    exhausted: bool,
    rows: u64,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Merges the rows of `inputs` from `cursor` on, stopping after the key
    /// that brings the merge to `max_rows` rows.
    async fn merge_range(
        &self,
        inputs: &[&GrepSegmentRef],
        cursor: &str,
        max_rows: usize,
    ) -> Result<MergedRange> {
        let start = if cursor.is_empty() {
            GRAM_ROW_PREFIX
        } else {
            cursor
        };
        let mut readers: Vec<SegmentRowIterator<IndexRow, GrepSegmentRef, ()>> = inputs
            .iter()
            .map(|segment| {
                SegmentRowIterator::new((), vec![(*segment).clone()], Some(start.to_owned()))
            })
            .collect();
        let mut merged = MergedRange {
            postings: BTreeMap::new(),
            next_cursor: String::new(),
            exhausted: false,
            rows: 0,
        };
        let mut last_key = String::new();
        while merged.rows < max_rows as u64 {
            refill_iterators(self, &mut readers, DEFAULT_MAX_COMPACTION_INPUT_BYTES).await?;
            let Some(position) = select_next_iterator(&readers, |_, row_key| row_key) else {
                merged.exhausted = true;
                return Ok(merged);
            };
            let key = readers[position]
                .head()
                .expect("the selected iterator should have a row")
                .0
                .to_owned();
            let object_key = self.object_key(current_segment_id(&readers[position]));
            let row = readers[position].take_head();
            merge_row(&mut merged, row, &object_key)?;
            loop {
                refill_iterators(self, &mut readers, DEFAULT_MAX_COMPACTION_INPUT_BYTES).await?;
                let mut found = false;
                for reader in &mut readers {
                    while reader.head().is_some_and(|(row_key, _)| row_key == key) {
                        let object_key = self.object_key(current_segment_id(reader));
                        let duplicate = reader.take_head();
                        merge_row(&mut merged, duplicate, &object_key)?;
                        found = true;
                    }
                }
                if !found {
                    break;
                }
            }
            last_key = key;
        }
        refill_iterators(self, &mut readers, DEFAULT_MAX_COMPACTION_INPUT_BYTES).await?;
        if readers.iter().any(|reader| reader.head().is_some()) {
            merged.next_cursor = format!("{last_key}\0");
        } else {
            merged.exhausted = true;
        }
        Ok(merged)
    }
}

fn current_segment_id(
    iterator: &SegmentRowIterator<IndexRow, GrepSegmentRef, ()>,
) -> &IndexSegmentId {
    &iterator
        .current_segment()
        .expect("an iterator with a row should have a current segment")
        .segment_id
}

fn merge_row(merged: &mut MergedRange, row: IndexRow, object_key: &str) -> Result<()> {
    let IndexRow::GramPostings { gram, .. } = &row;
    let gram = *gram;
    let batch = row.postings().map_err(|error| GrepError::IndexCorrupt {
        message: format!(
            "index segment `{object_key}` carries an unreadable posting batch: {error}"
        ),
    })?;
    merged.postings.entry(gram).or_default().extend(batch);
    merged.rows += 1;
    Ok(())
}
