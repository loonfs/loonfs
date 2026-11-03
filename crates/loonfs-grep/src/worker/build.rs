//! Grep index builds: planning one backfill or incremental step from the
//! checkpoint or the change feed, reading and indexing its content, and
//! publishing the result.

use super::index_rows::IndexRows;
use super::reorganize::INDEX_GRAMS_DELTA_LEVEL;
use super::{
    next_index_run_no, next_manifest_no, rebootstrap_required, GramIndexBuildPolicy,
    GrepBuildOutcome, GrepWorker, GREP_WORKER_REQUESTS_IN_FLIGHT,
};
use crate::cache::GrepBlockCache;
use crate::codec::{
    extract_grams, is_indexable_text_content, ContentValue, Gram, IndexRow,
    MAX_INDEX_GRAMS_FILE_BYTES,
};
use crate::manifest::{
    publish_grep_manifest, ChangeFeedResume, GrepIndexState, GrepIndexStatus, GrepManifestState,
    GrepSegmentRef, LoadedGrepManifest,
};
use crate::reads::{page_limit, published_revision, NamespaceReads};
use crate::segments::GrepSegments;
use crate::value_table::ValueTable;
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use loonfs::engine::{DecodedBlockCacheConfig, OperationClock};
use loonfs::{CheckpointFilesPageCursor, CoreError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::segment_blocks::DEFAULT_TARGET_BLOCK_BYTES;
use loonfs_types::{
    ChangeSeq, ContentRef, InodeId, NamespaceId, PinId, RevisionNo, MAX_PAGE_LIMIT,
};
use std::collections::{BTreeMap, BTreeSet};

/// Decoded bytes of the block cache that one build step reads index segments
/// through, so a step loads each segment's filter and index block once, and
/// of the cache that a worker's reorganize steps share. It fits 256 data
/// blocks, enough for a build step's lookups of 256 values.
pub(super) const GREP_STEP_BLOCK_CACHE_DECODED_BYTES: usize =
    256 * DEFAULT_TARGET_BLOCK_BYTES.get();

impl<S: ObjectStore + Clone> GrepWorker<S> {
    /// Runs one bounded checkpoint-backfill or incremental build unit.
    ///
    /// The step first reads the checkpoint listing or the change feed to
    /// learn what content it needs. Only then does it wait for a step permit,
    /// which it keeps while it reads that content and until it publishes. A
    /// step that finds the index up to date, or that restarts the backfill,
    /// takes no permit.
    pub async fn build_step(
        &self,
        namespace_id: &NamespaceId,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepBuildOutcome> {
        if policy.max_files_per_step.get() > MAX_PAGE_LIMIT as usize {
            return Err(loonfs::Error::InvalidRequest {
                message: format!("max_files_per_step must be at most {MAX_PAGE_LIMIT}"),
                param: "max_files_per_step",
            }
            .into());
        }
        let deadline = OperationClock::start(self.now_ms(), self.timer());
        let Some(current) = self.load_enabled(namespace_id, &deadline).await? else {
            return Ok(GrepBuildOutcome::NotEnabled);
        };
        let reads = self.reads(namespace_id);
        reads.head().await?;
        let planned = match current.manifest_state().status() {
            GrepIndexStatus::Backfilling {
                captured_seq,
                cursor_inode_id,
                pin_id,
            } => match plan_backfill_unit(&reads, pin_id, *captured_seq, *cursor_inode_id, policy)
                .await
            {
                Ok(planned) => planned,
                Err(error) if rebootstrap_required(&error) => {
                    return self
                        .restart_backfill(namespace_id, &current, &deadline)
                        .await;
                }
                Err(error) => return Err(error),
            },
            status @ GrepIndexStatus::Active { .. } => {
                let resume = status
                    .active_watermark()
                    .expect("an active grep status should have a watermark");
                match plan_incremental_unit(&reads, resume, policy).await {
                    Ok(IncrementalWork::Unit(planned)) => planned,
                    Ok(IncrementalWork::UpToDate(resume)) => {
                        return Ok(GrepBuildOutcome::UpToDate {
                            built_through_seq: resume.built_through_seq(),
                        });
                    }
                    Err(error) if rebootstrap_required(&error) => {
                        return self
                            .restart_backfill(namespace_id, &current, &deadline)
                            .await;
                    }
                    Err(error) => return Err(error),
                }
            }
            GrepIndexStatus::Disabled {} => return Ok(GrepBuildOutcome::NotEnabled),
        };
        let _permit = self.step_budget.permit().await;
        let block_cache = GrepBlockCache::new(DecodedBlockCacheConfig::new(
            GREP_STEP_BLOCK_CACHE_DECODED_BYTES,
        ));
        let index = GrepSegments {
            store: &self.store,
            namespace_id,
            block_cache: Some(&block_cache),
        };
        let unit = index_planned_unit(&reads, &index, current.manifest_state().segments(), planned)
            .await?;
        self.publish_build_unit(namespace_id, current, unit, policy, &deadline)
            .await
    }

    async fn publish_build_unit(
        &self,
        namespace_id: &NamespaceId,
        current: LoadedGrepManifest,
        unit: CollectedIndexUnit,
        policy: GramIndexBuildPolicy,
        deadline: &OperationClock,
    ) -> Result<GrepBuildOutcome> {
        let CollectedIndexUnit {
            rows,
            stats,
            progress,
        } = unit;
        let current_run_no = current.manifest_state().index().next_run_no;
        let next_run_no = if rows.is_empty() {
            current_run_no
        } else {
            next_index_run_no(current_run_no)?
        };
        let manifest_no = next_manifest_no(Some(&current))?;
        let segments = GrepSegments {
            store: &self.store,
            namespace_id,
            block_cache: None,
        };
        let new_segments = segments
            .write_run(
                current_run_no,
                INDEX_GRAMS_DELTA_LEVEL,
                rows,
                policy.max_rows_per_segment,
            )
            .await?;
        let segments_written = new_segments.len() as u64;
        let mut manifest_segments = current.manifest_state().segments().to_vec();
        manifest_segments.extend(new_segments);
        // A completed backfill is current through the sequence captured by
        // its checkpoint. The change feed resumes after that sequence.
        let (status, completed_pin_id, built_through_seq) = match progress {
            CollectedProgress::Backfill {
                pin_id,
                captured_seq,
                next_cursor,
            } => match next_cursor {
                Some(after_inode_id) => (
                    GrepIndexStatus::Backfilling {
                        captured_seq,
                        cursor_inode_id: Some(after_inode_id),
                        pin_id,
                    },
                    None,
                    captured_seq,
                ),
                None => (
                    GrepIndexStatus::Active {
                        captured_seq,
                        built_through_seq: captured_seq,
                        next_event_index: 0,
                    },
                    Some(pin_id),
                    captured_seq,
                ),
            },
            CollectedProgress::Incremental {
                built_through_seq,
                next_event_index,
            } => (
                GrepIndexStatus::Active {
                    captured_seq: current
                        .manifest_state()
                        .status()
                        .active_captured_seq()
                        .expect("an incremental unit should continue an active grep status"),
                    built_through_seq,
                    next_event_index,
                },
                None,
                built_through_seq,
            ),
        };
        let next = GrepManifestState::new(
            namespace_id.clone(),
            manifest_no,
            status,
            GrepIndexState {
                reorganize: current.manifest_state().index().reorganize.clone(),
                next_run_no,
            },
            manifest_segments,
        )
        .expect("a successor of a grep manifest that decoding validated should be valid");
        match publish_grep_manifest(&self.store, Some(&current), &next, deadline).await {
            Ok(_) => {
                if let Some(pin_id) = completed_pin_id {
                    self.delete_pin_if_present(namespace_id, &pin_id).await;
                }
                Ok(GrepBuildOutcome::Published {
                    built_through_seq,
                    indexed_revisions: stats.indexed_revisions,
                    skipped_revisions: stats.skipped_revisions,
                    segments_written,
                })
            }
            Err(GrepError::PublicationConflict { .. }) => Ok(GrepBuildOutcome::Superseded),
            Err(error) => Err(error),
        }
    }
}

struct CollectedIndexUnit {
    rows: Vec<IndexRow>,
    stats: IndexingStats,
    progress: CollectedProgress,
}

/// The file revisions one build step examines, and the progress it publishes
/// once their content is indexed.
struct PlannedIndexUnit {
    pending: Vec<PendingRevisionContent>,
    progress: CollectedProgress,
}

enum IncrementalWork {
    Unit(PlannedIndexUnit),
    UpToDate(ChangeFeedResume),
}

struct IncrementalCursor {
    resume: ChangeFeedResume,
}

impl IncrementalCursor {
    fn new(resume: ChangeFeedResume) -> Self {
        Self { resume }
    }

    fn stop_at(
        &mut self,
        change_seq: ChangeSeq,
        event_index: usize,
        event_count: usize,
        after_event: bool,
    ) -> Result<()> {
        if !after_event && event_index == 0 {
            return Ok(());
        }
        let next_index = event_index
            .checked_add(usize::from(after_event))
            .expect("an index below a commit's event count should not overflow");
        let next_event_index = if next_index < event_count {
            u32::try_from(next_index).map_err(|_| {
                CoreError::NamespaceCorrupt(format!(
                    "commit {change_seq} records {event_count} events, more than a grep \
                     cursor can address"
                ))
            })?
        } else {
            0
        };
        self.resume = ChangeFeedResume::new(change_seq, next_event_index);
        Ok(())
    }

    fn finish_change(&mut self, change_seq: ChangeSeq) {
        self.resume = ChangeFeedResume::new(change_seq, 0);
    }
}

#[derive(Default)]
struct IndexingStats {
    /// Revisions whose content value the step read and tokenized.
    indexed_revisions: u64,
    /// Revisions whose content value the step didn't tokenize, because the
    /// value is ineligible or the index has already examined it.
    skipped_revisions: u64,
}

/// The resume state collected by one worker step. Naming the two modes here
/// prevents a backfill cursor from being paired with change-feed progress.
enum CollectedProgress {
    Backfill {
        pin_id: PinId,
        captured_seq: ChangeSeq,
        next_cursor: Option<InodeId>,
    },
    Incremental {
        built_through_seq: ChangeSeq,
        next_event_index: u32,
    },
}

/// Plans one backfill step from the files the checkpoint pins. Reads no file
/// content.
///
/// The enumeration returns the checkpointed state directly — one current
/// revision per retained file, deleted or not, in ascending inode order — so
/// a step reads pages until one of its budgets is met and records the last
/// inode it consumed. The budgets are the ones the step always had: at most
/// `max_files_per_step` files examined, and content planning stops once
/// `max_content_bytes_per_step` is reached (the file that crosses it is
/// still included, exactly as the row walk did). A file over the eligibility
/// cap counts no content bytes, because the step never reads it.
async fn plan_backfill_unit(
    reads: &NamespaceReads,
    pin_id: &PinId,
    captured_seq: ChangeSeq,
    cursor: Option<InodeId>,
    policy: GramIndexBuildPolicy,
) -> Result<PlannedIndexUnit> {
    let mut next_cursor = cursor;
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut files_remaining = policy.max_files_per_step.get();
    let mut cursor = cursor.map(|after_inode_id| CheckpointFilesPageCursor { after_inode_id });
    loop {
        let page = reads
            .list_checkpoint_files(pin_id)
            .page_limit(page_limit(files_remaining))
            .cursor(cursor)
            .page()
            .await?;
        if page.captured_seq != captured_seq {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "checkpoint `{pin_id}` pins sequence `{}` but the grep manifest is \
                 backfilling sequence `{captured_seq}`",
                    page.captured_seq
                ),
            });
        }
        let page_exhausted_family = page.next_cursor.is_none();
        let mut budget_reached = false;
        for file in page.files {
            files_remaining = files_remaining.saturating_sub(1);
            if file.size_bytes <= MAX_INDEX_GRAMS_FILE_BYTES {
                planned_content_bytes += file.size_bytes;
            }
            next_cursor = Some(file.inode_id);
            pending.push(PendingRevisionContent {
                inode_id: file.inode_id,
                revision_no: file.revision_no,
                committed_seq: file.committed_seq,
                content_ref: file.content_ref,
            });
            if planned_content_bytes >= policy.max_content_bytes_per_step.get() {
                budget_reached = true;
                break;
            }
        }
        if budget_reached {
            break;
        }
        if page_exhausted_family {
            // Every file the checkpoint pins is indexed; the next published
            // manifest leaves backfill for the change feed.
            next_cursor = None;
            break;
        }
        cursor = page.next_cursor;
        if files_remaining == 0 {
            break;
        }
    }
    Ok(PlannedIndexUnit {
        pending,
        progress: CollectedProgress::Backfill {
            pin_id: pin_id.clone(),
            captured_seq,
            next_cursor,
        },
    })
}

/// Plans one incremental step from the semantic change feed. Reads no file
/// content.
///
/// `IncrementalWork::UpToDate` means the index is already at the namespace
/// head.
async fn plan_incremental_unit(
    reads: &NamespaceReads,
    resume: ChangeFeedResume,
    policy: GramIndexBuildPolicy,
) -> Result<IncrementalWork> {
    let feed = reads
        .list_changes_after(resume.after_seq(), policy.max_files_per_step.get())
        .await?;
    if feed.changes.is_empty() {
        return Ok(IncrementalWork::UpToDate(resume));
    }
    let mut cursor = IncrementalCursor::new(resume);
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut examined_files = 0usize;
    'changes: for change in feed.changes {
        let start_event_index = resume.start_event_index(change.committed_seq);
        let events = &change.events;
        if start_event_index > events.len() {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "grep event cursor `{}` exceeds commit `{}` length `{}`",
                    resume.next_event_index(),
                    change.committed_seq,
                    events.len()
                ),
            });
        }
        for (event_index, event) in events.iter().enumerate().skip(start_event_index) {
            let Some(revision) = published_revision(event) else {
                continue;
            };
            let content_bytes = Some(revision.content_ref.size_bytes)
                .filter(|size_bytes| *size_bytes <= MAX_INDEX_GRAMS_FILE_BYTES)
                .unwrap_or(0);
            let would_exceed_content_budget = planned_content_bytes > 0
                && planned_content_bytes.saturating_add(content_bytes)
                    > policy.max_content_bytes_per_step.get();
            if examined_files >= policy.max_files_per_step.get() || would_exceed_content_budget {
                cursor.stop_at(change.committed_seq, event_index, events.len(), false)?;
                break 'changes;
            }
            examined_files += 1;
            planned_content_bytes += content_bytes;
            pending.push(PendingRevisionContent {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: change.committed_seq,
                content_ref: revision.content_ref.clone(),
            });
            if examined_files >= policy.max_files_per_step.get()
                || planned_content_bytes >= policy.max_content_bytes_per_step.get()
            {
                cursor.stop_at(change.committed_seq, event_index, events.len(), true)?;
                break 'changes;
            }
        }
        cursor.finish_change(change.committed_seq);
    }
    if cursor.resume == resume {
        return Ok(IncrementalWork::UpToDate(resume));
    }
    Ok(IncrementalWork::Unit(PlannedIndexUnit {
        pending,
        progress: CollectedProgress::Incremental {
            built_through_seq: cursor.resume.built_through_seq(),
            next_event_index: cursor.resume.next_event_index(),
        },
    }))
}

struct PendingRevisionContent {
    inode_id: InodeId,
    revision_no: RevisionNo,
    committed_seq: ChangeSeq,
    content_ref: ContentRef,
}

/// Indexes the revisions a step planned: writes a `content_revisions` row for
/// each revision, and reads and tokenizes each content value that the
/// index's `texts` rows don't record, once however many revisions refer to
/// it.
async fn index_planned_unit<S: ObjectStore + ?Sized>(
    reads: &NamespaceReads,
    index: &GrepSegments<'_, S>,
    segments: &[GrepSegmentRef],
    planned: PlannedIndexUnit,
) -> Result<CollectedIndexUnit> {
    let PlannedIndexUnit { pending, progress } = planned;
    let mut rows = IndexRows::default();
    let mut postings: BTreeMap<Gram, Vec<ContentValue>> = BTreeMap::new();
    let mut values: BTreeMap<ContentValue, (&ContentRef, u64)> = BTreeMap::new();
    for revision in &pending {
        let value = ContentValue::of(&revision.content_ref);
        rows.add_content_revision(IndexRow::content_revision(
            &value,
            revision.inode_id,
            revision.revision_no,
            revision.committed_seq,
        ));
        values.entry(value).or_insert((&revision.content_ref, 0)).1 += 1;
    }
    let examined = index
        .text_rows(
            segments,
            &values.keys().cloned().collect::<BTreeSet<_>>(),
            GREP_WORKER_REQUESTS_IN_FLIGHT,
        )
        .await?;
    let mut stats = IndexingStats::default();
    let mut unread = Vec::new();
    for (value, (content_ref, revisions)) in values {
        if examined.contains_key(&value) {
            stats.skipped_revisions += revisions;
        } else if value.size_bytes > MAX_INDEX_GRAMS_FILE_BYTES {
            stats.skipped_revisions += revisions;
            rows.texts.insert(value, false);
        } else {
            unread.push((value, content_ref, revisions));
        }
    }
    let mut contents = stream::iter(0..unread.len())
        .map(|index| {
            // The eligibility cap is the worker's own read budget, and a value
            // past the cap is recorded as ineligible without a read.
            reads
                .read_content(unread[index].1, MAX_INDEX_GRAMS_FILE_BYTES)
                .map_ok(move |content| (index, content))
        })
        .buffered(GREP_WORKER_REQUESTS_IN_FLIGHT);
    while let Some((index, content)) = contents.try_next().await? {
        let (value, _, revisions) = &unread[index];
        let eligible = is_indexable_text_content(&content);
        if eligible {
            for gram in extract_grams(&content) {
                postings.entry(gram).or_default().push(*value);
            }
            stats.indexed_revisions += revisions;
        } else {
            stats.skipped_revisions += revisions;
        }
        rows.texts.insert(*value, eligible);
    }
    // The step's run lists each value it has a `texts` row for in its own
    // value table, whose rows end the run, and its postings refer to a value
    // by its ordinal in that table.
    let table = ValueTable::new(rows.texts.keys().copied().collect());
    rows.postings = postings
        .into_iter()
        .map(|(gram, values)| {
            let ordinals = values
                .iter()
                .map(|value| {
                    table
                        .ordinal(value)
                        .expect("a step's value table should list every value it read")
                })
                .collect();
            (gram, ordinals)
        })
        .collect();
    let mut rows = rows.into_rows();
    rows.extend(table.rows());
    Ok(CollectedIndexUnit {
        rows,
        stats,
        progress,
    })
}
