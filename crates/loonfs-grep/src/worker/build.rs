//! Grep index builds: planning one backfill or incremental step from the
//! checkpoint or the change feed, reading and indexing its content, and
//! publishing the result.

use super::reorganize::INDEX_GRAMS_DELTA_LEVEL;
use super::{
    next_index_run_no, next_manifest_no, rebootstrap_required, GramIndexBuildPolicy,
    GrepBuildOutcome, GrepWorker, GREP_WORKER_REQUESTS_IN_FLIGHT,
};
use crate::codec::{
    extract_grams, is_indexable_text_content, Gram, GramPosting, IndexRow,
    MAX_INDEX_GRAMS_FILE_BYTES,
};
use crate::manifest::{
    publish_grep_manifest, ChangeFeedResume, GrepIndexState, GrepIndexStatus, GrepManifestState,
    LoadedGrepManifest,
};
use crate::reads::{page_limit, published_revision, NamespaceReads};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use loonfs::engine::OperationClock;
use loonfs::{CheckpointFilesPageCursor, CoreError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    ChangeSeq, ContentRef, InodeId, NamespaceId, PinId, RevisionNo, MAX_PAGE_LIMIT,
};
use std::collections::BTreeMap;

const GRAM_POSTING_BATCH_TARGET: usize = 256;

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
        let unit = index_planned_unit(&reads, planned).await?;
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
            postings,
            stats,
            progress,
        } = unit;
        let rows = gram_postings_rows(postings);
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
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    stats: IndexingStats,
    progress: CollectedProgress,
}

/// The file content one build step reads, and the progress it publishes
/// once that content is indexed.
struct PlannedIndexUnit {
    pending: Vec<PendingRevisionContent>,
    stats: IndexingStats,
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
    indexed_revisions: u64,
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
/// still included, exactly as the row walk did).
async fn plan_backfill_unit(
    reads: &NamespaceReads,
    pin_id: &PinId,
    captured_seq: ChangeSeq,
    cursor: Option<InodeId>,
    policy: GramIndexBuildPolicy,
) -> Result<PlannedIndexUnit> {
    let mut stats = IndexingStats::default();
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
            if file.size_bytes > MAX_INDEX_GRAMS_FILE_BYTES {
                stats.skipped_revisions += 1;
            } else {
                planned_content_bytes += file.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id: file.inode_id,
                    revision_no: file.revision_no,
                    content_ref: file.content_ref,
                });
            }
            next_cursor = Some(file.inode_id);
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
        stats,
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
    let mut stats = IndexingStats::default();
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
            let would_exceed_content_budget = planned_content_bytes > 0
                && planned_content_bytes.saturating_add(revision.content_ref.size_bytes)
                    > policy.max_content_bytes_per_step.get();
            if examined_files >= policy.max_files_per_step.get() || would_exceed_content_budget {
                cursor.stop_at(change.committed_seq, event_index, events.len(), false)?;
                break 'changes;
            }
            examined_files += 1;
            if revision.content_ref.size_bytes > MAX_INDEX_GRAMS_FILE_BYTES {
                stats.skipped_revisions += 1;
            } else {
                planned_content_bytes += revision.content_ref.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    content_ref: revision.content_ref.clone(),
                });
            }
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
        stats,
        progress: CollectedProgress::Incremental {
            built_through_seq: cursor.resume.built_through_seq(),
            next_event_index: cursor.resume.next_event_index(),
        },
    }))
}

struct PendingRevisionContent {
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
}

/// Reads the file content a step planned and extracts its grams.
async fn index_planned_unit(
    reads: &NamespaceReads,
    planned: PlannedIndexUnit,
) -> Result<CollectedIndexUnit> {
    let PlannedIndexUnit {
        pending,
        mut stats,
        progress,
    } = planned;
    let mut postings: BTreeMap<Gram, Vec<GramPosting>> = BTreeMap::new();
    let mut contents = stream::iter(0..pending.len())
        .map(|index| {
            let revision = &pending[index];
            // Index eligibility is the worker's own read budget: content
            // past the cap was skipped before it was ever planned.
            reads
                .read_content(&revision.content_ref, MAX_INDEX_GRAMS_FILE_BYTES)
                .map_ok(move |content| (revision, content))
        })
        .buffered(GREP_WORKER_REQUESTS_IN_FLIGHT);
    while let Some((revision, content)) = contents.try_next().await? {
        if !is_indexable_text_content(&content) {
            stats.skipped_revisions += 1;
            continue;
        }
        let posting = GramPosting {
            inode_id: revision.inode_id,
            revision_no: revision.revision_no,
        };
        for gram in extract_grams(&content) {
            postings.entry(gram).or_default().push(posting);
        }
        stats.indexed_revisions += 1;
    }
    Ok(CollectedIndexUnit {
        postings,
        stats,
        progress,
    })
}

pub(super) fn gram_postings_rows(postings: BTreeMap<Gram, Vec<GramPosting>>) -> Vec<IndexRow> {
    let mut rows = Vec::new();
    for (gram, mut gram_postings) in postings {
        gram_postings.sort_unstable();
        gram_postings.dedup();
        for batch in gram_postings.chunks(GRAM_POSTING_BATCH_TARGET) {
            rows.push(
                IndexRow::gram_postings(gram, batch)
                    .expect("a non-empty gram postings batch should encode"),
            );
        }
    }
    rows
}
