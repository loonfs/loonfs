//! The pruning of a reorganization into the base level: the filters of the
//! revisions it keeps, which its first step builds from one scan of the
//! namespace, and the re-reference check before its last step publishes.

use super::tables::{TableReader, Translation};
use crate::codec::{ContentValue, IndexRow};
use crate::entry::{GrepPruning, GrepReorganizeState};
use crate::reads::{published_revision, NamespaceReads};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::format::segment_blocks::{SegmentFilter, SegmentFilterBuilder};
use loonfs_types::{u64_from_usize, CommitSeq, ErrorCode, InodeId, RevisionNo, RunNo};
use std::collections::{BTreeMap, BTreeSet};

/// Bytes of the revision filter's payload in one filter row, half of a data
/// block's target size.
const FILTER_ROW_PAYLOAD_BYTES: usize = 32 * 1024;

/// The most bytes that one pruning filter takes. A filter for more revisions
/// than this size holds at ten bits each has more false positives, so the
/// reorganization keeps more rows than the namespace retains.
const MAX_PRUNING_FILTER_BYTES: usize = 64 * 1024 * 1024;

/// The revisions that a reorganization into the base level keeps, as Bloom
/// filters, so they can err only toward keeping a row, and a later
/// reorganization drops what they kept.
pub(super) struct RetainedReferences {
    /// The content values that the revisions refer to. The first step builds
    /// it and decides the output run's value table from it, and later steps
    /// read that table instead.
    values: Option<SegmentFilter>,
    /// The revisions' `(inode_id, revision_no)` pairs.
    revisions: SegmentFilter,
    /// The payload of `revisions`, which the first step writes as the output
    /// run's filter rows.
    revision_payload: Option<Vec<u8>>,
}

impl RetainedReferences {
    /// The filters of a later step: the revision filter that the first step
    /// wrote in the output run.
    pub(super) fn written(revisions: SegmentFilter) -> Self {
        Self {
            values: None,
            revisions,
            revision_payload: None,
        }
    }

    /// Whether the value filter may contain `value`. A later step has no
    /// value filter, because it decides no values, and keeps every value.
    pub(super) fn keeps_value(&self, value: &ContentValue) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.may_contain(&value.filter_key()))
    }

    pub(super) fn keeps_revision(&self, inode_id: InodeId, revision_no: RevisionNo) -> bool {
        self.revisions
            .may_contain(&revision_filter_key(inode_id, revision_no))
    }

    /// The filter rows that the first step writes, in chunk order, or none in
    /// a later step.
    pub(super) fn filter_rows(&self) -> Vec<IndexRow> {
        self.revision_payload
            .iter()
            .flat_map(|payload| payload.chunks(FILTER_ROW_PAYLOAD_BYTES))
            .zip(0..)
            .map(|(chunk, number)| IndexRow::filter(number, chunk))
            .collect()
    }
}

/// Keys a revision by its inode and revision number, which is how a
/// `content_revisions` row identifies the revision it indexes.
fn revision_filter_key(inode_id: InodeId, revision_no: RevisionNo) -> String {
    format!("{}-{:020}", inode_id.body(), revision_no.0)
}

/// Builds the pruning filters of the first step of a reorganization into the
/// base level from one scan of the revisions that it keeps, and records in
/// `reorganize` the sequence and the floor that the filters describe.
///
/// The scan reads the namespace at its head, including the WAL tail,
/// because a build step indexes commits that aren't folded yet, and a filter
/// without them would drop a value that only an unfolded commit refers to.
/// Each filter is sized for the inputs' rows, an upper bound on the
/// revisions that the inputs index, and has at least
/// `MIN_KEEP_FILTER_BITS`, because a reorganization keeps every row that a
/// filter matches.
pub(super) async fn first_step_filters(
    reads: &NamespaceReads,
    reorganize: &mut GrepReorganizeState,
    inputs: &[ExtensionSegmentRef],
) -> Result<RetainedReferences> {
    let retention_floor_seq = reads.head().await?.retention_floor_seq;
    let view = reads.service_view().await?;
    let filter_seq = view.head_seq();
    let pruning = GrepPruning {
        filter_seq,
        retention_floor_seq: retention_floor_seq.min(filter_seq),
    };
    let expected_keys = inputs
        .iter()
        .map(|segment| usize::try_from(segment.row_count).unwrap_or(usize::MAX))
        .fold(0, usize::saturating_add)
        .min(MAX_PRUNING_FILTER_BYTES * 8 / 10);
    let mut values = SegmentFilterBuilder::keep_filter(expected_keys);
    let mut revisions = SegmentFilterBuilder::keep_filter(expected_keys);
    // The listing orders each file's revisions newest first, so the first
    // revision at or below the floor is the file's state at the floor.
    let mut floor_state: Option<(InodeId, bool)> = None;
    let mut cursor = None;
    loop {
        let page = view.retained_revisions_page(cursor).await?;
        for revision in &page.revisions {
            let kept_floor_state = match floor_state {
                Some((inode_id, kept)) if inode_id == revision.inode_id => kept,
                _ => false,
            };
            if revision.committed_seq <= pruning.retention_floor_seq {
                if kept_floor_state {
                    continue;
                }
                floor_state = Some((revision.inode_id, true));
            } else {
                floor_state = Some((revision.inode_id, false));
            }
            values.insert(&ContentValue::of(&revision.content_ref).filter_key());
            revisions.insert(&revision_filter_key(
                revision.inode_id,
                revision.revision_no,
            ));
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    reorganize.pruning = Some(pruning);
    let revision_payload = revisions.finish();
    Ok(RetainedReferences {
        values: Some(decode_filter(&values.finish())?),
        revisions: decode_filter(&revision_payload)?,
        revision_payload: Some(revision_payload),
    })
}

/// Reads back a filter that this module built.
fn decode_filter(payload: &[u8]) -> Result<SegmentFilter> {
    SegmentFilter::from_payload(payload).map_err(|error| GrepError::IndexCorrupt {
        message: format!("a pruning filter doesn't decode: {error}"),
    })
}

/// Reassembles the revision filter that `rows`, the filter rows of a
/// reorganization's output run, store, which `object_key` describes in an
/// error. Returns `None` when there are no rows.
pub(super) fn filter_from_rows(
    object_key: &str,
    mut rows: Vec<(u64, Vec<u8>)>,
) -> Result<Option<SegmentFilter>> {
    if rows.is_empty() {
        return Ok(None);
    }
    rows.sort_by_key(|(chunk, _)| *chunk);
    let mut payload = Vec::new();
    for (position, (chunk, bytes)) in rows.into_iter().enumerate() {
        if chunk != u64_from_usize(position) {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "the revision filter of `{object_key}` has chunk {chunk} out of place"
                ),
            });
        }
        payload.extend_from_slice(&bytes);
    }
    SegmentFilter::from_payload(&payload)
        .map(Some)
        .map_err(|error| GrepError::IndexCorrupt {
            message: format!("the revision filter of `{object_key}` doesn't decode: {error}"),
        })
}

/// Runs the re-reference check of a reorganization's last step, and returns
/// whether the reorganization must be abandoned: a commit after the filters'
/// sequence refers to a value that the reorganization drops, or the
/// retention floor passed that sequence.
///
/// A value is dropped when an input run's value table lists it and the
/// run's translation drops it, because a run's table lists every value that
/// the run has a `texts` row for. The check reads each input run's table from
/// its first value beside the sorted values that the commits refer to, a
/// block at a time, as a translation does, and stops after the last of
/// those values.
pub(super) async fn re_reference_check_fails<S: ObjectStore + ?Sized>(
    reads: &NamespaceReads,
    reorganize: &GrepReorganizeState,
    segments: &GrepSegments<'_, S>,
    translations: &BTreeMap<RunNo, Translation>,
) -> Result<bool> {
    let Some(pruning) = reorganize.pruning else {
        return Ok(false);
    };
    let values = match values_since(reads, pruning.filter_seq).await {
        Ok(values) => values,
        Err(error) if error.code() == ErrorCode::BelowRetentionFloor => return Ok(true),
        Err(error) => return Err(error),
    };
    for (run_no, translation) in translations {
        let mut table = TableReader::new(*run_no, &translation.segments, 0);
        let mut wanted = values.iter().peekable();
        while wanted.peek().is_some() {
            let Some((ordinal, value)) = table.next(segments).await? else {
                break;
            };
            while wanted.next_if(|wanted| **wanted < value).is_some() {}
            if wanted.next_if_eq(&&value).is_some() && translation.output(ordinal) == Some(None) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Reads the change feed after `filter_seq`, and returns the content values
/// of its `file_created` and `content_changed` events.
///
/// A build step that indexes such a commit finds the value's `texts` row in
/// the reorganization's inputs and skips the value, so if the reorganization
/// published without the value, the value would leave the index with no
/// indexing work left to bring it back.
async fn values_since(
    reads: &NamespaceReads,
    filter_seq: CommitSeq,
) -> Result<BTreeSet<ContentValue>> {
    let mut values = BTreeSet::new();
    let mut after_seq = filter_seq;
    loop {
        let feed = reads
            .list_changes_after(after_seq, loonfs_types::MAX_PAGE_LIMIT as usize)
            .await?;
        values.extend(
            feed.changes
                .iter()
                .flat_map(|change| &change.events)
                .filter_map(published_revision)
                .map(|revision| ContentValue::of(revision.content_ref)),
        );
        match feed.next_after_seq {
            Some(next_after_seq) => after_seq = next_after_seq,
            None => return Ok(values),
        }
    }
}
