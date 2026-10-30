//! Seq-scoped metadata lookups served from manifest segments via verified
//! scans.

use super::row_decode::{
    access_revision_from_manifest_row, active_deletion_from_manifest_row,
    attributes_revision_from_manifest_row, commit_from_manifest_row,
    commit_receipt_from_manifest_row, direntry_binding_from_manifest_row, inode_from_manifest_row,
    revision_from_manifest_row, tombstone_from_manifest_row,
};
use crate::error::Result;
use crate::metadata::{
    AccessRevisionRecord, ActiveDeletionRecord, AttributesRevisionRecord, CommitReceiptRecord,
    ContentLayoutRecord, DirentryBindingRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord,
};
use crate::segment::{Readahead, VerifiedMetadataSegments};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys;
use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::format::wal::WalCommitPayload;
use loonfs_types::{ChangeSeq, CommitId, InodeId, NameKey, RevisionNo};

pub(super) async fn inode_at_seq<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>> {
    let key = lookup_keys::inode_key(inode_id);
    segments
        .find_row(MetadataRowFamily::Inodes, &key, &key)
        .await?
        .map(inode_from_manifest_row)
        .transpose()
}

pub(super) async fn commit_at_seq<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    seq: ChangeSeq,
) -> Result<Option<WalCommitPayload>> {
    let key = lookup_keys::commit_row_key(seq);
    segments
        .find_row(MetadataRowFamily::Commits, &key, &key)
        .await?
        .map(commit_from_manifest_row)
        .transpose()
}

pub(crate) async fn commits_after_page<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    after_seq: ChangeSeq,
    limit: usize,
) -> Result<Vec<WalCommitPayload>> {
    segments
        .scan_range_page(
            MetadataRowFamily::Commits,
            &lookup_keys::after_row_key(&lookup_keys::commit_row_key(after_seq)),
            string_prefix_upper_bound(lookup_keys::COMMIT_ROW_PREFIX).as_deref(),
            limit,
        )
        .await?
        .into_iter()
        .map(commit_from_manifest_row)
        .collect()
}

/// One page of a directory's `direntry_binds` rows from `lower_bound`, with
/// each row's stored key.
///
/// The scan probes each segment's filter with the parent's probe, so it
/// skips the segments of a run that has no row under the parent.
pub(crate) async fn direntry_binds_page<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    parent_inode_id: InodeId,
    lower_bound: &str,
    limit: usize,
) -> Result<Vec<(String, DirentryBindingRecord)>> {
    let upper_bound =
        string_prefix_upper_bound(&lookup_keys::direntry_parent_prefix(parent_inode_id));
    segments
        .scan_range_page_with_keys_for_lookup(
            MetadataRowFamily::DirentryBinds,
            lower_bound,
            upper_bound.as_deref(),
            limit,
            &lookup_keys::direntry_parent_probe(parent_inode_id),
        )
        .await?
        .into_iter()
        .map(|(row_key, row)| Ok((row_key, direntry_binding_from_manifest_row(row)?)))
        .collect()
}

/// The first row at or below `visible_seq` of each edge of one directory
/// slot, which the slot's value at that sequence is among.
pub(super) async fn direntry_binds_for_parent_name<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    parent_inode_id: InodeId,
    name_key: &NameKey,
    visible_seq: ChangeSeq,
) -> Result<Vec<DirentryBindingRecord>> {
    newest_edge_rows(
        segments,
        MetadataRowFamily::DirentryBinds,
        &lookup_keys::direntry_bind_prefix(parent_inode_id, name_key),
        &lookup_keys::direntry_bind_probe(parent_inode_id, name_key),
        slot_edge_end,
        Readahead::Disabled,
        visible_seq,
    )
    .await
}

/// The first binding event at or below `visible_seq` of each edge of one
/// child, which the child's binding at that sequence is among.
pub(super) async fn direntry_binds_for_child<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    child_inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Result<Vec<DirentryBindingRecord>> {
    newest_edge_rows(
        segments,
        MetadataRowFamily::DirentryChildBinds,
        &lookup_keys::direntry_child_prefix(child_inode_id),
        &lookup_keys::direntry_child_probe(child_inode_id),
        child_edge_end,
        Readahead::Enabled,
        visible_seq,
    )
    .await
}

/// Rows one page of an edge walk reads. An edge's first row at or below the
/// read's sequence is usually the edge's first row, so a page covers a few
/// edges, and an edge with more rows costs one more page.
const EDGE_WALK_PAGE_ROWS: usize = 16;

/// Returns the key just past every row of `record`'s edge in the slot index,
/// where a scan that has read the edge's value resumes.
pub(crate) fn slot_edge_end(record: &DirentryBindingRecord) -> String {
    let edge_prefix = lookup_keys::direntry_bind_edge_prefix(
        record.parent_inode_id,
        &record.name_key,
        record.child_inode_id,
    );
    string_prefix_upper_bound(&edge_prefix).expect("an edge prefix should end in a hyphen")
}

/// Returns the key just past every row of `record`'s edge in the child index.
fn child_edge_end(record: &DirentryBindingRecord) -> String {
    let edge_prefix = lookup_keys::direntry_child_bind_edge_prefix(
        record.child_inode_id,
        record.parent_inode_id,
        &record.name_key,
    );
    string_prefix_upper_bound(&edge_prefix).expect("an edge prefix should end in a hyphen")
}

/// The first row at or below `visible_seq` of each edge under `prefix`, in
/// key order.
///
/// Both binding families sort an edge's rows newest first, so the walk takes
/// an edge's first row at or below the sequence and then starts its next
/// page past the edge, without reading the edge's older rows. A file whose
/// edge has ten thousand summary rows costs one row to resolve.
///
/// A read that sees every row of the manifest reads the newest run that has
/// a row in the remaining range, as
/// [`VerifiedMetadataSegments::scan_prefix_in_newest_run_for_lookup`]
/// describes, and a read below a run's sequence merges every run.
async fn newest_edge_rows<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    family: MetadataRowFamily,
    prefix: &str,
    filter_probe: &str,
    edge_end: fn(&DirentryBindingRecord) -> String,
    readahead: Readahead,
    visible_seq: ChangeSeq,
) -> Result<Vec<DirentryBindingRecord>> {
    let newest_run_only = segments.every_row_is_visible_at(visible_seq);
    let upper_bound = string_prefix_upper_bound(prefix);
    let mut lower_bound = prefix.to_owned();
    let mut edges: Vec<DirentryBindingRecord> = Vec::new();
    loop {
        let page = if newest_run_only {
            segments
                .scan_prefix_in_newest_run_for_lookup(
                    family,
                    prefix,
                    &lower_bound,
                    filter_probe,
                    EDGE_WALK_PAGE_ROWS,
                    readahead,
                )
                .await?
        } else {
            segments
                .scan_range_page_with_keys_for_lookup(
                    family,
                    &lower_bound,
                    upper_bound.as_deref(),
                    EDGE_WALK_PAGE_ROWS,
                    filter_probe,
                )
                .await?
        };
        let full = page.len() == EDGE_WALK_PAGE_ROWS;
        let Some(mut next_lower_bound) = page
            .last()
            .map(|(row_key, _)| lookup_keys::after_row_key(row_key))
        else {
            break;
        };
        let mut read_edge_end: Option<String> = None;
        for (row_key, row) in page {
            if read_edge_end
                .as_deref()
                .is_some_and(|end| row_key.as_str() < end)
            {
                continue;
            }
            let record = direntry_binding_from_manifest_row(row)?;
            if record.committed_seq > visible_seq {
                continue;
            }
            read_edge_end = Some(edge_end(&record));
            edges.push(record);
        }
        if !full {
            break;
        }
        if let Some(end) = read_edge_end {
            next_lower_bound = next_lower_bound.max(end);
        }
        lower_bound = next_lower_bound;
    }
    Ok(edges)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RevisionPagePosition {
    pub(super) revision_no: RevisionNo,
    pub(super) committed_seq: ChangeSeq,
    pub(super) revision_delta_index: u32,
}

impl RevisionPagePosition {
    pub(crate) fn after(
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        revision_delta_index: u32,
    ) -> Self {
        Self {
            revision_no,
            committed_seq,
            revision_delta_index,
        }
    }
}

/// The inode's revision with the highest number in the manifest, whatever
/// the read's sequence, as [`super::MetadataView::latest_revision_record`]
/// expects.
pub(super) async fn latest_revision_by_inode<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
) -> Result<Option<RevisionRecord>> {
    let prefix = lookup_keys::revision_prefix(inode_id);
    segments
        .scan_prefix_in_newest_run_for_lookup(
            MetadataRowFamily::Revisions,
            &prefix,
            &prefix,
            &lookup_keys::revision_probe(inode_id),
            1,
            Readahead::Enabled,
        )
        .await?
        .into_iter()
        .next()
        .map(|(_, row)| revision_from_manifest_row(row))
        .transpose()
}

pub(super) async fn revision_by_inode_no<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Option<RevisionRecord>> {
    let exact_prefix = lookup_keys::revision_no_prefix(inode_id, revision_no);
    let filter_probe = lookup_keys::revision_probe(inode_id);
    segments
        .scan_range_page_for_lookup(
            MetadataRowFamily::Revisions,
            &exact_prefix,
            string_prefix_upper_bound(&exact_prefix).as_deref(),
            1,
            &filter_probe,
        )
        .await?
        .into_iter()
        .next()
        .map(revision_from_manifest_row)
        .transpose()
}

pub(super) async fn revisions_by_inode_page_desc<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
    start_after: Option<RevisionPagePosition>,
    limit: usize,
) -> Result<Vec<RevisionRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let inode_prefix = lookup_keys::revision_prefix(inode_id);
    let filter_probe = lookup_keys::revision_probe(inode_id);
    let lower_bound = start_after
        .map(|position| {
            lookup_keys::after_row_key(&lookup_keys::revision_row_key(
                inode_id,
                position.revision_no,
                position.committed_seq,
                position.revision_delta_index,
            ))
        })
        .unwrap_or_else(|| inode_prefix.clone());
    segments
        .scan_range_page_for_lookup(
            MetadataRowFamily::Revisions,
            &lower_bound,
            string_prefix_upper_bound(&inode_prefix).as_deref(),
            limit,
            &filter_probe,
        )
        .await?
        .into_iter()
        .map(revision_from_manifest_row)
        .collect()
}

/// One key-ordered page of the derived active-deletion family, with each
/// row's stored key. The trash listing's whole durable read: the family is
/// keyed by deletion position, so a page is a range scan whose cost follows
/// the page, not the namespace's deletion history.
pub(super) async fn active_deletions_page<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    lower_bound: &str,
    upper_bound: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, ActiveDeletionRecord)>> {
    segments
        .scan_range_page_with_keys(
            MetadataRowFamily::ActiveDeletions,
            lower_bound,
            upper_bound,
            limit,
        )
        .await?
        .into_iter()
        .map(|(row_key, row)| Ok((row_key, active_deletion_from_manifest_row(row)?)))
        .collect()
}

/// The root's newest tombstone event at or below `visible_seq`, which
/// decides whether the root hides its subtree at that sequence.
pub(super) async fn newest_tombstone_for_root<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    root_inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Result<Option<SubtreeTombstoneRecord>> {
    newest_row_by_inode(
        segments,
        MetadataRowFamily::Tombstones,
        lookup_keys::tombstone_prefix(root_inode_id),
        lookup_keys::tombstone_probe(root_inode_id),
        visible_seq,
        tombstone_from_manifest_row,
        |record| record.committed_seq,
    )
    .await
}

pub(super) async fn content_layout<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    content_id: &loonfs_types::ContentId,
) -> Result<Option<ContentLayoutRecord>> {
    first_content_layout(
        segments,
        &lookup_keys::content_layout_prefix(content_id),
        &lookup_keys::content_layout_probe(content_id),
    )
    .await
}

const CONTENT_LAYOUT_PAGE_ROWS: usize = 8;

async fn first_content_layout<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    prefix: &str,
    probe: &str,
) -> Result<Option<ContentLayoutRecord>> {
    let upper_bound = string_prefix_upper_bound(prefix);
    let rows = segments
        .scan_range_page_with_keys_for_lookup(
            MetadataRowFamily::ContentLayouts,
            prefix,
            upper_bound.as_deref(),
            CONTENT_LAYOUT_PAGE_ROWS,
            probe,
        )
        .await?;
    rows.into_iter()
        .next()
        .map(|(_, row)| super::row_decode::content_layout_from_manifest_row(row))
        .transpose()
}

pub(super) async fn commit_receipt<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    commit_id: &CommitId,
) -> Result<Option<CommitReceiptRecord>> {
    let filter_probe = lookup_keys::commit_receipt_probe(commit_id);
    let prefix = lookup_keys::commit_receipt_prefix(commit_id);
    let receipts: Vec<CommitReceiptRecord> = segments
        .scan_prefix_for_lookup(
            MetadataRowFamily::CommitReceipts,
            &prefix,
            &filter_probe,
            Readahead::Disabled,
        )
        .await?
        .into_iter()
        .map(commit_receipt_from_manifest_row)
        .collect::<Result<_>>()?;
    Ok(receipts
        .into_iter()
        .max_by_key(|receipt| receipt.committed_seq))
}

/// Rows that one attribute lookup below a run's sequence fetches per
/// manifest round-trip, as it walks past the revisions committed after its
/// sequence.
const WHOLE_STATE_RAW_SCAN_LIMIT: usize = 16;

pub(super) async fn attributes_by_inode<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Result<Option<AttributesRevisionRecord>> {
    newest_row_by_inode(
        segments,
        MetadataRowFamily::Attributes,
        lookup_keys::attributes_prefix(inode_id),
        lookup_keys::attributes_probe(inode_id),
        visible_seq,
        attributes_revision_from_manifest_row,
        |record| record.committed_seq,
    )
    .await
}

pub(super) async fn access_by_inode<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Result<Option<AccessRevisionRecord>> {
    newest_row_by_inode(
        segments,
        MetadataRowFamily::Access,
        lookup_keys::access_prefix(inode_id),
        lookup_keys::access_probe(inode_id),
        visible_seq,
        access_revision_from_manifest_row,
        |record| record.committed_seq,
    )
    .await
}

/// The newest row of `inode_id` at or below `visible_seq` in a family whose
/// keys sort one inode's rows newest first (a whole-state family, which
/// inverts the revision, the sequence, and the delta index, or
/// `tombstones`, which inverts the sequence and the delta index), so the
/// first row at or below the sequence is the result.
///
/// A read that sees every row of the manifest takes the first row of the
/// newest run that has one. A read below a run's sequence merges every run,
/// because the newest run's rows could all be above the read.
async fn newest_row_by_inode<S, R>(
    segments: &VerifiedMetadataSegments<'_, S>,
    family: MetadataRowFamily,
    prefix: String,
    filter_probe: String,
    visible_seq: ChangeSeq,
    decode: fn(MetadataRow) -> Result<R>,
    committed_seq: fn(&R) -> ChangeSeq,
) -> Result<Option<R>>
where
    S: ObjectStore + ?Sized,
{
    if segments.every_row_is_visible_at(visible_seq) {
        return segments
            .scan_prefix_in_newest_run_for_lookup(
                family,
                &prefix,
                &prefix,
                &filter_probe,
                1,
                Readahead::Enabled,
            )
            .await?
            .into_iter()
            .next()
            .map(|(_, row)| decode(row))
            .transpose();
    }
    let upper_bound = string_prefix_upper_bound(&prefix);
    let mut lower_bound = prefix;
    loop {
        let page = segments
            .scan_range_page_for_lookup(
                family,
                &lower_bound,
                upper_bound.as_deref(),
                WHOLE_STATE_RAW_SCAN_LIMIT,
                &filter_probe,
            )
            .await?;
        let page_len = page.len();
        let last_row_key = page.last().map(|row| row.row_key_for_family(family));
        for row in page {
            let record = decode(row)?;
            if committed_seq(&record) <= visible_seq {
                return Ok(Some(record));
            }
        }
        if page_len < WHOLE_STATE_RAW_SCAN_LIMIT {
            return Ok(None);
        }
        let Some(last_row_key) = last_row_key else {
            return Ok(None);
        };
        lower_bound = lookup_keys::after_row_key(&last_row_key);
    }
}
