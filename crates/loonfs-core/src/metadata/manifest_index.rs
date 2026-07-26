//! Seq-scoped metadata lookups answered from manifest tables via verified
//! scans.

use super::row_decode::{
    commit_receipt_from_manifest_row, direntry_bind_from_manifest_row,
    direntry_unbind_from_manifest_row, inode_from_manifest_row, revision_from_manifest_row,
    tombstone_from_manifest_row,
};
use crate::checkpoint::{
    string_prefix_upper_bound, ManifestLoadError, Readahead, VerifiedMetadataTables,
};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::metadata::{
    unbind_matches_binding, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::lookup_keys;
use loonfs_api::wire::manifest::MetadataTableFamily;
use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey, RevisionNo};
use loonfs_objectstore::ObjectStore;

pub(super) fn manifest_error_to_core(error: ManifestLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
}

pub(super) async fn inode_at_seq<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>> {
    let key = lookup_keys::inode_key(inode_id);
    tables
        .get_for_lookup(MetadataTableFamily::Inodes, &key, &key)
        .await
        .map_err(manifest_error_to_core)?
        .map(inode_from_manifest_row)
        .transpose()
}

pub(super) struct ManifestDirentryBindCandidate {
    pub(super) row_key: String,
    pub(super) record: DirentryBindRecord,
}

pub(super) async fn direntry_binds_for_parent_name_key_page<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
    start_after_name_key: Option<&str>,
    start_after_row_key: Option<&str>,
    limit: usize,
) -> Result<Vec<ManifestDirentryBindCandidate>> {
    let parent_prefix = lookup_keys::direntry_parent_prefix(parent_inode_id);
    let lower_bound = if let Some(row_key) = start_after_row_key {
        resume_after_row_key(row_key)
    } else if let Some(name_key) = start_after_name_key {
        let exact_name_prefix = lookup_keys::direntry_bind_prefix(parent_inode_id, name_key);
        string_prefix_upper_bound(&exact_name_prefix).unwrap_or(exact_name_prefix)
    } else {
        parent_prefix.clone()
    };
    let upper_bound = string_prefix_upper_bound(&parent_prefix);
    tables
        .scan_range_page_with_keys(
            MetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            limit,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(|(row_key, row)| {
            Ok(ManifestDirentryBindCandidate {
                record: direntry_bind_from_manifest_row(row)?,
                row_key,
            })
        })
        .collect()
}

pub(super) async fn direntry_binds_for_parent_name<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<Vec<DirentryBindRecord>> {
    let filter_probe = lookup_keys::direntry_bind_probe(parent_inode_id, name_key.as_str());
    let prefix = lookup_keys::direntry_bind_prefix(parent_inode_id, name_key.as_str());
    tables
        .scan_prefix_for_lookup(
            MetadataTableFamily::DirentryBinds,
            &prefix,
            &filter_probe,
            Readahead::Disabled,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(direntry_bind_from_manifest_row)
        .collect()
}

pub(super) async fn direntry_binds_for_child<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    child_inode_id: InodeId,
) -> Result<Vec<DirentryBindRecord>> {
    let filter_probe = lookup_keys::direntry_child_probe(child_inode_id);
    let prefix = lookup_keys::direntry_child_prefix(child_inode_id);
    tables
        .scan_prefix_for_lookup(
            MetadataTableFamily::DirentryChildBinds,
            &prefix,
            &filter_probe,
            Readahead::Enabled,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(direntry_bind_from_manifest_row)
        .collect()
}

pub(super) async fn direntry_unbinds_for_binding<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    direntry: &DirentryBindRecord,
) -> Result<Vec<DirentryUnbindRecord>> {
    let filter_probe =
        lookup_keys::direntry_unbind_probe(direntry.parent_inode_id, direntry.name_key.as_str());
    let prefix = lookup_keys::direntry_unbind_binding_prefix(
        direntry.parent_inode_id,
        direntry.name_key.as_str(),
        direntry.bind_seq,
        direntry.bind_delta_index,
    );
    let unbinds: Vec<DirentryUnbindRecord> = tables
        .scan_prefix_for_lookup(
            MetadataTableFamily::DirentryUnbinds,
            &prefix,
            &filter_probe,
            Readahead::Disabled,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(direntry_unbind_from_manifest_row)
        .collect::<Result<_>>()?;
    Ok(unbinds
        .into_iter()
        .filter(|unbind| unbind_matches_binding(unbind, direntry))
        .collect())
}

/// Every unbind row for `parent_inode_id` whose name key falls in
/// `[first_name_key, last_name_key]`, paged internally to completeness: the
/// caller treats absence from the result as "no unbind exists" for names in
/// the range, so a partial scan would be a correctness bug, not a slow path.
pub(super) async fn direntry_unbinds_for_parent_name_range<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
    first_name_key: &NameKey,
    last_name_key: &NameKey,
) -> Result<Vec<DirentryUnbindRecord>> {
    const UNBIND_RANGE_SCAN_LIMIT: usize = 512;
    let mut lower_bound =
        lookup_keys::direntry_unbind_name_prefix(parent_inode_id, first_name_key.as_str());
    let last_name_prefix =
        lookup_keys::direntry_unbind_name_prefix(parent_inode_id, last_name_key.as_str());
    let upper_bound = string_prefix_upper_bound(&last_name_prefix);

    let mut unbinds = Vec::new();
    loop {
        let page = tables
            .scan_range_page(
                MetadataTableFamily::DirentryUnbinds,
                &lower_bound,
                upper_bound.as_deref(),
                UNBIND_RANGE_SCAN_LIMIT,
            )
            .await
            .map_err(manifest_error_to_core)?;
        let page_len = page.len();
        let last_row_key = page
            .last()
            .map(|row| row.row_key_for_family(MetadataTableFamily::DirentryUnbinds));
        for row in page {
            unbinds.push(direntry_unbind_from_manifest_row(row)?);
        }
        if page_len < UNBIND_RANGE_SCAN_LIMIT {
            break;
        }
        let Some(last_row_key) = last_row_key else {
            break;
        };
        lower_bound = resume_after_row_key(&last_row_key);
    }
    Ok(unbinds)
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

pub(super) async fn latest_revision_for_inode<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
) -> Result<Option<RevisionRecord>> {
    Ok(revisions_for_inode_page_desc(tables, inode_id, None, 1)
        .await?
        .into_iter()
        .next())
}

pub(super) async fn revision_for_inode_no<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Option<RevisionRecord>> {
    let exact_prefix = revision_by_inode_desc_exact_revision_prefix(inode_id, revision_no);
    let filter_probe = revision_by_inode_desc_filter_probe(inode_id);
    tables
        .scan_range_page_for_lookup(
            MetadataTableFamily::RevisionsByInodeDesc,
            &exact_prefix,
            string_prefix_upper_bound(&exact_prefix).as_deref(),
            1,
            &filter_probe,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .next()
        .map(revision_from_manifest_row)
        .transpose()
}

pub(super) async fn revisions_for_inode_page_desc<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
    start_after: Option<RevisionPagePosition>,
    limit: usize,
) -> Result<Vec<RevisionRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let inode_prefix = revision_by_inode_desc_inode_prefix(inode_id);
    let filter_probe = revision_by_inode_desc_filter_probe(inode_id);
    let lower_bound = start_after
        .map(|position| resume_after_row_key(&revision_by_inode_desc_row_key(inode_id, position)))
        .unwrap_or_else(|| inode_prefix.clone());
    tables
        .scan_range_page_for_lookup(
            MetadataTableFamily::RevisionsByInodeDesc,
            &lower_bound,
            string_prefix_upper_bound(&inode_prefix).as_deref(),
            limit,
            &filter_probe,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(revision_from_manifest_row)
        .collect()
}

/// Every tombstone event row in the namespace, across all roots. The trash
/// listing's raw material: rows are immortal (never dropped by compaction),
/// so this is complete for the namespace's whole history.
pub(super) async fn all_subtree_tombstones<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
) -> Result<Vec<SubtreeTombstoneRecord>> {
    tables
        .scan_prefix(MetadataTableFamily::Tombstones, "tombstone-")
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(tombstone_from_manifest_row)
        .collect()
}

pub(super) async fn tombstones_for_root<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    root_inode_id: InodeId,
) -> Result<Vec<SubtreeTombstoneRecord>> {
    let filter_probe = lookup_keys::tombstone_probe(root_inode_id);
    let prefix = lookup_keys::tombstone_prefix(root_inode_id);
    tables
        .scan_prefix_for_lookup(
            MetadataTableFamily::Tombstones,
            &prefix,
            &filter_probe,
            Readahead::Enabled,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(tombstone_from_manifest_row)
        .collect()
}

pub(super) async fn commit_receipt<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    commit_id: &CommitId,
) -> Result<Option<CommitReceiptRecord>> {
    let filter_probe = lookup_keys::commit_receipt_probe(commit_id.as_str());
    let prefix = lookup_keys::commit_receipt_prefix(commit_id.as_str());
    let receipts: Vec<CommitReceiptRecord> = tables
        .scan_prefix_for_lookup(
            MetadataTableFamily::CommitReceipts,
            &prefix,
            &filter_probe,
            Readahead::Disabled,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .map(commit_receipt_from_manifest_row)
        .collect::<Result<_>>()?;
    Ok(receipts
        .into_iter()
        .max_by_key(|receipt| receipt.committed_seq))
}

fn resume_after_row_key(row_key: &str) -> String {
    let mut lower_bound = String::with_capacity(row_key.len() + 1);
    lower_bound.push_str(row_key);
    lower_bound.push('\0');
    lower_bound
}

fn revision_by_inode_desc_inode_prefix(inode_id: InodeId) -> String {
    lookup_keys::revision_by_inode_desc_prefix(inode_id)
}

fn revision_by_inode_desc_filter_probe(inode_id: InodeId) -> String {
    lookup_keys::revision_by_inode_desc_probe(inode_id)
}

fn revision_by_inode_desc_exact_revision_prefix(
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> String {
    lookup_keys::revision_by_inode_desc_revision_prefix(inode_id, revision_no)
}

fn revision_by_inode_desc_row_key(inode_id: InodeId, position: RevisionPagePosition) -> String {
    lookup_keys::revision_by_inode_desc_row_key(
        inode_id,
        position.revision_no,
        position.committed_seq,
        position.revision_delta_index,
    )
}
