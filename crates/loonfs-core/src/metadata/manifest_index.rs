use super::row_decode::{
    commit_receipt_from_manifest_row, direntry_bind_from_manifest_row,
    direntry_unbind_from_manifest_row, inode_from_manifest_row, revision_from_manifest_row,
    tombstone_from_manifest_row,
};
use crate::checkpoint::{string_prefix_upper_bound, ManifestLoadError, VerifiedMetadataTables};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::metadata::{
    unbind_matches_binding, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::{hex_encode_row_key_component, MetadataRow, MetadataTableFamily};
use loonfs_api::{ChangeSeq, CommitId, InodeId, RevisionNo};
use loonfs_objectstore::ObjectStore;

pub(super) fn manifest_error_to_core(error: ManifestLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
}

pub(super) async fn inode_at_seq<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>> {
    let key = format!("inode-{:020}", inode_id.0);
    Ok(tables
        .get(MetadataTableFamily::Inodes, &key)
        .await
        .map_err(manifest_error_to_core)?
        .and_then(inode_from_manifest_row))
}

pub(super) async fn direntry_binds_for_parent<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
) -> Result<Vec<DirentryBindRecord>> {
    let prefix = format!("direntry-{:020}-", parent_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::DirentryBinds, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_manifest_row)
        .collect())
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
    let parent_prefix = format!("direntry-{:020}-", parent_inode_id.0);
    let lower_bound = if let Some(row_key) = start_after_row_key {
        resume_after_row_key(row_key)
    } else if let Some(name_key) = start_after_name_key {
        let encoded_name_key = hex_encode_row_key_component(name_key);
        let exact_name_prefix = format!("{parent_prefix}{encoded_name_key}-");
        string_prefix_upper_bound(&exact_name_prefix).unwrap_or(exact_name_prefix)
    } else {
        parent_prefix.clone()
    };
    let upper_bound = string_prefix_upper_bound(&parent_prefix);
    Ok(tables
        .scan_range_page(
            MetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            limit,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_candidate_from_manifest_row)
        .collect())
}

pub(super) async fn direntry_binds_for_parent_name<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
    name_key: &str,
) -> Result<Vec<DirentryBindRecord>> {
    let encoded_name_key = hex_encode_row_key_component(name_key);
    let prefix = format!("direntry-{:020}-{encoded_name_key}-", parent_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::DirentryBinds, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_manifest_row)
        .collect())
}

pub(super) async fn direntry_binds_for_child<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    child_inode_id: InodeId,
) -> Result<Vec<DirentryBindRecord>> {
    let prefix = format!("direntry-child-{:020}-", child_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::DirentryChildBinds, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_manifest_row)
        .collect())
}

pub(super) async fn direntry_unbinds_for_binding<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    direntry: &DirentryBindRecord,
) -> Result<Vec<DirentryUnbindRecord>> {
    let encoded_name_key = hex_encode_row_key_component(&direntry.name_key);
    let prefix = format!(
        "direntry-unbind-{:020}-{}-{:020}-{:010}-",
        direntry.parent_inode_id.0,
        encoded_name_key,
        direntry.bind_seq.0,
        direntry.bind_delta_index
    );
    Ok(tables
        .scan_prefix(MetadataTableFamily::DirentryUnbinds, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_unbind_from_manifest_row)
        .filter(|unbind| unbind_matches_binding(unbind, direntry))
        .collect())
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
    Ok(tables
        .scan_range_page(
            MetadataTableFamily::RevisionsByInodeDesc,
            &exact_prefix,
            string_prefix_upper_bound(&exact_prefix).as_deref(),
            1,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(revision_from_manifest_row)
        .next())
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
    let lower_bound = start_after
        .map(|position| resume_after_row_key(&revision_by_inode_desc_row_key(inode_id, position)))
        .unwrap_or_else(|| inode_prefix.clone());
    Ok(tables
        .scan_range_page(
            MetadataTableFamily::RevisionsByInodeDesc,
            &lower_bound,
            string_prefix_upper_bound(&inode_prefix).as_deref(),
            limit,
        )
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(revision_from_manifest_row)
        .collect())
}

pub(super) async fn tombstones_for_root<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    root_inode_id: InodeId,
) -> Result<Vec<SubtreeTombstoneRecord>> {
    let prefix = format!("tombstone-{:020}-", root_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::Tombstones, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(tombstone_from_manifest_row)
        .collect())
}

pub(super) async fn commit_receipt<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    commit_id: &CommitId,
) -> Result<Option<CommitReceiptRecord>> {
    let encoded_commit_id = hex_encode_row_key_component(commit_id.as_str());
    let prefix = format!("commit-receipt-{encoded_commit_id}-");
    Ok(tables
        .scan_prefix(MetadataTableFamily::CommitReceipts, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(commit_receipt_from_manifest_row)
        .max_by_key(|receipt| receipt.committed_seq))
}

fn direntry_bind_candidate_from_manifest_row(
    row: MetadataRow,
) -> Option<ManifestDirentryBindCandidate> {
    let row_key = row.row_key_for_family(MetadataTableFamily::DirentryBinds);
    direntry_bind_from_manifest_row(row)
        .map(|record| ManifestDirentryBindCandidate { row_key, record })
}

fn resume_after_row_key(row_key: &str) -> String {
    let mut lower_bound = String::with_capacity(row_key.len() + 1);
    lower_bound.push_str(row_key);
    lower_bound.push('\0');
    lower_bound
}

fn revision_by_inode_desc_inode_prefix(inode_id: InodeId) -> String {
    format!("revision-by-inode-desc-{:020}-", inode_id.0)
}

fn revision_by_inode_desc_exact_revision_prefix(
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> String {
    format!(
        "{}{:020}-",
        revision_by_inode_desc_inode_prefix(inode_id),
        u64::MAX - revision_no.0
    )
}

fn revision_by_inode_desc_row_key(inode_id: InodeId, position: RevisionPagePosition) -> String {
    format!(
        "{}{:020}-{:020}-{:010}",
        revision_by_inode_desc_inode_prefix(inode_id),
        u64::MAX - position.revision_no.0,
        u64::MAX - position.committed_seq.0,
        u32::MAX - position.revision_delta_index
    )
}
