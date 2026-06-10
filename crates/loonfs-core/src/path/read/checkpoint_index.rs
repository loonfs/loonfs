use super::row_decode::{
    direntry_bind_from_manifest_row, direntry_unbind_from_manifest_row, inode_from_manifest_row,
    revision_from_manifest_row, tombstone_from_manifest_row, unbind_matches_binding,
};
use crate::checkpoint::{ManifestLoadError, VerifiedMetadataTables};
use crate::error::CoreError;
use crate::metadata::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::namespace::basis::BasisLoadError;
use loonfs_api::wire::manifest::{hex_encode_row_key_component, MetadataTableFamily};
use loonfs_api::InodeId;
use loonfs_objectstore::ObjectStore;

pub(super) fn manifest_error_to_core(error: ManifestLoadError) -> CoreError {
    CoreError::Basis(BasisLoadError::ManifestLoad(error))
}

pub(super) async fn inode_at_seq<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>, CoreError> {
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
) -> Result<Vec<DirentryBindRecord>, CoreError> {
    let prefix = format!("direntry-{:020}-", parent_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::DirentryBinds, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_manifest_row)
        .collect())
}

pub(super) async fn direntry_binds_for_parent_name<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    parent_inode_id: InodeId,
    name_key: &str,
) -> Result<Vec<DirentryBindRecord>, CoreError> {
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
) -> Result<Vec<DirentryBindRecord>, CoreError> {
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
) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
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

pub(super) async fn revisions_for_inode<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    inode_id: InodeId,
) -> Result<Vec<RevisionRecord>, CoreError> {
    let prefix = format!("revision-{:020}-", inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::Revisions, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(revision_from_manifest_row)
        .collect())
}

pub(super) async fn tombstones_for_root<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    root_inode_id: InodeId,
) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
    let prefix = format!("tombstone-{:020}-", root_inode_id.0);
    Ok(tables
        .scan_prefix(MetadataTableFamily::Tombstones, &prefix)
        .await
        .map_err(manifest_error_to_core)?
        .into_iter()
        .filter_map(tombstone_from_manifest_row)
        .collect())
}
