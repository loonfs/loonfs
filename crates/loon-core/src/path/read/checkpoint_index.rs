use super::row_decode::{
    direntry_bind_from_checkpoint_row, direntry_unbind_from_checkpoint_row,
    inode_from_checkpoint_row, revision_from_checkpoint_row, tombstone_from_checkpoint_row,
    unbind_matches_binding,
};
use crate::checkpoint::{CheckpointLoadError, VerifiedCheckpointTables};
use crate::error::CoreError;
use crate::metadata::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::namespace::basis::BasisLoadError;
use loon_api::wire::checkpoint::{hex_encode_row_key_component, CheckpointTableFamily};
use loon_api::InodeId;
use loon_objectstore::ObjectStore;

pub(super) fn checkpoint_error_to_core(error: CheckpointLoadError) -> CoreError {
    CoreError::Basis(BasisLoadError::CheckpointLoad(error))
}

pub(super) fn inode_at_seq<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>, CoreError> {
    let key = format!("inode-{:020}", inode_id.0);
    Ok(tables
        .get(CheckpointTableFamily::Inodes, &key)
        .map_err(checkpoint_error_to_core)?
        .and_then(inode_from_checkpoint_row))
}

pub(super) fn direntry_binds_for_parent<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    parent_inode_id: InodeId,
) -> Result<Vec<DirentryBindRecord>, CoreError> {
    let prefix = format!("direntry-{:020}-", parent_inode_id.0);
    Ok(tables
        .scan_prefix(CheckpointTableFamily::DirentryBinds, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_checkpoint_row)
        .collect())
}

pub(super) fn direntry_binds_for_parent_name<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    parent_inode_id: InodeId,
    name_key: &str,
) -> Result<Vec<DirentryBindRecord>, CoreError> {
    let encoded_name_key = hex_encode_row_key_component(name_key);
    let prefix = format!("direntry-{:020}-{encoded_name_key}-", parent_inode_id.0);
    Ok(tables
        .scan_prefix(CheckpointTableFamily::DirentryBinds, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_checkpoint_row)
        .collect())
}

pub(super) fn direntry_binds_for_child<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    child_inode_id: InodeId,
) -> Result<Vec<DirentryBindRecord>, CoreError> {
    let prefix = format!("direntry-child-{:020}-", child_inode_id.0);
    Ok(tables
        .scan_prefix(CheckpointTableFamily::DirentryChildBinds, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(direntry_bind_from_checkpoint_row)
        .collect())
}

pub(super) fn direntry_unbinds_for_binding<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
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
        .scan_prefix(CheckpointTableFamily::DirentryUnbinds, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(direntry_unbind_from_checkpoint_row)
        .filter(|unbind| unbind_matches_binding(unbind, direntry))
        .collect())
}

pub(super) fn revisions_for_inode<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    inode_id: InodeId,
) -> Result<Vec<RevisionRecord>, CoreError> {
    let prefix = format!("revision-{:020}-", inode_id.0);
    Ok(tables
        .scan_prefix(CheckpointTableFamily::Revisions, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(revision_from_checkpoint_row)
        .collect())
}

pub(super) fn tombstones_for_root<S: ObjectStore + ?Sized>(
    tables: &VerifiedCheckpointTables<'_, S>,
    root_inode_id: InodeId,
) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
    let prefix = format!("tombstone-{:020}-", root_inode_id.0);
    Ok(tables
        .scan_prefix(CheckpointTableFamily::Tombstones, &prefix)
        .map_err(checkpoint_error_to_core)?
        .into_iter()
        .filter_map(tombstone_from_checkpoint_row)
        .collect())
}
