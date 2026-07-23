//! Pure validation of loaded manifests and SST segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use super::row::manifest_row_commit_seq;
use loonfs_api::wire::manifest::{
    MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{
    manifest_object_id_manifest_id, ChangeSeq, ManifestId, ManifestObjectId, NamespaceId,
};
use std::collections::BTreeSet;

pub(super) fn validate_namespace_manifest(
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    manifest_object_id: &ManifestObjectId,
    object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), ManifestLoadError> {
    if manifest.payload.namespace_id != *namespace_id {
        return Err(ManifestLoadError::ManifestNamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: namespace_id.clone(),
            actual: manifest.payload.namespace_id.clone(),
        });
    }
    if manifest.payload.manifest_id != manifest_id {
        return Err(ManifestLoadError::ManifestIdMismatch {
            object_key: object_key.to_owned(),
            expected: manifest_id,
            actual: manifest.payload.manifest_id,
        });
    }
    if manifest_object_id_manifest_id(manifest.payload.manifest_object_id.as_str())
        != Some(manifest.payload.manifest_id)
    {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "manifest object id `{}` does not encode manifest id `{}`",
                manifest.payload.manifest_object_id, manifest.payload.manifest_id
            ),
        });
    }
    if manifest.payload.manifest_object_id != *manifest_object_id {
        return Err(ManifestLoadError::ManifestObjectIdMismatch {
            object_key: object_key.to_owned(),
            expected: manifest_object_id.clone(),
            actual: manifest.payload.manifest_object_id.clone(),
        });
    }
    Ok(())
}

pub(super) fn validate_manifest_materialization_ranges(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    if payload.base_seq > payload.head_seq {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "base_seq `{}` is after manifest head_seq `{}`",
                payload.base_seq, payload.head_seq
            ),
        });
    }

    if payload.metadata_files.is_empty() {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "namespace manifest must reference at least one metadata file".to_owned(),
        });
    }

    let mut saw_base_seq_file = false;
    let mut saw_head_seq_file = false;
    let mut seen_table_ids = Vec::new();
    for metadata_file in &payload.metadata_files {
        // The id shape is validated on decode: `table_id` is a typed
        // `MetadataTableId`, so only well-formed ids can reach this point.
        if seen_table_ids.contains(&metadata_file.table_id.as_str()) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate metadata table id `{}`", metadata_file.table_id),
            });
        }
        seen_table_ids.push(metadata_file.table_id.as_str());
        if metadata_file.run_seq < payload.base_seq || metadata_file.run_seq > payload.head_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata file `{}` run seq `{}` is outside [`{}`, `{}`]",
                    metadata_file.table_id,
                    metadata_file.run_seq,
                    payload.base_seq,
                    payload.head_seq
                ),
            });
        }
        saw_base_seq_file |= metadata_file.run_seq == payload.base_seq;
        saw_head_seq_file |= metadata_file.run_seq == payload.head_seq;
    }

    if !saw_base_seq_file {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at base_seq `{}`",
                payload.base_seq
            ),
        });
    }
    if !saw_head_seq_file {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at head_seq `{}`",
                payload.head_seq
            ),
        });
    }

    Ok(())
}

pub(super) fn validate_manifest_row_seq_range<'a>(
    object_key: &str,
    rows: impl IntoIterator<Item = &'a MetadataRow>,
    max_seq: ChangeSeq,
) -> Result<(), ManifestLoadError> {
    for (row_index, row) in rows.into_iter().enumerate() {
        let row_seq = manifest_row_commit_seq(row);
        if row_seq > max_seq {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "row {row_index} seq `{row_seq}` is after expected max `{max_seq}`"
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_direntry_child_bind_index(
    object_key: &str,
    mut direntry_bind_rows: Vec<MetadataRow>,
    mut direntry_child_bind_rows: Vec<MetadataRow>,
) -> Result<(), ManifestLoadError> {
    direntry_bind_rows
        .sort_by_key(|row| row.row_key_for_family(MetadataTableFamily::DirentryChildBinds));
    direntry_child_bind_rows
        .sort_by_key(|row| row.row_key_for_family(MetadataTableFamily::DirentryChildBinds));

    if direntry_bind_rows != direntry_child_bind_rows {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: object_key.to_owned(),
            message: "direntry-child-binds index does not match canonical direntry-binds"
                .to_owned(),
        });
    }

    Ok(())
}

pub(super) fn validate_revision_by_inode_desc_index(
    object_key: &str,
    mut revision_rows: Vec<MetadataRow>,
    mut revision_by_inode_desc_rows: Vec<MetadataRow>,
) -> Result<(), ManifestLoadError> {
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataTableFamily::Revisions,
        &revision_rows,
    )?;
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataTableFamily::RevisionsByInodeDesc,
        &revision_by_inode_desc_rows,
    )?;

    revision_rows.sort_by_key(revision_logical_key);
    revision_by_inode_desc_rows.sort_by_key(revision_logical_key);

    if revision_rows != revision_by_inode_desc_rows {
        return Err(ManifestLoadError::RevisionIndexMismatch {
            object_key: object_key.to_owned(),
        });
    }

    Ok(())
}

fn validate_revision_rows_have_unique_keys(
    object_key: &str,
    family: MetadataTableFamily,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    let mut seen = BTreeSet::new();
    for row in rows {
        let row_key = revision_logical_key(row);
        if !seen.insert(row_key.clone()) {
            return Err(ManifestLoadError::DuplicateRevisionRow {
                object_key: object_key.to_owned(),
                family,
                row_key,
            });
        }
    }
    Ok(())
}

fn revision_logical_key(row: &MetadataRow) -> String {
    row.row_key_for_family(MetadataTableFamily::Revisions)
}
