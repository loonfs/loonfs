//! Pure validation of loaded manifests and SST segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use super::row::manifest_row_commit_seq;
use loonfs_api::wire::manifest::{
    MetadataRow, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{
    manifest_object_id_manifest_id, ChangeSeq, ManifestId, ManifestObjectId, NamespaceId,
};

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
    if !manifest.payload.verified {
        return Err(ManifestLoadError::ManifestNotVerified {
            object_key: object_key.to_owned(),
        });
    }
    if !manifest.payload.initialized {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "namespace manifest is not initialized".to_owned(),
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

pub(super) fn validate_manifest_row_seq_range(
    object_key: &str,
    rows: &[MetadataRow],
    min_seq: Option<ChangeSeq>,
    max_seq: ChangeSeq,
) -> Result<(), ManifestLoadError> {
    for (row_index, row) in rows.iter().enumerate() {
        let row_seq = manifest_row_commit_seq(row);
        if let Some(min_seq) = min_seq {
            if row_seq < min_seq {
                return Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "row {row_index} seq `{row_seq}` is before expected min `{min_seq}`"
                    ),
                });
            }
        }
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
