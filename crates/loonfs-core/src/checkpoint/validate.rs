//! Pure validation of loaded manifests and their segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use super::row::manifest_row_commit_seq;
#[cfg(test)]
use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_api::wire::manifest::{
    MetadataRow, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{
    manifest_object_id_manifest_no, ChangeSeq, ManifestNo, ManifestObjectId, NamespaceId,
};
#[cfg(test)]
use std::collections::BTreeSet;

pub(super) fn validate_namespace_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
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
    if manifest.payload.manifest_no != manifest_no {
        return Err(ManifestLoadError::ManifestNoMismatch {
            object_key: object_key.to_owned(),
            expected: manifest_no,
            actual: manifest.payload.manifest_no,
        });
    }
    if manifest_object_id_manifest_no(manifest.payload.manifest_object_id.as_str())
        != Some(manifest.payload.manifest_no)
    {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "manifest object id `{}` does not encode manifest number `{}`",
                manifest.payload.manifest_object_id, manifest.payload.manifest_no
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

    if payload.segments.is_empty() {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "namespace manifest must reference at least one metadata file".to_owned(),
        });
    }

    let mut saw_base_seq_segment = false;
    let mut saw_head_seq_segment = false;
    let mut seen_segment_ids = Vec::new();
    for descriptor in &payload.segments {
        // The id shape is validated on decode: `segment_id` is a typed
        // `MetadataSegmentId`, so only well-formed ids can reach this point.
        if seen_segment_ids.contains(&descriptor.segment_id.as_str()) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate metadata segment id `{}`", descriptor.segment_id),
            });
        }
        seen_segment_ids.push(descriptor.segment_id.as_str());
        if descriptor.run_seq < payload.base_seq || descriptor.run_seq > payload.head_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata file `{}` run seq `{}` is outside [`{}`, `{}`]",
                    descriptor.segment_id, descriptor.run_seq, payload.base_seq, payload.head_seq
                ),
            });
        }
        saw_base_seq_segment |= descriptor.run_seq == payload.base_seq;
        saw_head_seq_segment |= descriptor.run_seq == payload.head_seq;
    }

    if !saw_base_seq_segment {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at base_seq `{}`",
                payload.base_seq
            ),
        });
    }
    if !saw_head_seq_segment {
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

/// Compares a bind family against its reverse index outright, over rows a
/// caller holds all of.
///
/// Only the test-only inspection materialization holds a whole family, so only
/// it can afford this. A merge never holds one and checks parity by digesting
/// the rows it writes instead
/// (`super::streaming_compaction::GroupMerge::refuse_a_run_whose_index_disagrees`).
#[cfg(test)]
pub(super) fn validate_direntry_child_bind_index(
    object_key: &str,
    direntry_bind_rows: &[MetadataRow],
    direntry_child_bind_rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    let mut direntry_bind_rows = direntry_bind_rows.iter().collect::<Vec<_>>();
    let mut direntry_child_bind_rows = direntry_child_bind_rows.iter().collect::<Vec<_>>();
    direntry_bind_rows
        .sort_by_cached_key(|row| row.row_key_for_family(MetadataRowFamily::DirentryChildBinds));
    direntry_child_bind_rows
        .sort_by_cached_key(|row| row.row_key_for_family(MetadataRowFamily::DirentryChildBinds));

    if direntry_bind_rows != direntry_child_bind_rows {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: object_key.to_owned(),
            message: "direntry-child-binds index does not match canonical direntry-binds"
                .to_owned(),
        });
    }

    Ok(())
}

/// [`validate_direntry_child_bind_index`] for the revision pair, and
/// test-only for the same reason.
#[cfg(test)]
pub(super) fn validate_revision_by_inode_desc_index(
    object_key: &str,
    revision_rows: &[MetadataRow],
    revision_by_inode_desc_rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataRowFamily::Revisions,
        revision_rows,
    )?;
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataRowFamily::RevisionsByInodeDesc,
        revision_by_inode_desc_rows,
    )?;

    let mut revision_rows = revision_rows.iter().collect::<Vec<_>>();
    let mut revision_by_inode_desc_rows = revision_by_inode_desc_rows.iter().collect::<Vec<_>>();
    revision_rows.sort_by_cached_key(|row| revision_logical_key(row));
    revision_by_inode_desc_rows.sort_by_cached_key(|row| revision_logical_key(row));

    if revision_rows != revision_by_inode_desc_rows {
        return Err(ManifestLoadError::RevisionIndexMismatch {
            object_key: object_key.to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
fn validate_revision_rows_have_unique_keys(
    object_key: &str,
    family: MetadataRowFamily,
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

#[cfg(test)]
fn revision_logical_key(row: &MetadataRow) -> String {
    row.row_key_for_family(MetadataRowFamily::Revisions)
}
