//! Pure validation of loaded manifests and SST segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use super::row::{manifest_row_commit_seq, manifest_row_kind, manifest_row_matches_family};
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataPage, MetadataRow, MetadataSstEnvelope, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};

pub(super) fn validate_namespace_manifest(
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
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

pub(super) fn validate_manifest_segment(
    run_seq: ChangeSeq,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    segment: &MetadataSstEnvelope,
) -> Result<Vec<MetadataRow>, ManifestLoadError> {
    if segment.payload.namespace_id != descriptor.owner_namespace_id {
        return Err(ManifestLoadError::SegmentNamespaceMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.owner_namespace_id.clone(),
            actual: segment.payload.namespace_id.clone(),
        });
    }
    if segment.payload.table_id != descriptor.table_id {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "table id mismatch: expected `{}`, actual `{}`",
                descriptor.table_id, segment.payload.table_id
            ),
        });
    }
    if segment.payload.run_seq != run_seq {
        return Err(ManifestLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: run_seq,
            actual: segment.payload.run_seq,
        });
    }
    if segment.payload.level != descriptor.level {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "level mismatch: expected `{}`, actual `{}`",
                descriptor.level, segment.payload.level
            ),
        });
    }
    if segment.payload.family != family {
        return Err(ManifestLoadError::SegmentFamilyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: family,
            actual: segment.payload.family,
        });
    }
    if segment.payload.segment_index != descriptor.segment_index {
        return Err(ManifestLoadError::SegmentIndexMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_index,
            actual: segment.payload.segment_index,
        });
    }
    if segment.payload.segment_key != descriptor.segment_key {
        return Err(ManifestLoadError::SegmentKeyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_key.clone(),
            actual: segment.payload.segment_key.clone(),
        });
    }

    // The codec already verified `segment.payload_checksum` against the
    // stored payload bytes; here the manifest's descriptor must agree, which
    // binds this exact file to the manifest that references it.
    if descriptor.payload_checksum != segment.payload_checksum {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "payload checksum mismatch: expected `{}`, actual `{}`",
                descriptor.payload_checksum, segment.payload_checksum
            ),
        });
    }

    let mut collected_rows = Vec::new();
    for page in &segment.payload.pages {
        validate_manifest_page(family, descriptor, page)?;
        collected_rows.extend(page.rows.iter().cloned());
    }

    if segment.payload.row_count != collected_rows.len() as u64 {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "row count mismatch: expected {}, actual {}",
                segment.payload.row_count,
                collected_rows.len()
            ),
        });
    }
    let row_keys = collected_rows
        .iter()
        .map(|row| row.row_key_for_family(family))
        .collect::<Vec<_>>();
    if let (Some(first), Some(last)) = (row_keys.first(), row_keys.last()) {
        if segment.payload.min_key != *first || segment.payload.max_key != *last {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: descriptor.object_key.clone(),
                message: "payload min/max key mismatch".to_owned(),
            });
        }
    } else if segment.payload.row_count != 0 {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "non-zero row count with no rows".to_owned(),
        });
    }

    if descriptor.row_count != segment.payload.row_count
        || descriptor.min_key != segment.payload.min_key
        || descriptor.max_key != segment.payload.max_key
    {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "descriptor row summary mismatch".to_owned(),
        });
    }

    Ok(collected_rows)
}

pub(super) fn validate_manifest_page(
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    page: &MetadataPage,
) -> Result<(), ManifestLoadError> {
    if page.row_keys.len() != page.rows.len() {
        return Err(ManifestLoadError::PageShapeMismatch {
            object_key: descriptor.object_key.clone(),
            page_index: page.page_index,
            message: format!(
                "row_keys length {} does not match rows length {}",
                page.row_keys.len(),
                page.rows.len()
            ),
        });
    }

    for (index, row) in page.rows.iter().enumerate() {
        if !manifest_row_matches_family(row, family) {
            return Err(ManifestLoadError::TableRowKindMismatch {
                object_key: descriptor.object_key.clone(),
                family,
                row_kind: manifest_row_kind(row).to_owned(),
            });
        }
        let actual = row.row_key_for_family(family);
        let expected = page.row_keys.get(index).cloned().unwrap_or_default();
        if actual != expected {
            return Err(ManifestLoadError::RowKeyMismatch {
                object_key: descriptor.object_key.clone(),
                page_index: page.page_index,
                row_index: index,
                expected,
                actual,
            });
        }
    }

    if let (Some(first), Some(last)) = (page.row_keys.first(), page.row_keys.last()) {
        if page.min_key != *first || page.max_key != *last {
            return Err(ManifestLoadError::PageShapeMismatch {
                object_key: descriptor.object_key.clone(),
                page_index: page.page_index,
                message: "page min/max key mismatch".to_owned(),
            });
        }
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
