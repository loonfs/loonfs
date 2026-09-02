//! Pure validation of loaded manifests and their segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use super::row::manifest_row_commit_seq;
use super::runs::{runs_in_materialization_order, MetadataRunManifest, REORGANIZE_FAMILY_GROUPS};
use super::scan::ordered_manifest_segments;
use loonfs_api::wire::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
    NamespaceManifestPayload, RunTier,
};
use loonfs_api::{
    manifest_object_id_manifest_no, ChangeSeq, ManifestNo, ManifestObjectId, NamespaceId, RunNo,
};
use loonfs_objectstore::keys::metadata_segment_object_key;
#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::HashSet;

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

pub(super) fn validate_manifest(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    validate_manifest_materialization_ranges(object_key, payload)?;
    let runs = runs_in_materialization_order(payload);
    validate_one_base_run_per_family_group(object_key, &runs)?;
    validate_segment_block_layout(object_key, &runs)?;
    validate_segment_key_ranges(object_key, &runs)?;
    validate_run_index_parity(object_key, &runs)?;
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

    if payload.runs.is_empty() {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "namespace manifest must reference at least one metadata file".to_owned(),
        });
    }

    let mut saw_base_seq_run = false;
    let mut saw_head_seq_run = false;
    let mut seen_run_nos = HashSet::new();
    let mut seen_segment_ids = HashSet::new();
    for run in &payload.runs {
        if run.run_no >= payload.next_run_no {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` is not below next run `{}`",
                    run.run_no, payload.next_run_no
                ),
            });
        }
        if !seen_run_nos.insert(run.run_no) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate metadata run number `{}`", run.run_no),
            });
        }
        if run.run_seq < payload.base_seq || run.run_seq > payload.head_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` seq `{}` is outside [`{}`, `{}`]",
                    run.run_no, run.run_seq, payload.base_seq, payload.head_seq
                ),
            });
        }
        if !run.segments.is_empty() {
            saw_base_seq_run |= run.run_seq == payload.base_seq;
            saw_head_seq_run |= run.run_seq == payload.head_seq;
        }
        for descriptor in &run.segments {
            if !seen_segment_ids.insert(descriptor.segment_id.as_str()) {
                return Err(ManifestLoadError::RunManifestMismatch {
                    object_key: object_key.to_owned(),
                    message: format!("duplicate metadata segment id `{}`", descriptor.segment_id),
                });
            }
        }
    }

    if !saw_base_seq_run {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at base_seq `{}`",
                payload.base_seq
            ),
        });
    }
    if !saw_head_seq_run {
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

fn validate_segment_block_layout(
    object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for run in runs {
        for family_segments in ordered_manifest_segments(object_key, &run.segments)? {
            for descriptor in &family_segments.segments {
                let filter_end =
                    descriptor.filter_block.offset + u64::from(descriptor.filter_block.stored_len);
                if filter_end != descriptor.index_block.offset {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: metadata_segment_object_key(descriptor),
                        message: format!(
                            "filter block ends at {filter_end} but the index block starts at {}; the filter must directly precede the index",
                            descriptor.index_block.offset
                        ),
                    });
                }
                if let Some(inline) = &descriptor.filter_inline {
                    let expected_hex_len = 2 * descriptor.filter_block.stored_len as usize;
                    if inline.len() != expected_hex_len {
                        return Err(ManifestLoadError::SegmentDescriptorMismatch {
                            object_key: metadata_segment_object_key(descriptor),
                            message: format!(
                                "inline filter is {} hex chars but the filter block stores {} bytes",
                                inline.len(),
                                descriptor.filter_block.stored_len
                            ),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_segment_key_ranges(
    object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for run in runs {
        for family_segments in ordered_manifest_segments(object_key, &run.segments)? {
            validate_segment_numbering(run.run_no, &family_segments.segments)?;
            validate_segment_key_order(run.run_no, &family_segments.segments)?;
            for descriptor in &family_segments.segments {
                if descriptor.row_count > 0
                    && (descriptor.min_row_key.is_empty()
                        || descriptor.max_row_key.is_empty()
                        || descriptor.min_row_key > descriptor.max_row_key)
                {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: metadata_segment_object_key(descriptor),
                        message: format!(
                            "segment holds {} rows with key range `{}`..=`{}`; a segment with rows must carry a non-empty ascending key range",
                            descriptor.row_count, descriptor.min_row_key, descriptor.max_row_key
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

fn validate_run_index_parity(
    object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for run in runs {
        let ordered = ordered_manifest_segments(object_key, &run.segments)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut revision_rows = 0u64;
        let mut revision_by_inode_desc_rows = 0u64;
        for family_segments in ordered {
            let rows = family_segments
                .segments
                .iter()
                .map(|descriptor| descriptor.row_count)
                .fold(0u64, u64::saturating_add);
            match family_segments.family {
                MetadataRowFamily::DirentryBinds => direntry_bind_rows = rows,
                MetadataRowFamily::DirentryChildBinds => direntry_child_bind_rows = rows,
                MetadataRowFamily::Revisions => revision_rows = rows,
                MetadataRowFamily::RevisionsByInodeDesc => revision_by_inode_desc_rows = rows,
                MetadataRowFamily::Inodes
                | MetadataRowFamily::DirentryUnbinds
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::CommitReceipts
                | MetadataRowFamily::Attributes => {}
            }
        }
        if direntry_bind_rows != direntry_child_bind_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {direntry_bind_rows} direntry bind rows but {direntry_child_bind_rows} child-bind index rows",
                    run.run_no
                ),
            });
        }
        if revision_rows != revision_by_inode_desc_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {revision_rows} revision rows but {revision_by_inode_desc_rows} revision index rows",
                    run.run_no
                ),
            });
        }
    }
    Ok(())
}

fn validate_segment_numbering(
    run_no: RunNo,
    descriptors: &[MetadataSegmentRef],
) -> Result<(), ManifestLoadError> {
    for (position, descriptor) in descriptors.iter().enumerate() {
        if descriptor.segment_index as usize != position {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: metadata_segment_object_key(descriptor),
                message: format!(
                    "segment carries index {} at position {position} of family `{:?}` in run `{run_no}`; a family's segments within one run are numbered from zero, once each, in the order they were written",
                    descriptor.segment_index, descriptor.family
                ),
            });
        }
    }
    Ok(())
}

fn validate_segment_key_order(
    run_no: RunNo,
    descriptors: &[MetadataSegmentRef],
) -> Result<(), ManifestLoadError> {
    let mut previous: Option<&MetadataSegmentRef> = None;
    for descriptor in descriptors {
        if descriptor.row_count == 0 {
            continue;
        }
        if let Some(previous) = previous {
            if descriptor.min_row_key <= previous.max_row_key {
                return Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: metadata_segment_object_key(descriptor),
                    message: format!(
                        "segment starts at `{}`, at or below the preceding segment's last row key `{}`, in family `{:?}` of run `{run_no}`; one producer writes a family's segments in ascending key order, so consecutive ranges never touch",
                        descriptor.min_row_key, previous.max_row_key, descriptor.family
                    ),
                });
            }
        }
        previous = Some(descriptor);
    }
    Ok(())
}

fn validate_one_base_run_per_family_group(
    object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for group in REORGANIZE_FAMILY_GROUPS {
        let mut base_runs = runs.iter().filter(|run| {
            run.tier == RunTier::Base
                && run.segments.iter().any(|family_segments| {
                    group.contains(family_segments.family) && !family_segments.segments.is_empty()
                })
        });
        let (Some(first), Some(second)) = (base_runs.next(), base_runs.next()) else {
            continue;
        };
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "family group {:?} holds base-tier runs `{}` and `{}`; a group holds at most one base run, because a merge writes one only when it starts at the group's oldest run and then replaces it",
                group.families(), first.run_no, second.run_no
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
            message: "direntry_child_binds index does not match canonical direntry_binds"
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
