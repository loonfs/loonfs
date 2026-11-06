//! Pure validation of loaded manifests and their segments against their
//! descriptors: identity, checksums, ranges, ordering, and row shape.

use super::error::ManifestLoadError;
use crate::segment::row::manifest_row_commit_seq;
use crate::segment::runs::{runs_oldest_first, MetadataFamilyGroup, MetadataRunManifest};
use crate::segment::scan::ordered_manifest_segments;
use loonfs_objectstore::keys::{extension_segment_object_key, metadata_segment_object_key};
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
    NamespaceManifestPayload, RunTier, METADATA_SEGMENT_ENCODING,
};
use loonfs_types::format::segment_blocks::BlockHandle;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId, RunNo};
#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::HashSet;

pub(super) fn validate_namespace_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), ManifestLoadError> {
    if manifest.payload().namespace_id != *namespace_id {
        return Err(ManifestLoadError::ManifestNamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: namespace_id.clone(),
            actual: manifest.payload().namespace_id.clone(),
        });
    }
    if manifest.payload().manifest_no != manifest_no {
        return Err(ManifestLoadError::ManifestNoMismatch {
            object_key: object_key.to_owned(),
            expected: manifest_no,
            actual: manifest.payload().manifest_no,
        });
    }
    Ok(())
}

pub(super) fn validate_manifest(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    if payload.fork_basis.as_ref().is_some_and(|basis| {
        basis.manifest.owner_namespace_id == payload.namespace_id
            || payload.is_family_root()
            || basis.manifest.head_seq > payload.retention_floor_seq
    }) {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "fork provenance disagrees with namespace identity, family root, or \
                      retention floor"
                .to_owned(),
        });
    }
    // The fork call copies a fork's family root from the pinned source
    // manifest, and checking that value here would need the source manifest.
    if payload.fork_basis.is_none() && !payload.is_family_root() {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "a manifest without a fork basis records `{}` as its family root instead of itself",
                payload.family_root_id
            ),
        });
    }
    validate_manifest_sequence_ranges(object_key, payload)?;
    validate_extension_runs(object_key, payload)?;
    let runs = runs_oldest_first(payload);
    validate_one_base_run_per_family_group(object_key, &runs)?;
    validate_segment_block_layout(object_key, &runs)?;
    validate_segment_key_ranges(object_key, &runs)?;
    validate_run_index_parity(object_key, &runs)?;
    Ok(())
}

pub(super) fn validate_manifest_sequence_ranges(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    if payload.retention_floor_seq > payload.head_seq {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "retention floor is beyond the manifest head sequence".to_owned(),
        });
    }

    validate_unique_run_numbers(object_key, payload)?;
    // An extension's publication can allocate run numbers before the first
    // fold, so an empty manifest's next run number can be above zero.
    if payload.runs.is_empty() {
        if payload.head_seq == ChangeSeq(0) {
            return Ok(());
        }
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "an empty manifest must describe genesis".to_owned(),
        });
    }

    let mut saw_head_seq_run = false;
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
        if run.run_seq > payload.head_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` seq `{}` exceeds head seq `{}`",
                    run.run_no, run.run_seq, payload.head_seq
                ),
            });
        }
        if !run.segments.is_empty() {
            saw_head_seq_run |= run.run_seq == payload.head_seq;
        }
        for descriptor in &run.segments {
            if descriptor.encoding != METADATA_SEGMENT_ENCODING {
                return Err(ManifestLoadError::RunManifestMismatch {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "metadata segment `{}` uses unsupported encoding `{}`; a newer binary is required",
                        descriptor.segment_id, descriptor.encoding
                    ),
                });
            }
            if !seen_segment_ids.insert(descriptor.segment_id.as_str()) {
                return Err(ManifestLoadError::RunManifestMismatch {
                    object_key: object_key.to_owned(),
                    message: format!("duplicate metadata segment id `{}`", descriptor.segment_id),
                });
            }
        }
    }

    if !saw_head_seq_run {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at `head_seq` {}",
                payload.head_seq
            ),
        });
    }

    Ok(())
}

/// Checks that no two runs of the manifest share a number. One counter,
/// `next_run_no`, allocates the numbers of the metadata runs and of every
/// extension's runs, so a number identifies one run among all of them.
fn validate_unique_run_numbers(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    let mut run_nos = HashSet::new();
    let extension_runs = payload.extensions.values().flat_map(|entry| &entry.runs);
    for run_no in payload
        .runs
        .iter()
        .map(|run| run.run_no)
        .chain(extension_runs.map(|run| run.run_no))
    {
        if !run_nos.insert(run_no) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate run number `{run_no}`"),
            });
        }
    }
    Ok(())
}

/// Checks the runs that each extension entry lists: a run number below the
/// manifest's next run number, unique segment IDs, and segments with rows in
/// a nonempty ascending key range and the block layout of a metadata
/// segment. Core never reads an extension's rows, so these are the
/// descriptors' own rules.
fn validate_extension_runs(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    for (name, entry) in &payload.extensions {
        if let Some(run) = entry
            .runs
            .iter()
            .find(|run| run.run_no >= payload.next_run_no)
        {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "extension `{name}` run `{}` is not below next run `{}`",
                    run.run_no, payload.next_run_no
                ),
            });
        }
        let mut seen_segment_ids = HashSet::new();
        for descriptor in entry.segments() {
            let segment_key = extension_segment_object_key(name, descriptor);
            let invalid = |message: String| {
                Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: segment_key.clone(),
                    message,
                })
            };
            if !seen_segment_ids.insert(&descriptor.segment_id) {
                return Err(ManifestLoadError::RunManifestMismatch {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "extension `{name}` lists segment `{}` twice",
                        descriptor.segment_id
                    ),
                });
            }
            if descriptor.row_count == 0
                || descriptor.min_row_key.is_empty()
                || descriptor.min_row_key > descriptor.max_row_key
            {
                return invalid(format!(
                    "segment holds {} rows with key range `{}`..=`{}`; a segment must carry \
                     rows in a non-empty ascending key range",
                    descriptor.row_count, descriptor.min_row_key, descriptor.max_row_key
                ));
            }
            if let Err(message) = check_block_layout(
                descriptor.filter_block,
                None,
                descriptor.index_block,
                descriptor.filter_inline.as_deref(),
            ) {
                return invalid(message);
            }
        }
    }
    Ok(())
}

/// Checks that a segment's extent filter, when it has one, directly precedes
/// its filter block, that the filter block directly precedes its index block
/// within the address space, and that an inline filter has the filter block's
/// stored length.
fn check_block_layout(
    filter_block: BlockHandle,
    extent_filter_block: Option<BlockHandle>,
    index_block: BlockHandle,
    filter_inline: Option<&str>,
) -> Result<(), String> {
    for handle in [extent_filter_block, Some(filter_block), Some(index_block)]
        .into_iter()
        .flatten()
    {
        if handle
            .offset
            .checked_add(u64::from(handle.stored_bytes))
            .is_none()
        {
            return Err("section byte range exceeds address space".to_owned());
        }
    }
    if let Some(extent_filter_block) = extent_filter_block {
        let extent_filter_end =
            extent_filter_block.offset + u64::from(extent_filter_block.stored_bytes);
        if extent_filter_end != filter_block.offset {
            return Err(format!(
                "extent filter ends at {extent_filter_end} but the filter block starts at {}; the extent filter must directly precede the filter",
                filter_block.offset
            ));
        }
    }
    let filter_end = filter_block.offset + u64::from(filter_block.stored_bytes);
    if filter_end != index_block.offset {
        return Err(format!(
            "filter block ends at {filter_end} but the index block starts at {}; the filter must directly precede the index",
            index_block.offset
        ));
    }
    if let Some(inline) = filter_inline {
        let expected_hex_len = 2 * u64::from(filter_block.stored_bytes);
        if inline.len() as u64 != expected_hex_len {
            return Err(format!(
                "inline filter is {} hex chars but the filter block stores {} bytes",
                inline.len(),
                filter_block.stored_bytes
            ));
        }
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
                let has_extent_filter = descriptor.family == MetadataRowFamily::ContentLayouts;
                if descriptor.extent_filter_block.is_some() != has_extent_filter {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: metadata_segment_object_key(descriptor),
                        message: format!(
                            "a `{}` segment descriptor {} an extent filter; only a `content_layouts` segment has one",
                            descriptor.family.as_str(),
                            if has_extent_filter { "lacks" } else { "names" }
                        ),
                    });
                }
                check_block_layout(
                    descriptor.filter_block,
                    descriptor.extent_filter_block,
                    descriptor.index_block,
                    descriptor.filter_inline.as_deref(),
                )
                .map_err(|message| {
                    ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: metadata_segment_object_key(descriptor),
                        message,
                    }
                })?;
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
            validate_segment_key_order(run.run_no, &family_segments.segments)?;
            for descriptor in &family_segments.segments {
                if descriptor.min_row_key.is_empty()
                    || descriptor.max_row_key.is_empty()
                    || descriptor.min_row_key > descriptor.max_row_key
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

/// Compares the row counts of each run's paired families, as the segment
/// descriptors state them.
///
/// The child index stores the slot index's binding events, and the slot
/// index also stores a summary row for each revision of a bound file, so a
/// run's slot index has at least as many rows as its child index. A
/// descriptor doesn't say how many of its rows are summary rows, so the
/// check is an inequality. A compaction compares the binding events themselves
/// with digests
/// (`crate::compaction::group_compaction::GroupCompaction::refuse_a_run_whose_index_disagrees`).
/// Each commit has one commit row and one receipt, so those counts are
/// equal.
fn validate_run_index_parity(
    object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for run in runs {
        let ordered = ordered_manifest_segments(object_key, &run.segments)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut commit_rows = 0u64;
        let mut commit_receipt_rows = 0u64;
        for family_segments in ordered {
            let rows = family_segments
                .segments
                .iter()
                .map(|descriptor| descriptor.row_count)
                .fold(0u64, u64::saturating_add);
            match family_segments.family {
                MetadataRowFamily::DirentryBinds => direntry_bind_rows = rows,
                MetadataRowFamily::DirentryChildBinds => direntry_child_bind_rows = rows,
                MetadataRowFamily::Commits => commit_rows = rows,
                MetadataRowFamily::CommitReceipts => commit_receipt_rows = rows,
                MetadataRowFamily::Revisions
                | MetadataRowFamily::Inodes
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::ContentLayouts
                | MetadataRowFamily::Attributes
                | MetadataRowFamily::Access
                | MetadataRowFamily::Merges => {}
            }
        }
        if direntry_bind_rows < direntry_child_bind_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {direntry_bind_rows} direntry bind rows but more child-bind index rows, {direntry_child_bind_rows}",
                    run.run_no
                ),
            });
        }
        if commit_rows != commit_receipt_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {commit_rows} commit rows but {commit_receipt_rows} commit receipt rows",
                    run.run_no
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
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: metadata_segment_object_key(descriptor),
                message: "metadata segment has no rows".to_owned(),
            });
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
    for group in MetadataFamilyGroup::ALL {
        let mut base_runs = runs.iter().filter(|run| {
            run.tier == RunTier::Base
                && run.segments.iter().any(|family_segments| {
                    group.families().contains(&family_segments.family)
                        && !family_segments.segments.is_empty()
                })
        });
        let (Some(first), Some(second)) = (base_runs.next(), base_runs.next()) else {
            continue;
        };
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "family group {:?} holds base-tier runs `{}` and `{}`; a group holds at most one base run, because a compaction writes one only when it starts at the group's oldest run and then replaces it",
                group.families(), first.run_no, second.run_no
            ),
        });
    }
    Ok(())
}

pub(crate) fn validate_manifest_row_seq_range<'a>(
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

/// Compares the binding events of a bind family against its reverse index
/// outright, over rows a caller has all of. The bind family's summary rows
/// have no copy in the reverse index.
///
/// Only the test-only inspection load keeps a whole family, so only
/// it can afford this. A compaction never keeps a whole family and checks parity by digesting
/// the rows it writes instead
/// (`crate::compaction::group_compaction::GroupCompaction::refuse_a_run_whose_index_disagrees`).
#[cfg(test)]
pub(super) fn validate_direntry_child_bind_index(
    object_key: &str,
    direntry_bind_rows: &[MetadataRow],
    direntry_child_bind_rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    let mut direntry_bind_rows = direntry_bind_rows
        .iter()
        .filter(|row| {
            !matches!(row, MetadataRow::DirentryBinding(binding) if !binding.is_binding_event())
        })
        .collect::<Vec<_>>();
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

#[cfg(test)]
pub(super) fn validate_revision_rows_have_unique_keys(
    object_key: &str,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    let mut seen = BTreeSet::new();
    for row in rows {
        let row_key = row.row_key_for_family(MetadataRowFamily::Revisions);
        if !seen.insert(row_key.clone()) {
            return Err(ManifestLoadError::DuplicateRevisionRow {
                object_key: object_key.to_owned(),
                family: MetadataRowFamily::Revisions,
                row_key,
            });
        }
    }
    Ok(())
}
