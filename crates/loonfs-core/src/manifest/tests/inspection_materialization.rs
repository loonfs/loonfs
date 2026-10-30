//! Test-only full-row manifest materialization and metadata-state inspection.

use super::super::error::ManifestLoadError;
use super::super::load::load_namespace_manifest_envelope_if_present;
use super::super::validate::validate_manifest_row_seq_range;
use super::super::validate::{
    validate_direntry_child_bind_index, validate_manifest_materialization_ranges,
    validate_revision_rows_have_unique_keys,
};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::metadata::{MetadataState, MetadataStateBuilder};
use crate::segment::block_memo::{load_segment_blocks_with_readahead, SessionBlockMemo};
use crate::segment::cache::read_ahead_window_bytes;
use crate::segment::cache::MetadataSegmentCache;
use crate::segment::data_block_load::read_ahead_window_len;
use crate::segment::index_block_load::load_segment_index;
use crate::segment::row::manifest_row_kind;
use crate::segment::runs::{runs_in_materialization_order, MetadataFamilySegments};
use crate::segment::scan::{
    ordered_manifest_segments, ManifestMaterializationForInspection, Readahead,
};
use futures::future::try_join_all;
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_segment_object_key};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
};
use loonfs_types::format::segment_blocks::{index_blocks_for_key_range, DecodedDataBlock};
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
use std::sync::Arc;

#[cfg(test)]
pub(crate) async fn load_manifest_materialization_for_inspection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> Result<ManifestMaterializationForInspection, ManifestLoadError> {
    let manifest_number = manifest_no;
    load_manifest_materialization_for_inspection_if_present(store, namespace_id, &manifest_number)
        .await?
        .ok_or_else(|| ManifestLoadError::MissingManifest {
            object_key: metadata_manifest_object(namespace_id, &manifest_number),
        })
}

#[cfg(test)]
pub(super) async fn load_manifest_materialization_for_inspection_if_present<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_number: &ManifestNo,
) -> Result<Option<ManifestMaterializationForInspection>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id, manifest_number);
    let manifest =
        load_namespace_manifest_envelope_if_present(store, namespace_id, manifest_number).await?;
    let Some((manifest, _)) = manifest else {
        return Ok(None);
    };
    let metadata_state = load_manifest_metadata_state_for_inspection_from_manifest(
        store,
        namespace_id,
        &manifest_key,
        &manifest,
    )
    .await?;
    Ok(Some(ManifestMaterializationForInspection {
        manifest,
        metadata_state,
    }))
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = crate::Phase::LoadManifestSegments.as_str(), key_class = "metadata_segment")
)]
#[cfg(test)]
pub(crate) async fn load_manifest_metadata_state_for_inspection_from_manifest<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<MetadataState, ManifestLoadError> {
    let mut metadata_state = MetadataStateBuilder::default();
    validate_manifest_materialization_ranges(manifest_object_key, manifest.payload())?;
    for run in runs_in_materialization_order(manifest.payload()) {
        append_manifest_segments_to_metadata(
            store,
            namespace_id,
            manifest_object_key,
            run.run_seq,
            &run.segments,
            &mut metadata_state,
        )
        .await?;
    }

    Ok(metadata_state.build())
}

#[cfg(test)]
pub(super) async fn append_manifest_segments_to_metadata<S>(
    store: &S,
    _namespace_id: &NamespaceId,
    manifest_object_key: &str,
    run_seq: ChangeSeq,
    segments_by_family: &[MetadataFamilySegments],
    metadata_state: &mut MetadataStateBuilder,
) -> Result<(), ManifestLoadError>
where
    S: ObjectStore + ?Sized,
{
    let ordered = ordered_manifest_segments(manifest_object_key, segments_by_family)?;
    let mut direntry_bind_rows = Vec::new();
    let mut direntry_child_bind_rows = Vec::new();
    let mut revision_rows = Vec::new();
    for family_segments in ordered {
        let mut loaded_segments = Vec::with_capacity(family_segments.segments.len());
        for chunk in family_segments.segments.chunks(STORE_READS_IN_FLIGHT) {
            loaded_segments.extend(
                try_join_all(
                    chunk
                        .iter()
                        .map(|descriptor| load_manifest_segment_rows(store, run_seq, descriptor)),
                )
                .await?,
            );
        }

        for (descriptor, row_set) in family_segments.segments.iter().zip(loaded_segments) {
            let rows: Vec<MetadataRow> = row_set.rows().cloned().collect();
            match family_segments.family {
                MetadataRowFamily::DirentryBinds => {
                    direntry_bind_rows.extend(rows.iter().cloned());
                }
                MetadataRowFamily::DirentryChildBinds => {
                    direntry_child_bind_rows.extend(rows.iter().cloned());
                }
                MetadataRowFamily::Revisions => {
                    revision_rows.extend(rows.iter().cloned());
                }
                _ => {}
            }
            append_rows_to_metadata(
                metadata_state,
                family_segments.family,
                &metadata_segment_object_key(descriptor),
                &rows,
            )?;
        }
    }

    validate_direntry_child_bind_index(
        manifest_object_key,
        &direntry_bind_rows,
        &direntry_child_bind_rows,
    )?;
    validate_revision_rows_have_unique_keys(manifest_object_key, &revision_rows)
}

#[cfg(test)]
pub(super) async fn load_manifest_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    run_seq: ChangeSeq,
    descriptor: &MetadataSegmentRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    load_manifest_segment_rows_with_cache(store, None, run_seq, descriptor).await
}

/// Loads a segment's full row set: every data block, in key order, checked
/// against the descriptor's row count and key bounds. Production reads go
/// through the key-range loader; full materialization is inspection-only.
#[cfg(test)]
pub(super) async fn load_manifest_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    run_seq: ChangeSeq,
    descriptor: &MetadataSegmentRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let row_set = load_manifest_segment_rows_in_key_range_with_cache(
        store,
        segment_cache,
        &SessionBlockMemo::default(),
        descriptor,
        run_seq,
        "",
        None,
        usize::MAX,
        Readahead::Disabled,
    )
    .await?;
    let row_count = row_set.rows().count();
    if row_count as u64 != descriptor.row_count {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: metadata_segment_object_key(descriptor),
            message: format!(
                "row count mismatch: expected {}, actual {row_count}",
                descriptor.row_count,
            ),
        });
    }
    if let (Some(first), Some(last)) = (row_set.row_keys().next(), row_set.row_keys().last()) {
        if descriptor.min_row_key != *first || descriptor.max_row_key != *last {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: metadata_segment_object_key(descriptor),
                message: "descriptor min/max key mismatch".to_owned(),
            });
        }
    }
    Ok(row_set)
}

/// Projects rows into a metadata-state builder through the same decoders the
/// lookup path uses, re-attributing a foreign-kind row to the object that
/// carried it. Index-only families are kind-checked but not projected; their
/// contents are validated against the canonical family separately.
#[cfg(test)]
pub(crate) fn append_rows_to_metadata(
    metadata_state: &mut MetadataStateBuilder,
    family: MetadataRowFamily,
    object_key: &str,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    use crate::metadata::row_decode;
    for row in rows {
        let mismatch = |_: crate::error::CoreError| ManifestLoadError::SegmentRowKindMismatch {
            object_key: object_key.to_owned(),
            family,
            row_kind: manifest_row_kind(row).to_owned(),
        };
        match family {
            MetadataRowFamily::Inodes => metadata_state
                .push_inode(row_decode::inode_from_manifest_row(row.clone()).map_err(mismatch)?),
            MetadataRowFamily::DirentryBinds => metadata_state.push_direntry_binding(
                row_decode::direntry_binding_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::DirentryChildBinds => {
                row_decode::direntry_binding_from_manifest_row(row.clone()).map_err(mismatch)?;
            }
            MetadataRowFamily::DirentryRevisions => metadata_state.push_direntry_revision(
                row_decode::direntry_revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::Revisions => metadata_state.push_revision(
                row_decode::revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::Tombstones => metadata_state.push_subtree_tombstone(
                row_decode::tombstone_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            // Derived from the tombstone rows above, like the secondary
            // indexes: decoding proves the row belongs to the family, and
            // materialization re-derives it.
            MetadataRowFamily::ActiveDeletions => {
                row_decode::active_deletion_from_manifest_row(row.clone()).map_err(mismatch)?;
            }
            MetadataRowFamily::ContentLayouts => metadata_state.push_content_layout(
                row_decode::content_layout_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::CommitReceipts => metadata_state.push_commit_receipt(
                row_decode::commit_receipt_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::Commits => metadata_state
                .push_commit(row_decode::commit_from_manifest_row(row.clone()).map_err(mismatch)?),
            MetadataRowFamily::Attributes => metadata_state.push_attributes_revision(
                row_decode::attributes_revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataRowFamily::Access => metadata_state.push_access_revision(
                row_decode::access_revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
        }
    }
    Ok(())
}

/// The data blocks of one segment that can hold keys in
/// `[lower_bound, upper_bound)`, shared straight from the decoded-block
/// memo and caches. Row access borrows from the blocks rather than building
/// an owned row set, which would clone every row and key of every touched
/// block on every scan.
#[cfg(test)]
pub(crate) struct SegmentKeyRangeBlocks {
    blocks: Vec<Arc<DecodedDataBlock>>,
}

#[cfg(test)]
impl SegmentKeyRangeBlocks {
    /// Rows whose keys fall in `[lower_bound, upper_bound)`, in row order,
    /// found by binary search over each block's decode-validated key order.
    /// Express a prefix scan as `[prefix, string_prefix_upper_bound(prefix))`.
    pub(crate) fn rows_in_key_range<'a>(
        &'a self,
        lower_bound: &'a str,
        upper_bound: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a MetadataRow)> + 'a {
        self.blocks.iter().flat_map(move |block| {
            let start = block
                .row_keys
                .partition_point(|key| key.as_str() < lower_bound);
            let end = upper_bound.map_or(block.row_keys.len(), |upper_bound| {
                block
                    .row_keys
                    .partition_point(|key| key.as_str() < upper_bound)
            });
            let range = start..end.max(start);
            block.row_keys[range.clone()]
                .iter()
                .zip(&block.rows[range])
                .map(|(key, row)| (key.as_str(), row))
        })
    }

    #[cfg(test)]
    pub(crate) fn rows(&self) -> impl Iterator<Item = &MetadataRow> {
        self.blocks.iter().flat_map(|block| block.rows.iter())
    }

    #[cfg(test)]
    pub(crate) fn row_keys(&self) -> impl Iterator<Item = &String> {
        self.blocks.iter().flat_map(|block| block.row_keys.iter())
    }
}

/// Loads the rows of one segment whose keys can fall in
/// `[lower_bound, upper_bound)`: index first, then only the data blocks the
/// index says can match. Callers trim edge blocks with
/// [`SegmentKeyRangeBlocks::rows_in_key_range`].
#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the segment scan inputs stay explicit at the shared load boundary"
)]
pub(crate) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    max_seq: ChangeSeq,
    lower_bound: &str,
    upper_bound: Option<&str>,
    row_limit: usize,
    readahead: Readahead,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let index = load_segment_index(store, segment_cache, memo, descriptor).await?;
    let needed = index_blocks_for_key_range(&index, lower_bound, upper_bound);

    let mut result = SegmentKeyRangeBlocks { blocks: Vec::new() };
    let mut remaining_rows = row_limit;
    let mut start = needed.start;
    while start < needed.end && remaining_rows > 0 {
        let required_end = if readahead == Readahead::Enabled {
            start + 1
        } else if row_limit == usize::MAX {
            needed.end
        } else {
            (start + read_ahead_window_len(&index[start..], read_ahead_window_bytes(segment_cache)))
                .min(needed.end)
        };
        let blocks = load_segment_blocks_with_readahead(
            store,
            segment_cache,
            memo,
            descriptor,
            &index,
            start..required_end,
            readahead,
        )
        .await?;
        let batch = SegmentKeyRangeBlocks { blocks };
        if row_limit != usize::MAX {
            remaining_rows = remaining_rows
                .saturating_sub(batch.rows_in_key_range(lower_bound, upper_bound).count());
        }
        result.blocks.extend(batch.blocks);
        start = required_end;
    }

    validate_manifest_row_seq_range(
        &metadata_segment_object_key(descriptor),
        result.blocks.iter().flat_map(|block| block.rows.iter()),
        max_seq,
    )?;
    Ok(result)
}
