//! Read-side manifest loading.
//!
//! There are two intentionally separate levels:
//!
//! 1. `load_namespace_manifest_envelope` validates only the manifest envelope.
//! 2. `load_verified_manifest_tables` validates the manifest and table
//!    descriptors without fetching SST row payloads.
//!
//! Full-row inspection materialization exists only for tests.

use super::cache::{
    DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheKey,
};
use super::error::ManifestLoadError;
#[cfg(test)]
use super::row::manifest_row_kind;
use super::runs::{runs_in_materialization_order, runs_in_scan_order};
#[cfg(test)]
use super::runs::{MetadataTableManifest, MAX_MAINTENANCE_TABLE_IO};
#[cfg(test)]
use super::scan::ManifestMaterializationForInspection;
use super::scan::{ordered_manifest_tables, VerifiedMetadataTables};
use super::validate::{
    validate_manifest_materialization_ranges, validate_manifest_row_seq_range,
    validate_namespace_manifest,
};
#[cfg(test)]
use crate::metadata::{MetadataState, MetadataStateBuilder};
#[cfg(test)]
use futures::future::try_join_all;
#[cfg(test)]
use loonfs_api::manifest_object_id_manifest_id;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataFileRef, MetadataRow, MetadataTableFamily,
    NamespaceManifestEnvelope,
};
use loonfs_api::wire::sst_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, index_blocks_for_key_range,
    BlockHandle, DecodedDataBlock, SegmentFilter, SegmentIndexEntry,
};
#[cfg(test)]
use loonfs_api::ManifestId;
use loonfs_api::{ManifestObjectId, NamespaceId};
#[cfg(test)]
use loonfs_objectstore::keys::metadata_manifest_prefix;
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_table};
use loonfs_objectstore::ByteRange;
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::Instrument;

#[cfg(test)]
pub(crate) async fn load_manifest_materialization_for_inspection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<ManifestMaterializationForInspection, ManifestLoadError> {
    let manifest_object_id =
        manifest_object_id_for_manifest_id(store, namespace_id, manifest_id).await?;
    load_manifest_materialization_for_inspection_if_present(
        store,
        namespace_id,
        &manifest_object_id,
    )
    .await?
    .ok_or_else(|| ManifestLoadError::MissingManifest {
        object_key: metadata_manifest_object(namespace_id.as_str(), &manifest_object_id),
    })
}

#[cfg(test)]
async fn manifest_object_id_for_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<ManifestObjectId, ManifestLoadError> {
    let prefix = metadata_manifest_prefix(namespace_id.as_str());
    let keys =
        store
            .list_prefix(&prefix)
            .await
            .map_err(|error| ManifestLoadError::ReadManifest {
                object_key: prefix.clone(),
                message: error.to_string(),
            })?;
    for key in keys {
        let Some(file_name) = key.rsplit('/').next() else {
            continue;
        };
        let Some(raw_id) = file_name.strip_suffix(".manifest.json") else {
            continue;
        };
        let Ok(object_id) = ManifestObjectId::parse(raw_id) else {
            continue;
        };
        if manifest_object_id_manifest_id(object_id.as_str()) == Some(manifest_id) {
            return Ok(object_id);
        }
    }
    Err(ManifestLoadError::MissingManifest {
        object_key: format!("{prefix}{:020}-*.manifest.json", manifest_id.0),
    })
}

/// Loads and validates only the manifest envelope, without fetching its
/// metadata tables. This is enough for callers that need manifest framing,
/// not table descriptors or rows.
pub(crate) async fn load_namespace_manifest_envelope<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<NamespaceManifestEnvelope, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id.as_str(), manifest_object_id);
    load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        manifest_object_id,
        &manifest_key,
    )
    .await?
    .ok_or(ManifestLoadError::MissingManifest {
        object_key: manifest_key,
    })
}

/// Loads the current manifest's verified table descriptors without fetching
/// metadata SST row payloads or constructing `MetadataState`.
pub(crate) async fn load_verified_manifest_tables<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<VerifiedMetadataTables<'a, S>, ManifestLoadError> {
    load_verified_manifest_tables_with_cache(store, None, namespace_id, manifest_object_id).await
}

pub(crate) async fn load_verified_manifest_tables_with_cache<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<VerifiedMetadataTables<'a, S>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id.as_str(), manifest_object_id);
    // Manifests are immutable per object key, so the decoded and validated
    // envelope is cacheable forever under that key.
    let fetch = || async {
        let Some(manifest_bytes) = store
            .get(&manifest_key, None)
            .instrument(tracing::info_span!(
                "loon.phase",
                phase = "load_namespace_manifest",
                key_class = "manifest_table"
            ))
            .await
            .map_err(|err| ManifestLoadError::ReadManifest {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            })?
        else {
            return Err(ManifestLoadError::MissingManifest {
                object_key: manifest_key.clone(),
            });
        };
        let manifest = decode_namespace_manifest_json(&manifest_bytes).map_err(|err| {
            ManifestLoadError::ManifestCodec {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            }
        })?;
        validate_namespace_manifest(
            namespace_id,
            manifest.payload.manifest_id,
            manifest_object_id,
            &manifest_key,
            &manifest,
        )?;
        validate_manifest_materialization_ranges(&manifest_key, &manifest.payload)?;
        validate_manifest_table_descriptors(&manifest_key, &manifest)?;
        let scan_runs = Arc::new(runs_in_scan_order(&manifest.payload));
        Ok(DecodedMetadataTableBlock::Manifest {
            manifest: Arc::new(manifest),
            scan_runs,
            // The entry retains the envelope plus its regrouped descriptor
            // list, which clones every metadata file ref once.
            decoded_byte_len: manifest_bytes.len().saturating_mul(2),
        })
    };
    let decoded = match table_cache {
        Some(cache) => {
            let cache_key = MetadataTableCacheKey {
                identity: manifest_key.clone(),
                block_kind: MetadataTableBlockKind::Manifest,
                block_offset: 0,
            };
            cache.get_or_fetch(&cache_key, fetch).await?
        }
        None => fetch().await?,
    };
    let (manifest, scan_runs) = match decoded {
        DecodedMetadataTableBlock::Manifest {
            manifest,
            scan_runs,
            ..
        } => (manifest, scan_runs),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Data { .. } => {
            return Err(segment_codec_error(
                &manifest_key,
                "cache returned a non-manifest entry for a manifest key",
            ));
        }
    };
    let tables = VerifiedMetadataTables {
        store,
        table_cache,
        manifest_object_key: manifest_key,
        manifest,
        scan_runs,
        block_memo: SessionBlockMemo::default(),
    };
    Ok(tables)
}

pub(crate) fn head_from_manifest(
    current_head: &HeadState,
    manifest: &NamespaceManifestEnvelope,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: manifest.payload.head_seq,
        head_commit_id: manifest.payload.head_commit_id.clone(),
        // The manifest records the manifest-time writer epoch. That may lag the
        // live head if writer takeover advanced the epoch without WAL replay.
        writer_epoch: manifest.payload.writer_epoch,
        writer: current_head.writer.clone(),
        next_inode_id: manifest.payload.next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: current_head.state,
    }
}

#[cfg(test)]
pub(super) async fn load_manifest_materialization_for_inspection_if_present<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<Option<ManifestMaterializationForInspection>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id.as_str(), manifest_object_id);
    let manifest = load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        manifest_object_id,
        &manifest_key,
    )
    .await?;
    let Some(manifest) = manifest else {
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

pub(crate) async fn load_namespace_manifest_envelope_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
    manifest_key: &str,
) -> Result<Option<NamespaceManifestEnvelope>, ManifestLoadError> {
    let Some(manifest_bytes) = store
        .get(manifest_key, None)
        .instrument(tracing::info_span!(
            "loon.phase",
            phase = "load_namespace_manifest",
            key_class = "manifest_table"
        ))
        .await
        .map_err(|err| ManifestLoadError::ReadManifest {
            object_key: manifest_key.to_owned(),
            message: err.to_string(),
        })?
    else {
        return Ok(None);
    };
    let manifest = decode_namespace_manifest_json(&manifest_bytes).map_err(|err| {
        ManifestLoadError::ManifestCodec {
            object_key: manifest_key.to_owned(),
            message: err.to_string(),
        }
    })?;
    validate_namespace_manifest(
        namespace_id,
        manifest.payload.manifest_id,
        manifest_object_id,
        manifest_key,
        &manifest,
    )?;
    Ok(Some(manifest))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "load_manifest_tables", key_class = "manifest_table")
)]
#[cfg(test)]
pub(super) async fn load_manifest_metadata_state_for_inspection_from_manifest<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<MetadataState, ManifestLoadError> {
    let mut metadata_state = MetadataStateBuilder::default();
    validate_manifest_materialization_ranges(manifest_object_key, &manifest.payload)?;
    for run in runs_in_materialization_order(&manifest.payload) {
        append_manifest_tables_to_metadata(
            store,
            namespace_id,
            manifest_object_key,
            &run.tables,
            &mut metadata_state,
        )
        .await?;
    }

    Ok(metadata_state.finish())
}

fn validate_manifest_table_descriptors(
    manifest_object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), ManifestLoadError> {
    for run in runs_in_materialization_order(&manifest.payload) {
        let ordered_tables = ordered_manifest_tables(manifest_object_key, &run.tables)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut revision_rows = 0u64;
        let mut revision_by_inode_desc_rows = 0u64;
        for table in ordered_tables {
            for descriptor in &table.segments {
                let expected_key = metadata_file_object_key(descriptor);
                if descriptor.object_key != expected_key {
                    return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                        object_key: descriptor.object_key.clone(),
                        expected: expected_key,
                    });
                }
                // The one segment layout: the filter block sits immediately
                // before the index block at the object tail. The read path
                // assumes it (a filter fetch extends through the index; the
                // index ends the object), so a descriptor that disagrees is
                // rejected here rather than tolerated with slower fetches.
                let filter_end =
                    descriptor.filter_block.offset + u64::from(descriptor.filter_block.stored_len);
                if filter_end != descriptor.index_block.offset {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: descriptor.object_key.clone(),
                        message: format!(
                            "filter block ends at {filter_end} but the index block starts at {}; \
                             the filter must directly precede the index",
                            descriptor.index_block.offset
                        ),
                    });
                }
                if let Some(inline) = &descriptor.filter_inline {
                    let expected_hex_len = 2 * descriptor.filter_block.stored_len as usize;
                    if inline.len() != expected_hex_len {
                        return Err(ManifestLoadError::SegmentDescriptorMismatch {
                            object_key: descriptor.object_key.clone(),
                            message: format!(
                                "inline filter is {} hex chars but the filter block stores {} bytes",
                                inline.len(),
                                descriptor.filter_block.stored_len
                            ),
                        });
                    }
                }
                match table.family {
                    MetadataTableFamily::DirentryBinds => {
                        direntry_bind_rows =
                            direntry_bind_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::DirentryChildBinds => {
                        direntry_child_bind_rows =
                            direntry_child_bind_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::Revisions => {
                        revision_rows = revision_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::RevisionsByInodeDesc => {
                        revision_by_inode_desc_rows =
                            revision_by_inode_desc_rows.saturating_add(descriptor.row_count);
                    }
                    _ => {}
                }
            }
        }

        if direntry_bind_rows != direntry_child_bind_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: manifest_object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {direntry_bind_rows} direntry bind rows but {direntry_child_bind_rows} child-bind index rows",
                    run.run_seq
                ),
            });
        }
        if revision_rows != revision_by_inode_desc_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: manifest_object_key.to_owned(),
                message: format!(
                    "metadata run `{}` has {revision_rows} revision rows but {revision_by_inode_desc_rows} revision index rows",
                    run.run_seq
                ),
            });
        }
    }

    Ok(())
}

pub(super) fn metadata_file_object_key(descriptor: &MetadataFileRef) -> String {
    metadata_table(
        descriptor.owner_namespace_id.as_str(),
        descriptor.table_id.as_str(),
    )
}

#[cfg(test)]
pub(super) async fn append_manifest_tables_to_metadata<S>(
    store: &S,
    _namespace_id: &NamespaceId,
    manifest_object_key: &str,
    tables: &[MetadataTableManifest],
    metadata_state: &mut MetadataStateBuilder,
) -> Result<(), ManifestLoadError>
where
    S: ObjectStore + ?Sized,
{
    let ordered_tables = ordered_manifest_tables(manifest_object_key, tables)?;
    let mut direntry_bind_rows = Vec::new();
    let mut direntry_child_bind_rows = Vec::new();
    let mut revision_rows = Vec::new();
    let mut revision_by_inode_desc_rows = Vec::new();
    for table in ordered_tables {
        let mut descriptors = Vec::with_capacity(table.segments.len());
        for descriptor in &table.segments {
            let expected_key = metadata_file_object_key(descriptor);
            if descriptor.object_key != expected_key {
                return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            descriptors.push(descriptor);
        }

        let mut loaded_segments = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(MAX_MAINTENANCE_TABLE_IO) {
            loaded_segments.extend(
                try_join_all(
                    chunk.iter().map(|descriptor| {
                        load_manifest_segment_rows(store, table.family, descriptor)
                    }),
                )
                .await?,
            );
        }

        for (descriptor, row_set) in descriptors.into_iter().zip(loaded_segments) {
            let rows: Vec<MetadataRow> = row_set.rows().cloned().collect();
            match table.family {
                MetadataTableFamily::DirentryBinds => {
                    direntry_bind_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::DirentryChildBinds => {
                    direntry_child_bind_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::Revisions => {
                    revision_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::RevisionsByInodeDesc => {
                    revision_by_inode_desc_rows.extend(rows.iter().cloned());
                }
                _ => {}
            }
            append_rows_to_metadata(metadata_state, table.family, &descriptor.object_key, &rows)?;
        }
    }

    validate_direntry_child_bind_index(
        manifest_object_key,
        direntry_bind_rows,
        direntry_child_bind_rows,
    )?;
    validate_revision_by_inode_desc_index(
        manifest_object_key,
        revision_rows,
        revision_by_inode_desc_rows,
    )
}

#[cfg(test)]
pub(super) async fn load_manifest_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    load_manifest_segment_rows_with_cache(store, None, family, descriptor).await
}

/// The data blocks of one segment that can hold keys in
/// `[lower_bound, upper_bound)`, shared straight from the decoded-block
/// memo and caches. Row access borrows from the blocks rather than building
/// an owned row set, which would clone every row and key of every touched
/// block on every scan.
pub(crate) struct SegmentKeyRangeBlocks {
    blocks: Vec<Arc<DecodedDataBlock>>,
}

impl SegmentKeyRangeBlocks {
    /// Rows whose keys fall in `[lower_bound, upper_bound)`, in row order,
    /// found by binary search over each block's decode-validated key order.
    /// Express a prefix scan as `[prefix, string_prefix_upper_bound(prefix))`.
    pub(super) fn rows_in_key_range<'a>(
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
    fn rows(&self) -> impl Iterator<Item = &MetadataRow> {
        self.blocks.iter().flat_map(|block| block.rows.iter())
    }

    #[cfg(test)]
    fn row_keys(&self) -> impl Iterator<Item = &String> {
        self.blocks.iter().flat_map(|block| block.row_keys.iter())
    }
}

/// Per-view memo of decoded blocks, so one operation never re-fetches or
/// re-decodes a block it already saw — the reuse that keeps cache-disabled
/// paths (cold boot, diagnostics) linear in the blocks they touch. Entries
/// share the decoded allocations with the shared cache and concurrent
/// readers; the memo retains pointers, not copies, for the view's lifetime.
#[derive(Debug, Default)]
pub(super) struct SessionBlockMemo {
    blocks: Mutex<HashMap<MetadataTableCacheKey, DecodedMetadataTableBlock>>,
}

impl SessionBlockMemo {
    fn get(&self, cache_key: &MetadataTableCacheKey) -> Option<DecodedMetadataTableBlock> {
        self.blocks
            .lock()
            .expect("session block memo lock poisoned")
            .get(cache_key)
            .cloned()
    }

    fn record(&self, cache_key: &MetadataTableCacheKey, block: &DecodedMetadataTableBlock) {
        self.blocks
            .lock()
            .expect("session block memo lock poisoned")
            .insert(cache_key.clone(), block.clone());
    }
}

/// Blocks a range scan reads ahead within a segment. Paged scans ask for a
/// couple of blocks at a time while marching through whole segments; without
/// readahead every page pays its own small GETs, and request counts scale
/// with pages instead of bytes. Read-ahead blocks land in the per-view memo
/// and shared cache, so the following pages are memory hits. 32 blocks of
/// the 8 KiB target is a ~256 KiB ranged GET.
const RANGE_SCAN_READAHEAD_BLOCKS: usize = 32;
/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
const MAX_BULK_FETCH_BYTES: u64 = 4 * 1024 * 1024;

/// Loads a segment's full row set: every data block, in key order, checked
/// against the descriptor's row count and key bounds. Production reads go
/// through the key-range loader; full materialization is inspection-only.
#[cfg(test)]
pub(super) async fn load_manifest_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let row_set = load_manifest_segment_rows_in_key_range_with_cache(
        store,
        table_cache,
        &SessionBlockMemo::default(),
        family,
        descriptor,
        "",
        None,
        false,
    )
    .await?;
    let row_count = row_set.rows().count();
    if row_count as u64 != descriptor.row_count {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "row count mismatch: expected {}, actual {row_count}",
                descriptor.row_count,
            ),
        });
    }
    if let (Some(first), Some(last)) = (row_set.row_keys().next(), row_set.row_keys().last()) {
        if descriptor.min_key != *first || descriptor.max_key != *last {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: descriptor.object_key.clone(),
                message: "descriptor min/max key mismatch".to_owned(),
            });
        }
    }
    Ok(row_set)
}

/// Loads the rows of one segment whose keys can fall in
/// `[lower_bound, upper_bound)`: index first, then only the data blocks the
/// index says can match. Callers trim edge blocks with
/// [`SegmentKeyRangeBlocks::rows_in_key_range`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    lower_bound: &str,
    upper_bound: Option<&str>,
    readahead: bool,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let index = load_segment_index(store, table_cache, memo, descriptor).await?;
    let needed = index_blocks_for_key_range(&index, lower_bound, upper_bound);

    // A paged scan marches onward through the segment: read ahead so the
    // following pages are served from the memo instead of their own GETs.
    let extended_end = if readahead {
        needed
            .start
            .saturating_add(RANGE_SCAN_READAHEAD_BLOCKS)
            .max(needed.end)
            .min(index.len())
    } else {
        needed.end
    };
    let blocks: Vec<_> = load_segment_data_block_span(
        store,
        table_cache,
        memo,
        family,
        descriptor,
        &index[needed.start..extended_end],
    )
    .await?
    .into_iter()
    .take(needed.len())
    .collect();

    validate_manifest_row_seq_range(
        &descriptor.object_key,
        blocks.iter().flat_map(|block| block.rows.iter()),
        descriptor.run_seq,
    )?;
    Ok(SegmentKeyRangeBlocks { blocks })
}

fn segment_block_cache_key(
    descriptor: &MetadataFileRef,
    block_kind: MetadataTableBlockKind,
    block_offset: u64,
) -> MetadataTableCacheKey {
    MetadataTableCacheKey {
        identity: descriptor.payload_checksum.clone(),
        block_kind,
        block_offset,
    }
}

/// Fetches exactly the byte range a handle names.
async fn load_section_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, ManifestLoadError> {
    let Some(bytes) = store
        .get(
            object_key,
            Some(ByteRange {
                start_inclusive: offset,
                end_exclusive: offset + len,
            }),
        )
        .await
        .map_err(|err| ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        })?
    else {
        return Err(ManifestLoadError::MissingSegment {
            object_key: object_key.to_owned(),
        });
    };
    if bytes.len() as u64 != len {
        return Err(ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: format!("ranged read returned {} bytes, expected {len}", bytes.len()),
        });
    }
    Ok(bytes.to_vec())
}

fn segment_codec_error(object_key: &str, err: impl std::fmt::Display) -> ManifestLoadError {
    ManifestLoadError::SegmentCodec {
        object_key: object_key.to_owned(),
        message: err.to_string(),
    }
}

/// Largest segment object fetched whole on first touch, in stored bytes.
/// Below this, splitting the index and data reads into separate ranged GETs
/// costs more round-trips than the whole object costs bytes; one GET
/// publishes every section to the memo and shared cache. Sized to catch
/// delta-run segments (one or two data blocks) while leaving base segments
/// on the per-section path.
const WHOLE_SEGMENT_FETCH_MAX_BYTES: u64 = 128 * 1024;

/// A segment object's total stored length: the index block is the last
/// section, so it ends the object.
fn segment_object_len(descriptor: &MetadataFileRef) -> u64 {
    descriptor.index_block.offset + u64::from(descriptor.index_block.stored_len)
}

/// Fetches the byte span that answers one filter or index load with a single
/// GET, decoding and publishing every section the span covers: the whole
/// object when it is small (index, filter, and all data blocks), otherwise
/// everything from the requested section to the end of the object — for a
/// filter that is the filter plus the index that directly follows it
/// (manifest loading rejects any other layout), and for an index exactly the
/// index, which ends the object. Returns the requested block.
async fn load_and_publish_segment_sections<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
    want: MetadataTableBlockKind,
) -> Result<DecodedMetadataTableBlock, ManifestLoadError> {
    let filter_handle = descriptor.filter_block;
    let index_handle = descriptor.index_block;
    let object_len = segment_object_len(descriptor);
    let fetch_whole_object = object_len <= WHOLE_SEGMENT_FETCH_MAX_BYTES;
    let fetch_offset = match want {
        MetadataTableBlockKind::Data | MetadataTableBlockKind::Manifest => {
            return Err(segment_codec_error(
                &descriptor.object_key,
                "segment section fetch supports only filter and index blocks",
            ));
        }
        _ if fetch_whole_object => 0,
        MetadataTableBlockKind::Filter => filter_handle.offset,
        MetadataTableBlockKind::Index => index_handle.offset,
    };
    let bytes = load_section_bytes(
        store,
        &descriptor.object_key,
        fetch_offset,
        object_len - fetch_offset,
    )
    .await?;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset.checked_sub(fetch_offset)?).ok()?;
        bytes.get(start..start + handle.stored_len as usize)
    };

    let index_entries = match section(&index_handle) {
        Some(stored) => {
            let entries = Arc::new(
                decode_index_block(stored, &index_handle)
                    .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
            );
            let block = DecodedMetadataTableBlock::Index {
                decoded_byte_len: index_handle.decoded_len as usize,
                entries: Arc::clone(&entries),
            };
            publish_segment_block(
                table_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataTableBlockKind::Index,
                    index_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    let filter_block = match section(&filter_handle) {
        Some(stored) => {
            let filter = decode_filter_block(stored, &filter_handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?;
            let block = DecodedMetadataTableBlock::Filter {
                decoded_byte_len: filter_handle.decoded_len as usize,
                filter: Arc::new(filter),
            };
            publish_segment_block(
                table_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataTableBlockKind::Filter,
                    filter_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    if fetch_whole_object {
        if let Some(DecodedMetadataTableBlock::Index { entries, .. }) = &index_entries {
            for entry in entries.iter() {
                let Some(stored) = section(&entry.block) else {
                    return Err(segment_codec_error(
                        &descriptor.object_key,
                        "data block outside the segment object bounds",
                    ));
                };
                let decoded = decode_data_block(stored, &entry.block)
                    .map_err(|err| segment_codec_error(&descriptor.object_key, err))?;
                let block = decoded_data_cache_block(descriptor.family, decoded);
                publish_segment_block(
                    table_cache,
                    memo,
                    segment_block_cache_key(
                        descriptor,
                        MetadataTableBlockKind::Data,
                        entry.block.offset,
                    ),
                    &block,
                );
            }
        }
    }

    let wanted = match want {
        MetadataTableBlockKind::Filter => filter_block,
        _ => index_entries,
    };
    wanted.ok_or_else(|| {
        segment_codec_error(
            &descriptor.object_key,
            "requested section outside the segment object bounds",
        )
    })
}

fn publish_segment_block(
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    cache_key: MetadataTableCacheKey,
    block: &DecodedMetadataTableBlock,
) {
    memo.record(&cache_key, block);
    if let Some(cache) = table_cache {
        cache.insert(cache_key, block.clone());
    }
}

async fn load_segment_index<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    let handle = descriptor.index_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Index, handle.offset);
    if let Some(DecodedMetadataTableBlock::Index { entries, .. }) = memo.get(&cache_key) {
        return Ok(entries);
    }
    let fetch = || async {
        load_and_publish_segment_sections(
            store,
            table_cache,
            memo,
            descriptor,
            MetadataTableBlockKind::Index,
        )
        .await
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Index { entries, .. } => Ok(entries),
        DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-index block for an index key",
        )),
    }
}

/// Loads a segment's bloom filter block: the cheap pre-index check a lookup
/// consults to skip the segment entirely. A copy inlined in the manifest
/// descriptor answers without any object fetch; otherwise one ranged GET
/// covers the filter together with the adjacent index block (and the whole
/// object when it is small), so an admitted lookup does not pay a second
/// round-trip for the index.
pub(super) async fn load_segment_filter<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
) -> Result<Arc<SegmentFilter>, ManifestLoadError> {
    let handle = descriptor.filter_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Filter, handle.offset);
    if let Some(DecodedMetadataTableBlock::Filter { filter, .. }) = memo.get(&cache_key) {
        return Ok(filter);
    }
    if let Some(inline) = &descriptor.filter_inline {
        let bytes = hex_decode_bytes(inline).map_err(|err| {
            segment_codec_error(&descriptor.object_key, format!("inline filter: {err}"))
        })?;
        // The handle names and verifies the durable filter block; decoding
        // the inline copy against it proves the two are byte-identical.
        let filter = Arc::new(
            decode_filter_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        );
        let block = DecodedMetadataTableBlock::Filter {
            decoded_byte_len: handle.decoded_len as usize,
            filter: Arc::clone(&filter),
        };
        publish_segment_block(table_cache, memo, cache_key, &block);
        return Ok(filter);
    }
    let fetch = || async {
        load_and_publish_segment_sections(
            store,
            table_cache,
            memo,
            descriptor,
            MetadataTableBlockKind::Filter,
        )
        .await
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Filter { filter, .. } => Ok(filter),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-filter block",
        )),
    }
}

async fn load_segment_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    entry: &SegmentIndexEntry,
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entry.block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataTableBlock::Data { block, .. }) = memo.get(&cache_key) {
        return Ok(block);
    }
    let fetch = || async {
        let bytes = load_section_bytes(
            store,
            &descriptor.object_key,
            handle.offset,
            handle.stored_len as u64,
        )
        .await?;
        Ok(decoded_data_cache_block(
            family,
            decode_data_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        ))
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Data { block, .. } => Ok(block),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-data block for a data key",
        )),
    }
}

fn decoded_data_cache_block(
    family: MetadataTableFamily,
    block: DecodedDataBlock,
) -> DecodedMetadataTableBlock {
    DecodedMetadataTableBlock::Data {
        decoded_byte_len: decoded_manifest_block_weight(family, &block.rows),
        block: Arc::new(block),
    }
}

/// Bulk path for wide selections: consult the cache per block, group the
/// misses into consecutive spans, and fetch each span with coalesced ranged
/// GETs instead of one request per block. Duplicate concurrent span fetches
/// are possible and benign; the narrow path keeps single-flight for the hot
/// point lookups.
async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks: Vec<Option<Arc<DecodedDataBlock>>> = vec![None; entries.len()];
    // One probe key reused across the span: a fresh key per block would
    // clone the segment checksum once per block on every warm scan.
    let mut probe_key = segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, 0);
    for (position, entry) in entries.iter().enumerate() {
        probe_key.block_offset = entry.block.offset;
        if let Some(DecodedMetadataTableBlock::Data { block, .. }) = memo.get(&probe_key) {
            blocks[position] = Some(block);
            continue;
        }
        if let Some(cache) = table_cache {
            if let Some(DecodedMetadataTableBlock::Data { block, .. }) = cache.get(&probe_key) {
                blocks[position] = Some(block);
            }
        }
    }

    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut cursor = 0;
    while cursor < entries.len() {
        if blocks[cursor].is_some() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut span_bytes = 0u64;
        while cursor < entries.len()
            && blocks[cursor].is_none()
            && span_bytes + u64::from(entries[cursor].block.stored_len) <= MAX_BULK_FETCH_BYTES
        {
            span_bytes += u64::from(entries[cursor].block.stored_len);
            cursor += 1;
        }
        // A single block larger than the fetch cap still fetches alone.
        if cursor == start {
            cursor += 1;
        }
        spans.push((start, cursor));
    }

    futures::future::try_join_all(spans.iter().map(|(start, end)| {
        let span = &entries[*start..*end];
        async move {
            // Single-flight on the span's first block: the winning fetch
            // decodes and publishes the whole span, so a concurrent scan
            // waiting here finds the remaining blocks in the shared cache
            // instead of re-fetching the span.
            let first_key = segment_block_cache_key(
                descriptor,
                MetadataTableBlockKind::Data,
                span[0].block.offset,
            );
            let fetch = || async {
                load_and_publish_span(store, table_cache, memo, family, descriptor, span).await
            };
            match table_cache {
                Some(cache) => {
                    cache.get_or_fetch(&first_key, fetch).await?;
                }
                None => {
                    fetch().await?;
                }
            }
            Ok::<_, ManifestLoadError>(())
        }
    }))
    .await?;

    // Everything the winner published (or this call fetched) is resolvable
    // now; blocks that lost a cache race are fetched alone.
    for (position, entry) in entries.iter().enumerate() {
        if blocks[position].is_some() {
            continue;
        }
        blocks[position] = Some(
            load_segment_data_block(store, table_cache, memo, family, descriptor, entry).await?,
        );
    }

    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block is resolved above"))
        .collect())
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, and publishes them to the per-view memo and (when populating) the
/// shared cache. Returns the first block's cache entry for the
/// single-flight cell.
async fn load_and_publish_span<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    span: &[SegmentIndexEntry],
) -> Result<DecodedMetadataTableBlock, ManifestLoadError> {
    let first = &span[0].block;
    let last = &span[span.len() - 1].block;
    let span_len = last.offset + u64::from(last.stored_len) - first.offset;
    let bytes = load_section_bytes(store, &descriptor.object_key, first.offset, span_len).await?;
    let mut first_block = None;
    for entry in span {
        let handle = entry.block;
        let begin = (handle.offset - first.offset) as usize;
        let stored = &bytes[begin..begin + handle.stored_len as usize];
        let decoded = Arc::new(
            decode_data_block(stored, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        );
        let cache_key =
            segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, handle.offset);
        let cache_block = DecodedMetadataTableBlock::Data {
            decoded_byte_len: decoded_manifest_block_weight(family, &decoded.rows),
            block: decoded,
        };
        memo.record(&cache_key, &cache_block);
        if let Some(cache) = table_cache {
            // The first block is what the single-flight cell publishes;
            // inserting it here too keeps the whole span uniformly cached.
            cache.insert(cache_key, cache_block.clone());
        }
        if first_block.is_none() {
            first_block = Some(cache_block);
        }
    }
    Ok(first_block.expect("a span always holds at least one block"))
}

pub(super) fn decoded_manifest_block_weight(
    family: MetadataTableFamily,
    rows: &[MetadataRow],
) -> usize {
    let row_weight = rows
        .iter()
        .map(|row| 64 + row.row_key_for_family(family).len() + decoded_manifest_row_weight(row))
        .sum::<usize>();
    row_weight.saturating_add(128)
}

pub(super) fn decoded_manifest_row_weight(row: &MetadataRow) -> usize {
    match row {
        MetadataRow::Inode { .. } => 32,
        MetadataRow::DirentryBind {
            name_key,
            display_name,
            ..
        } => 96 + name_key.as_str().len() + display_name.len(),
        MetadataRow::DirentryUnbind { name_key, .. } => 96 + name_key.as_str().len(),
        MetadataRow::Revision { content_ref, .. } => 96 + content_ref.digest.len(),
        MetadataRow::Tombstone { .. } => 32,
        MetadataRow::CommitReceipt {
            commit_id,
            semantic_commit_fingerprint,
            message,
            ..
        } => {
            96 + commit_id.as_str().len()
                + semantic_commit_fingerprint.len()
                + message.as_ref().map_or(0, String::len)
        }
    }
}

/// Projects rows into a metadata-state builder through the same decoders the
/// lookup path uses, re-attributing a foreign-kind row to the object that
/// carried it. Index-only families are kind-checked but not projected; their
/// contents are validated against the canonical family separately.
#[cfg(test)]
pub(super) fn append_rows_to_metadata(
    metadata_state: &mut MetadataStateBuilder,
    family: MetadataTableFamily,
    object_key: &str,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    use crate::metadata::row_decode;
    for row in rows {
        let mismatch = |_: crate::error::CoreError| ManifestLoadError::TableRowKindMismatch {
            object_key: object_key.to_owned(),
            family,
            row_kind: manifest_row_kind(row).to_owned(),
        };
        match family {
            MetadataTableFamily::Inodes => metadata_state
                .push_inode(row_decode::inode_from_manifest_row(row.clone()).map_err(mismatch)?),
            MetadataTableFamily::DirentryBinds => metadata_state.push_direntry_bind(
                row_decode::direntry_bind_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataTableFamily::DirentryChildBinds => {
                row_decode::direntry_bind_from_manifest_row(row.clone()).map_err(mismatch)?;
            }
            MetadataTableFamily::DirentryUnbinds => metadata_state.push_direntry_unbind(
                row_decode::direntry_unbind_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataTableFamily::Revisions => metadata_state.push_revision(
                row_decode::revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataTableFamily::RevisionsByInodeDesc => {
                row_decode::revision_from_manifest_row(row.clone()).map_err(mismatch)?;
            }
            MetadataTableFamily::Tombstones => metadata_state.push_subtree_tombstone(
                row_decode::tombstone_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataTableFamily::CommitReceipts => metadata_state.push_commit_receipt(
                row_decode::commit_receipt_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
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
