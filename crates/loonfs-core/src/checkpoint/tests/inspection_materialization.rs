//! Test-only full-row manifest materialization and metadata-state inspection.

use super::super::block_load::{
    load_manifest_segment_rows_in_key_range_with_cache, SegmentKeyRangeBlocks, SessionBlockMemo,
};
use super::super::cache::MetadataTableCache;
use super::super::error::ManifestLoadError;
use super::super::load::{ensure_segment_object_key, load_namespace_manifest_envelope_if_present};
use super::super::row::manifest_row_kind;
use super::super::runs::{
    runs_in_materialization_order, MetadataTableManifest, MAX_MAINTENANCE_TABLE_IO,
};
use super::super::scan::{
    ordered_manifest_tables, ManifestMaterializationForInspection, Readahead,
};
use super::super::validate::{
    validate_direntry_child_bind_index, validate_manifest_materialization_ranges,
    validate_revision_by_inode_desc_index,
};
use crate::metadata::{MetadataState, MetadataStateBuilder};
use futures::future::try_join_all;
use loonfs_api::manifest_object_id_manifest_id;
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
};
use loonfs_api::{ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_objectstore::ObjectStore;

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
        object_key: metadata_manifest_object(namespace_id, &manifest_object_id),
    })
}

#[cfg(test)]
async fn manifest_object_id_for_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<ManifestObjectId, ManifestLoadError> {
    let prefix = metadata_manifest_prefix(namespace_id);
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

#[cfg(test)]
pub(super) async fn load_manifest_materialization_for_inspection_if_present<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<Option<ManifestMaterializationForInspection>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id, manifest_object_id);
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

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "load_manifest_tables", key_class = "metadata_sst")
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
            ensure_segment_object_key(descriptor)?;
            descriptors.push(descriptor);
        }

        let mut loaded_segments = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(MAX_MAINTENANCE_TABLE_IO) {
            loaded_segments.extend(
                try_join_all(
                    chunk
                        .iter()
                        .map(|descriptor| load_manifest_segment_rows(store, descriptor)),
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
        &direntry_bind_rows,
        &direntry_child_bind_rows,
    )?;
    validate_revision_by_inode_desc_index(
        manifest_object_key,
        &revision_rows,
        &revision_by_inode_desc_rows,
    )
}

#[cfg(test)]
pub(super) async fn load_manifest_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    descriptor: &MetadataFileRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    load_manifest_segment_rows_with_cache(store, None, descriptor).await
}

/// Loads a segment's full row set: every data block, in key order, checked
/// against the descriptor's row count and key bounds. Production reads go
/// through the key-range loader; full materialization is inspection-only.
#[cfg(test)]
pub(super) async fn load_manifest_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    descriptor: &MetadataFileRef,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let row_set = load_manifest_segment_rows_in_key_range_with_cache(
        store,
        table_cache,
        &SessionBlockMemo::default(),
        descriptor,
        "",
        None,
        Readahead::Disabled,
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

/// Projects rows into a metadata-state builder through the same decoders the
/// lookup path uses, re-attributing a foreign-kind row to the object that
/// carried it. Index-only families are kind-checked but not projected; their
/// contents are validated against the canonical family separately.
#[cfg(test)]
pub(crate) fn append_rows_to_metadata(
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
            // Derived from the tombstone rows above, like the secondary
            // indexes: decoding proves the row belongs to the family, and
            // materialization re-derives it.
            MetadataTableFamily::ActiveDeletions => {
                row_decode::active_deletion_from_manifest_row(row.clone()).map_err(mismatch)?;
            }
            MetadataTableFamily::CommitReceipts => metadata_state.push_commit_receipt(
                row_decode::commit_receipt_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
            MetadataTableFamily::Attributes => metadata_state.push_attributes_revision(
                row_decode::attributes_revision_from_manifest_row(row.clone()).map_err(mismatch)?,
            ),
        }
    }
    Ok(())
}
