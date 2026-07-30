//! Manifest-envelope loading and descriptor verification.
//!
//! This module validates manifest framing and table descriptors without
//! fetching SST row payloads. Block IO lives in the block-loading siblings;
//! full-row inspection materialization is test-only.

use super::block_fetch::segment_codec_error;
pub(super) use super::block_load::SessionBlockMemo;
use super::cache::{
    DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheKey,
};
use super::error::ManifestLoadError;
use super::runs::{runs_in_materialization_order, runs_in_scan_order};
use super::scan::{ordered_manifest_tables, VerifiedMetadataTables};
use super::validate::{validate_manifest_materialization_ranges, validate_namespace_manifest};
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::basis::{genesis_next_inode_id, MetadataBasis};
use crate::namespace::bootstrap::bootstrap_metadata_state;
use loonfs_api::wire::control::{genesis_commit_id, HeadState, MetadataRootState};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataFileRef, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestKind, NamespaceManifestPayload,
    NAMESPACE_MANIFEST_FORMAT_VERSION,
};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId, WriterEpoch};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_table};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use tracing::Instrument;

pub(super) use super::block_fetch::load_segment_filter;
pub(super) use super::block_load::{
    load_manifest_segment_rows_in_key_range_with_cache, SegmentKeyRangeBlocks,
};
#[cfg(test)]
pub(super) use super::tests::inspection_materialization::append_rows_to_metadata;
#[cfg(test)]
pub(crate) use super::tests::inspection_materialization::{
    load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest,
};
pub(super) use super::validate::{
    validate_direntry_child_bind_index, validate_revision_by_inode_desc_index,
};

pub(super) fn ensure_root_matches_manifest(
    namespace_id: &NamespaceId,
    root: &MetadataRootState,
    manifest: &NamespaceManifestEnvelope,
) -> crate::error::Result<()> {
    if manifest.payload_checksum != root.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "metadata root for namespace `{namespace_id}` references manifest `{}` with checksum `{}`, but the manifest carries `{}`",
            root.manifest_id,
            root.manifest_payload_checksum,
            manifest.payload_checksum,
        )));
    }
    Ok(())
}

/// The genesis basis: one root-inode row and no metadata files.
///
/// A created namespace publishes no manifest, so reads before its first
/// flush replay the WAL over this synthesized state. The object id is a
/// sentinel that no generator produces and that nothing ever writes; it
/// exists because the manifest payload shape requires one.
const GENESIS_MANIFEST_OBJECT_ID: &str = "00000000000000000000-0000000000000000";

fn genesis_basis_manifest(namespace_id: &NamespaceId) -> NamespaceManifestEnvelope {
    NamespaceManifestEnvelope {
        kind: NamespaceManifestKind::NamespaceManifest,
        format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
        payload_checksum: String::new(),
        payload: NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id: ManifestId(0),
            manifest_object_id: ManifestObjectId::parse(GENESIS_MANIFEST_OBJECT_ID)
                .expect("genesis manifest object id is valid"),
            head_seq: ChangeSeq(0),
            head_commit_id: genesis_commit_id(),
            base_seq: ChangeSeq(0),
            writer_epoch: WriterEpoch(0),
            next_inode_id: genesis_next_inode_id(),
            retention_floor_seq: ChangeSeq(0),
            metadata_files: Vec::new(),
        },
    }
}

/// The materialized tables a basis resolves to, plus the in-memory rows the
/// WAL tail replays over.
pub(crate) struct BasisMetadataTables<'a, S: ObjectStore + ?Sized> {
    pub(crate) tables: VerifiedMetadataTables<'a, S>,
    /// Rows the basis contributes outside any SST: the genesis root inode,
    /// and nothing at all once a manifest exists.
    pub(crate) base_state: MetadataState,
}

/// Loads the tables a resolved basis names.
///
/// A genesis basis loads nothing: its single row is synthesized. A manifest
/// basis is loaded under its owner's prefix — this namespace's own root, or
/// the fork source the head authorizes — and is validated against the
/// checksum the authorizing object recorded. A mismatch is corruption; there
/// is no second attempt against another basis.
pub(crate) async fn load_basis_metadata_tables<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    basis: &MetadataBasis,
) -> crate::error::Result<BasisMetadataTables<'a, S>> {
    let Some(manifest) = basis.manifest() else {
        return Ok(BasisMetadataTables {
            tables: VerifiedMetadataTables::synthesized(
                store,
                genesis_basis_manifest(namespace_id),
            ),
            base_state: bootstrap_metadata_state(),
        });
    };
    let tables = load_verified_manifest_tables_with_cache(
        store,
        table_cache,
        &manifest.owner_namespace_id,
        &manifest.manifest_object_id,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    if tables.manifest().payload_checksum != manifest.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "namespace `{namespace_id}` resolves its metadata basis to manifest `{}` in namespace \
             `{}` with checksum `{}`, but the manifest carries `{}`",
            manifest.manifest_object_id,
            manifest.owner_namespace_id,
            manifest.manifest_payload_checksum,
            tables.manifest().payload_checksum,
        )));
    }
    Ok(BasisMetadataTables {
        tables,
        base_state: MetadataState::default(),
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
                "loonfs.phase",
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
            cache.get_or_load(&cache_key, fetch).await?
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
        content_store_id: current_head.content_store_id.clone(),
        fork_basis: current_head.fork_basis.clone(),
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

pub(crate) async fn load_namespace_manifest_envelope_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
    manifest_key: &str,
) -> Result<Option<NamespaceManifestEnvelope>, ManifestLoadError> {
    let Some(manifest_bytes) = store
        .get(manifest_key, None)
        .instrument(tracing::info_span!(
            "loonfs.phase",
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
