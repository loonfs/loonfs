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
use super::runs::{
    runs_in_materialization_order, runs_in_scan_order, MetadataRunManifest,
    CHECKPOINT_L0_RUN_LEVEL, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::{ordered_manifest_tables, VerifiedMetadataTables};
use super::validate::{validate_manifest_materialization_ranges, validate_namespace_manifest};
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::basis::{genesis_next_inode_id, MetadataBasis, MetadataBasisIdentity};
use crate::namespace::bootstrap::bootstrap_metadata_state;
use loonfs_api::wire::control::{genesis_commit_id, HeadState, MetadataRootState};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataFileRef, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestKind, NamespaceManifestPayload,
    NAMESPACE_MANIFEST_FORMAT_VERSION,
};
use loonfs_api::{
    ChangeSeq, ManifestId, ManifestObjectId, MetadataCompactionId, NamespaceId, WriterEpoch,
};
use loonfs_objectstore::keys::{
    metadata_compaction_job_id_from_key, metadata_compaction_table, metadata_manifest_object,
    metadata_table,
};
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

pub(super) fn genesis_basis_manifest(namespace_id: &NamespaceId) -> NamespaceManifestEnvelope {
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

/// One verified basis load: its semantic identity, materialized tables, and
/// the in-memory rows the WAL tail replays over.
pub(crate) struct LoadedMetadataBasis<'a, S: ObjectStore + ?Sized> {
    pub(crate) identity: MetadataBasisIdentity,
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
    genesis_created_at_ms: u64,
) -> crate::error::Result<LoadedMetadataBasis<'a, S>> {
    let Some(manifest) = basis.manifest() else {
        let tables =
            VerifiedMetadataTables::synthesized(store, genesis_basis_manifest(namespace_id));
        return Ok(LoadedMetadataBasis {
            identity: MetadataBasisIdentity::from_verified_basis(
                basis.clone(),
                tables.manifest().payload.head_seq,
            ),
            tables,
            base_state: bootstrap_metadata_state(genesis_created_at_ms),
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
    let manifest_head_seq = tables.manifest().payload.head_seq;
    Ok(LoadedMetadataBasis {
        identity: MetadataBasisIdentity::from_verified_basis(basis.clone(), manifest_head_seq),
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
    let manifest_key = metadata_manifest_object(namespace_id, manifest_object_id);
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
    let manifest_key = metadata_manifest_object(namespace_id, manifest_object_id);
    // Manifests are immutable per object key, so the decoded and validated
    // envelope is cacheable forever under that key.
    let fetch = || async {
        let Some(manifest_bytes) = store
            .get(&manifest_key, None)
            .instrument(tracing::debug_span!(
                "loonfs.phase",
                phase = "load_namespace_manifest",
                key_class = "namespace_manifest"
            ))
            .await
            .map_err(|err| ManifestLoadError::ReadManifest {
                object_key: manifest_key.clone(),
                message: err.public_message().into_owned(),
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
            decoded_bytes: manifest_bytes.len().saturating_mul(2),
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
        created_at_ms: current_head.created_at_ms,
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
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "load_namespace_manifest",
            key_class = "namespace_manifest"
        ))
        .await
        .map_err(|err| ManifestLoadError::ReadManifest {
            object_key: manifest_key.to_owned(),
            message: err.public_message().into_owned(),
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
    let runs = runs_in_materialization_order(&manifest.payload);
    validate_one_base_run_per_family_group(manifest_object_key, &runs)?;
    for run in &runs {
        let ordered_tables = ordered_manifest_tables(manifest_object_key, &run.tables)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut revision_rows = 0u64;
        let mut revision_by_inode_desc_rows = 0u64;
        for table in ordered_tables {
            validate_segment_numbering(&table.segments)?;
            validate_segment_key_order(&table.segments)?;
            for descriptor in &table.segments {
                ensure_segment_object_key(descriptor)?;
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
                // Keyed scans prune a segment by this key range: a segment
                // whose max key sorts below the scan bound is skipped
                // without a read. An empty or descending range hides every
                // row the segment holds, so a manifest that carries one is
                // rejected here rather than answering "not found".
                if descriptor.row_count > 0
                    && (descriptor.min_key.is_empty()
                        || descriptor.max_key.is_empty()
                        || descriptor.min_key > descriptor.max_key)
                {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: descriptor.object_key.clone(),
                        message: format!(
                            "segment holds {} rows with key range `{}`..=`{}`; a segment with \
                             rows must carry a non-empty ascending key range",
                            descriptor.row_count, descriptor.min_key, descriptor.max_key
                        ),
                    });
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

/// Checks that one family's segments inside one run are numbered the way the
/// producer that wrote them numbered them: from zero, once each, in the order
/// they were written.
///
/// One family in one run has exactly one producer — a flush's delta run or a
/// merge's output — and both number from zero. Two producers writing one
/// family at one run identity is what this rejects. That used to be
/// reachable: a merge that had to start above the group's base wrote its
/// output at the base tier stamped at the manifest head, so a group whose
/// base already sat at that identity ended up with two sets of segments both
/// numbered from zero. A merge above the base now writes a delta run at its
/// newest input's identity instead, and that identity's segments for the
/// group leave the file set in the same publication the output's enter it.
///
/// Segments reach this sorted by index (`runs_from_metadata_files`), so
/// comparing each index against its position catches a repeat, a gap, and a
/// number no producer would have written.
fn validate_segment_numbering(descriptors: &[MetadataFileRef]) -> Result<(), ManifestLoadError> {
    for (position, descriptor) in descriptors.iter().enumerate() {
        if descriptor.segment_index as usize != position {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: descriptor.object_key.clone(),
                message: format!(
                    "segment carries index {} at position {position} of family `{:?}` in run seq \
                     `{}` level {}; a family's segments within one run are numbered from zero, \
                     once each, in the order they were written",
                    descriptor.segment_index,
                    descriptor.family,
                    descriptor.run_seq,
                    descriptor.level
                ),
            });
        }
    }
    Ok(())
}

/// Checks that one family's segments inside one run ascend by key range in
/// index order, without overlap.
///
/// One producer writes a family's rows in ascending key order and never
/// writes a key twice, so each of its segments starts strictly above where
/// the previous one ended (`build::segment_rows_by_row_key_range`). The
/// numbering check cannot notice a descriptor stamped with the wrong run
/// identity when the list it joins stays densely numbered; this check notices
/// it as soon as the stray range overlaps a neighbour's, and the wide ranges
/// of folded segments make that the common case.
fn validate_segment_key_order(descriptors: &[MetadataFileRef]) -> Result<(), ManifestLoadError> {
    let mut previous: Option<&MetadataFileRef> = None;
    for descriptor in descriptors {
        if descriptor.row_count == 0 {
            continue;
        }
        if let Some(previous) = previous {
            if descriptor.min_key <= previous.max_key {
                return Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: descriptor.object_key.clone(),
                    message: format!(
                        "segment starts at `{}`, at or below the preceding segment's last key \
                         `{}`, in family `{:?}` of run seq `{}` level {}; one producer writes a \
                         family's segments in ascending key order, so consecutive ranges never \
                         touch",
                        descriptor.min_key,
                        previous.max_key,
                        descriptor.family,
                        descriptor.run_seq,
                        descriptor.level
                    ),
                });
            }
        }
        previous = Some(descriptor);
    }
    Ok(())
}

/// Checks that no family group holds more than one base-tier run.
///
/// A merge writes a base-tier run only when its window starts at the group's
/// oldest run, and such a window always contains the group's existing base
/// run, so the merge replaces it rather than adding one. Nothing else in the
/// system mints a base run: a flush appends a delta run, and a merge above
/// the base writes a delta run too.
///
/// A group with two base runs is the state a merge above the base used to
/// leave behind, and it is worse than untidy. Each fragment on its own stays
/// under the per-step budget, so nothing reports the group as too large to
/// fold from its oldest run, the group goes on merging delta runs above the
/// fragments instead, and the rows its retention floor covers can never be
/// dropped — dropping needs a merge that starts at the group's oldest run.
///
/// Different groups may share one base-tier run identity, and usually do:
/// each group folds at the manifest head, so the base runs they write land at
/// the same sequence and become one run holding several groups' families.
/// That is one run per group, which is what this counts.
fn validate_one_base_run_per_family_group(
    manifest_object_key: &str,
    runs: &[MetadataRunManifest],
) -> Result<(), ManifestLoadError> {
    for group in REORGANIZE_FAMILY_GROUPS {
        let mut base_runs = runs.iter().filter(|run| {
            run.level != CHECKPOINT_L0_RUN_LEVEL
                && run
                    .tables
                    .iter()
                    .any(|table| group.contains(table.family) && !table.segments.is_empty())
        });
        let (Some(first), Some(second)) = (base_runs.next(), base_runs.next()) else {
            continue;
        };
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: manifest_object_key.to_owned(),
            message: format!(
                "family group {:?} holds base-tier runs at seq `{}` level {} and seq `{}` \
                 level {}; a group holds at most one base run, because a merge writes one only \
                 when it starts at the group's oldest run and then replaces it",
                group.families(),
                first.run_seq,
                first.level,
                second.run_seq,
                second.level
            ),
        });
    }
    Ok(())
}

pub(super) fn metadata_file_object_key(descriptor: &MetadataFileRef) -> String {
    metadata_table(&descriptor.owner_namespace_id, &descriptor.table_id)
}

/// Checks that a descriptor names one of the two keys a segment of its
/// identity may live at.
///
/// The ordinary key is the metadata-table key its owner and table id build.
/// The other is one streaming compaction job's own prefix: a compaction
/// writes its output before any manifest names it, so its segments must sit
/// outside the listing a collector sweeps for unreferenced tables (format
/// spec, "Compaction"). Publication moves no bytes, so the manifest that swaps
/// a compaction's output in names the staged keys. The segment is an ordinary
/// one either way — same encoding, same descriptor — and the table id still
/// makes the key its producer's alone. The job id is read back out of the key
/// rather than carried on the descriptor, because which job wrote a segment
/// stops mattering the moment a manifest names it.
pub(super) fn ensure_segment_object_key(
    descriptor: &MetadataFileRef,
) -> Result<(), ManifestLoadError> {
    let expected = metadata_file_object_key(descriptor);
    let staged = metadata_compaction_job_id_from_key(&descriptor.object_key)
        .and_then(|job_id| MetadataCompactionId::parse(job_id).ok())
        .map(|metadata_compaction_id| {
            metadata_compaction_table(
                &descriptor.owner_namespace_id,
                &metadata_compaction_id,
                &descriptor.table_id,
            )
        });
    if descriptor.object_key == expected || staged.as_deref() == Some(&descriptor.object_key) {
        return Ok(());
    }
    Err(ManifestLoadError::SegmentObjectKeyMismatch {
        object_key: descriptor.object_key.clone(),
        expected,
    })
}
