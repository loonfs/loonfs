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
use super::partial_fold::parse_row_digest;
use super::partition::{GroupPartitioning, PartitionCursor};
use super::runs::{
    runs_in_materialization_order, runs_in_scan_order, MetadataRunManifest,
    CHECKPOINT_L0_RUN_LEVEL, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::{ordered_manifest_tables, VerifiedMetadataTables};
use super::validate::{validate_manifest_materialization_ranges, validate_namespace_manifest};
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::basis::{genesis_next_inode_id, MetadataBasis};
use crate::namespace::bootstrap::bootstrap_metadata_state;
use loonfs_api::wire::control::{genesis_commit_id, HeadState, MetadataRootState};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataFileRef, MetadataRunId, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestKind, NamespaceManifestPayload,
    NAMESPACE_MANIFEST_FORMAT_VERSION,
};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId, WriterEpoch};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_table};
use loonfs_objectstore::ObjectStore;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
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
            reorganize: None,
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
        validate_manifest_reorganize_progress(&manifest_key, &manifest.payload)?;
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
    let runs = runs_in_materialization_order(&manifest.payload);
    validate_one_base_run_per_family_group(manifest_object_key, &runs)?;
    for run in &runs {
        let ordered_tables = ordered_manifest_tables(manifest_object_key, &run.tables)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut revision_rows = 0u64;
        let mut revision_by_inode_desc_rows = 0u64;
        for table in ordered_tables {
            validate_segment_descriptors(&table.segments)?;
            validate_segment_numbering(&table.segments)?;
            for descriptor in &table.segments {
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

/// Checks one family's segment descriptors within one run.
///
/// Every descriptor the read path may reach goes through this, whether it
/// sits in `metadata_files` or in the output list of a partial fold: a fold's
/// outputs become run segments at the swap, and a malformed one is no more
/// acceptable for having been written by a fold in progress. Rejecting it at
/// load is what stops a fold from building a root that fails on its next
/// load.
fn validate_segment_descriptors(descriptors: &[MetadataFileRef]) -> Result<(), ManifestLoadError> {
    for descriptor in descriptors {
        let expected_key = metadata_file_object_key(descriptor);
        if descriptor.object_key != expected_key {
            return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                object_key: descriptor.object_key.clone(),
                expected: expected_key,
            });
        }
        // The one segment layout: the filter block sits immediately before
        // the index block at the object tail. The read path assumes it (a
        // filter fetch extends through the index; the index ends the
        // object), so a descriptor that disagrees is rejected here rather
        // than tolerated with slower fetches.
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
        validate_segment_key_range(descriptor)?;
    }
    Ok(())
}

/// Checks that one family's segments inside one run are numbered the way the
/// producer that wrote them numbered them: from zero, once each, in the order
/// they were written.
///
/// One family in one run has exactly one producer — a flush's delta run, a
/// merge's output, or the run a partial fold builds across its slices — and
/// every one of them numbers from zero. Two producers writing one family at
/// one run identity is what this rejects. That used to be reachable: a merge
/// that had to start above the group's base wrote its output at the base tier
/// stamped at the manifest head, so a group whose base already sat at that
/// identity ended up with two sets of segments both numbered from zero. A
/// merge above the base now writes a delta run at its newest input's identity
/// instead, and that identity's segments for the group leave the file set in
/// the same publication the output's enter it.
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

/// Checks that no family group holds more than one base-tier run.
///
/// A merge writes a base-tier run only when its window starts at the group's
/// oldest run, and such a window always contains the group's existing base
/// run, so the merge replaces it rather than adding one. A completed partial
/// fold does the same over the whole group. Nothing else in the system mints
/// a base run: a flush appends a delta run, and a merge above the base writes
/// a delta run too.
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
                    .any(|table| group.contains(&table.family) && !table.segments.is_empty())
        });
        let (Some(first), Some(second)) = (base_runs.next(), base_runs.next()) else {
            continue;
        };
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: manifest_object_key.to_owned(),
            message: format!(
                "family group {group:?} holds base-tier runs at seq `{}` level {} and seq `{}` \
                 level {}; a group holds at most one base run, because a merge writes one only \
                 when it starts at the group's oldest run and then replaces it",
                first.run_seq, first.level, second.run_seq, second.level
            ),
        });
    }
    Ok(())
}

/// Keyed scans prune a segment by this key range: a segment whose max key
/// sorts below the scan bound is skipped without a read. An empty or
/// descending range hides every row the segment holds, so a manifest that
/// carries one is rejected here rather than answering "not found".
fn validate_segment_key_range(descriptor: &MetadataFileRef) -> Result<(), ManifestLoadError> {
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
    Ok(())
}

/// Checks the partial fold a manifest carries, if it carries one.
///
/// The builder's invariants are the loader's checks: a fold that resumes
/// from a manifest trusts every field below, so a manifest that disagrees
/// with itself is refused rather than resumed. See the design doc,
/// `docs/design/metadata-partial-fold.md`.
fn validate_manifest_reorganize_progress(
    manifest_object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    let Some(progress) = &payload.reorganize else {
        return Ok(());
    };
    let invalid = |message: String| ManifestLoadError::RunManifestMismatch {
        object_key: manifest_object_key.to_owned(),
        message,
    };

    // The group is what makes the fold's drop rules legal, so it must be one
    // of the groups the reorganizer folds, spelled exactly.
    if !REORGANIZE_FAMILY_GROUPS.contains(&progress.families.as_slice()) {
        return Err(invalid(format!(
            "reorganize progress names families {:?}, which is not a reorganization family group",
            progress.families
        )));
    }

    // The input runs stay in `metadata_files` and serve reads until the
    // swap, so a name with no run behind it means the fold has already lost
    // its input. A fold with no input at all merges nothing and would swap an
    // empty run in for a group it never read.
    if progress.input_runs.is_empty() {
        return Err(invalid(
            "reorganize progress names no input run, so it is merging nothing".to_owned(),
        ));
    }
    let runs: BTreeSet<MetadataRunId> = payload
        .metadata_files
        .iter()
        .map(|descriptor| MetadataRunId {
            run_seq: descriptor.run_seq,
            level: descriptor.level,
        })
        .collect();
    let mut named_inputs = BTreeSet::new();
    for input in &progress.input_runs {
        if !runs.contains(input) {
            return Err(invalid(format!(
                "reorganize progress names input run seq `{}` level {}, which the manifest does \
                 not reference",
                input.run_seq, input.level
            )));
        }
        // The completing step drops every named run from the file set at
        // once, and the selector counts a group's waiting work by excluding
        // them. A run named twice is a state no builder produces.
        if !named_inputs.insert(*input) {
            return Err(invalid(format!(
                "reorganize progress names input run seq `{}` level {} twice",
                input.run_seq, input.level
            )));
        }
    }

    // The digests are how the completing step decides whether the run it
    // built holds the same rows in a secondary index as in its canonical
    // family. One that does not parse leaves that question unanswerable.
    for (field, spelled) in [
        ("canonical_rows_digest", &progress.canonical_rows_digest),
        ("index_rows_digest", &progress.index_rows_digest),
    ] {
        if parse_row_digest(spelled).is_none() {
            return Err(invalid(format!(
                "reorganize progress carries `{field}` `{spelled}`, which is not thirty-two \
                 lowercase hex characters"
            )));
        }
    }

    // The frozen floor is what every step and every resumption decides
    // drops against. A floor above the manifest's own would drop rows the
    // manifest still promises to retain.
    if progress.frozen_floor_seq > payload.retention_floor_seq {
        return Err(invalid(format!(
            "reorganize progress froze the retention floor at `{}`, which is above the manifest's \
             floor `{}`",
            progress.frozen_floor_seq, payload.retention_floor_seq
        )));
    }

    // The cursor is the fold's whole position. A fold resumed from one that
    // does not parse would rewrite the wrong slice of the keyspace, so a
    // state that cannot say where it stands is refused here.
    let partitioning = GroupPartitioning::for_group(&progress.families).ok_or_else(|| {
        invalid(format!(
            "reorganize progress names families {:?}, whose row keys share no partition grammar",
            progress.families
        ))
    })?;
    let cursor = partitioning.parse_cursor(&progress.cursor).map_err(|why| {
        invalid(format!(
            "reorganize progress carries an unusable cursor: {why}"
        ))
    })?;

    // A fold that stands inside one oversized partition says so with an
    // offset, which names the family it is working through and how far. The
    // outputs it has written reach further for the families already done than
    // for the ones not started, so the bound below reads the offset too.
    let offset = match &progress.partition_offset {
        Some(spelled) => Some(
            partitioning
                .parse_partition_offset(&progress.families, &cursor, spelled)
                .map_err(|why| {
                    invalid(format!(
                        "reorganize progress carries an unusable partition offset: {why}"
                    ))
                })?,
        ),
        None => None,
    };
    let written_through = |family: MetadataTableFamily| -> WrittenThrough {
        let Some(offset) = &offset else {
            return WrittenThrough::Below(partitioning.family_lower_bound(family, &cursor));
        };
        let PartitionCursor::At(partition) = &cursor else {
            unreachable!("an offset only parses against a cursor standing at a partition")
        };
        let position = |family| progress.families.iter().position(|held| *held == family);
        match position(family).cmp(&position(offset.family)) {
            // A family the fold has finished for this partition: its rows
            // reach the partition's end and no further.
            Ordering::Less => {
                WrittenThrough::Below(partitioning.family_partition_end(family, partition))
            }
            Ordering::Equal => WrittenThrough::AtOrBelow(offset.written_through.clone()),
            // A family the fold has not reached in this partition: its rows
            // stop where the partition starts.
            Ordering::Greater => {
                WrittenThrough::Below(partitioning.family_lower_bound(family, &cursor))
            }
        }
    };

    // Output segments become run segments at the swap, so they pass the same
    // checks a run's descriptors pass, per family and in the order the fold
    // wrote them.
    let mut segments_by_family: BTreeMap<MetadataTableFamily, Vec<MetadataFileRef>> =
        BTreeMap::new();
    for segment in &progress.output_segments {
        if !progress.families.contains(&segment.family) {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: segment.object_key.clone(),
                message: format!(
                    "reorganize output segment holds family `{:?}`, which is outside the folded \
                     group {:?}",
                    segment.family, progress.families
                ),
            });
        }
        // Every output segment belongs to the run the fold is building. One
        // stamped otherwise would land in some other run at the swap.
        if segment.run_seq != progress.output_run_seq || segment.level != progress.output_level {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: segment.object_key.clone(),
                message: format!(
                    "reorganize output segment carries run seq `{}` level {}, but the fold builds \
                     run seq `{}` level {}",
                    segment.run_seq, segment.level, progress.output_run_seq, progress.output_level
                ),
            });
        }
        segments_by_family
            .entry(segment.family)
            .or_default()
            .push(segment.clone());
    }
    for (family, segments) in &segments_by_family {
        validate_segment_descriptors(segments)?;
        validate_segment_numbering(segments)?;
        let bound = written_through(*family);
        let mut previous: Option<&MetadataFileRef> = None;
        for segment in segments {
            // A fold writes one family's outputs in ascending key order and
            // never revisits a key, so two output segments of one family can
            // neither overlap nor descend. Only segments holding rows carry a
            // range to compare.
            if segment.row_count == 0 {
                continue;
            }
            // Everything written so far belongs to a key the fold has
            // passed, so it sorts below where the fold now stands. A row at
            // or above that would be rewritten again by the next step and
            // land in the finished run twice.
            if !bound.admits(&segment.max_key) {
                return Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: segment.object_key.clone(),
                    message: format!(
                        "reorganize output segment ends at `{}`, past `{}`, which is as far as \
                         the fold has written family `{family:?}`",
                        segment.max_key,
                        bound.key()
                    ),
                });
            }
            if let Some(previous) = previous {
                if segment.min_key <= previous.max_key {
                    return Err(ManifestLoadError::SegmentDescriptorMismatch {
                        object_key: segment.object_key.clone(),
                        message: format!(
                            "reorganize output segment starts at `{}`, at or below the preceding \
                             segment's last key `{}` in family `{family:?}`",
                            segment.min_key, previous.max_key
                        ),
                    });
                }
            }
            previous = Some(segment);
        }
    }

    Ok(())
}

/// How far into one family's keyspace a partial fold has written.
enum WrittenThrough {
    /// Every row written sorts strictly below this key.
    Below(String),
    /// Every row written sorts at or below this key, which is the last one
    /// the fold wrote.
    AtOrBelow(String),
}

impl WrittenThrough {
    fn admits(&self, max_key: &str) -> bool {
        match self {
            Self::Below(bound) => max_key < bound.as_str(),
            Self::AtOrBelow(bound) => max_key <= bound.as_str(),
        }
    }

    fn key(&self) -> &str {
        match self {
            Self::Below(bound) | Self::AtOrBelow(bound) => bound.as_str(),
        }
    }
}

pub(super) fn metadata_file_object_key(descriptor: &MetadataFileRef) -> String {
    metadata_table(
        descriptor.owner_namespace_id.as_str(),
        descriptor.table_id.as_str(),
    )
}
