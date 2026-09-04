//! Manifest-envelope loading and descriptor verification.
//!
//! This module validates manifest framing and segment descriptors without
//! fetching row data. Other checkpoint modules load blocks. Full
//! materialization is available only in tests.

pub(super) use super::block_load::SessionBlockMemo;
use super::cache::{
    DecodedMetadataSegmentBlock, MetadataSegmentBlockKind, MetadataSegmentCache,
    MetadataSegmentCacheKey,
};
use super::error::ManifestLoadError;
use super::runs::runs_in_reorganization_order;
use super::scan::VerifiedMetadataSegments;
use super::validate::{validate_manifest, validate_namespace_manifest};
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::basis::{genesis_next_inode_id, MetadataBasis, MetadataBasisIdentity};
use crate::namespace::bootstrap::bootstrap_metadata_state;
use loonfs_api::wire::control::{genesis_commit_id, HeadState, ManifestRef};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, NamespaceManifestEnvelope, NamespaceManifestKind,
    NamespaceManifestPayload, NAMESPACE_MANIFEST_FORMAT_VERSION,
};
use loonfs_api::{ChangeSeq, ManifestNo, ManifestObjectId, NamespaceId, RunNo, WriterEpoch};
use loonfs_objectstore::keys::metadata_manifest_object;
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

/// Verifies every coordinate by which a durable control object binds one
/// immutable manifest.
///
/// Loading by owner and object id already proves the object's storage
/// location. The remaining fields are still authority: callers use the
/// manifest number for future allocation and the head sequence for replay
/// and retention boundaries. A checksum match alone must not let those
/// coordinates disagree with the payload it pins.
pub(crate) fn ensure_manifest_reference_matches(
    reference_name: &str,
    reference: &ManifestRef,
    manifest: &NamespaceManifestEnvelope,
) -> crate::error::Result<()> {
    let payload = &manifest.payload;
    let mismatch = if reference.owner_namespace_id != payload.namespace_id {
        Some((
            "owner_namespace_id",
            reference.owner_namespace_id.to_string(),
            payload.namespace_id.to_string(),
        ))
    } else if reference.manifest_no != payload.manifest_no {
        Some((
            "manifest_no",
            reference.manifest_no.to_string(),
            payload.manifest_no.to_string(),
        ))
    } else if reference.manifest_object_id != payload.manifest_object_id {
        Some((
            "manifest_object_id",
            reference.manifest_object_id.to_string(),
            payload.manifest_object_id.to_string(),
        ))
    } else if reference.manifest_head_seq != payload.head_seq {
        Some((
            "manifest_head_seq",
            reference.manifest_head_seq.to_string(),
            payload.head_seq.to_string(),
        ))
    } else if reference.manifest_payload_checksum != manifest.payload_checksum {
        Some((
            "manifest_payload_checksum",
            reference.manifest_payload_checksum.clone(),
            manifest.payload_checksum.clone(),
        ))
    } else {
        None
    };
    let Some((field, referenced, actual)) = mismatch else {
        return Ok(());
    };
    Err(CoreError::NamespaceCorrupt(format!(
        "{reference_name} records manifest `{}` field `{field}` as `{referenced}`, but the manifest carries `{actual}`",
        reference.manifest_object_id,
    )))
}

/// The genesis basis: one root-inode row and no metadata files.
///
/// A created namespace publishes no manifest, so reads before its first
/// flush replay the WAL over this synthesized state. The object id is a
/// sentinel that no generator produces and that nothing ever writes; it
/// exists because the manifest payload shape requires one.
const GENESIS_MANIFEST_OBJECT_ID: &str = "man_00000000000000000000-0000000000000000";

pub(super) fn genesis_basis_manifest(namespace_id: &NamespaceId) -> NamespaceManifestEnvelope {
    NamespaceManifestEnvelope {
        kind: NamespaceManifestKind::NamespaceManifest,
        format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
        payload_checksum: String::new(),
        payload: NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(0),
            manifest_object_id: ManifestObjectId::parse(GENESIS_MANIFEST_OBJECT_ID)
                .expect("genesis manifest object id is valid"),
            head_seq: ChangeSeq(0),
            head_commit_id: genesis_commit_id(),
            base_seq: ChangeSeq(0),
            writer_epoch: WriterEpoch(0),
            next_inode_id: genesis_next_inode_id(),
            next_run_no: RunNo(0),
            frozen_base_delta_merges: Default::default(),
            retention_floor_seq: ChangeSeq(0),
            runs: Vec::new(),
        },
    }
}

/// A verified basis with its identity, segments, and in-memory base rows.
pub(crate) struct LoadedMetadataBasis<'a, S: ObjectStore + ?Sized> {
    pub(crate) identity: MetadataBasisIdentity,
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
    /// Rows the basis contributes outside any segment: the genesis root inode,
    /// and nothing at all once a manifest exists.
    pub(crate) base_state: MetadataState,
}

/// Loads the segments referenced by a resolved basis.
///
/// A genesis basis loads nothing: its single row is synthesized. A manifest
/// basis is loaded under its owner's prefix — this namespace's own root, or
/// the fork source the head authorizes — and is validated against the
/// checksum the authorizing object recorded. A mismatch is corruption; there
/// is no second attempt against another basis.
pub(crate) async fn load_basis_metadata_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    basis: &MetadataBasis,
    genesis_created_at_ms: u64,
) -> crate::error::Result<LoadedMetadataBasis<'a, S>> {
    let Some(manifest) = basis.manifest() else {
        let segments =
            VerifiedMetadataSegments::synthesized(store, genesis_basis_manifest(namespace_id));
        return Ok(LoadedMetadataBasis {
            identity: MetadataBasisIdentity::from_verified_basis(
                basis.clone(),
                segments.manifest().payload.head_seq,
            ),
            segments,
            base_state: bootstrap_metadata_state(genesis_created_at_ms),
        });
    };
    let segments = load_manifest_segments(store, segment_cache, manifest).await?;
    let manifest_head_seq = segments.manifest().payload.head_seq;
    Ok(LoadedMetadataBasis {
        identity: MetadataBasisIdentity::from_verified_basis(basis.clone(), manifest_head_seq),
        segments,
        base_state: MetadataState::default(),
    })
}

/// Loads and validates only the manifest envelope, without fetching its
/// metadata segments. This is enough for callers that need manifest framing,
/// not segment descriptors or rows.
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

/// Loads a durable manifest reference and verifies every field before exposing its segments.
pub(crate) async fn load_manifest_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    reference: &ManifestRef,
) -> crate::error::Result<VerifiedMetadataSegments<'a, S>> {
    let segments = load_manifest_segments_for_inspection(
        store,
        segment_cache,
        &reference.owner_namespace_id,
        &reference.manifest_object_id,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    ensure_manifest_reference_matches(
        &format!(
            "namespace `{}` manifest reference",
            reference.owner_namespace_id
        ),
        reference,
        segments.manifest(),
    )?;
    Ok(segments)
}

/// Inspects an object by ID, validating its envelope and segment descriptors.
/// This does not verify a root or checkpoint reference. Authoritative reads
/// and publication paths must use `load_manifest_segments` instead.
pub(crate) async fn load_manifest_segments_for_inspection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> Result<VerifiedMetadataSegments<'a, S>, ManifestLoadError> {
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
            manifest.payload.manifest_no,
            manifest_object_id,
            &manifest_key,
            &manifest,
        )?;
        validate_manifest(&manifest_key, &manifest.payload)?;
        let scan_runs = Arc::new(runs_in_reorganization_order(&manifest.payload));
        Ok(DecodedMetadataSegmentBlock::Manifest {
            manifest: (Arc::new(manifest), scan_runs),
            // The entry retains the envelope plus its scan-ordered run list.
            decoded_bytes: manifest_bytes.len().saturating_mul(2),
        })
    };
    let decoded = match segment_cache {
        Some(cache) => {
            let cache_key = MetadataSegmentCacheKey {
                identity: manifest_key.clone(),
                block_kind: MetadataSegmentBlockKind::Manifest,
                block_offset: 0,
            };
            cache.get_or_load(&cache_key, fetch).await?
        }
        None => fetch().await?,
    };
    let (manifest, scan_runs) = decoded.into_manifest(&manifest_key)?;
    let segments = VerifiedMetadataSegments {
        store,
        segment_cache,
        manifest_object_key: manifest_key,
        manifest: Some(manifest),
        scan_runs,
        block_memo: SessionBlockMemo::default(),
    };
    Ok(segments)
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
        status: current_head.status,
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
        manifest.payload.manifest_no,
        manifest_object_id,
        manifest_key,
        &manifest,
    )?;
    Ok(Some(manifest))
}
