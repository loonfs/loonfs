//! Manifest-envelope loading and descriptor verification.
//!
//! This module validates manifest framing and segment descriptors without
//! fetching row data. Other manifest modules load blocks. Full
//! materialization is available only in tests.

use super::error::ManifestLoadError;
use super::validate::{validate_manifest, validate_namespace_manifest};
use crate::commit::publish_view::WriterPosition;
use crate::error::CoreError;
use crate::metadata::MetadataState;
use crate::namespace::basis::{MetadataBasis, PublicationBasis};
use crate::namespace::bootstrap::bootstrap_metadata_state;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::segment::cache::{
    DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey,
};
use crate::segment::runs::{manifest_cache_heap_bytes, runs_newest_first};
use crate::segment::scan::VerifiedMetadataSegments;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::format::manifest::{decode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_types::{ManifestNo, NamespaceId};
use std::sync::Arc;
use tracing::Instrument;

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
    let payload = &manifest.payload();
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
    } else if reference.head_seq != payload.head_seq {
        Some((
            "head_seq",
            reference.head_seq.to_string(),
            payload.head_seq.to_string(),
        ))
    } else if &reference.payload_checksum != manifest.payload_checksum() {
        Some((
            "payload_checksum",
            reference.payload_checksum.to_string(),
            manifest.payload_checksum().to_string(),
        ))
    } else {
        None
    };
    let Some((field, referenced, actual)) = mismatch else {
        return Ok(());
    };
    Err(CoreError::NamespaceCorrupt(format!(
        "{reference_name} records manifest `{}` field `{field}` as `{referenced}`, but the manifest carries `{actual}`",
        reference.manifest_no,
    )))
}

/// A verified basis with its identity, segments, and in-memory base rows.
pub(crate) struct LoadedMetadataBasis<'a, S: ObjectStore + ?Sized> {
    pub(crate) segments: VerifiedMetadataSegments<'a, S>,
    /// The root inode contributed by an empty manifest.
    pub(crate) base_state: MetadataState,
}

impl<S: ObjectStore + ?Sized> LoadedMetadataBasis<'_, S> {
    /// Reconstructs the head from which this basis replays its WAL tail.
    pub(crate) fn replay_head(&self, current_head: &NamespaceReadState) -> NamespaceReadState {
        head_from_manifest(current_head, self.segments.manifest())
    }
}

pub(crate) async fn load_basis_metadata_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    basis: &MetadataBasis,
) -> crate::error::Result<LoadedMetadataBasis<'a, S>> {
    let manifest = basis.manifest();
    let segments = load_manifest_segments(store, segment_cache, manifest).await?;
    Ok(metadata_basis_from_segments(segments))
}

/// Returns the manifest of a writer session's position as a basis that a
/// publication can build on, loaded through `segment_cache`, or `None` when
/// the position's last check is at the revalidation bound.
pub async fn load_position_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    position: &WriterPosition,
) -> crate::error::Result<Option<PublicationBasis>> {
    if !position.basis_checked.is_within_revalidation_bound() {
        return Ok(None);
    }
    let segments = load_manifest_segments(
        store,
        Some(segment_cache),
        position.read_basis.basis.manifest(),
    )
    .await?;
    Ok(Some(PublicationBasis {
        manifest: segments.loaded_manifest(),
        checked: position.basis_checked.clone(),
    }))
}

pub(crate) fn metadata_basis_from_manifest<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    manifest: &LoadedManifest,
) -> LoadedMetadataBasis<'a, S> {
    let scan_runs = Arc::new(runs_newest_first(manifest.payload()));
    if let Some(cache) = segment_cache {
        cache.insert(
            MetadataSegmentCacheKey::manifest(&manifest.object_key),
            DecodedMetadataSegmentBlock::Manifest {
                decoded_bytes: manifest_cache_heap_bytes(&manifest.envelope, &scan_runs),
                manifest: (
                    Arc::clone(&manifest.envelope),
                    Arc::clone(&scan_runs),
                    manifest.manifest_bytes,
                ),
            },
        );
    }
    metadata_basis_from_segments(VerifiedMetadataSegments::new(
        store,
        segment_cache,
        manifest.object_key.clone(),
        Arc::clone(&manifest.envelope),
        manifest.manifest_bytes,
        scan_runs,
    ))
}

fn metadata_basis_from_segments<S: ObjectStore + ?Sized>(
    segments: VerifiedMetadataSegments<'_, S>,
) -> LoadedMetadataBasis<'_, S> {
    LoadedMetadataBasis {
        base_state: if segments.manifest().payload().runs.is_empty() {
            bootstrap_metadata_state(
                segments.manifest().payload().created_at_ms,
                &segments.manifest().payload().access,
            )
        } else {
            MetadataState::default()
        },
        segments,
    }
}

/// Loads and validates only the manifest envelope, without fetching its
/// metadata segments. This is enough for callers that need manifest framing,
/// not segment descriptors or rows.
pub(crate) async fn load_namespace_manifest_envelope<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> Result<NamespaceManifestEnvelope, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id, manifest_no);
    load_namespace_manifest_envelope_if_present(store, namespace_id, manifest_no)
        .await?
        .map(|(envelope, _)| envelope)
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
        &reference.manifest_no,
    )
    .await?;
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
/// This does not verify a root or pin reference. Authoritative reads
/// and publication paths must use `load_manifest_segments` instead.
pub(crate) async fn load_manifest_segments_for_inspection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> Result<VerifiedMetadataSegments<'a, S>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id, manifest_no);
    // Manifests are immutable per object key, so the decoded and validated
    // envelope is cacheable forever under that key.
    let fetch = || async {
        let (manifest, manifest_bytes) =
            load_namespace_manifest_envelope_if_present(store, namespace_id, manifest_no)
                .await?
                .ok_or_else(|| ManifestLoadError::MissingManifest {
                    object_key: manifest_key.clone(),
                })?;
        let scan_runs = Arc::new(runs_newest_first(manifest.payload()));
        Ok(DecodedMetadataSegmentBlock::Manifest {
            decoded_bytes: manifest_cache_heap_bytes(&manifest, &scan_runs),
            manifest: (Arc::new(manifest), scan_runs, manifest_bytes),
        })
    };
    let decoded = match segment_cache {
        Some(cache) => {
            cache
                .get_or_load(&MetadataSegmentCacheKey::manifest(&manifest_key), fetch)
                .await?
        }
        None => fetch().await?,
    };
    let (manifest, scan_runs, manifest_bytes) = decoded.into_manifest(&manifest_key)?;
    Ok(VerifiedMetadataSegments::new(
        store,
        segment_cache,
        manifest_key,
        manifest,
        manifest_bytes,
        scan_runs,
    ))
}

pub(crate) fn head_from_manifest(
    current_head: &NamespaceReadState,
    manifest: &NamespaceManifestEnvelope,
) -> NamespaceReadState {
    NamespaceReadState {
        status: current_head.status,
        writer_epoch: current_head.writer_epoch,
        writer: current_head.writer.clone(),
        ..NamespaceReadState::from(manifest.payload())
    }
}

pub(crate) async fn load_namespace_manifest_envelope_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> Result<Option<(NamespaceManifestEnvelope, u64)>, ManifestLoadError> {
    let manifest_key = metadata_manifest_object(namespace_id, manifest_no);
    let Some(manifest_bytes) = store
        .get(&manifest_key, None)
        .instrument(crate::phase_span!(
            crate::Phase::LoadNamespaceManifest,
            key_class = "manifest"
        ))
        .await
        .map_err(|err| ManifestLoadError::ReadManifest {
            object_key: manifest_key.to_owned(),
            message: err.public_message().into_owned(),
            kind: err.kind(),
        })?
    else {
        return Ok(None);
    };
    let manifest = decode_manifest_at(namespace_id, *manifest_no, &manifest_key, &manifest_bytes)?;
    Ok(Some((manifest, manifest_bytes.len() as u64)))
}

pub(crate) fn decode_manifest_at(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    manifest_key: &str,
    manifest_bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, ManifestLoadError> {
    let manifest = decode_namespace_manifest_json(manifest_bytes).map_err(|error| {
        ManifestLoadError::ManifestCodec {
            object_key: manifest_key.to_owned(),
            message: error.to_string(),
        }
    })?;
    validate_namespace_manifest(namespace_id, manifest_no, manifest_key, &manifest)?;
    validate_manifest(manifest_key, manifest.payload())?;
    Ok(manifest)
}
