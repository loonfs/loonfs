//! Manifest-envelope loading and descriptor verification.
//!
//! This module validates manifest framing and segment descriptors without
//! fetching row data. Other manifest modules load blocks. Loading every row
//! of a manifest into memory is available only in tests.

use super::error::ManifestLoadError;
use super::validate::{validate_manifest, validate_namespace_manifest};
use crate::commit::publish_view::WriterPosition;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::bootstrap::bootstrap_metadata_state;
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::segment::cache::{
    DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey,
};
use crate::segment::runs::{manifest_cache_heap_bytes, runs_newest_first, MetadataRunManifest};
use crate::segment::scan::VerifiedMetadataSegments;
use crate::segment::{KeptBy, SegmentKeeper};
use crate::wal::{replay_discovered_tail, ProjectedWalTail};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::format::manifest::{decode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_types::{ManifestNo, NamespaceId};
use std::sync::Arc;
use tracing::Instrument;

#[cfg(test)]
pub(super) use super::tests::inspection_state::append_rows_to_metadata;
#[cfg(test)]
pub(crate) use super::tests::inspection_state::{
    load_manifest_metadata_state_for_inspection_from_manifest, load_manifest_state_for_inspection,
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

    /// Replays `anchor`'s WAL tail over this basis, which is loaded from the
    /// anchor's manifest.
    pub(crate) fn project_tail(
        &self,
        anchor: &NamespaceReadAnchor,
    ) -> crate::error::Result<Arc<ProjectedWalTail>> {
        project_manifest_tail(self.segments.manifest(), &self.base_state, anchor)
    }
}

/// Returns the rows that `manifest` contributes outside its segments: the
/// root inode of a manifest that lists no runs, and no row otherwise.
pub(crate) fn manifest_base_state(manifest: &NamespaceManifestEnvelope) -> MetadataState {
    if manifest.payload().runs.is_empty() {
        bootstrap_metadata_state(manifest.payload().created_at_ms)
    } else {
        MetadataState::default()
    }
}

/// Replays `anchor`'s WAL tail above `manifest`, the anchor's manifest, over
/// `base_state`, the rows that the manifest contributes outside its
/// segments.
pub(crate) fn project_manifest_tail(
    manifest: &NamespaceManifestEnvelope,
    base_state: &MetadataState,
    anchor: &NamespaceReadAnchor,
) -> crate::error::Result<Arc<ProjectedWalTail>> {
    let replayed = replay_discovered_tail(
        &head_from_manifest(&anchor.read_state, manifest),
        base_state,
        &anchor.tail,
    )
    .map_err(CoreError::MetadataProjection)?;
    Ok(Arc::new(replayed.projected_tail))
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns the keeper `kept_by`, which this namespace's store checks.
    pub(crate) fn keeper(&self, kept_by: KeptBy) -> SegmentKeeper<'_> {
        SegmentKeeper::new(self, kept_by)
    }

    /// Returns the segments of `decoded`, which `keeper` keeps in the store,
    /// read through this namespace's segment cache.
    pub(crate) fn segments<'a>(
        &'a self,
        decoded: DecodedManifest,
        keeper: SegmentKeeper<'a>,
    ) -> LoadedMetadataBasis<'a, S> {
        metadata_basis_from_segments(VerifiedMetadataSegments::from_decoded(
            &self.store,
            Some(&self.segment_cache),
            decoded,
            keeper,
        ))
    }

    /// Returns the segments of `manifest`, a manifest of this namespace that
    /// was current when this process loaded it.
    pub(crate) fn current_segments(&self, manifest: &LoadedManifest) -> LoadedMetadataBasis<'_, S> {
        let decoded = DecodedManifest {
            manifest: manifest.clone(),
            scan_runs: Arc::new(runs_newest_first(manifest.payload())),
        };
        self.segments(decoded, self.keeper(KeptBy::Current))
    }

    /// Loads the segments of `manifest`, which a read context reads above
    /// with the manifest boundary `kept`, kept in the store by `kept_by`.
    ///
    /// A current manifest that is missing from the store and at or below the
    /// boundary returns `manifest_replaced` (see [`Self::load_held_manifest`]).
    /// Any other missing manifest returns the load's error.
    pub(crate) async fn load_context_segments(
        &self,
        manifest: &ManifestRef,
        kept: KeptBoundary,
        kept_by: &KeptBy,
    ) -> crate::error::Result<LoadedMetadataBasis<'_, S>> {
        let decoded = match kept_by {
            KeptBy::Current => match self.load_held_manifest(manifest, kept).await? {
                Some(decoded) => decoded,
                None => {
                    self.ensure_manifest_current(manifest.manifest_no).await?;
                    return Err(ManifestLoadError::MissingManifest {
                        object_key: metadata_manifest_object(
                            &manifest.owner_namespace_id,
                            &manifest.manifest_no,
                        ),
                    }
                    .into());
                }
            },
            KeptBy::Snapshot(_) => {
                load_referenced_manifest(&self.store, Some(&self.segment_cache), manifest).await?
            }
        };
        Ok(self.segments(decoded, self.keeper(kept_by.clone())))
    }

    /// Loads `manifest`, which a holder keeps by reference with the manifest
    /// boundary `kept`, through the segment cache, or returns `None` when the
    /// store doesn't have the manifest and the manifest is at or below the
    /// boundary, so that the holder finds the newest manifest from the start.
    ///
    /// A manifest in the segment cache is loaded without a request, and any
    /// other manifest is read from the store. Neither load checks whether a
    /// newer manifest replaced the manifest, so each caller finds that out
    /// later, in the same way after either load. A manifest publication
    /// reads the boundary after its put. A commit's WAL put is valid however
    /// old its manifest is, and collides with the fence of any writer that
    /// claimed the namespace since, because garbage collection never deletes
    /// a fence. A snapshot is verified after its record is written, and an
    /// operation that finds a segment missing checks whether the manifest is
    /// still the current one. A read that finds the manifest missing reads
    /// the boundary, and a missing manifest above the boundary is a corrupt
    /// store.
    pub(crate) async fn load_held_manifest(
        &self,
        manifest: &ManifestRef,
        kept: KeptBoundary,
    ) -> crate::error::Result<Option<DecodedManifest>> {
        let loaded =
            load_referenced_manifest(&self.store, Some(&self.segment_cache), manifest).await;
        if matches!(
            loaded,
            Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::ManifestLoad(
                    ManifestLoadError::MissingManifest { .. }
                )
            ))
        ) && !self.above_boundary(manifest.manifest_no, kept).await?.0
        {
            return Ok(None);
        }
        loaded.map(Some)
    }

    /// Loads the manifest numbered `manifest_no` through the segment cache, or
    /// returns `None` when the store doesn't have the manifest.
    ///
    /// A manifest in the segment cache is loaded without a request. A manifest
    /// read from the store is inserted in the segment cache. The caller decides
    /// whether the manifest is live, such as by reading the manifest boundary
    /// after the load.
    pub(crate) async fn load_manifest_if_present(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<Option<LoadedManifest>, ManifestLoadError> {
        match load_decoded_manifest(
            &self.store,
            Some(&self.segment_cache),
            &self.namespace_id,
            &manifest_no,
        )
        .await
        {
            Ok(decoded) => Ok(Some(decoded.manifest)),
            Err(ManifestLoadError::MissingManifest { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Returns the manifest of a writer session's position as a basis that a
    /// publication can build on, with the position's check time and the
    /// manifest boundary that the position keeps, or `None` when the manifest
    /// is missing from the store and at or below the boundary (see
    /// [`Self::load_held_manifest`]).
    pub async fn load_position_basis(
        &self,
        position: &WriterPosition,
    ) -> crate::error::Result<Option<Held<LoadedManifest>>> {
        let decoded = self
            .load_held_manifest(
                position.read_basis().basis.manifest(),
                position.basis().boundary().clone(),
            )
            .await?;
        Ok(decoded.map(|decoded| position.basis().decoded(decoded.manifest)))
    }
}

/// Returns the segments of `manifest`, a current manifest in `store`, for a
/// test that reads its rows through `segment_cache`.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn metadata_basis_from_manifest<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    manifest: &LoadedManifest,
) -> LoadedMetadataBasis<'a, S> {
    let scan_runs = Arc::new(runs_newest_first(manifest.payload()));
    let keeper = SegmentKeeper::new(
        crate::test_support::namespace_store(store, &manifest.payload().namespace_id),
        KeptBy::Current,
    );
    metadata_basis_from_segments(VerifiedMetadataSegments::new(
        store,
        segment_cache,
        manifest.object_key.clone(),
        Arc::clone(&manifest.envelope),
        manifest.manifest_bytes,
        scan_runs,
        keeper,
    ))
}

/// Caches `manifest` in `segment_cache` under its object key, with
/// `scan_runs`, its runs newest first, so that a read of the manifest sends
/// no request.
pub(crate) fn cache_manifest(
    segment_cache: &MetadataSegmentCache,
    manifest: &LoadedManifest,
    scan_runs: &Arc<Vec<MetadataRunManifest>>,
) {
    segment_cache.insert(
        MetadataSegmentCacheKey::manifest(&manifest.object_key),
        DecodedMetadataSegmentBlock::Manifest {
            decoded_bytes: manifest_cache_heap_bytes(&manifest.envelope, scan_runs),
            manifest: (
                Arc::clone(&manifest.envelope),
                Arc::clone(scan_runs),
                manifest.manifest_bytes,
            ),
        },
    );
}

fn metadata_basis_from_segments<S: ObjectStore + ?Sized>(
    segments: VerifiedMetadataSegments<'_, S>,
) -> LoadedMetadataBasis<'_, S> {
    LoadedMetadataBasis {
        base_state: manifest_base_state(segments.manifest()),
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

/// A manifest loaded and validated through the segment cache, with its runs
/// newest first. The cache entry keeps the runs, so a load of a cached
/// manifest doesn't group them again.
pub(crate) struct DecodedManifest {
    pub(crate) manifest: LoadedManifest,
    pub(crate) scan_runs: Arc<Vec<MetadataRunManifest>>,
}

/// Loads the manifest that a durable reference identifies, and checks every
/// field of the reference against the manifest.
pub(crate) async fn load_referenced_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    reference: &ManifestRef,
) -> crate::error::Result<DecodedManifest> {
    let decoded = load_decoded_manifest(
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
        &decoded.manifest.envelope,
    )?;
    Ok(decoded)
}

/// Loads manifest `manifest_no` of `namespace_id` by its number, and
/// validates its envelope and segment descriptors. This doesn't check a
/// root or snapshot reference, so reads and publications load a manifest
/// with [`load_referenced_manifest`] instead.
pub(crate) async fn load_decoded_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> Result<DecodedManifest, ManifestLoadError> {
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
    let (envelope, scan_runs, manifest_bytes) = decoded.into_manifest(&manifest_key)?;
    Ok(DecodedManifest {
        manifest: LoadedManifest {
            object_key: manifest_key,
            manifest_bytes,
            envelope,
        },
        scan_runs,
    })
}

/// Loads manifest `manifest_no` of `namespace_id` as
/// [`load_decoded_manifest`] does, for a test that reads its rows.
#[cfg(test)]
pub(crate) async fn load_manifest_segments_for_inspection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> Result<VerifiedMetadataSegments<'a, S>, ManifestLoadError> {
    let decoded = load_decoded_manifest(store, segment_cache, namespace_id, manifest_no).await?;
    let keeper = SegmentKeeper::new(
        crate::test_support::namespace_store(store, namespace_id),
        KeptBy::Current,
    );
    Ok(VerifiedMetadataSegments::from_decoded(
        store,
        segment_cache,
        decoded,
        keeper,
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
