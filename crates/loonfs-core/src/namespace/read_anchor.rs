//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult};
use crate::limits::MAX_POSITION_MOVES;
use crate::manifest::{metadata_basis_from_manifest, MetadataSegmentCache, PublishedManifest};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::{
    load_current_manifest_with_hint, load_manifest_by_no, CurrentManifest, LoadedHint,
    LoadedManifest,
};
use crate::namespace::state::NamespaceReadState;
use crate::wal::{
    discover_tail, replay_discovered_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId, WriterEpoch};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NamespaceReadAnchor {
    pub read_state: NamespaceReadState,
    pub(crate) manifest: LoadedManifest,
    pub(crate) hint: LoadedHint,
    pub(crate) tail: ValidatedWalTail,
}

impl NamespaceReadAnchor {
    pub fn retention_floor_seq(&self) -> ChangeSeq {
        self.manifest.state.retention_floor_seq()
    }

    pub fn basis(&self) -> MetadataBasis {
        MetadataBasis(self.manifest.state.manifest().clone())
    }

    /// Returns the current manifest that the anchor loaded before it read
    /// the WAL tail above that manifest.
    pub fn current_manifest(&self) -> &CurrentManifest {
        &self.manifest.state
    }

    /// Returns the hint that the anchor's discovery started from.
    pub fn hint(&self) -> &LoadedHint {
        &self.hint
    }
}

pub async fn load_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    observed: crate::time::Observation,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    let (manifest, hint) = load_current_manifest_with_hint(store, namespace_id).await?;
    load_read_anchor_from_manifest(store, namespace_id, manifest, hint, observed).await
}

/// Loads the read anchor of a namespace that is not deleted.
pub async fn load_live_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    observed: crate::time::Observation,
) -> CoreResult<NamespaceReadAnchor> {
    let anchor = load_read_anchor(store, namespace_id, observed).await?;
    crate::namespace::control::ensure_namespace_live(&anchor.read_state)?;
    Ok(anchor)
}

pub(crate) async fn load_read_anchor_from_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut manifest: LoadedManifest,
    mut hint: LoadedHint,
    mut observed: crate::time::Observation,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    for _attempt in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
        match discover_tail(store, namespace_id, &manifest).await {
            Ok(DiscoveredTail { head: state, tail }) => {
                if !state.status.is_deleted() {
                    let successor = manifest_has_successor(
                        store,
                        namespace_id,
                        manifest.state.manifest().manifest_no,
                    )
                    .await?;
                    if successor || !observed.is_within_revalidation_bound() {
                        observed = observed.renew();
                        (manifest, hint) =
                            load_current_manifest_with_hint(store, namespace_id).await?;
                        continue;
                    }
                }
                return Ok(NamespaceReadAnchor {
                    read_state: state,
                    manifest,
                    hint,
                    tail,
                });
            }
            Err(error @ ControlObjectLoadError::Codec { .. }) => {
                observed = observed.renew();
                let (current, current_hint) =
                    load_current_manifest_with_hint(store, namespace_id).await?;
                if current.state.manifest().manifest_no == manifest.state.manifest().manifest_no {
                    return Err(error);
                }
                manifest = current;
                hint = current_hint;
            }
            Err(error) => return Err(error),
        }
    }
    Err(ControlObjectLoadError::StoreContention {
        object_key: manifest.object_key,
    })
}

pub async fn project_anchor_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    anchor: &NamespaceReadAnchor,
) -> CoreResult<Arc<ProjectedWalTail>> {
    let loaded_basis = metadata_basis_from_manifest(store, segment_cache, &anchor.manifest);
    let replayed = replay_discovered_tail(
        &loaded_basis.replay_head(&anchor.read_state),
        &loaded_basis.base_state,
        &anchor.tail,
    )
    .map_err(CoreError::MetadataProjection)?;
    Ok(Arc::new(replayed.projected_tail))
}

pub async fn manifest_has_successor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> Result<bool, ControlObjectLoadError> {
    let Ok(next) = manifest_no.successor() else {
        return Ok(false);
    };
    let object_key = loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &next);
    Ok(store
        .head(&object_key)
        .await
        .map_err(|error| ControlObjectLoadError::Store {
            object_key: object_key.clone(),
            message: error.public_message().into_owned(),
            kind: loonfs_objectstore::ObjectStoreErrorKind::of(&error),
        })?
        .is_some())
}

/// Returns the manifests above `basis`, oldest first, for a position at
/// `basis` to move onto, while each is an active manifest of `writer_epoch`.
///
/// Manifest numbers are consecutive, so the walk loads each successor until
/// one is missing. It returns `None` when `basis` has no successor, when it
/// finds a manifest of another writer epoch (another writer's claim) or a
/// tombstone, which settles that the position can't move, and when it finds
/// more than [`MAX_POSITION_MOVES`] manifests.
pub async fn same_epoch_manifests_above<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &ManifestRef,
    writer_epoch: WriterEpoch,
) -> Result<Option<Vec<PublishedManifest>>, ControlObjectLoadError> {
    let mut moves = Vec::new();
    let mut manifest_no = basis.manifest_no;
    while moves.len() <= MAX_POSITION_MOVES {
        let Ok(next) = manifest_no.successor() else {
            break;
        };
        let Some(loaded) = load_manifest_by_no(store, namespace_id, next).await? else {
            return Ok((!moves.is_empty()).then_some(moves));
        };
        let payload = loaded.state.envelope.payload();
        if payload.writer_epoch != writer_epoch || payload.status.is_deleted() {
            break;
        }
        moves.push(PublishedManifest::new(&loaded.state.envelope, None));
        manifest_no = next;
    }
    Ok(None)
}
