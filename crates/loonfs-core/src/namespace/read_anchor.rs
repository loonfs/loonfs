//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult};
use crate::limits::MAX_POSITION_MOVES;
use crate::manifest::{metadata_basis_from_manifest, MetadataSegmentCache, PublishedManifest};
use crate::namespace::basis::{MetadataBasis, PublicationBasis};
use crate::namespace::control::{CurrentManifest, LoadedHint, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::wal::{
    discover_tail, replay_discovered_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::{ChangeSeq, ManifestNo, WriterEpoch};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NamespaceReadAnchor {
    pub read_state: NamespaceReadState,
    pub(crate) manifest: LoadedManifest,
    pub(crate) hint: LoadedHint,
    pub(crate) tail: ValidatedWalTail,
    /// When the anchor's checks began, read before the anchor loaded
    /// `manifest` and confirmed it with a successor check.
    pub(crate) checked: crate::time::Observation,
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

    /// Returns the anchor's manifest, with the time that the anchor's checks
    /// began, as a basis that a publication can build on.
    pub fn publication_basis(&self) -> PublicationBasis {
        PublicationBasis {
            manifest: self.manifest.clone(),
            checked: self.checked.clone(),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub async fn load_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
        let (manifest, hint) = self.load_current_manifest_with_hint().await?;
        self.load_read_anchor_from_manifest(manifest, hint, observed)
            .await
    }

    /// Loads the read anchor of a namespace that is not deleted.
    pub async fn load_live_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let anchor = self.load_read_anchor(observed).await?;
        crate::namespace::control::ensure_namespace_live(&anchor.read_state)?;
        Ok(anchor)
    }

    pub(crate) async fn load_read_anchor_from_manifest(
        &self,
        mut manifest: LoadedManifest,
        mut hint: LoadedHint,
        mut observed: crate::time::Observation,
    ) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
        for _attempt in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
            match discover_tail(&self.store, &manifest).await {
                Ok(DiscoveredTail { head: state, tail }) => {
                    if !state.status.is_deleted() {
                        let successor = self
                            .manifest_has_successor(manifest.state.manifest().manifest_no)
                            .await?;
                        if successor || !observed.is_within_revalidation_bound() {
                            observed = observed.renew();
                            (manifest, hint) = self.load_current_manifest_with_hint().await?;
                            continue;
                        }
                    }
                    return Ok(NamespaceReadAnchor {
                        read_state: state,
                        manifest,
                        hint,
                        tail,
                        checked: observed,
                    });
                }
                Err(error @ ControlObjectLoadError::Codec { .. }) => {
                    observed = observed.renew();
                    let (current, current_hint) = self.load_current_manifest_with_hint().await?;
                    if current.state.manifest().manifest_no == manifest.state.manifest().manifest_no
                    {
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

    pub async fn manifest_has_successor(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<bool, ControlObjectLoadError> {
        let Ok(next) = manifest_no.successor() else {
            return Ok(false);
        };
        self.manifest_exists(next).await
    }

    /// Returns the manifests above `basis`, oldest first, for a position at
    /// `basis` to move onto, while each is an active manifest of `writer_epoch`.
    ///
    /// Manifest numbers are consecutive, so the walk loads each successor until
    /// one is missing. It returns `None` when `basis` has no successor, when it
    /// finds a manifest of another writer epoch (another writer's claim) or a
    /// tombstone, which settles that the position can't move, and when it finds
    /// more than [`MAX_POSITION_MOVES`] manifests.
    pub async fn same_epoch_manifests_above(
        &self,
        basis: &ManifestRef,
        writer_epoch: WriterEpoch,
    ) -> Result<Option<Vec<PublishedManifest>>, ControlObjectLoadError> {
        let mut moves = Vec::new();
        let mut manifest_no = basis.manifest_no;
        while moves.len() <= MAX_POSITION_MOVES {
            let Ok(next) = manifest_no.successor() else {
                break;
            };
            let Some(loaded) = self.load_manifest_by_no(next).await? else {
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
}

pub fn project_anchor_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    anchor: &NamespaceReadAnchor,
) -> CoreResult<Arc<ProjectedWalTail>> {
    let loaded_basis = metadata_basis_from_manifest(store, Some(segment_cache), &anchor.manifest);
    let replayed = replay_discovered_tail(
        &loaded_basis.replay_head(&anchor.read_state),
        &loaded_basis.base_state,
        &anchor.tail,
    )
    .map_err(CoreError::MetadataProjection)?;
    Ok(Arc::new(replayed.projected_tail))
}
