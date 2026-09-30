//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::checkpoint::{metadata_basis_from_manifest, MetadataSegmentCache};
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::{load_current_manifest_with_hint, LoadedHint, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::wal::{
    discover_tail, replay_discovered_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail,
};
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId};
use loonfs_objectstore::ObjectStore;
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
}

pub async fn load_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    let observed =
        crate::time::Observation::now(Arc::new(crate::time::StdMonotonicTimer::default()));
    let (manifest, hint) = load_current_manifest_with_hint(store, namespace_id).await?;
    load_read_anchor_from_manifest(store, namespace_id, manifest, hint, observed).await
}

pub(crate) async fn load_read_anchor_from_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut manifest: LoadedManifest,
    mut hint: LoadedHint,
    mut observed: crate::time::Observation,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    loop {
        match discover_tail(store, namespace_id, &manifest).await {
            Ok(DiscoveredTail {
                head: state,
                segments,
            }) => {
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
                    tail: segments,
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
}

pub async fn project_anchor_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    anchor: &NamespaceReadAnchor,
) -> CoreResult<Arc<ProjectedWalTail>> {
    let loaded_basis = metadata_basis_from_manifest(store, segment_cache, &anchor.manifest);
    let replayed = replay_discovered_tail(
        &loaded_basis.replay_head(&anchor.read_state),
        &anchor.read_state,
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
            class: crate::error::StoreFailureClass::of(&error),
        })?
        .is_some())
}
