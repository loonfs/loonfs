//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::{load_current_manifest, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::wal::discover_tip;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) struct NamespaceReadAnchor {
    pub(crate) read_state: NamespaceReadState,
    pub(crate) manifest: LoadedManifest,
    pub(crate) retention_floor_seq: ChangeSeq,
}

impl NamespaceReadAnchor {
    pub(crate) fn basis(&self) -> MetadataBasis {
        MetadataBasis(self.manifest.state.manifest.clone())
    }
}

pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: NamespaceReadState,
    pub(crate) basis: MetadataBasis,
    pub(crate) retention_floor_seq: ChangeSeq,
}

pub(crate) async fn load_head_and_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(NamespaceReadState, ChangeSeq), ControlObjectLoadError> {
    let anchor = load_read_anchor(store, namespace_id).await?;
    Ok((anchor.read_state, anchor.retention_floor_seq))
}

pub(crate) async fn load_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let anchor = load_read_anchor(store, namespace_id).await?;
    Ok(LoadedNamespaceBasis {
        basis: anchor.basis(),
        retention_floor_seq: anchor.retention_floor_seq,
        head: anchor.read_state,
    })
}

pub(crate) async fn load_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    let mut manifest = load_current_manifest(store, namespace_id).await?;
    loop {
        match discover_tip(store, namespace_id, &manifest).await {
            Ok(state) => {
                if let Ok(next) = manifest.state.manifest.manifest_no.successor() {
                    let key =
                        loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &next);
                    if store
                        .head(&key)
                        .await
                        .map_err(|error| ControlObjectLoadError::Store {
                            object_key: key.clone(),
                            message: error.public_message().into_owned(),
                            class: crate::error::StoreFailureClass::of(&error),
                        })?
                        .is_some()
                    {
                        manifest = load_current_manifest(store, namespace_id).await?;
                        continue;
                    }
                }
                return Ok(NamespaceReadAnchor {
                    retention_floor_seq: manifest.state.retention_floor_seq,
                    read_state: state,
                    manifest,
                });
            }
            Err(error @ ControlObjectLoadError::Codec { .. }) => {
                let current = load_current_manifest(store, namespace_id).await?;
                if current.state.manifest.manifest_no == manifest.state.manifest.manifest_no {
                    return Err(error);
                }
                manifest = current;
            }
            Err(error) => return Err(error),
        }
    }
}
