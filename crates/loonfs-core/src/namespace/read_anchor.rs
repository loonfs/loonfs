//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::{load_current_manifest, LoadedManifest};
use crate::namespace::state::NamespaceReadState;
use crate::wal::discover_tip;
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub struct NamespaceReadAnchor {
    pub read_state: NamespaceReadState,
    pub(crate) manifest: LoadedManifest,
}

impl NamespaceReadAnchor {
    pub fn retention_floor_seq(&self) -> ChangeSeq {
        self.manifest.state.retention_floor_seq
    }

    pub fn basis(&self) -> MetadataBasis {
        MetadataBasis(self.manifest.state.manifest.clone())
    }
}

pub async fn load_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceReadAnchor, ControlObjectLoadError> {
    let mut manifest = load_current_manifest(store, namespace_id).await?;
    loop {
        match discover_tip(store, namespace_id, &manifest).await {
            Ok(state) => {
                if manifest_has_successor(store, namespace_id, manifest.state.manifest.manifest_no)
                    .await?
                {
                    manifest = load_current_manifest(store, namespace_id).await?;
                    continue;
                }
                return Ok(NamespaceReadAnchor {
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
