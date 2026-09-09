//! Loads a consistent head and current manifest with its retention floor.

use crate::control_object::ControlObjectLoadError;
use crate::limits::CONTROL_SNAPSHOT_REREAD_LIMIT;
use crate::namespace::basis::{metadata_basis_without_root, namespace_birth_seq, MetadataBasis};
use crate::namespace::control::{
    load_current_manifest_if_present, load_head_object, LoadedHeadObject, LoadedManifest,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) struct NamespaceControlSnapshot {
    pub(crate) head: LoadedHeadObject,
    pub(crate) root: Option<LoadedManifest>,
    pub(crate) retention_floor_seq: ChangeSeq,
}

impl NamespaceControlSnapshot {
    pub(crate) fn basis(&self) -> MetadataBasis {
        self.root.as_ref().map_or_else(
            || metadata_basis_without_root(&self.head.state),
            |root| MetadataBasis::Manifest(root.state.manifest.clone()),
        )
    }
}

pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: LoadedHeadObject,
    pub(crate) basis: MetadataBasis,
    pub(crate) retention_floor_seq: Option<ChangeSeq>,
}

pub(crate) async fn load_head_and_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(LoadedHeadObject, ChangeSeq), ControlObjectLoadError> {
    let snapshot = load_control_snapshot(store, namespace_id).await?;
    Ok((snapshot.head, snapshot.retention_floor_seq))
}

pub(crate) async fn load_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let snapshot = load_control_snapshot(store, namespace_id).await?;
    Ok(LoadedNamespaceBasis {
        basis: snapshot.basis(),
        retention_floor_seq: Some(snapshot.retention_floor_seq),
        head: snapshot.head,
    })
}

pub(crate) async fn load_control_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceControlSnapshot, ControlObjectLoadError> {
    let (head, root) = futures::join!(
        load_head_object(store, namespace_id),
        load_current_manifest_if_present(store, namespace_id)
    );
    let mut head = head?;
    let root = root?;
    if let Some(root) = &root {
        let manifest_head_seq = root.state.manifest.manifest_head_seq;
        for _ in 0..CONTROL_SNAPSHOT_REREAD_LIMIT {
            if manifest_head_seq <= head.state.seq {
                break;
            }
            head = load_head_object(store, namespace_id).await?;
        }
        if manifest_head_seq > head.state.seq {
            return Err(ControlObjectLoadError::RootAheadOfHead {
                root_manifest_head_seq: manifest_head_seq,
                head_seq: head.state.seq,
            });
        }
    }
    let retention_floor_seq = root.as_ref().map_or_else(
        || namespace_birth_seq(&head.state),
        |root| root.state.retention_floor_seq,
    );
    Ok(NamespaceControlSnapshot {
        head,
        root,
        retention_floor_seq,
    })
}

pub(crate) async fn resolve_retention_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    head: &HeadState,
) -> Result<ChangeSeq, ControlObjectLoadError> {
    Ok(load_current_manifest_if_present(store, &head.namespace_id)
        .await?
        .map_or_else(
            || namespace_birth_seq(head),
            |root| root.state.retention_floor_seq,
        ))
}
