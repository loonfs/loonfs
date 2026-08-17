//! Typed loaders for the namespace's control objects: head, metadata root,
//! and WAL floor.

use crate::control_object::{
    expect_namespace, load_control_object, ControlObjectLoadError, LoadedControl,
};
use crate::namespace::basis::{load_head_and_metadata_basis, MetadataBasis};
use loonfs_api::wire::control::{ControlObjectKind, HeadState, MetadataRootState, WalFloorState};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{metadata_root, wal_floor, wal_head};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};

pub(crate) type LoadedHeadObject = LoadedControl<HeadState>;
pub(crate) type LoadedMetadataRootObject = LoadedControl<MetadataRootState>;
pub(crate) type LoadedWalFloorObject = LoadedControl<WalFloorState>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectIdentity {
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedMetadataRootControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: MetadataRootState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedHeadControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: HeadState,
}

pub(crate) async fn load_wal_floor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedWalFloorObject, ControlObjectLoadError> {
    let object_key = wal_floor(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::WalFloor,
        |state: &WalFloorState| expect_namespace(expected_namespace_id, &state.namespace_id),
    )
    .await
}

pub(crate) async fn load_metadata_root_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedMetadataRootObject, ControlObjectLoadError> {
    let object_key = metadata_root(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::MetadataRoot,
        |state: &MetadataRootState| expect_namespace(expected_namespace_id, &state.namespace_id),
    )
    .await
}

pub(crate) async fn load_metadata_root_object_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<Option<LoadedMetadataRootObject>, ControlObjectLoadError> {
    match load_metadata_root_object(store, expected_namespace_id).await {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Reads the WAL head and metadata root together (concurrently), tolerating
/// a namespace that has not materialized a root yet.
///
/// The root is published by the first flush, so its absence is ordinary for
/// a young namespace and the caller resolves the basis from the head
/// instead. When the root is there it can only ever reference published
/// state, so observing `root.manifest_head_seq > head.seq` means the head
/// read raced an in-flight commit CAS; a fresh head read observes at least
/// the root's seq. Bounded reloads resolve the race without treating it as
/// corruption (format spec, "metadata/root.json").
pub(crate) async fn load_head_and_metadata_root_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<(LoadedHeadObject, Option<LoadedMetadataRootObject>), ControlObjectLoadError> {
    const ROOT_AHEAD_HEAD_RELOADS: usize = 3;
    let (head, root) = futures::join!(
        load_head_object(store, expected_namespace_id),
        load_metadata_root_object_if_present(store, expected_namespace_id)
    );
    let mut head = head?;
    let Some(root) = root? else {
        return Ok((head, None));
    };
    for _reload in 0..=ROOT_AHEAD_HEAD_RELOADS {
        if root.state.manifest_head_seq <= head.state.seq {
            return Ok((head, Some(root)));
        }
        head = load_head_object(store, expected_namespace_id).await?;
    }
    Err(ControlObjectLoadError::RootAheadOfHead {
        root_manifest_head_seq: root.state.manifest_head_seq,
        head_seq: head.state.seq,
    })
}

pub(crate) async fn load_head_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    let object_key = wal_head(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::WalHead,
        |state: &HeadState| expect_namespace(expected_namespace_id, &state.namespace_id),
    )
    .await
}

pub async fn load_namespace_checkpoint_record_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
) -> Result<Option<loonfs_api::wire::control::CheckpointRecordState>, crate::error::CoreError> {
    Ok(
        crate::checkpoint::load_checkpoint_record(store, expected_namespace_id, checkpoint_id)
            .await?
            .map(|loaded| loaded.state),
    )
}

pub async fn load_namespace_metadata_root_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedMetadataRootControl, ControlObjectLoadError> {
    let loaded = load_metadata_root_object(store, expected_namespace_id).await?;
    Ok(LoadedMetadataRootControl {
        object_key: loaded.object_key,
        identity: ControlObjectIdentity { etag: loaded.etag },
        state: loaded.state,
    })
}

/// Loads the head together with the metadata basis it authorizes, as one
/// consistent read anchor (see [`load_head_and_metadata_root_if_present`]
/// for the race rule).
pub async fn load_namespace_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<(LoadedHeadControl, MetadataBasis), ControlObjectLoadError> {
    let loaded = load_head_and_metadata_basis(store, expected_namespace_id).await?;
    Ok((
        LoadedHeadControl {
            object_key: loaded.head.object_key,
            identity: ControlObjectIdentity {
                etag: loaded.head.etag,
            },
            state: loaded.head.state,
        },
        loaded.basis,
    ))
}

pub async fn load_namespace_head_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedHeadControl, ControlObjectLoadError> {
    let loaded = load_head_object(store, expected_namespace_id).await?;
    Ok(LoadedHeadControl {
        object_key: loaded.object_key,
        identity: ControlObjectIdentity { etag: loaded.etag },
        state: loaded.state,
    })
}
