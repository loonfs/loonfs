use loonfs_api::wire::control::{
    decode_control_object, ContentStoreDescriptorEnvelope, ContentStoreDescriptorState,
    ControlCodecError, ControlObjectKind, HeadState, HeadStateEnvelope, MetadataRootEnvelope,
    MetadataRootState, NamespaceConfigEnvelope, NamespaceConfigState, WalFloorEnvelope,
};
use loonfs_api::{ContentStoreId, NamespaceId};
use loonfs_objectstore::keys::{
    content_store_descriptor, metadata_root, namespace_config, wal_floor, wal_head,
};
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::{ObjectMetadata, ObjectStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedHeadObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: HeadStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedMetadataRootObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: MetadataRootEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedWalFloorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: WalFloorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedNamespaceDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: NamespaceConfigEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedContentStoreDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: ContentStoreDescriptorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectIdentity {
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedNamespaceDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: NamespaceConfigState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedContentStoreDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: ContentStoreDescriptorState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedMetadataRootControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: MetadataRootState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedWalFloorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: loonfs_api::wire::control::WalFloorState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedHeadControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: HeadState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ControlObjectLoadError {
    #[error("invalid namespace_id {namespace_id:?}: {message}")]
    InvalidNamespaceId {
        namespace_id: String,
        message: String,
    },
    #[error("missing control object `{object_key}`")]
    MissingObject { object_key: String },
    #[error("missing control object after head `{object_key}`")]
    MissingObjectAfterHead { object_key: String },
    #[error(
        "metadata root references seq {root_manifest_head_seq:?} beyond the reloaded head seq {head_seq:?}"
    )]
    RootAheadOfHead {
        root_manifest_head_seq: loonfs_api::ChangeSeq,
        head_seq: loonfs_api::ChangeSeq,
    },
    #[error(
        "control object namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    NamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "control object content store mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ContentStoreMismatch {
        object_key: String,
        expected: ContentStoreId,
        actual: ContentStoreId,
    },
    #[error(
        "control object checksum mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error: {0}")]
    Store(String),
}

pub(crate) async fn read_namespace_descriptor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedNamespaceDescriptorObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_config(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: NamespaceConfigEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::NamespaceConfig)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    validate_expected_namespace(
        &object_key,
        expected_namespace,
        &envelope.state.namespace_id,
    )?;

    Ok(LoadedNamespaceDescriptorObject {
        object_key,
        metadata,
        envelope,
    })
}

pub(crate) async fn read_content_store_descriptor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_content_store: &ContentStoreId,
) -> Result<LoadedContentStoreDescriptorObject, ControlObjectLoadError> {
    let object_key = content_store_descriptor(expected_content_store.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: ContentStoreDescriptorEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::ContentStoreDescriptor)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    if envelope.state.content_store_id != *expected_content_store {
        return Err(ControlObjectLoadError::ContentStoreMismatch {
            object_key,
            expected: expected_content_store.clone(),
            actual: envelope.state.content_store_id.clone(),
        });
    }

    Ok(LoadedContentStoreDescriptorObject {
        object_key,
        metadata,
        envelope,
    })
}

pub(crate) async fn read_wal_floor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedWalFloorObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = wal_floor(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: WalFloorEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::WalFloor)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    validate_expected_namespace(
        &object_key,
        expected_namespace,
        &envelope.state.namespace_id,
    )?;

    Ok(LoadedWalFloorObject {
        object_key,
        metadata,
        envelope,
    })
}

/// Reads the floor, treating a missing object as seq 0: a lost floor means
/// "retain more history", never less.
pub(crate) async fn read_wal_floor_seq_or_zero<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<loonfs_api::ChangeSeq, ControlObjectLoadError> {
    match read_wal_floor_object(store, expected_namespace).await {
        Ok(loaded) => Ok(loaded.envelope.state.floor_seq),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(loonfs_api::ChangeSeq(0)),
        Err(error) => Err(error),
    }
}

pub(crate) async fn read_metadata_root_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedMetadataRootObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = metadata_root(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: MetadataRootEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::MetadataRoot)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    validate_expected_namespace(
        &object_key,
        expected_namespace,
        &envelope.state.namespace_id,
    )?;

    Ok(LoadedMetadataRootObject {
        object_key,
        metadata,
        envelope,
    })
}

/// Reads the WAL head and metadata root together (concurrently).
///
/// The root can only ever reference published state, so observing
/// `root.manifest_head_seq > head.seq` means the head read raced an
/// in-flight commit CAS; a fresh head read observes at least the root's
/// seq. Bounded reloads resolve the race without treating it as corruption
/// (format spec, "metadata/root.json").
pub(crate) async fn read_head_and_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<(LoadedHeadObject, LoadedMetadataRootObject), ControlObjectLoadError> {
    const ROOT_AHEAD_HEAD_RELOADS: usize = 3;
    let (head, root) = futures::join!(
        read_head_object(store, expected_namespace),
        read_metadata_root_object(store, expected_namespace)
    );
    let mut head = head?;
    let root = root?;
    for _reload in 0..=ROOT_AHEAD_HEAD_RELOADS {
        if root.envelope.state.manifest_head_seq <= head.envelope.state.seq {
            return Ok((head, root));
        }
        head = read_head_object(store, expected_namespace).await?;
    }
    Err(ControlObjectLoadError::RootAheadOfHead {
        root_manifest_head_seq: root.envelope.state.manifest_head_seq,
        head_seq: head.envelope.state.seq,
    })
}

pub(crate) async fn read_head_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = wal_head(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: HeadStateEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::WalHead)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    validate_expected_namespace(
        &object_key,
        expected_namespace,
        &envelope.state.namespace_id,
    )?;

    Ok(LoadedHeadObject {
        object_key,
        metadata,
        envelope,
    })
}

pub async fn load_namespace_descriptor_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedNamespaceDescriptorControl, ControlObjectLoadError> {
    let loaded = read_namespace_descriptor_object(store, expected_namespace).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedNamespaceDescriptorControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub async fn load_content_store_descriptor_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_content_store: &ContentStoreId,
) -> Result<LoadedContentStoreDescriptorControl, ControlObjectLoadError> {
    let loaded = read_content_store_descriptor_object(store, expected_content_store).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedContentStoreDescriptorControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub async fn load_namespace_wal_floor_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedWalFloorControl, ControlObjectLoadError> {
    let loaded = read_wal_floor_object(store, expected_namespace).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedWalFloorControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub async fn load_namespace_checkpoint_record_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
) -> Result<Option<loonfs_api::wire::control::CheckpointRecordState>, crate::error::CoreError> {
    Ok(
        crate::checkpoint::read_checkpoint_record(store, expected_namespace, checkpoint_id)
            .await?
            .map(|loaded| loaded.state),
    )
}

pub async fn load_namespace_metadata_root_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedMetadataRootControl, ControlObjectLoadError> {
    let loaded = read_metadata_root_object(store, expected_namespace).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedMetadataRootControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

/// Loads the head and metadata root together as a consistent read anchor
/// (see [`read_head_and_metadata_root`] for the race rule).
pub async fn load_namespace_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<(LoadedHeadControl, LoadedMetadataRootControl), ControlObjectLoadError> {
    let (head, root) = read_head_and_metadata_root(store, expected_namespace).await?;
    let head_identity = control_identity(&head.object_key, &head.metadata)?;
    let root_identity = control_identity(&root.object_key, &root.metadata)?;
    Ok((
        LoadedHeadControl {
            object_key: head.object_key,
            identity: head_identity,
            state: head.envelope.state,
        },
        LoadedMetadataRootControl {
            object_key: root.object_key,
            identity: root_identity,
            state: root.envelope.state,
        },
    ))
}

pub async fn load_namespace_head_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadControl, ControlObjectLoadError> {
    let loaded = read_head_object(store, expected_namespace).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedHeadControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

fn control_identity(
    object_key: &str,
    metadata: &ObjectMetadata,
) -> Result<ControlObjectIdentity, ControlObjectLoadError> {
    let etag = metadata.etag.clone().ok_or_else(|| {
        ControlObjectLoadError::Store(format!("missing control object etag for `{object_key}`"))
    })?;
    Ok(ControlObjectIdentity { etag })
}

async fn read_control_object_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> Result<(ObjectMetadata, Vec<u8>), ControlObjectLoadError> {
    let body = store
        .get_with_metadata(object_key)
        .await
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.to_owned(),
        })?;
    Ok((body.metadata, body.bytes))
}

fn validate_namespace_id_for_control_key(
    namespace_id: &NamespaceId,
) -> Result<(), ControlObjectLoadError> {
    NamespaceId::parse(namespace_id.as_str())
        .map(|_| ())
        .map_err(|err| ControlObjectLoadError::InvalidNamespaceId {
            namespace_id: namespace_id.as_str().to_owned(),
            message: err.reason().to_owned(),
        })
}

fn validate_expected_namespace(
    object_key: &str,
    expected: &NamespaceId,
    actual: &NamespaceId,
) -> Result<(), ControlObjectLoadError> {
    if actual != expected {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected.clone(),
            actual: actual.clone(),
        });
    }
    Ok(())
}

pub(crate) fn map_control_codec_error(
    object_key: &str,
    err: ControlCodecError,
) -> ControlObjectLoadError {
    match err {
        ControlCodecError::ChecksumMismatch { expected, actual } => {
            ControlObjectLoadError::ChecksumMismatch {
                object_key: object_key.to_owned(),
                expected,
                actual,
            }
        }
        other => ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: other.to_string(),
        },
    }
}

fn map_store_load_error(err: ObjectStoreError) -> ControlObjectLoadError {
    ControlObjectLoadError::Store(err.to_string())
}
