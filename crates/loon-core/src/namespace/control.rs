use loon_api::wire::control::{
    decode_control_object, ContentStoreDescriptorEnvelope, ContentStoreDescriptorState,
    ControlCodecError, ControlObjectKind, HeadState, HeadStateEnvelope, LeaseState,
    LeaseStateEnvelope, NamespaceDescriptorEnvelope, NamespaceDescriptorState,
};
use loon_api::{ContentStoreId, NamespaceId};
use loon_objectstore::keys::{
    content_store_descriptor, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::ObjectStoreError;
use loon_objectstore::{ObjectMetadata, ObjectStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedHeadObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: HeadStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedNamespaceDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: NamespaceDescriptorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedContentStoreDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: ContentStoreDescriptorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedLeaseObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: LeaseStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectIdentity {
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedNamespaceDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: NamespaceDescriptorState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedContentStoreDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: ContentStoreDescriptorState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedHeadControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: HeadState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedLeaseControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: LeaseState,
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
    let object_key = namespace_descriptor(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: NamespaceDescriptorEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::NamespaceDescriptor)
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

pub(crate) async fn read_head_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_head(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: HeadStateEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::NamespaceHead)
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

pub(crate) async fn read_lease_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedLeaseObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_lease(expected_namespace.as_str());
    let (metadata, encoded_bytes) = read_control_object_bytes(store, &object_key).await?;
    let envelope: LeaseStateEnvelope =
        decode_control_object(&encoded_bytes, ControlObjectKind::NamespaceLease)
            .map_err(|err| map_control_codec_error(&object_key, err))?;
    validate_expected_namespace(
        &object_key,
        expected_namespace,
        &envelope.state.namespace_id,
    )?;

    Ok(LoadedLeaseObject {
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

pub async fn load_namespace_lease_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedLeaseControl, ControlObjectLoadError> {
    let loaded = read_lease_object(store, expected_namespace).await?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedLeaseControl {
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
