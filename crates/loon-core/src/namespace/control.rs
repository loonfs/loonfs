use loon_api::wire::control::{
    payload_checksum_sha256, ContentStoreDescriptorEnvelope, ContentStoreDescriptorState,
    ControlObjectKind, HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope,
    NamespaceDescriptorEnvelope, NamespaceDescriptorState, CONTROL_OBJECT_FORMAT_VERSION,
};
use loon_api::{ContentStoreId, NamespaceId};
use loon_objectstore::keys::{
    content_store_descriptor, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::ObjectStoreError;
use loon_objectstore::{ObjectMetadata, ObjectStore};
use serde::Serialize;
use serde::{Deserialize, Serialize as DeriveSerialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub(crate) struct LoadedHeadObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: HeadStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub(crate) struct LoadedNamespaceDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: NamespaceDescriptorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub(crate) struct LoadedContentStoreDescriptorObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: ContentStoreDescriptorEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub(crate) struct LoadedLeaseObject {
    pub(crate) object_key: String,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) envelope: LeaseStateEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub struct ControlObjectIdentity {
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub struct LoadedNamespaceDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: NamespaceDescriptorState,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub struct LoadedContentStoreDescriptorControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: ContentStoreDescriptorState,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub struct LoadedHeadControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: HeadState,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize)]
pub struct LoadedLeaseControl {
    pub object_key: String,
    pub identity: ControlObjectIdentity,
    pub state: LeaseState,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize, Error)]
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
        "control object kind mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    KindMismatch {
        object_key: String,
        expected: ControlObjectKind,
        actual: ControlObjectKind,
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

pub(crate) fn read_namespace_descriptor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedNamespaceDescriptorObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_descriptor(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let envelope: NamespaceDescriptorEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_namespace_descriptor_envelope(expected_namespace, &object_key, &envelope)?;

    Ok(LoadedNamespaceDescriptorObject {
        object_key,
        metadata,
        envelope,
    })
}

pub(crate) fn read_content_store_descriptor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_content_store: &ContentStoreId,
) -> Result<LoadedContentStoreDescriptorObject, ControlObjectLoadError> {
    let object_key = content_store_descriptor(expected_content_store.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let envelope: ContentStoreDescriptorEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_content_store_descriptor_envelope(expected_content_store, &object_key, &envelope)?;

    Ok(LoadedContentStoreDescriptorObject {
        object_key,
        metadata,
        envelope,
    })
}

pub(crate) fn read_head_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let envelope: HeadStateEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_head_envelope(expected_namespace, &object_key, &envelope)?;

    Ok(LoadedHeadObject {
        object_key,
        metadata,
        envelope,
    })
}

pub(crate) fn read_lease_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedLeaseObject, ControlObjectLoadError> {
    validate_namespace_id_for_control_key(expected_namespace)?;
    let object_key = namespace_lease(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_store_load_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let envelope: LeaseStateEnvelope =
        serde_json::from_slice(&encoded_bytes).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?;
    validate_lease_envelope(expected_namespace, &object_key, &envelope)?;

    Ok(LoadedLeaseObject {
        object_key,
        metadata,
        envelope,
    })
}

pub fn load_namespace_descriptor_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedNamespaceDescriptorControl, ControlObjectLoadError> {
    let loaded = read_namespace_descriptor_object(store, expected_namespace)?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedNamespaceDescriptorControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub fn load_content_store_descriptor_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_content_store: &ContentStoreId,
) -> Result<LoadedContentStoreDescriptorControl, ControlObjectLoadError> {
    let loaded = read_content_store_descriptor_object(store, expected_content_store)?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedContentStoreDescriptorControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub fn load_namespace_head_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedHeadControl, ControlObjectLoadError> {
    let loaded = read_head_object(store, expected_namespace)?;
    let identity = control_identity(&loaded.object_key, &loaded.metadata)?;
    Ok(LoadedHeadControl {
        object_key: loaded.object_key,
        identity,
        state: loaded.envelope.state,
    })
}

pub fn load_namespace_lease_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<LoadedLeaseControl, ControlObjectLoadError> {
    let loaded = read_lease_object(store, expected_namespace)?;
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

fn validate_namespace_descriptor_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &NamespaceDescriptorEnvelope,
) -> Result<(), ControlObjectLoadError> {
    validate_control_format_version(object_key, envelope.format_version)?;
    if envelope.kind != ControlObjectKind::NamespaceDescriptor {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::NamespaceDescriptor,
            actual: envelope.kind,
        });
    }
    if envelope.state.namespace_id != *expected_namespace {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }
    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )
}

fn validate_content_store_descriptor_envelope(
    expected_content_store: &ContentStoreId,
    object_key: &str,
    envelope: &ContentStoreDescriptorEnvelope,
) -> Result<(), ControlObjectLoadError> {
    validate_control_format_version(object_key, envelope.format_version)?;
    if envelope.kind != ControlObjectKind::ContentStoreDescriptor {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::ContentStoreDescriptor,
            actual: envelope.kind,
        });
    }
    if envelope.state.content_store_id != *expected_content_store {
        return Err(ControlObjectLoadError::ContentStoreMismatch {
            object_key: object_key.to_owned(),
            expected: expected_content_store.clone(),
            actual: envelope.state.content_store_id.clone(),
        });
    }
    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )
}

fn validate_head_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &HeadStateEnvelope,
) -> Result<(), ControlObjectLoadError> {
    validate_control_format_version(object_key, envelope.format_version)?;
    if envelope.kind != ControlObjectKind::NamespaceHead {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::NamespaceHead,
            actual: envelope.kind,
        });
    }

    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )?;

    if &envelope.state.namespace_id != expected_namespace {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }

    Ok(())
}

fn validate_lease_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &LeaseStateEnvelope,
) -> Result<(), ControlObjectLoadError> {
    validate_control_format_version(object_key, envelope.format_version)?;
    if envelope.kind != ControlObjectKind::NamespaceLease {
        return Err(ControlObjectLoadError::KindMismatch {
            object_key: object_key.to_owned(),
            expected: ControlObjectKind::NamespaceLease,
            actual: envelope.kind,
        });
    }

    validate_control_checksum(
        object_key,
        &envelope.payload_checksum_sha256,
        &envelope.state,
    )?;

    if &envelope.state.namespace_id != expected_namespace {
        return Err(ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }

    Ok(())
}

fn validate_control_format_version(
    object_key: &str,
    format_version: u32,
) -> Result<(), ControlObjectLoadError> {
    if format_version != CONTROL_OBJECT_FORMAT_VERSION {
        return Err(ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: format!("unsupported control object format version `{format_version}`"),
        });
    }
    Ok(())
}

fn validate_control_checksum<T: Serialize>(
    object_key: &str,
    expected_checksum: &str,
    state: &T,
) -> Result<(), ControlObjectLoadError> {
    let actual_checksum =
        payload_checksum_sha256(state).map_err(|err| ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        })?;

    if expected_checksum != actual_checksum {
        return Err(ControlObjectLoadError::ChecksumMismatch {
            object_key: object_key.to_owned(),
            expected: expected_checksum.to_owned(),
            actual: actual_checksum,
        });
    }

    Ok(())
}

fn map_store_load_error(err: ObjectStoreError) -> ControlObjectLoadError {
    ControlObjectLoadError::Store(err.to_string())
}
