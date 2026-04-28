use loon_api::{
    payload_checksum_sha256, ContentStoreDescriptorEnvelope, ContentStoreId, ControlObjectKind,
    HeadStateEnvelope, LeaseStateEnvelope, NamespaceDescriptorEnvelope, NamespaceId,
};
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

#[derive(Debug, Clone, PartialEq, Eq, DeriveSerialize, Deserialize, Error)]
pub enum ControlObjectLoadError {
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

fn validate_namespace_descriptor_envelope(
    expected_namespace: &NamespaceId,
    object_key: &str,
    envelope: &NamespaceDescriptorEnvelope,
) -> Result<(), ControlObjectLoadError> {
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
