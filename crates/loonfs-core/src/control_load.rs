//! Loading and classification shared by mutable control-object families.

use crate::error::{CoreError, StoreFailureClass};
use crate::namespace::control::ControlObjectLoadError;
use loonfs_api::wire::control::{decode_control_object, ControlObjectKind};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::NamespaceId;
use loonfs_objectstore::layout::parse_object_key;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadedControl<T> {
    pub(crate) object_key: String,
    pub(crate) etag: String,
    pub(crate) state: T,
}

pub(crate) enum EmbeddedIdentityMismatch {
    Namespace {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    Field {
        field: &'static str,
        expected: String,
        actual: String,
    },
}

enum ControlLoadFailure {
    Absent,
    Decode(EnvelopeCodecError),
    EmbeddedIdentity(EmbeddedIdentityMismatch),
    MissingEtag,
    Store(ObjectStoreError),
}

pub(crate) async fn load_control_object<S, T, F>(
    store: &S,
    object_key: String,
    kind: ControlObjectKind,
    validate_identity: F,
) -> Result<LoadedControl<T>, ControlObjectLoadError>
where
    S: ObjectStore + ?Sized,
    T: DeserializeOwned,
    F: FnOnce(&T) -> Result<(), EmbeddedIdentityMismatch>,
{
    let body = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|error| classify(&object_key, ControlLoadFailure::Store(error)))?
        .ok_or_else(|| classify(&object_key, ControlLoadFailure::Absent))?;
    let etag = body
        .metadata
        .etag
        .ok_or_else(|| classify(&object_key, ControlLoadFailure::MissingEtag))?;
    let envelope = decode_control_object(&body.bytes, kind)
        .map_err(|error| classify(&object_key, ControlLoadFailure::Decode(error)))?;
    validate_identity(&envelope.state)
        .map_err(|error| classify(&object_key, ControlLoadFailure::EmbeddedIdentity(error)))?;
    Ok(LoadedControl {
        object_key,
        etag,
        state: envelope.state,
    })
}

pub(crate) fn expect_namespace(
    expected: &NamespaceId,
    actual: &NamespaceId,
) -> Result<(), EmbeddedIdentityMismatch> {
    if actual == expected {
        return Ok(());
    }
    Err(EmbeddedIdentityMismatch::Namespace {
        expected: expected.clone(),
        actual: actual.clone(),
    })
}

pub(crate) fn expect_identity_field(
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), EmbeddedIdentityMismatch> {
    if actual == expected {
        return Ok(());
    }
    Err(EmbeddedIdentityMismatch::Field {
        field,
        expected: expected.to_owned(),
        actual: actual.to_owned(),
    })
}

pub(crate) fn expect_key_namespace(
    object_key: &str,
    actual: &NamespaceId,
) -> Result<(), EmbeddedIdentityMismatch> {
    let expected = parse_object_key(object_key)
        .and_then(|parsed| parsed.owner_namespace_id())
        .unwrap_or("a recognized namespace key");
    expect_identity_field("namespace id", expected, actual.as_str())
}

pub(crate) fn core_control_load_error(error: ControlObjectLoadError) -> CoreError {
    match error {
        ControlObjectLoadError::Store {
            object_key,
            message,
            class,
        } => CoreError::Store {
            object_key,
            message,
            class,
        },
        error => CoreError::NamespaceCorrupt(error.to_string()),
    }
}

fn classify(object_key: &str, failure: ControlLoadFailure) -> ControlObjectLoadError {
    match failure {
        ControlLoadFailure::Absent => ControlObjectLoadError::MissingObject {
            object_key: object_key.to_owned(),
        },
        ControlLoadFailure::Decode(EnvelopeCodecError::ChecksumMismatch { expected, actual }) => {
            ControlObjectLoadError::ChecksumMismatch {
                object_key: object_key.to_owned(),
                expected,
                actual,
            }
        }
        ControlLoadFailure::Decode(error) => ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: error.to_string(),
        },
        ControlLoadFailure::EmbeddedIdentity(EmbeddedIdentityMismatch::Namespace {
            expected,
            actual,
        }) => ControlObjectLoadError::NamespaceMismatch {
            object_key: object_key.to_owned(),
            expected,
            actual,
        },
        ControlLoadFailure::EmbeddedIdentity(EmbeddedIdentityMismatch::Field {
            field,
            expected,
            actual,
        }) => ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: format!("embedded {field} mismatch: expected `{expected}`, actual `{actual}`"),
        },
        ControlLoadFailure::MissingEtag => ControlObjectLoadError::Store {
            object_key: object_key.to_owned(),
            message: "object store omitted the required control-object etag".to_owned(),
            class: StoreFailureClass::Other,
        },
        ControlLoadFailure::Store(error) => ControlObjectLoadError::Store {
            object_key: object_key.to_owned(),
            message: error.message(),
            class: StoreFailureClass::of(&error),
        },
    }
}
