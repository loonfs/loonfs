//! Loading and classification shared by mutable control-object families.

use super::ControlObjectLoadError;
use crate::error::StoreFailureClass;
use loonfs_api::wire::control::{decode_control_object, ControlObjectKind, ForkBasis, ManifestRef};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::NamespaceId;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedControl<T> {
    pub object_key: String,
    pub etag: String,
    pub state: T,
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
    /// A fork basis incorrectly references its target namespace.
    ForkBasisOwner { namespace_id: NamespaceId },
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
    validate_identity(envelope.payload())
        .map_err(|error| classify(&object_key, ControlLoadFailure::EmbeddedIdentity(error)))?;
    Ok(LoadedControl {
        object_key,
        etag,
        state: envelope.into_payload(),
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

/// Requires a root or checkpoint record to reference its own namespace.
pub(crate) fn expect_own_manifest(
    namespace_id: &NamespaceId,
    manifest: &ManifestRef,
) -> Result<(), EmbeddedIdentityMismatch> {
    expect_identity_field(
        "manifest owner namespace",
        namespace_id.as_str(),
        manifest.owner_namespace_id.as_str(),
    )
}

/// Requires a fork basis to reference a different namespace.
pub(crate) fn expect_foreign_fork_basis(
    namespace_id: &NamespaceId,
    fork_basis: &ForkBasis,
) -> Result<(), EmbeddedIdentityMismatch> {
    if fork_basis.manifest.owner_namespace_id != *namespace_id {
        return Ok(());
    }
    Err(EmbeddedIdentityMismatch::ForkBasisOwner {
        namespace_id: namespace_id.clone(),
    })
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
        }) => ControlObjectLoadError::IdentityMismatch {
            object_key: object_key.to_owned(),
            field: field.to_owned(),
            expected,
            actual,
        },
        ControlLoadFailure::EmbeddedIdentity(EmbeddedIdentityMismatch::ForkBasisOwner {
            namespace_id,
        }) => ControlObjectLoadError::ForkBasisOwnerIsSelf {
            object_key: object_key.to_owned(),
            namespace_id,
        },
        ControlLoadFailure::MissingEtag => ControlObjectLoadError::Store {
            object_key: object_key.to_owned(),
            message: "object store omitted the required control-object etag".to_owned(),
            class: StoreFailureClass::Other,
        },
        ControlLoadFailure::Store(error) => ControlObjectLoadError::Store {
            object_key: object_key.to_owned(),
            message: error.public_message().into_owned(),
            class: StoreFailureClass::of(&error),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StoreFailureClass;
    use bytes::Bytes;
    use loonfs_api::wire::control::HeadState;
    use loonfs_api::{ContentStoreId, NamespaceId};
    use loonfs_objectstore::keys::wal_head;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    };
    use serde_json::Value;
    use tempfile::{tempdir, TempDir};

    fn local_store() -> (TempDir, LocalFsStore) {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        (directory, store)
    }

    fn namespace(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("valid namespace id")
    }

    fn encoded_head(namespace_id: &NamespaceId) -> (HeadState, Vec<u8>) {
        let state = HeadState::initial(namespace_id.clone(), ContentStoreId::generate(), 1_000);
        let bytes =
            loonfs_api::wire::control::encode_control_state(ControlObjectKind::WalHead, &state)
                .expect("head bytes");
        (state, bytes)
    }

    async fn write_bytes(store: &LocalFsStore, object_key: &str, bytes: Vec<u8>) {
        store
            .put_overwrite(object_key, Bytes::from(bytes))
            .await
            .expect("write test object");
    }

    async fn load_head<S: ObjectStore + ?Sized>(
        store: &S,
        object_key: &str,
        expected_namespace_id: &NamespaceId,
    ) -> Result<LoadedControl<HeadState>, ControlObjectLoadError> {
        load_control_object(
            store,
            object_key.to_owned(),
            ControlObjectKind::WalHead,
            |state: &HeadState| expect_namespace(expected_namespace_id, &state.namespace_id),
        )
        .await
    }

    #[tokio::test]
    async fn absence_is_missing_object() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("absent object should fail");

        assert_eq!(error, ControlObjectLoadError::MissingObject { object_key });
    }

    #[tokio::test]
    async fn permission_failure_preserves_its_store_class() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key.clone()),
            OperationClass::GetWithMetadata,
            InjectedError::PermissionDenied("read forbidden".to_owned()),
        );
        store.fail_next(1);

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("permission failure should surface");

        assert!(matches!(
            error,
            ControlObjectLoadError::Store {
                class: StoreFailureClass::PermissionDenied,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn other_provider_failure_preserves_its_store_class() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key.clone()),
            OperationClass::GetWithMetadata,
            InjectedError::Transport("read timed out".to_owned()),
        );
        store.fail_next(1);

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("provider failure should surface");

        assert!(matches!(
            error,
            ControlObjectLoadError::Store {
                class: StoreFailureClass::Other,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn missing_etag_is_a_store_failure() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let (_, bytes) = encoded_head(&namespace_id);
        write_bytes(&inner, &object_key, bytes).await;
        let store = MetadataMapStore::without_etag(inner, KeyPredicate::exact(object_key.clone()));

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("missing etag should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::Store {
                class: StoreFailureClass::Other,
                message,
                ..
            } if message.contains("required control-object etag")
        ));
    }

    #[tokio::test]
    async fn malformed_envelope_is_a_codec_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        write_bytes(&store, &object_key, b"not json".to_vec()).await;

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("malformed envelope should fail");

        assert!(matches!(error, ControlObjectLoadError::Codec { .. }));
    }

    #[tokio::test]
    async fn wrong_envelope_kind_is_a_codec_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let (_, bytes) = encoded_head(&namespace_id);
        let mut document: Value = serde_json::from_slice(&bytes).expect("envelope json");
        document["kind"] = Value::String(ControlObjectKind::WalFloor.as_str().to_owned());
        write_bytes(
            &store,
            &object_key,
            serde_json::to_vec(&document).expect("modified envelope"),
        )
        .await;

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("wrong kind should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::Codec { message, .. }
                if message.contains("kind mismatch")
        ));
    }

    #[tokio::test]
    async fn checksum_disagreement_has_its_own_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let (_, bytes) = encoded_head(&namespace_id);
        let mut document: Value = serde_json::from_slice(&bytes).expect("envelope json");
        let recorded = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        document["payload_checksum"] = Value::String(recorded.to_owned());
        write_bytes(
            &store,
            &object_key,
            serde_json::to_vec(&document).expect("modified envelope"),
        )
        .await;

        let error = load_head(&store, &object_key, &namespace_id)
            .await
            .expect_err("checksum mismatch should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::ChecksumMismatch {
                expected,
                actual,
                ..
            } if expected == recorded && actual != recorded
        ));
    }

    #[tokio::test]
    async fn namespace_disagreement_is_an_identity_error() {
        let (_directory, store) = local_store();
        let expected_namespace_id = namespace("demo");
        let actual_namespace_id = namespace("other");
        let object_key = wal_head(&expected_namespace_id);
        let (_, bytes) = encoded_head(&actual_namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let error = load_head(&store, &object_key, &expected_namespace_id)
            .await
            .expect_err("namespace mismatch should fail");

        assert_eq!(
            error,
            ControlObjectLoadError::NamespaceMismatch {
                object_key,
                expected: expected_namespace_id,
                actual: actual_namespace_id,
            }
        );
    }

    #[tokio::test]
    async fn other_identity_disagreement_is_not_a_codec_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let (_, bytes) = encoded_head(&namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let error = load_control_object::<_, HeadState, _>(
            &store,
            object_key.clone(),
            ControlObjectKind::WalHead,
            |_state| expect_identity_field("test id", "expected", "actual"),
        )
        .await
        .expect_err("identity mismatch should fail");

        assert_eq!(
            error,
            ControlObjectLoadError::IdentityMismatch {
                object_key,
                field: "test id".to_owned(),
                expected: "expected".to_owned(),
                actual: "actual".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn successful_load_returns_state_key_and_etag() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = wal_head(&namespace_id);
        let (state, bytes) = encoded_head(&namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let loaded = load_head(&store, &object_key, &namespace_id)
            .await
            .expect("valid control object");

        assert_eq!(loaded.object_key, object_key);
        assert!(!loaded.etag.is_empty());
        assert_eq!(loaded.state, state);
    }
}
