//! Loading and classification shared by mutable control-object families.

use super::ControlObjectLoadError;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{decode_control_object, ControlObjectKind};
use loonfs_types::format::envelope::EnvelopeCodecError;
use loonfs_types::NamespaceId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedControl<T> {
    pub object_key: String,
    pub etag: String,
    pub state: T,
}

/// Loads the control object at `object_key`, decodes it as `kind`, and
/// checks the identity its payload embeds with `validate_identity`, which is
/// passed the object key and the payload.
pub(crate) async fn load_control_object<S, T, F>(
    store: &S,
    object_key: String,
    kind: ControlObjectKind,
    validate_identity: F,
) -> Result<LoadedControl<T>, ControlObjectLoadError>
where
    S: ObjectStore + ?Sized,
    T: DeserializeOwned,
    F: FnOnce(&str, &T) -> Result<(), ControlObjectLoadError>,
{
    let store_error = |error: ObjectStoreError| ControlObjectLoadError::Store {
        object_key: object_key.clone(),
        message: error.public_message().into_owned(),
        kind: error.kind(),
    };
    let body = store
        .get_with_metadata(&object_key)
        .await
        .map_err(store_error)?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let etag =
        loonfs_objectstore::required_etag(&object_key, body.metadata.etag).map_err(store_error)?;
    let envelope = decode_control_object(&body.bytes, kind).map_err(|error| match error {
        EnvelopeCodecError::ChecksumMismatch { expected, actual } => {
            ControlObjectLoadError::ChecksumMismatch {
                object_key: object_key.clone(),
                expected: expected.to_string(),
                actual: actual.to_string(),
            }
        }
        error => ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: error.to_string(),
        },
    })?;
    validate_identity(&object_key, envelope.payload())?;
    Ok(LoadedControl {
        object_key,
        etag,
        state: envelope.into_payload(),
    })
}

/// Returns `namespace_mismatch` for the object at `object_key` when the
/// namespace its payload embeds isn't `expected`.
pub(crate) fn expect_namespace(
    object_key: &str,
    expected: &NamespaceId,
    actual: &NamespaceId,
) -> Result<(), ControlObjectLoadError> {
    if actual == expected {
        return Ok(());
    }
    Err(ControlObjectLoadError::NamespaceMismatch {
        object_key: object_key.to_owned(),
        expected: expected.clone(),
        actual: actual.clone(),
    })
}

/// Returns `identity_mismatch` for the object at `object_key` when the
/// identity field its payload embeds isn't `expected`.
pub(crate) fn expect_identity_field(
    object_key: &str,
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), ControlObjectLoadError> {
    if actual == expected {
        return Ok(());
    }
    Err(ControlObjectLoadError::IdentityMismatch {
        object_key: object_key.to_owned(),
        field: field.to_owned(),
        expected: expected.to_owned(),
        actual: actual.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_objectstore::keys::hint;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_objectstore::ObjectStoreErrorKind;
    use loonfs_test_support::stores::{
        replace_object, FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationSelector,
    };
    use loonfs_types::format::control::HintPayload;
    use loonfs_types::NamespaceId;
    use serde_json::Value;
    use std::assert_matches;
    use tempfile::{tempdir, TempDir};

    fn local_store() -> (TempDir, LocalFsStore) {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        (directory, store)
    }

    fn namespace(value: &str) -> NamespaceId {
        loonfs_test_support::ids::namespace_id(value)
    }

    fn encoded_hint(namespace_id: &NamespaceId) -> (HintPayload, Vec<u8>) {
        let state = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: loonfs_types::ManifestNo(1),
        };
        let bytes =
            loonfs_types::format::control::encode_control_state(ControlObjectKind::Hint, &state)
                .expect("hint bytes");
        (state, bytes)
    }

    async fn write_bytes(store: &LocalFsStore, object_key: &str, bytes: Vec<u8>) {
        replace_object(&store, object_key, Bytes::from(bytes))
            .await
            .expect("write test object");
    }

    async fn load_hint<S: ObjectStore + ?Sized>(
        store: &S,
        object_key: &str,
        expected_namespace_id: &NamespaceId,
    ) -> Result<LoadedControl<HintPayload>, ControlObjectLoadError> {
        load_control_object(
            store,
            object_key.to_owned(),
            ControlObjectKind::Hint,
            |object_key, state: &HintPayload| {
                expect_namespace(object_key, expected_namespace_id, &state.namespace_id)
            },
        )
        .await
    }

    #[tokio::test]
    async fn absence_is_missing_object() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("absent object should fail");

        assert_eq!(error, ControlObjectLoadError::MissingObject { object_key });
    }

    #[tokio::test]
    async fn permission_failure_preserves_its_store_class() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key.clone()),
            OperationSelector::GetWithMetadata,
            InjectedError::PermissionDenied("read forbidden".to_owned()),
        );
        store.fail_next(1);

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("permission failure should surface");

        assert_matches!(
            error,
            ControlObjectLoadError::Store {
                kind: ObjectStoreErrorKind::PermissionDenied,
                ..
            }
        );
    }

    #[tokio::test]
    async fn other_provider_failure_preserves_its_store_class() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key.clone()),
            OperationSelector::GetWithMetadata,
            InjectedError::Transport("read timed out".to_owned()),
        );
        store.fail_next(1);

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("provider failure should surface");

        assert_matches!(
            error,
            ControlObjectLoadError::Store {
                kind: ObjectStoreErrorKind::Other,
                ..
            }
        );
    }

    #[tokio::test]
    async fn missing_etag_is_a_store_failure() {
        let (_directory, inner) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        let (_, bytes) = encoded_hint(&namespace_id);
        write_bytes(&inner, &object_key, bytes).await;
        let store = MetadataMapStore::without_etag(inner, KeyPredicate::exact(object_key.clone()));

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("missing etag should fail");

        assert_matches!(
            error,
            ControlObjectLoadError::Store {
                kind: ObjectStoreErrorKind::Other,
                message,
                ..
            } if message == ObjectStoreErrorKind::Other.public_message()
        );
    }

    #[tokio::test]
    async fn malformed_envelope_is_a_codec_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        write_bytes(&store, &object_key, b"not json".to_vec()).await;

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("malformed envelope should fail");

        assert_matches!(error, ControlObjectLoadError::Codec { .. });
    }

    #[tokio::test]
    async fn wrong_envelope_kind_is_a_codec_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        let (_, bytes) = encoded_hint(&namespace_id);
        let mut document: Value = serde_json::from_slice(&bytes).expect("envelope json");
        document["kind"] = Value::String(ControlObjectKind::Pin.as_str().to_owned());
        write_bytes(
            &store,
            &object_key,
            serde_json::to_vec(&document).expect("modified envelope"),
        )
        .await;

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("wrong kind should fail");

        assert_matches!(
            error,
            ControlObjectLoadError::Codec { message, .. }
                if message.contains("kind mismatch")
        );
    }

    #[tokio::test]
    async fn checksum_disagreement_has_its_own_error() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let object_key = hint(&namespace_id);
        let (_, bytes) = encoded_hint(&namespace_id);
        let mut document: Value = serde_json::from_slice(&bytes).expect("envelope json");
        let recorded = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        document["payload_checksum"] = Value::String(recorded.to_owned());
        write_bytes(
            &store,
            &object_key,
            serde_json::to_vec(&document).expect("modified envelope"),
        )
        .await;

        let error = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect_err("checksum mismatch should fail");

        assert_matches!(
            error,
            ControlObjectLoadError::ChecksumMismatch {
                expected,
                actual,
                ..
            } if expected == recorded && actual != recorded
        );
    }

    #[tokio::test]
    async fn namespace_disagreement_is_an_identity_error() {
        let (_directory, store) = local_store();
        let expected_namespace_id = namespace("demo");
        let actual_namespace_id = namespace("other");
        let object_key = hint(&expected_namespace_id);
        let (_, bytes) = encoded_hint(&actual_namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let error = load_hint(&store, &object_key, &expected_namespace_id)
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
        let object_key = hint(&namespace_id);
        let (_, bytes) = encoded_hint(&namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let error = load_control_object::<_, HintPayload, _>(
            &store,
            object_key.clone(),
            ControlObjectKind::Hint,
            |object_key, _state| expect_identity_field(object_key, "test id", "expected", "actual"),
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
        let object_key = hint(&namespace_id);
        let (state, bytes) = encoded_hint(&namespace_id);
        write_bytes(&store, &object_key, bytes).await;

        let loaded = load_hint(&store, &object_key, &namespace_id)
            .await
            .expect("valid control object");

        assert_eq!(loaded.object_key, object_key);
        assert!(!loaded.etag.is_empty());
        assert_eq!(loaded.state, state);
    }
}
