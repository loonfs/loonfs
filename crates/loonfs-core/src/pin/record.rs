//! Pin creation, point reads, deletion, and manifest verification.

use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
    LoadedControl,
};
use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, PinOwner, PinPayload};
use loonfs_api::{NamespaceId, PinId};
use loonfs_objectstore::keys::pin;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ObjectStore;

pub(crate) fn encode_pin(record: &PinPayload) -> crate::error::Result<Bytes> {
    let object_key = pin(&record.namespace_id, &record.pin_id);
    encode_control_state(ControlObjectKind::Pin, record)
        .map(Bytes::from)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}

/// Writes a record under its freshly generated [`PinId`].
pub(crate) async fn write_pin<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
) -> Result<()> {
    let encoded = encode_pin(record)?;
    let object_key = pin(&record.namespace_id, &record.pin_id);
    create_control_object_under_generated_id(store, &object_key, encoded).await?;
    Ok(())
}

pub(crate) type LoadedPin = LoadedControl<PinPayload>;

/// Loads the exact pin key returned by a prefix listing.
///
/// The key is durable identity, so a scan must validate the same namespace
/// and pin id that a point read validates instead of trusting only the
/// decoded namespace. Invalid identifier text is a key-layout failure; bytes
/// that decode but disagree with the key are identity failures.
pub(crate) async fn load_pin_at_key<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> std::result::Result<LoadedPin, ControlObjectLoadError> {
    let (namespace_id, pin_id) = pin_key_ids(object_key)?;
    load_control_object(
        store,
        object_key.to_owned(),
        ControlObjectKind::Pin,
        |state: &PinPayload| {
            expect_namespace(&namespace_id, &state.namespace_id)?;
            expect_identity_field("pin id", pin_id.as_str(), state.pin_id.as_str())
        },
    )
    .await
}

pub(crate) fn pin_key_ids(
    object_key: &str,
) -> std::result::Result<(NamespaceId, PinId), ControlObjectLoadError> {
    let expected_family = "pin";
    let parsed = parse_object_key(object_key).ok_or_else(|| ControlObjectLoadError::KeyLayout {
        object_key: object_key.to_owned(),
        expected_family: expected_family.to_owned(),
        reason: "the key does not match a recognized durable object family".to_owned(),
    })?;
    if parsed.family() != DurableObjectFamily::Pin {
        return Err(ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the key belongs to durable family `{:?}`", parsed.family()),
        });
    }
    let namespace = parsed.owner_namespace_id();
    let identifier = parsed
        .identifier()
        .expect("pin keys carry a pin identifier");
    let namespace_id =
        NamespaceId::parse(namespace).map_err(|error| ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the namespace path component is invalid: {error}"),
        })?;
    let pin_id = PinId::parse(identifier).map_err(|error| ControlObjectLoadError::KeyLayout {
        object_key: object_key.to_owned(),
        expected_family: expected_family.to_owned(),
        reason: format!("the pin filename id is invalid: {error}"),
    })?;
    Ok((namespace_id, pin_id))
}

pub(crate) async fn load_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
) -> Result<Option<LoadedPin>> {
    let object_key = pin(namespace_id, pin_id);
    let loaded = load_pin_at_key(store, &object_key).await;
    match loaded {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

/// The owner kind an operation serves: checkpoint operations serve user pins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PinOwnerKind {
    User,
    Snapshot,
}

/// Loads the pin `pin_id` if `owner_kind` owns it. Otherwise returns
/// the not-found error of that kind, so an id of another kind does not reveal
/// its pin.
pub(crate) async fn load_owned_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
    owner_kind: PinOwnerKind,
) -> Result<LoadedPin> {
    load_pin(store, namespace_id, pin_id)
        .await?
        .filter(|loaded| {
            matches!(
                (owner_kind, &loaded.state.owner),
                (PinOwnerKind::User, PinOwner::User { .. })
                    | (PinOwnerKind::Snapshot, PinOwner::Snapshot { .. })
            )
        })
        .ok_or_else(|| match owner_kind {
            PinOwnerKind::User => CoreError::CheckpointNotFound {
                checkpoint_id: pin_id.clone(),
            },
            PinOwnerKind::Snapshot => CoreError::SnapshotNotFound {
                snapshot_id: pin_id.clone(),
            },
        })
}

pub(crate) async fn delete_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
) -> Result<()> {
    let object_key = pin(namespace_id, pin_id);
    store
        .delete(&object_key)
        .await
        .map_err(|error| CoreError::store(&object_key, &error))
}

/// Deletes a pin whose creation failed with `error`. A failed delete is
/// only logged, and collection removes the pin later.
pub(crate) async fn delete_failed_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pin_id: &PinId,
    error: &CoreError,
) {
    if let Err(cleanup_error) = delete_pin(store, namespace_id, pin_id).await {
        tracing::warn!(
            namespace_id = %namespace_id,
            pin_id = %pin_id,
            original_error = %error,
            cleanup_error = %cleanup_error,
            "failed to delete a pin after its creation failed"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PinBasisVerification {
    Verified,
    Invalid,
}

pub(crate) async fn verify_pin_basis<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
) -> Result<PinBasisVerification> {
    let manifest = load_current_manifest(store, &record.namespace_id).await?;
    let pinned = record.manifest();
    Ok(
        if manifest.state.envelope.payload().status.is_deleted()
            || manifest.state.manifest().manifest_no != pinned.manifest_no
            || manifest.state.manifest().payload_checksum != pinned.payload_checksum
        {
            PinBasisVerification::Invalid
        } else {
            PinBasisVerification::Verified
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::ChangeSeq;
    use loonfs_objectstore::keys::hint;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::{tempdir, TempDir};

    fn local_store() -> (TempDir, LocalFsStore) {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        (directory, store)
    }

    fn namespace(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("valid namespace id")
    }

    fn pin_id(value: &str) -> PinId {
        PinId::parse(value).expect("valid pin id")
    }

    fn record(namespace_id: NamespaceId, pin_id: PinId) -> PinPayload {
        PinPayload {
            pin_id,
            namespace_id: namespace_id.clone(),
            head_seq: ChangeSeq(1),
            payload_checksum: "sha256:test".to_owned(),
            created_at_ms: 1,
            owner: PinOwner::User {
                name: "test".to_owned(),
                expires_at_ms: None,
            },
        }
    }

    #[tokio::test]
    async fn listed_loader_rejects_a_different_durable_family() {
        let (_directory, store) = local_store();
        let object_key = hint(&loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"));

        let error = load_pin_at_key(&store, &object_key)
            .await
            .expect_err("wrong family should fail");

        assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
    }

    #[tokio::test]
    async fn listed_loader_rejects_invalid_key_ids() {
        let (_directory, store) = local_store();
        let pin_id = "pin_00000000000000000001-0000000000000001";
        let invalid_keys = [
            format!("namespaces/not valid/pins/{pin_id}.json"),
            "namespaces/demo/pins/not-a-pin.json".to_owned(),
        ];

        for object_key in invalid_keys {
            let error = load_pin_at_key(&store, &object_key)
                .await
                .expect_err("invalid key id should fail");
            assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
        }
    }

    #[tokio::test]
    async fn listed_loader_validates_the_record_against_its_key() {
        enum Mismatch {
            Namespace,
            PinId,
        }

        let (_directory, store) = local_store();
        let key_namespace_id = namespace("demo");
        let key_pin_id = pin_id("pin_00000000000000000001-0000000000000001");
        let object_key = pin(&key_namespace_id, &key_pin_id);

        let cases = [
            (
                "embedded namespace",
                record(namespace("other"), key_pin_id.clone()),
                Mismatch::Namespace,
            ),
            (
                "embedded pin id",
                record(
                    key_namespace_id.clone(),
                    pin_id("pin_00000000000000000001-0000000000000002"),
                ),
                Mismatch::PinId,
            ),
        ];

        for (label, forged, expected) in cases {
            let bytes = encode_pin(&forged).expect("record bytes");
            store
                .put_overwrite(&object_key, bytes)
                .await
                .expect("write record");

            let error = load_pin_at_key(&store, &object_key)
                .await
                .expect_err("a record that does not describe its key should fail");
            match expected {
                Mismatch::Namespace => assert!(
                    matches!(error, ControlObjectLoadError::NamespaceMismatch { .. }),
                    "for `{label}`: {error:?}"
                ),
                Mismatch::PinId => assert!(
                    matches!(
                        error,
                        ControlObjectLoadError::IdentityMismatch { ref field, .. }
                            if field == "pin id"
                    ),
                    "for `{label}`: {error:?}"
                ),
            }
        }
    }
}
