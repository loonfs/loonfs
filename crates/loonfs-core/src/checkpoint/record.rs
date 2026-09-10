//! Pin creation, point reads, deletion, and manifest verification.

use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
    LoadedControl,
};
use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, CheckpointRecordState, ControlObjectKind};
use loonfs_api::{CheckpointId, NamespaceId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) fn encode_checkpoint_record(
    record: &CheckpointRecordState,
) -> crate::error::Result<Bytes> {
    let object_key = checkpoint_record(&record.namespace_id, &record.pin_id);
    encode_control_state(ControlObjectKind::CheckpointRecord, record)
        .map(Bytes::from)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}

/// Writes a record under its freshly generated [`CheckpointId`].
pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<()> {
    let encoded = encode_checkpoint_record(record)?;
    let object_key = checkpoint_record(&record.namespace_id, &record.pin_id);
    create_control_object_under_generated_id(store, &object_key, encoded).await?;
    Ok(())
}

pub(crate) type LoadedCheckpointRecord = LoadedControl<CheckpointRecordState>;

/// Loads the exact checkpoint key returned by a prefix listing.
///
/// The key is durable identity, so a scan must validate the same namespace
/// and checkpoint id that a point read validates instead of trusting only the
/// decoded namespace. Invalid identifier text is a key-layout failure; bytes
/// that decode but disagree with the key are identity failures.
pub(crate) async fn load_checkpoint_record_at_key<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> std::result::Result<LoadedCheckpointRecord, ControlObjectLoadError> {
    let (namespace_id, checkpoint_id) = checkpoint_key_ids(object_key)?;
    load_control_object(
        store,
        object_key.to_owned(),
        ControlObjectKind::CheckpointRecord,
        |state: &CheckpointRecordState| {
            expect_namespace(&namespace_id, &state.namespace_id)?;
            expect_identity_field(
                "checkpoint id",
                checkpoint_id.as_str(),
                state.pin_id.as_str(),
            )?;
            expect_identity_field(
                "manifest number",
                &checkpoint_id.manifest_no().to_string(),
                &state.manifest_no.to_string(),
            )
        },
    )
    .await
}

pub(crate) fn checkpoint_key_ids(
    object_key: &str,
) -> std::result::Result<(NamespaceId, CheckpointId), ControlObjectLoadError> {
    let expected_family = "checkpoint record";
    let parsed = parse_object_key(object_key).ok_or_else(|| ControlObjectLoadError::KeyLayout {
        object_key: object_key.to_owned(),
        expected_family: expected_family.to_owned(),
        reason: "the key does not match a recognized durable object family".to_owned(),
    })?;
    if parsed.family() != DurableObjectFamily::CheckpointRecord {
        return Err(ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the key belongs to durable family `{:?}`", parsed.family()),
        });
    }
    let namespace = parsed
        .owner_namespace_id()
        .expect("checkpoint record keys carry a namespace identifier");
    let checkpoint = parsed
        .identifier()
        .expect("checkpoint record keys carry a checkpoint identifier");
    let namespace_id =
        NamespaceId::parse(namespace).map_err(|error| ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the namespace path component is invalid: {error}"),
        })?;
    let checkpoint_id =
        CheckpointId::parse(checkpoint).map_err(|error| ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the checkpoint filename id is invalid: {error}"),
        })?;
    Ok((namespace_id, checkpoint_id))
}

pub(crate) async fn load_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<Option<LoadedCheckpointRecord>> {
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    let loaded = load_checkpoint_record_at_key(store, &object_key).await;
    match loaded {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

pub(crate) async fn delete_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<()> {
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    match store.delete(&object_key).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointBasisVerification {
    Verified,
    Invalid,
}

pub(crate) async fn verify_checkpoint_basis<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<CheckpointBasisVerification> {
    let manifest = load_current_manifest(store, &record.namespace_id).await?;
    let pinned = record.manifest();
    Ok(
        if manifest.envelope.payload().status.is_deleted()
            || manifest.state.retention_floor_seq > record.manifest_head_seq
            || manifest.state.manifest.manifest_no != pinned.manifest_no
            || manifest.state.manifest.manifest_payload_checksum != pinned.manifest_payload_checksum
        {
            CheckpointBasisVerification::Invalid
        } else {
            CheckpointBasisVerification::Verified
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::CheckpointOwner;
    use loonfs_api::{ChangeSeq, CommitId, ManifestNo};
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

    fn checkpoint(value: &str) -> CheckpointId {
        CheckpointId::parse(value).expect("valid checkpoint id")
    }

    fn record(namespace_id: NamespaceId, checkpoint_id: CheckpointId) -> CheckpointRecordState {
        CheckpointRecordState {
            pin_id: checkpoint_id,
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),

            manifest_head_seq: ChangeSeq(1),
            manifest_payload_checksum: "sha256:test".to_owned(),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                .expect("commit id"),
            created_at_ms: 1,
            owner: CheckpointOwner::User {
                name: "test".to_owned(),
                expires_at_ms: None,
            },
        }
    }

    #[tokio::test]
    async fn listed_loader_rejects_a_different_durable_family() {
        let (_directory, store) = local_store();
        let object_key = hint(&loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"));

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("wrong family should fail");

        assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
    }

    #[tokio::test]
    async fn listed_loader_rejects_invalid_key_ids() {
        let (_directory, store) = local_store();
        let checkpoint_id = "pin_00000000000000000001-0000000000000001";
        let invalid_keys = [
            format!("namespaces/not valid/pins/{checkpoint_id}.json"),
            "namespaces/demo/pins/not-a-checkpoint.json".to_owned(),
        ];

        for object_key in invalid_keys {
            let error = load_checkpoint_record_at_key(&store, &object_key)
                .await
                .expect_err("invalid key id should fail");
            assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
        }
    }

    #[tokio::test]
    async fn loader_rejects_a_pin_number_that_disagrees_with_its_key() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let checkpoint_id = checkpoint("pin_00000000000000000001-0000000000000001");
        let object_key = checkpoint_record(&namespace_id, &checkpoint_id);
        let mut foreign = record(namespace_id, checkpoint_id);
        foreign.manifest_no = ManifestNo(2);
        let bytes = encode_checkpoint_record(&foreign).expect("record bytes");
        store
            .put_overwrite(&object_key, bytes)
            .await
            .expect("write record");

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("a mismatched manifest number should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::IdentityMismatch { field, .. }
                if field == "manifest number"
        ));
    }

    #[tokio::test]
    async fn listed_loader_validates_the_record_against_its_key() {
        enum Mismatch {
            Namespace,
            CheckpointId,
        }

        let (_directory, store) = local_store();
        let key_namespace_id = namespace("demo");
        let key_checkpoint_id = checkpoint("pin_00000000000000000001-0000000000000001");
        let object_key = checkpoint_record(&key_namespace_id, &key_checkpoint_id);

        let cases = [
            (
                "embedded namespace",
                record(namespace("other"), key_checkpoint_id.clone()),
                Mismatch::Namespace,
            ),
            (
                "embedded checkpoint id",
                record(
                    key_namespace_id.clone(),
                    checkpoint("pin_00000000000000000001-0000000000000002"),
                ),
                Mismatch::CheckpointId,
            ),
        ];

        for (label, forged, expected) in cases {
            let bytes = encode_checkpoint_record(&forged).expect("record bytes");
            store
                .put_overwrite(&object_key, bytes)
                .await
                .expect("write record");

            let error = load_checkpoint_record_at_key(&store, &object_key)
                .await
                .expect_err("a record that does not describe its key should fail");
            match expected {
                Mismatch::Namespace => assert!(
                    matches!(error, ControlObjectLoadError::NamespaceMismatch { .. }),
                    "for `{label}`: {error:?}"
                ),
                Mismatch::CheckpointId => assert!(
                    matches!(
                        error,
                        ControlObjectLoadError::IdentityMismatch { ref field, .. }
                            if field == "checkpoint id"
                    ),
                    "for `{label}`: {error:?}"
                ),
            }
        }
    }
}
