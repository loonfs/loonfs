//! Checkpoint records stored under `checkpoints/`.
//!
//! A checkpoint record pins one metadata manifest (its "basis") so that
//! garbage collection keeps everything the manifest references. Creation is
//! write-then-verify: the record is written as `active`, then the basis is
//! checked against the live retention floor, and the record flips to
//! `released` if the check fails. Release is the one state change a record
//! ever makes and it is one-way; garbage collection deletes the released
//! record outright once it has aged. Records never decide what readers see
//! as latest, and they are stored as standalone objects, never inside
//! manifests.

use super::load::load_namespace_manifest_envelope;
use super::ManifestLoadError;
use crate::control_object::{
    core_control_load_error, expect_identity_field, expect_namespace, load_control_object,
    ControlObjectLoadError, LoadedControl,
};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::load_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
};
use loonfs_api::{CheckpointId, NamespaceId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) fn encode_checkpoint_record(
    record: &CheckpointRecordState,
) -> crate::error::Result<Bytes> {
    let object_key = checkpoint_record(&record.namespace_id, &record.checkpoint_id);
    encode_control_state(ControlObjectKind::CheckpointRecord, record)
        .map(Bytes::from)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}

/// Writes a record under its freshly generated id with create-if-absent.
///
/// The id comes from [`CheckpointId::generate`], so the key is this pin's
/// alone: an occupied key means two generated ids collided, which is a
/// broken generator rather than a lifecycle a caller could resolve.
pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<()> {
    let encoded = encode_checkpoint_record(record)?;
    let object_key = checkpoint_record(&record.namespace_id, &record.checkpoint_id);
    // A checkpoint record is mutable control state, so its first lifecycle state is a conditional create.
    match store.put_if_absent(&object_key, encoded).await {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Err(CoreError::Internal(format!(
            "generated checkpoint id collided with the existing record `{object_key}`"
        ))),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
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
                state.checkpoint_id.as_str(),
            )
        },
    )
    .await
}

fn checkpoint_key_ids(
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
    let segments: Vec<_> = object_key.split('/').collect();
    let ["namespaces", namespace, "checkpoints", file_name] = segments.as_slice() else {
        return Err(ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: "the key does not have the checkpoint-record path shape".to_owned(),
        });
    };
    let checkpoint =
        file_name
            .strip_suffix(".json")
            .ok_or_else(|| ControlObjectLoadError::KeyLayout {
                object_key: object_key.to_owned(),
                expected_family: expected_family.to_owned(),
                reason: "the checkpoint filename does not end in `.json`".to_owned(),
            })?;
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
        Err(error) => Err(core_control_load_error(error)),
    }
}

/// Moves a record `active -> released` by compare-and-swap, stamping
/// `released_at_ms`.
///
/// This is the only state change a checkpoint record ever makes, and it is
/// one-way, so every caller converges: the owner asking for it and garbage
/// collection acting on a passed expiry reach the same end state, and a
/// record that is already released is left exactly as the winner wrote it.
pub(crate) async fn release_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    released_at_ms: u64,
) -> Result<()> {
    const RELEASE_CAS_ATTEMPTS: usize = 4;
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    for _attempt in 0..RELEASE_CAS_ATTEMPTS {
        let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
            // Reaped underneath us: no active pin under this id is exactly
            // what the release asked for.
            return Ok(());
        };
        if matches!(
            loaded.state.state,
            CheckpointRecordLifecycle::Released { .. }
        ) {
            return Ok(());
        }
        let mut next = loaded.state;
        next.state = CheckpointRecordLifecycle::Released { released_at_ms };
        let encoded = encode_checkpoint_record(&next)?;
        match store
            .compare_and_swap(&object_key, &loaded.etag, encoded)
            .await
        {
            Ok(_) => return Ok(()),
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
    Err(CoreError::Internal(format!(
        "checkpoint record `{object_key}` release retries exhausted"
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointBasisVerification {
    Verified,
    Invalid,
}

/// Checks that a record's basis is still intact: the retention floor has
/// not passed it, and the basis manifest still loads with the expected
/// checksum.
///
/// Creation calls this after the record is durable. Without the re-check, a
/// record written just as garbage collection decides to trim the same
/// manifest could pin state that is already gone.
pub(crate) async fn verify_checkpoint_basis<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<CheckpointBasisVerification> {
    let head = load_head_object(store, &record.namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .state;
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
    if floor_seq > record.manifest_head_seq {
        return Ok(CheckpointBasisVerification::Invalid);
    }
    // House rule: Err is not converted into absence or false unless the name says so.
    let manifest = match load_namespace_manifest_envelope(
        store,
        &record.namespace_id,
        &record.manifest_object_id,
    )
    .await
    {
        Ok(manifest) => manifest,
        Err(ManifestLoadError::MissingManifest { .. }) => {
            return Ok(CheckpointBasisVerification::Invalid)
        }
        Err(error) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::ManifestLoad(error),
            ))
        }
    };
    if manifest.payload_checksum != record.manifest_payload_checksum {
        // Both durable objects loaded successfully, so their disagreement is
        // corruption rather than a retention race that a new checkpoint can fix.
        return Err(CoreError::NamespaceCorrupt(format!(
            "checkpoint `{}` for namespace `{}` records manifest `{}` payload checksum `{}`, but the manifest carries `{}`",
            record.checkpoint_id,
            record.namespace_id,
            record.manifest_object_id,
            record.manifest_payload_checksum,
            manifest.payload_checksum,
        )));
    }
    Ok(CheckpointBasisVerification::Verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordLifecycle};
    use loonfs_api::{ChangeSeq, CommitId, ManifestId, ManifestObjectId};
    use loonfs_objectstore::keys::wal_head;
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
            checkpoint_id,
            namespace_id,
            manifest_id: ManifestId(1),
            manifest_object_id: ManifestObjectId::parse("00000000000000000001-0123456789abcdef")
                .expect("manifest object id"),
            manifest_head_seq: ChangeSeq(1),
            manifest_payload_checksum: "sha256:test".to_owned(),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                .expect("commit id"),
            created_at_ms: 1,
            owner: CheckpointOwner::User {
                name: "test".to_owned(),
                expires_at_ms: None,
            },
            state: CheckpointRecordLifecycle::Active {},
        }
    }

    #[tokio::test]
    async fn listed_loader_rejects_a_different_durable_family() {
        let (_directory, store) = local_store();
        let object_key =
            wal_head(&loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"));

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("wrong family should fail");

        assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
    }

    #[tokio::test]
    async fn listed_loader_rejects_invalid_key_ids() {
        let (_directory, store) = local_store();
        let checkpoint_id = "chk_00000000000000000000000000000001";
        let invalid_keys = [
            format!("namespaces/not valid/checkpoints/{checkpoint_id}.json"),
            "namespaces/demo/checkpoints/not-a-checkpoint.json".to_owned(),
        ];

        for object_key in invalid_keys {
            let error = load_checkpoint_record_at_key(&store, &object_key)
                .await
                .expect_err("invalid key id should fail");
            assert!(matches!(error, ControlObjectLoadError::KeyLayout { .. }));
        }
    }

    #[tokio::test]
    async fn listed_loader_validates_embedded_namespace_against_the_key() {
        let (_directory, store) = local_store();
        let key_namespace_id = namespace("demo");
        let embedded_namespace_id = namespace("other");
        let checkpoint_id = checkpoint("chk_00000000000000000000000000000001");
        let object_key = checkpoint_record(&key_namespace_id, &checkpoint_id);
        let bytes = encode_checkpoint_record(&record(embedded_namespace_id, checkpoint_id))
            .expect("record bytes");
        store
            .put_overwrite(&object_key, bytes)
            .await
            .expect("write record");

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("namespace mismatch should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::NamespaceMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn listed_loader_validates_embedded_checkpoint_id_against_the_key() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let key_checkpoint_id = checkpoint("chk_00000000000000000000000000000001");
        let embedded_checkpoint_id = checkpoint("chk_00000000000000000000000000000002");
        let object_key = checkpoint_record(&namespace_id, &key_checkpoint_id);
        let bytes = encode_checkpoint_record(&record(namespace_id, embedded_checkpoint_id))
            .expect("record bytes");
        store
            .put_overwrite(&object_key, bytes)
            .await
            .expect("write record");

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("checkpoint mismatch should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::IdentityMismatch { field, .. }
                if field == "checkpoint id"
        ));
    }
}
