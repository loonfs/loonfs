//! Checkpoint records stored under `checkpoints/`.
//!
//! A checkpoint record pins one metadata manifest for garbage collection.
//! Creation writes an active record and then verifies its basis against the
//! retention floor. Failed verification releases the record. Release is
//! one-way, and garbage collection later removes aged released records.

use super::load::{ensure_manifest_reference_matches, load_namespace_manifest_envelope};
use super::ManifestLoadError;
use crate::control_object::{
    expect_identity_field, expect_namespace, expect_own_manifest, load_control_object,
    ControlObjectLoadError, LoadedControl,
};
use crate::control_update::{
    create_control_object_under_generated_id, retry_while_contended, CasAttempt,
};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::FORK_CHECKPOINT_LEASE_MS;
use crate::namespace::control::load_head_object;
use crate::namespace::control_snapshot::resolve_retention_floor_seq;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, CheckpointOwner, CheckpointRecordState, CheckpointStatus,
    ControlObjectKind,
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

/// Writes a record under its freshly generated [`CheckpointId`].
pub(crate) async fn write_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
) -> Result<()> {
    let encoded = encode_checkpoint_record(record)?;
    let object_key = checkpoint_record(&record.namespace_id, &record.checkpoint_id);
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
                state.checkpoint_id.as_str(),
            )?;
            expect_own_manifest(&state.namespace_id, &state.manifest)
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
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointRelease {
    Released,
    LostRace,
}

/// Tries one `active -> released` transition using the loaded ETag.
pub(crate) async fn release_inspected_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    loaded: LoadedCheckpointRecord,
    released_at_ms: u64,
) -> Result<CheckpointRelease> {
    let mut next = loaded.state;
    next.status = CheckpointStatus::Released { released_at_ms };
    let encoded = encode_checkpoint_record(&next)?;
    match store
        .compare_and_swap(object_key, &loaded.etag, encoded)
        .await
    {
        Ok(_) => Ok(CheckpointRelease::Released),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CheckpointRelease::LostRace),
        Err(error) => Err(CoreError::store(object_key, &error)),
    }
}

/// Releases a checkpoint record, retrying CAS conflicts.
pub(crate) async fn release_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    released_at_ms: u64,
) -> Result<()> {
    let object_key = &checkpoint_record(namespace_id, checkpoint_id);
    let released = retry_while_contended(|| async move {
        let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id)
            .await?
            .filter(|loaded| loaded.state.status == (CheckpointStatus::Active {}))
        else {
            // Reaped or released underneath us: either way no active pin
            // stands under this id, which is what the release asked for.
            return Ok::<_, CoreError>(CasAttempt::Settled(()));
        };
        Ok(
            match release_inspected_checkpoint_record(store, object_key, loaded, released_at_ms)
                .await?
            {
                CheckpointRelease::Released => CasAttempt::Settled(()),
                CheckpointRelease::LostRace => CasAttempt::Contended,
            },
        )
    })
    .await?;
    released.ok_or_else(|| CoreError::contention_exhausted(object_key))
}

/// Renews a fork checkpoint immediately before installing its target.
///
/// This compare-and-swap races with GC release. The new expiry must differ
/// from the stored value so a stale release cannot use the old ETag.
pub(crate) async fn renew_fork_checkpoint_for_install<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    expected_target_namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<u64> {
    let object_key = &checkpoint_record(source_namespace_id, checkpoint_id);
    let unavailable = &|reason: String| {
        CoreError::CheckpointUnavailable(format!(
            "fork of `{source_namespace_id}` into `{expected_target_namespace_id}` cannot renew \
             its source checkpoint `{checkpoint_id}`: {reason}"
        ))
    };
    let renewed = retry_while_contended(|| async move {
        let Some(loaded) =
            load_checkpoint_record(store, source_namespace_id, checkpoint_id).await?
        else {
            return Err(unavailable("the record is gone".to_owned()));
        };
        let mut next = loaded.state;
        if next.status != (CheckpointStatus::Active {}) {
            return Err(unavailable(format!("the record is `{}`", next.status)));
        }
        let CheckpointOwner::Fork {
            target_namespace_id,
            expires_at_ms,
        } = &mut next.owner
        else {
            return Err(unavailable("the record is not fork-owned".to_owned()));
        };
        if target_namespace_id != expected_target_namespace_id {
            return Err(unavailable(format!(
                "the record pins target `{target_namespace_id}`"
            )));
        }
        let later_expiry = expires_at_ms
            .checked_add(1)
            .ok_or_else(|| unavailable("the lease expiry cannot be extended".to_owned()))?;
        *expires_at_ms = later_expiry.max(now_ms.saturating_add(FORK_CHECKPOINT_LEASE_MS));
        let renewed_expiry = *expires_at_ms;
        let encoded = encode_checkpoint_record(&next)?;
        match store
            .compare_and_swap(object_key, &loaded.etag, encoded)
            .await
        {
            Ok(_) => Ok(CasAttempt::Settled(renewed_expiry)),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
            // Do not install a target unless the renewal is confirmed.
            Err(error) => Err(CoreError::store(object_key, &error)),
        }
    })
    .await?;
    renewed.ok_or_else(|| CoreError::contention_exhausted(object_key))
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
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if floor_seq > record.manifest.manifest_head_seq {
        return Ok(CheckpointBasisVerification::Invalid);
    }
    // House rule: Err is not converted into absence or false unless the name says so.
    let manifest = match load_namespace_manifest_envelope(
        store,
        &record.namespace_id,
        &record.manifest.manifest_object_id,
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
    // Both durable objects loaded successfully, so any disagreement is
    // corruption rather than a retention race that a new checkpoint can fix.
    ensure_manifest_reference_matches(
        &format!(
            "checkpoint `{}` for namespace `{}`",
            record.checkpoint_id, record.namespace_id
        ),
        &record.manifest,
        &manifest,
    )?;
    Ok(CheckpointBasisVerification::Verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus, ManifestRef};
    use loonfs_api::{ChangeSeq, CommitId, ManifestNo, ManifestObjectId};
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
            namespace_id: namespace_id.clone(),
            manifest: ManifestRef {
                owner_namespace_id: namespace_id,
                manifest_no: ManifestNo(1),
                manifest_object_id: ManifestObjectId::parse(
                    "man_00000000000000000001-0123456789abcdef",
                )
                .expect("manifest object id"),
                manifest_head_seq: ChangeSeq(1),
                manifest_payload_checksum: "sha256:test".to_owned(),
            },
            head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                .expect("commit id"),
            created_at_ms: 1,
            owner: CheckpointOwner::User {
                name: "test".to_owned(),
                expires_at_ms: None,
            },
            status: CheckpointStatus::Active {},
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
    async fn loader_rejects_a_record_pinning_another_namespaces_manifest() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let checkpoint_id = checkpoint("chk_00000000000000000000000000000001");
        let object_key = checkpoint_record(&namespace_id, &checkpoint_id);
        let mut foreign = record(namespace_id, checkpoint_id);
        foreign.manifest.owner_namespace_id = namespace("other");
        let bytes = encode_checkpoint_record(&foreign).expect("record bytes");
        store
            .put_overwrite(&object_key, bytes)
            .await
            .expect("write record");

        let error = load_checkpoint_record_at_key(&store, &object_key)
            .await
            .expect_err("a foreign manifest owner should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::IdentityMismatch { field, .. }
                if field == "manifest owner namespace"
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
        let key_checkpoint_id = checkpoint("chk_00000000000000000000000000000001");
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
                    checkpoint("chk_00000000000000000000000000000002"),
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
