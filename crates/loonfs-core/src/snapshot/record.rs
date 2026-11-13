//! Snapshot creation, point reads, deletion, and manifest verification.

use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
    LoadedControl,
};
use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use bytes::Bytes;
use loonfs_objectstore::keys::snapshot_record;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily, KeyPrefix};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{
    encode_control_state, ControlObjectKind, SnapshotOwner, SnapshotPayload,
};
use loonfs_types::{NamespaceId, SnapshotId};

pub(crate) fn encode_snapshot_record(record: &SnapshotPayload) -> Bytes {
    encode_control_state(ControlObjectKind::Snapshot, record)
        .map(Bytes::from)
        .expect("a snapshot record should encode")
}

/// Writes a record under its freshly generated [`SnapshotId`].
pub(crate) async fn write_snapshot_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &SnapshotPayload,
) -> Result<()> {
    let encoded = encode_snapshot_record(record);
    let object_key = snapshot_record(&record.namespace_id, &record.snapshot_id);
    if let Err(error) = create_control_object_under_generated_id(store, &object_key, encoded).await
    {
        tracing::error!(
            namespace_id = %record.namespace_id,
            object_key = %object_key,
            error = %error,
            "snapshot record write failed"
        );
        return Err(error);
    }
    Ok(())
}

pub(crate) type LoadedSnapshotRecord = LoadedControl<SnapshotPayload>;

/// Loads the exact snapshot key returned by a prefix listing.
///
/// The key is durable identity, so a scan must validate the same namespace
/// and snapshot id that a point read validates instead of trusting only the
/// decoded namespace. Invalid identifier text is a key-layout failure; bytes
/// that decode but disagree with the key are identity failures.
pub(crate) async fn load_snapshot_record_at_key<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> std::result::Result<LoadedSnapshotRecord, ControlObjectLoadError> {
    let (namespace_id, snapshot_id) = snapshot_record_key_ids(object_key)?;
    load_control_object(
        store,
        object_key.to_owned(),
        ControlObjectKind::Snapshot,
        |object_key, state: &SnapshotPayload| {
            expect_namespace(object_key, &namespace_id, &state.namespace_id)?;
            expect_identity_field(
                object_key,
                "snapshot id",
                snapshot_id.as_str(),
                state.snapshot_id.as_str(),
            )
        },
    )
    .await
}

pub(crate) fn snapshot_record_key_ids(
    object_key: &str,
) -> std::result::Result<(NamespaceId, SnapshotId), ControlObjectLoadError> {
    let expected_family = "snapshot";
    let parsed = parse_object_key(object_key).ok_or_else(|| ControlObjectLoadError::KeyLayout {
        object_key: object_key.to_owned(),
        expected_family: expected_family.to_owned(),
        reason: "the key does not match a recognized durable object family".to_owned(),
    })?;
    if parsed.family() != DurableObjectFamily::Snapshot {
        return Err(ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the key belongs to durable family `{:?}`", parsed.family()),
        });
    }
    let namespace = match parsed.prefix() {
        KeyPrefix::Namespace(namespace) => Some(namespace),
        KeyPrefix::Pool(_) => None,
    }
    .expect("`parse_object_key` should find every snapshot key under a namespace prefix");
    let identifier = parsed
        .identifier()
        .expect("`parse_object_key` should give every snapshot key its snapshot ID");
    let namespace_id =
        NamespaceId::parse(namespace).map_err(|error| ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the namespace path component is invalid: {error}"),
        })?;
    let snapshot_id =
        SnapshotId::parse(identifier).map_err(|error| ControlObjectLoadError::KeyLayout {
            object_key: object_key.to_owned(),
            expected_family: expected_family.to_owned(),
            reason: format!("the snapshot record filename id is invalid: {error}"),
        })?;
    Ok((namespace_id, snapshot_id))
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub(crate) async fn load_snapshot_record(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<Option<LoadedSnapshotRecord>> {
        let object_key = snapshot_record(&self.namespace_id, snapshot_id);
        let loaded = load_snapshot_record_at_key(&self.store, &object_key).await;
        match loaded {
            Ok(loaded) => Ok(Some(loaded)),
            Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
            Err(error) => Err(CoreError::ControlObjectLoad(error)),
        }
    }

    /// Loads the user snapshot `snapshot_id`. A missing snapshot, or a fork snapshot, returns
    /// `snapshot_not_found`, because the snapshot API reads only user snapshots.
    pub(crate) async fn load_user_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<LoadedSnapshotRecord> {
        self.load_snapshot_record(snapshot_id)
            .await?
            .filter(|loaded| matches!(loaded.state.owner, SnapshotOwner::User { .. }))
            .ok_or_else(|| CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            })
    }

    pub(crate) async fn delete_snapshot_record(&self, snapshot_id: &SnapshotId) -> Result<()> {
        let object_key = snapshot_record(&self.namespace_id, snapshot_id);
        self.store
            .delete(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))
    }

    /// Deletes a snapshot whose creation failed with `error`. A failed delete is
    /// only logged, and collection removes the snapshot later.
    pub(crate) async fn delete_failed_snapshot_record(
        &self,
        snapshot_id: &SnapshotId,
        error: &CoreError,
    ) {
        if let Err(cleanup_error) = self.delete_snapshot_record(snapshot_id).await {
            tracing::warn!(
                namespace_id = %self.namespace_id,
                snapshot_id = %snapshot_id,
                original_error = %error,
                cleanup_error = %cleanup_error,
                "failed to delete a snapshot record after its creation failed"
            );
        }
    }

    pub(crate) async fn verify_snapshot_basis(
        &self,
        record: &SnapshotPayload,
    ) -> Result<SnapshotBasisVerification> {
        let manifest = self.load_current_manifest().await?;
        let kept = record.manifest();
        Ok(
            if manifest.payload().status.is_deleted()
                || manifest.manifest().manifest_no != kept.manifest_no
                || manifest.manifest().payload_checksum != kept.payload_checksum
            {
                SnapshotBasisVerification::Invalid
            } else {
                SnapshotBasisVerification::Verified
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotBasisVerification {
    Verified,
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::keys::hint;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::replace_object;
    use loonfs_types::CommitSeq;
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

    fn snapshot_id(value: &str) -> SnapshotId {
        SnapshotId::parse(value).expect("valid snapshot id")
    }

    fn record(namespace_id: NamespaceId, snapshot_id: SnapshotId) -> SnapshotPayload {
        SnapshotPayload {
            snapshot_id,
            namespace_id: namespace_id.clone(),
            head_seq: CommitSeq(1),
            payload_checksum: loonfs_types::sha256_digest(b"test"),
            created_at_ms: 1,
            owner: SnapshotOwner::User {
                name: Some("test".to_owned()),
                expires_at_ms: None,
            },
        }
    }

    #[tokio::test]
    async fn listed_loader_rejects_a_different_durable_family() {
        let (_directory, store) = local_store();
        let object_key =
            hint(&loonfs_types::NamespaceId::parse("demo").expect("valid namespace id"));

        let error = load_snapshot_record_at_key(&store, &object_key)
            .await
            .expect_err("wrong family should fail");

        assert_matches!(error, ControlObjectLoadError::KeyLayout { .. });
    }

    #[tokio::test]
    async fn listed_loader_rejects_invalid_key_ids() {
        let (_directory, store) = local_store();
        let snapshot_id = "snp_00000000000000000001-00000000000000000000000001";
        let invalid_keys = [
            format!("namespaces/not valid/snapshots/{snapshot_id}.json"),
            "namespaces/demo/snapshots/not-a-snapshot.json".to_owned(),
        ];

        for object_key in invalid_keys {
            let error = load_snapshot_record_at_key(&store, &object_key)
                .await
                .expect_err("invalid key id should fail");
            assert_matches!(error, ControlObjectLoadError::KeyLayout { .. });
        }
    }

    #[tokio::test]
    async fn listed_loader_validates_the_record_against_its_key() {
        enum Mismatch {
            Namespace,
            SnapshotId,
        }

        let (_directory, store) = local_store();
        let key_namespace_id = namespace("demo");
        let key_snapshot_id = snapshot_id("snp_00000000000000000001-00000000000000000000000001");
        let object_key = snapshot_record(&key_namespace_id, &key_snapshot_id);

        let cases = [
            (
                "embedded namespace",
                record(namespace("other"), key_snapshot_id.clone()),
                Mismatch::Namespace,
            ),
            (
                "embedded snapshot id",
                record(
                    key_namespace_id.clone(),
                    snapshot_id("snp_00000000000000000001-00000000000000000000000002"),
                ),
                Mismatch::SnapshotId,
            ),
        ];

        for (label, forged, expected) in cases {
            let bytes = encode_snapshot_record(&forged);
            replace_object(&store, &object_key, bytes)
                .await
                .expect("write record");

            let error = load_snapshot_record_at_key(&store, &object_key)
                .await
                .expect_err("a record that does not describe its key should fail");
            match expected {
                Mismatch::Namespace => assert_matches!(
                    error,
                    ControlObjectLoadError::NamespaceMismatch { .. },
                    "for `{label}`: {error:?}"
                ),
                Mismatch::SnapshotId => assert_matches!(
                    error,
                    ControlObjectLoadError::IdentityMismatch { ref field, .. }
                        if field == "snapshot id",
                    "for `{label}`: {error:?}"
                ),
            }
        }
    }
}
