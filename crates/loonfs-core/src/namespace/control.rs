//! Typed loaders for the namespace's control objects: head, metadata root,
//! and WAL floor.

use crate::control_object::{
    expect_foreign_fork_basis, expect_namespace, expect_own_manifest, load_control_object,
    ControlObjectLoadError, LoadedControl,
};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control_snapshot::load_head_and_metadata_basis;
use loonfs_api::wire::control::{ControlObjectKind, HeadState, MetadataRootState, WalFloorState};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{metadata_root, wal_floor, wal_head};
use loonfs_objectstore::ObjectStore;

pub(crate) type LoadedHeadObject = LoadedControl<HeadState>;
pub(crate) type LoadedMetadataRootObject = LoadedControl<MetadataRootState>;
pub(crate) type LoadedWalFloorObject = LoadedControl<WalFloorState>;

pub(crate) async fn load_wal_floor_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedWalFloorObject, ControlObjectLoadError> {
    let object_key = wal_floor(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::WalFloor,
        |state: &WalFloorState| expect_namespace(expected_namespace_id, &state.namespace_id),
    )
    .await
}

pub(crate) async fn load_metadata_root_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedMetadataRootObject, ControlObjectLoadError> {
    let object_key = metadata_root(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::MetadataRoot,
        |state: &MetadataRootState| {
            expect_namespace(expected_namespace_id, &state.namespace_id)?;
            expect_own_manifest(&state.namespace_id, &state.manifest)
        },
    )
    .await
}

pub(crate) async fn load_metadata_root_object_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<Option<LoadedMetadataRootObject>, ControlObjectLoadError> {
    match load_metadata_root_object(store, expected_namespace_id).await {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) async fn load_head_object<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedHeadObject, ControlObjectLoadError> {
    let object_key = wal_head(expected_namespace_id);
    load_control_object(
        store,
        object_key,
        ControlObjectKind::WalHead,
        |state: &HeadState| {
            expect_namespace(expected_namespace_id, &state.namespace_id)?;
            match &state.fork_basis {
                None => Ok(()),
                Some(fork_basis) => expect_foreign_fork_basis(&state.namespace_id, fork_basis),
            }
        },
    )
    .await
}

pub async fn load_namespace_checkpoint_record_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
) -> Result<Option<loonfs_api::wire::control::CheckpointRecordState>, crate::error::CoreError> {
    Ok(
        crate::checkpoint::load_checkpoint_record(store, expected_namespace_id, checkpoint_id)
            .await?
            .map(|loaded| loaded.state),
    )
}

pub async fn load_namespace_metadata_root_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedControl<MetadataRootState>, ControlObjectLoadError> {
    load_metadata_root_object(store, expected_namespace_id).await
}

/// Loads the head and authorized metadata basis as one consistent read anchor.
pub async fn load_namespace_read_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<(LoadedControl<HeadState>, MetadataBasis), ControlObjectLoadError> {
    let loaded = load_head_and_metadata_basis(store, expected_namespace_id).await?;
    Ok((loaded.head, loaded.basis))
}

pub async fn load_namespace_head_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedControl<HeadState>, ControlObjectLoadError> {
    load_head_object(store, expected_namespace_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_api::wire::control::{
        encode_control_state, ForkBasis, ManifestRef, NamespaceStatus,
    };
    use loonfs_api::{
        ChangeSeq, CheckpointId, ContentStoreId, ManifestNo, ManifestObjectId, WriterEpoch,
    };
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

    fn manifest_ref(owner: &NamespaceId) -> ManifestRef {
        ManifestRef {
            owner_namespace_id: owner.clone(),
            manifest_no: ManifestNo(1),
            manifest_object_id: ManifestObjectId::parse(
                "man_00000000000000000001-0123456789abcdef",
            )
            .expect("valid manifest object id"),
            manifest_head_seq: ChangeSeq(1),
            manifest_payload_checksum: "sha256:test".to_owned(),
        }
    }

    async fn write_control<T: serde::Serialize>(
        store: &LocalFsStore,
        object_key: &str,
        kind: ControlObjectKind,
        state: &T,
    ) {
        let bytes = encode_control_state(kind, state).expect("encode control state");
        store
            .put_overwrite(object_key, Bytes::from(bytes))
            .await
            .expect("write control object");
    }

    #[tokio::test]
    async fn root_loader_rejects_a_manifest_owned_by_another_namespace() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let root = MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest: manifest_ref(&namespace("other")),
            updated_at_ms: 1_000,
        };
        write_control(
            &store,
            &metadata_root(&namespace_id),
            ControlObjectKind::MetadataRoot,
            &root,
        )
        .await;

        let error = load_metadata_root_object(&store, &namespace_id)
            .await
            .expect_err("a foreign manifest owner should fail");

        assert!(matches!(
            error,
            ControlObjectLoadError::IdentityMismatch { field, .. }
                if field == "manifest owner namespace"
        ));
    }

    #[tokio::test]
    async fn head_loader_rejects_a_fork_basis_owned_by_the_namespace_itself() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("demo");
        let mut head = HeadState::initial(
            namespace_id.clone(),
            ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef")
                .expect("valid content store id"),
            1_000,
        );
        head.fork_basis = Some(ForkBasis {
            manifest: manifest_ref(&namespace_id),
            source_checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000001")
                .expect("valid checkpoint id"),
        });
        write_control(
            &store,
            &wal_head(&namespace_id),
            ControlObjectKind::WalHead,
            &head,
        )
        .await;

        let error = load_head_object(&store, &namespace_id)
            .await
            .expect_err("a self-owned fork basis should fail");

        assert_eq!(
            error,
            ControlObjectLoadError::ForkBasisOwnerIsSelf {
                object_key: wal_head(&namespace_id),
                namespace_id,
            }
        );
    }

    #[tokio::test]
    async fn head_loader_accepts_a_fork_basis_owned_by_the_source() {
        let (_directory, store) = local_store();
        let namespace_id = namespace("clone");
        let source_id = namespace("source");
        let mut head = HeadState::initial(
            namespace_id.clone(),
            ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef")
                .expect("valid content store id"),
            1_000,
        );
        head.seq = ChangeSeq(1);
        head.writer_epoch = WriterEpoch(0);
        head.status = NamespaceStatus::Active {};
        head.fork_basis = Some(ForkBasis {
            manifest: manifest_ref(&source_id),
            source_checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000001")
                .expect("valid checkpoint id"),
        });
        write_control(
            &store,
            &wal_head(&namespace_id),
            ControlObjectKind::WalHead,
            &head,
        )
        .await;

        let loaded = load_head_object(&store, &namespace_id)
            .await
            .expect("a fork target head loads");

        assert_eq!(loaded.state, head);
    }
}
