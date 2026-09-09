//! Loads namespace heads and discovers numbered manifests through their hints.

use crate::control_object::{
    expect_foreign_fork_basis, expect_namespace, load_control_object, ControlObjectLoadError,
    LoadedControl,
};
use crate::error::CoreError;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control_snapshot::load_head_and_metadata_basis;
use loonfs_api::wire::control::{ControlObjectKind, HeadState, HintState, ManifestRef};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{hint, wal_head};
use loonfs_objectstore::ObjectStore;

pub(crate) type LoadedHeadObject = LoadedControl<HeadState>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifest {
    pub manifest: ManifestRef,
    pub retention_floor_seq: loonfs_api::ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedManifest {
    pub object_key: String,
    pub discovery_start_manifest_no: loonfs_api::ManifestNo,
    pub state: CurrentManifest,
    pub envelope: loonfs_api::wire::manifest::NamespaceManifestEnvelope,
}

pub(crate) fn ensure_namespace_live(head: &HeadState) -> crate::error::Result<()> {
    if head.status.is_deleted() {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: head.namespace_id.clone(),
        });
    }
    Ok(())
}

pub(crate) async fn update_manifest_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_api::ManifestNo,
) -> crate::error::Result<()> {
    let object_key = hint(namespace_id);
    let bytes = loonfs_api::wire::control::encode_control_state(
        ControlObjectKind::Hint,
        &HintState {
            namespace_id: namespace_id.clone(),
            manifest_no,
        },
    )
    .map_err(|error| CoreError::Codec {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    store
        .put_overwrite(&object_key, bytes::Bytes::from(bytes))
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?;
    Ok(())
}

pub(crate) async fn load_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedManifest, ControlObjectLoadError> {
    load_current_manifest_if_present(store, namespace_id)
        .await?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: loonfs_objectstore::keys::metadata_manifest_object(
                namespace_id,
                &loonfs_api::ManifestNo(1),
            ),
        })
}

pub(crate) async fn load_current_manifest_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    let object_key = hint(namespace_id);
    let hint = load_control_object(
        store,
        object_key.clone(),
        ControlObjectKind::Hint,
        |state: &HintState| expect_namespace(namespace_id, &state.namespace_id),
    )
    .await
    .map_err(|error| match error {
        ControlObjectLoadError::MissingObject { .. } => ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: "namespace hint is missing".to_owned(),
        },
        error => error,
    })?;
    let mut manifest_no = hint.state.manifest_no;
    let mut current = if manifest_no == loonfs_api::ManifestNo(0) {
        None
    } else {
        Some(
            load_discovered_manifest(store, namespace_id, manifest_no)
                .await?
                .ok_or_else(|| ControlObjectLoadError::Codec {
                    object_key,
                    message: format!("hinted manifest `{manifest_no}` is missing"),
                })?,
        )
    };
    while let Ok(next) = manifest_no.successor() {
        let Some(manifest) = load_discovered_manifest(store, namespace_id, next).await? else {
            break;
        };
        if current.as_ref().is_some_and(|previous| {
            previous.state.manifest.manifest_head_seq > manifest.state.manifest.manifest_head_seq
                || previous.state.retention_floor_seq > manifest.state.retention_floor_seq
        }) {
            return Err(ControlObjectLoadError::Codec {
                object_key: manifest.object_key,
                message: "manifest lowers its predecessor's head sequence or retention floor"
                    .to_owned(),
            });
        }
        current = Some(manifest);
        manifest_no = next;
    }
    if let Some(current) = &mut current {
        current.discovery_start_manifest_no = hint.state.manifest_no;
    }
    Ok(current)
}

async fn load_discovered_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_api::ManifestNo,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    let object_key = loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &manifest_no);
    let bytes =
        store
            .get(&object_key, None)
            .await
            .map_err(|error| ControlObjectLoadError::Store {
                object_key: object_key.clone(),
                message: error.public_message().into_owned(),
                class: crate::error::StoreFailureClass::of(&error),
            })?;
    let envelope = bytes
        .map(|bytes| {
            crate::checkpoint::decode_manifest_at(namespace_id, manifest_no, &object_key, &bytes)
        })
        .transpose()
        .map_err(|error| ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: error.to_string(),
        })?;
    Ok(envelope.map(|envelope| LoadedManifest {
        object_key,
        discovery_start_manifest_no: manifest_no,
        state: CurrentManifest {
            manifest: ManifestRef {
                owner_namespace_id: namespace_id.clone(),
                manifest_no,
                manifest_head_seq: envelope.payload().head_seq,
                manifest_payload_checksum: envelope.payload_checksum().to_owned(),
            },
            retention_floor_seq: envelope.payload().retention_floor_seq,
        },
        envelope,
    }))
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

pub async fn load_namespace_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedManifest, ControlObjectLoadError> {
    load_current_manifest(store, expected_namespace_id).await
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
    use loonfs_api::{ChangeSeq, CheckpointId, ContentStoreId, ManifestNo, WriterEpoch};
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
