//! Derives namespace read state from numbered manifests and the WAL tail.

use crate::control_object::{
    expect_namespace, load_control_object, ControlObjectLoadError, LoadedControl,
};
use crate::error::CoreError;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::control::{ControlObjectKind, HintPayload, ManifestRef};
use loonfs_api::CompactorEpoch;
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::hint;
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifest {
    pub envelope: std::sync::Arc<loonfs_api::wire::manifest::NamespaceManifestEnvelope>,
}

impl CurrentManifest {
    pub fn manifest(&self) -> ManifestRef {
        crate::checkpoint::publish::manifest_ref_for(
            &self.envelope.payload().namespace_id,
            &self.envelope,
        )
    }

    pub fn retention_floor_seq(&self) -> loonfs_api::ChangeSeq {
        self.envelope.payload().retention_floor_seq
    }

    pub fn folded_wal_no(&self) -> loonfs_api::WalNo {
        self.envelope.payload().folded_wal_no
    }

    pub fn compactor_epoch(&self) -> CompactorEpoch {
        self.envelope.payload().compactor_epoch
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedManifest {
    pub object_key: String,
    pub(crate) manifest_bytes: u64,
    pub state: CurrentManifest,
}

pub(crate) fn ensure_namespace_live(head: &NamespaceReadState) -> crate::error::Result<()> {
    if head.status.is_deleted() {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: head.namespace_id.clone(),
        });
    }
    Ok(())
}

pub type LoadedHint = LoadedControl<HintPayload>;

pub(crate) async fn load_namespace_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<LoadedHint> {
    load_hint(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)
}

pub(crate) async fn load_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedHint, ControlObjectLoadError> {
    load_control_object(
        store,
        hint(namespace_id),
        ControlObjectKind::Hint,
        |state: &HintPayload| expect_namespace(namespace_id, &state.namespace_id),
    )
    .await
}

/// Raises the hint to at least the given numbers and returns the hint as
/// written. `known` is the hint as the caller last saw it; a raise from a
/// current token needs no read. Each number only ever increases, so a
/// stale actor cannot regress what a newer one wrote.
pub(crate) async fn raise_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_api::ManifestNo,
    wal_no: loonfs_api::WalNo,
    known: Option<LoadedHint>,
) -> crate::error::Result<LoadedHint> {
    let object_key = hint(namespace_id);
    let mut current = match known {
        Some(known) => known,
        None => load_namespace_hint(store, namespace_id).await?,
    };
    loop {
        let raised = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: current.state.manifest_no.max(manifest_no),
            wal_no: current.state.wal_no.max(wal_no),
        };
        if raised.manifest_no == current.state.manifest_no && raised.wal_no == current.state.wal_no
        {
            return Ok(current);
        }
        let bytes =
            loonfs_api::wire::control::encode_control_state(ControlObjectKind::Hint, &raised)
                .map_err(|error| CoreError::Codec {
                    object_key: object_key.clone(),
                    message: error.to_string(),
                })?;
        match store
            .compare_and_swap(&object_key, &current.etag, bytes::Bytes::from(bytes))
            .await
        {
            Ok(metadata) => {
                return Ok(LoadedControl {
                    etag: loonfs_objectstore::required_etag(&object_key, metadata.etag)
                        .map_err(|error| CoreError::store(&object_key, &error))?,
                    object_key,
                    state: raised,
                })
            }
            Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. }) => {
                current = load_namespace_hint(store, namespace_id).await?;
            }
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
    }
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
    Ok(discover_manifest(store, namespace_id)
        .await?
        .map(|(manifest, _)| manifest))
}

pub(crate) async fn load_current_manifest_with_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(LoadedManifest, LoadedHint), ControlObjectLoadError> {
    discover_manifest(store, namespace_id)
        .await?
        .ok_or_else(|| ControlObjectLoadError::MissingObject {
            object_key: loonfs_objectstore::keys::metadata_manifest_object(
                namespace_id,
                &loonfs_api::ManifestNo(1),
            ),
        })
}

async fn discover_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<(LoadedManifest, LoadedHint)>, ControlObjectLoadError> {
    let mut hint = match load_hint(store, namespace_id).await {
        Ok(hint) => hint,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    loop {
        let mut manifest_no = hint.state.manifest_no;
        if manifest_no == loonfs_api::ManifestNo(0) {
            return Err(ControlObjectLoadError::Codec {
                object_key: hint.object_key,
                message: "manifest hint must be at least one".to_owned(),
            });
        }
        let mut current = load_manifest_by_number(store, namespace_id, manifest_no).await?;
        while let Some(previous) = &current {
            // A tombstone is the last manifest of its namespace.
            if previous.state.envelope.payload().status.is_deleted() {
                return Ok(current.map(|manifest| (manifest, hint)));
            }
            let Ok(next) = manifest_no.successor() else {
                break;
            };
            let Some(manifest) = load_manifest_by_number(store, namespace_id, next).await? else {
                break;
            };
            previous
                .state
                .envelope
                .payload()
                .ensure_successor(manifest.state.envelope.payload())
                .map_err(|error| ControlObjectLoadError::Codec {
                    object_key: manifest.object_key.clone(),
                    message: error.to_string(),
                })?;
            current = Some(manifest);
            manifest_no = next;
        }

        // A newer hint lets GC remove an old discovery path. Recheck after a
        // missing object before treating it as either absence or the tip.
        let refreshed = load_hint(store, namespace_id).await?;
        if refreshed.state.manifest_no > manifest_no {
            hint = refreshed;
            continue;
        }
        if current.is_none() && manifest_no != loonfs_api::ManifestNo(1) {
            return Err(ControlObjectLoadError::Codec {
                object_key: hint.object_key,
                message: format!("hinted manifest `{manifest_no}` is missing"),
            });
        }
        return Ok(current.map(|manifest| (manifest, hint)));
    }
}

pub(crate) async fn load_manifest_by_number<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_api::ManifestNo,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    let object_key = loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &manifest_no);
    let envelope = crate::checkpoint::load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        &manifest_no,
    )
    .await
    .map_err(|error| match error {
        crate::checkpoint::ManifestLoadError::ReadManifest {
            object_key,
            message,
            class,
        } => ControlObjectLoadError::Store {
            object_key,
            message,
            class,
        },
        error => ControlObjectLoadError::Codec {
            object_key: object_key.clone(),
            message: error.to_string(),
        },
    })?;
    Ok(envelope.map(|(envelope, manifest_bytes)| LoadedManifest {
        object_key,
        manifest_bytes,
        state: CurrentManifest {
            envelope: std::sync::Arc::new(envelope),
        },
    }))
}

pub async fn load_namespace_read_state<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceReadState, ControlObjectLoadError> {
    Ok(
        crate::namespace::read_anchor::load_read_anchor(store, expected_namespace_id)
            .await?
            .read_state,
    )
}

pub async fn load_namespace_checkpoint_record_control<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::PinId,
) -> Result<Option<loonfs_api::wire::control::PinPayload>, crate::error::CoreError> {
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

/// Raises the discovery start without changing the committed WAL tip.
pub async fn raise_namespace_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: loonfs_api::WalNo,
    known: Option<LoadedHint>,
) -> crate::error::Result<LoadedHint> {
    raise_hint(
        store,
        namespace_id,
        loonfs_api::ManifestNo(1),
        wal_no,
        known,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{ManifestNo, WalNo};
    use loonfs_test_support::stores::{
        KeyPredicate, MetadataMapStore, RecordedOperation, RecordingStore,
    };

    #[tokio::test]
    async fn a_hint_raise_requires_the_returned_etag() {
        let directory = tempfile::tempdir().expect("directory");
        let store =
            loonfs_objectstore::local_fs_store::LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("etag");
        let state = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
            wal_no: WalNo(0),
        };
        let bytes =
            loonfs_api::wire::control::encode_control_state(ControlObjectKind::Hint, &state)
                .expect("hint");
        store
            .put_if_absent(&hint(&namespace_id), bytes.into())
            .await
            .expect("create hint");
        let known = load_hint(&store, &namespace_id).await.expect("hint");
        let store = RecordingStore::new(
            MetadataMapStore::without_etag(store, KeyPredicate::any()),
            KeyPredicate::any(),
        );
        let error = raise_hint(&store, &namespace_id, ManifestNo(2), WalNo(0), Some(known))
            .await
            .expect_err("etag required");
        assert!(matches!(error, CoreError::Store { .. }));
        assert_eq!(
            store
                .take()
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::CompareAndSwap { .. }))
                .count(),
            1
        );
    }
}
