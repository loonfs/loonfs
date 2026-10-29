//! Derives namespace read state from numbered manifests and the WAL tail.

use crate::control_object::{
    expect_namespace, load_control_object, ControlObjectLoadError, LoadedControl,
};
use crate::error::CoreError;
use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::keys::hint;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ControlObjectKind, HintPayload, ManifestRef};
use loonfs_types::CompactorEpoch;
use loonfs_types::NamespaceId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifest {
    pub envelope: std::sync::Arc<loonfs_types::format::manifest::NamespaceManifestEnvelope>,
}

impl CurrentManifest {
    pub fn manifest(&self) -> ManifestRef {
        crate::manifest::publish::manifest_ref_for(
            &self.envelope.payload().namespace_id,
            &self.envelope,
        )
    }

    pub fn retention_floor_seq(&self) -> loonfs_types::ChangeSeq {
        self.envelope.payload().retention_floor_seq
    }

    pub fn acknowledged_floor_seq(&self) -> loonfs_types::ChangeSeq {
        self.envelope.payload().acknowledged_floor_seq
    }

    pub fn folded_wal_no(&self) -> loonfs_types::WalNo {
        self.envelope.payload().folded_wal_no
    }

    pub fn compactor_epoch(&self) -> CompactorEpoch {
        self.envelope.payload().compactor_epoch
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedManifest {
    pub object_key: String,
    /// Stored length of the manifest object, which is the compressed document.
    pub(crate) manifest_bytes: u64,
    pub state: CurrentManifest,
}

pub(crate) fn ensure_head_identity(
    head: &NamespaceReadState,
    namespace_id: &NamespaceId,
) -> crate::error::Result<()> {
    if &head.namespace_id != namespace_id {
        return Err(CoreError::NamespaceCorrupt(format!(
            "head namespace `{}` does not match requested namespace `{}`",
            head.namespace_id, namespace_id
        )));
    }
    Ok(())
}

pub(crate) fn ensure_namespace_live(head: &NamespaceReadState) -> crate::error::Result<()> {
    if head.status.is_deleted() {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: head.namespace_id.clone(),
        });
    }
    Ok(())
}

/// Returns `namespace_exists`, or `namespace_deleted` for a tombstone, when
/// the namespace has a current manifest.
pub(crate) async fn ensure_namespace_absent<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<()> {
    let Some(current) = load_current_manifest_if_present(store, namespace_id).await? else {
        return Ok(());
    };
    Err(if current.state.envelope.payload().status.is_deleted() {
        CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        }
    } else {
        CoreError::NamespaceExists {
            namespace_id: namespace_id.clone(),
        }
    })
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

/// Raises the namespace's hint to at least `manifest_no`, and returns the hint
/// as written, or as loaded when its number was already that high.
///
/// The runtime that published a manifest calls this after the publication
/// returns (format spec, "The current manifest and WAL tip"). `known` is the
/// hint as the caller last saw it, so a raise from a current hint needs no
/// read, and with `None` the call loads the hint first. After a lost
/// compare-and-swap, the call loads the hint and tries again. Once a
/// publication budget timed from the start of the call runs out, the call
/// returns the hint as last loaded. The number never decreases, so a raise
/// from an old hint can't lower the number that a newer raise wrote.
pub async fn raise_namespace_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_types::ManifestNo,
    known: Option<LoadedHint>,
    timer: std::sync::Arc<dyn MonotonicTimer>,
) -> crate::error::Result<LoadedHint> {
    let deadline = Deadline::start(timer);
    let object_key = hint(namespace_id);
    let mut current = match known {
        Some(known) => known,
        None => load_namespace_hint(store, namespace_id).await?,
    };
    while deadline.elapsed_ms() <= METADATA_PUBLICATION_BUDGET_MS {
        let raised = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: current.state.manifest_no.max(manifest_no),
        };
        if raised.manifest_no == current.state.manifest_no {
            break;
        }
        let bytes =
            loonfs_types::format::control::encode_control_state(ControlObjectKind::Hint, &raised)
                .expect("a namespace hint should encode");
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
    Ok(current)
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
                &loonfs_types::ManifestNo(1),
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
                &loonfs_types::ManifestNo(1),
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
        if manifest_no == loonfs_types::ManifestNo(0) {
            return Err(ControlObjectLoadError::Codec {
                object_key: hint.object_key,
                message: "manifest hint must be at least one".to_owned(),
            });
        }
        let mut current = load_manifest_by_no(store, namespace_id, manifest_no).await?;
        while let Some(previous) = &current {
            // A tombstone is the last manifest of its namespace.
            if previous.state.envelope.payload().status.is_deleted() {
                return Ok(current.map(|manifest| (manifest, hint)));
            }
            let Ok(next) = manifest_no.successor() else {
                break;
            };
            let Some(manifest) = load_manifest_by_no(store, namespace_id, next).await? else {
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
        if current.is_none() && manifest_no != loonfs_types::ManifestNo(1) {
            return Err(ControlObjectLoadError::Codec {
                object_key: hint.object_key,
                message: format!("hinted manifest `{manifest_no}` is missing"),
            });
        }
        return Ok(current.map(|manifest| (manifest, hint)));
    }
}

pub(crate) async fn load_manifest_by_no<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: loonfs_types::ManifestNo,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    let object_key = loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &manifest_no);
    let envelope = crate::manifest::load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        &manifest_no,
    )
    .await
    .map_err(|error| match error {
        crate::manifest::ManifestLoadError::ReadManifest {
            object_key,
            message,
            kind,
        } => ControlObjectLoadError::Store {
            object_key,
            message,
            kind,
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
    timer: std::sync::Arc<dyn crate::time::MonotonicTimer>,
) -> Result<NamespaceReadState, ControlObjectLoadError> {
    let observed = crate::time::Observation::now(timer);
    Ok(
        crate::namespace::read_anchor::load_read_anchor(store, expected_namespace_id, observed)
            .await?
            .read_state,
    )
}

pub async fn load_namespace_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedManifest, ControlObjectLoadError> {
    load_current_manifest(store, expected_namespace_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::StdMonotonicTimer;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationSelector,
        RecordedOperation, RecordingStore,
    };
    use loonfs_types::ManifestNo;
    use std::assert_matches;
    use std::sync::Arc;

    async fn seed_hint(store: &LocalFsStore, namespace_id: &NamespaceId) -> LoadedHint {
        let state = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
        };
        let bytes =
            loonfs_types::format::control::encode_control_state(ControlObjectKind::Hint, &state)
                .expect("hint");
        store
            .put_if_absent(&hint(namespace_id), bytes.into())
            .await
            .expect("create hint");
        load_hint(store, namespace_id).await.expect("hint")
    }

    #[tokio::test]
    async fn a_hint_raise_requires_the_returned_etag() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("etag");
        let known = seed_hint(&store, &namespace_id).await;
        let store = RecordingStore::new(
            MetadataMapStore::without_etag(store, KeyPredicate::any()),
            KeyPredicate::any(),
        );
        let error = raise_namespace_hint(
            &store,
            &namespace_id,
            ManifestNo(2),
            Some(known),
            Arc::new(StdMonotonicTimer::default()),
        )
        .await
        .expect_err("etag required");
        assert_matches!(error, CoreError::Store { .. });
        assert_eq!(
            store
                .take()
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::CompareAndSwap { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_hint_raise_that_keeps_losing_ends_with_its_budget_and_returns_the_hint_it_saw() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("contended");
        let known = seed_hint(&store, &namespace_id).await;
        let clock = Arc::new(ManualClock::new(0));
        let reload = Arc::clone(&clock);
        let store = FailStore::new(
            MetadataMapStore::new(store, KeyPredicate::hint(&namespace_id), move |metadata| {
                reload.advance_ms(METADATA_PUBLICATION_BUDGET_MS / 2 + 1);
                metadata
            }),
            KeyPredicate::hint(&namespace_id),
            OperationSelector::CompareAndSwap,
            InjectedError::PreconditionFailed,
        );
        store.fail_all();
        let raised = raise_namespace_hint(
            &store,
            &namespace_id,
            ManifestNo(2),
            Some(known.clone()),
            clock,
        )
        .await
        .expect("a raise past its budget returns the hint it saw");
        assert_eq!(raised, known);
        assert_eq!(store.attempts(), 2);
    }

    #[tokio::test]
    async fn a_hint_raise_loads_an_unknown_hint_and_writes_only_a_higher_number() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("unknown");
        seed_hint(&store, &namespace_id).await;
        let store = RecordingStore::new(store, KeyPredicate::any());
        let timer = || Arc::new(StdMonotonicTimer::default());
        let raised = raise_namespace_hint(&store, &namespace_id, ManifestNo(3), None, timer())
            .await
            .expect("raise");
        assert_eq!(raised.state.manifest_no, ManifestNo(3));
        let counts = store.counts();
        assert_eq!(
            (counts.gets_with_metadata, counts.compare_and_swaps),
            (1, 1)
        );
        store.reset();
        let kept = raise_namespace_hint(
            &store,
            &namespace_id,
            ManifestNo(2),
            Some(raised.clone()),
            timer(),
        )
        .await
        .expect("a raise to a lower number");
        assert_eq!(kept, raised);
        assert!(store.take().is_empty());
        assert_eq!(
            load_hint(&store, &namespace_id).await.expect("hint"),
            raised
        );
    }
}
