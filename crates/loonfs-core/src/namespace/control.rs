//! Derives namespace read state from numbered manifests and the WAL tail.

use crate::control_object::{
    expect_namespace, load_control_object, ControlObjectLoadError, LoadedControl,
};
use crate::error::CoreError;
use crate::limits::MANIFEST_PROBE_WINDOW;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::hint;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ControlObjectKind, HintPayload, ManifestRef};
use loonfs_types::{CompactorEpoch, ManifestNo, NamespaceId};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentManifest {
    pub envelope: std::sync::Arc<loonfs_types::format::manifest::NamespaceManifestEnvelope>,
}

impl CurrentManifest {
    pub fn manifest(&self) -> ManifestRef {
        crate::manifest::publish::manifest_ref_for(&self.envelope)
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

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns `namespace_exists`, or `namespace_deleted` for a tombstone, when
    /// the namespace has a current manifest.
    pub(crate) async fn ensure_absent(&self) -> crate::error::Result<()> {
        let Some(current) = self.load_current_manifest_if_present().await? else {
            return Ok(());
        };
        Err(if current.state.envelope.payload().status.is_deleted() {
            CoreError::NamespaceDeleted {
                namespace_id: self.namespace_id.clone(),
            }
        } else {
            CoreError::NamespaceExists {
                namespace_id: self.namespace_id.clone(),
            }
        })
    }

    pub(crate) async fn load_hint(&self) -> Result<LoadedHint, ControlObjectLoadError> {
        load_control_object(
            &self.store,
            hint(&self.namespace_id),
            ControlObjectKind::Hint,
            |state: &HintPayload| expect_namespace(&self.namespace_id, &state.namespace_id),
        )
        .await
    }

    /// Raises the namespace's hint to at least `manifest_no` with one
    /// compare-and-swap, and returns the hint as written, or as loaded when its
    /// number was already that high.
    ///
    /// The runtime that published a manifest calls this after the publication
    /// returns (format spec, "The current manifest and WAL tip"). `known` is the
    /// hint as the caller last saw it, so a raise from a current hint needs no
    /// read, and with `None` the call loads the hint first. A compare-and-swap
    /// that loses to another raise returns `store_contention` without another
    /// attempt, because some providers accept about one write per second to one
    /// object. The number never decreases, so a raise from an old hint can't
    /// lower the number that a newer raise wrote.
    pub async fn raise_hint(
        &self,
        manifest_no: ManifestNo,
        known: Option<LoadedHint>,
    ) -> crate::error::Result<LoadedHint> {
        let object_key = hint(&self.namespace_id);
        let current = match known {
            Some(known) => known,
            None => self
                .load_hint()
                .await
                .map_err(CoreError::ControlObjectLoad)?,
        };
        if current.state.manifest_no >= manifest_no {
            return Ok(current);
        }
        let raised = HintPayload {
            namespace_id: self.namespace_id.clone(),
            manifest_no,
        };
        let bytes =
            loonfs_types::format::control::encode_control_state(ControlObjectKind::Hint, &raised)
                .expect("a namespace hint should encode");
        match self
            .store
            .compare_and_swap(&object_key, &current.etag, bytes::Bytes::from(bytes))
            .await
        {
            Ok(metadata) => Ok(LoadedControl {
                etag: loonfs_objectstore::required_etag(&object_key, metadata.etag)
                    .map_err(|error| CoreError::store(&object_key, &error))?,
                object_key,
                state: raised,
            }),
            Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. }) => {
                Err(CoreError::contention_exhausted(&object_key))
            }
            Err(error) => Err(CoreError::store(&object_key, &error)),
        }
    }

    /// Loads the namespace's current manifest through the hint.
    pub async fn load_current_manifest(&self) -> Result<LoadedManifest, ControlObjectLoadError> {
        self.load_current_manifest_if_present()
            .await?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: loonfs_objectstore::keys::metadata_manifest_object(
                    &self.namespace_id,
                    &loonfs_types::ManifestNo(1),
                ),
            })
    }

    pub(crate) async fn load_current_manifest_if_present(
        &self,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        Ok(self
            .discover_manifest()
            .await?
            .map(|(manifest, _)| manifest))
    }

    pub(crate) async fn load_current_manifest_with_hint(
        &self,
    ) -> Result<(LoadedManifest, LoadedHint), ControlObjectLoadError> {
        self.discover_manifest()
            .await?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: loonfs_objectstore::keys::metadata_manifest_object(
                    &self.namespace_id,
                    &loonfs_types::ManifestNo(1),
                ),
            })
    }

    /// Loads the current manifest through the hint (format spec, "Resolving the
    /// metadata basis"), with the hint as last read, or returns `None` when the
    /// namespace has no hint.
    ///
    /// Discovery loads the hinted manifest, finds the newest manifest above it
    /// with [`find_newest_manifest_above`], and reads the hint again before it
    /// returns, unless it found a tombstone.
    async fn discover_manifest(
        &self,
    ) -> Result<Option<(LoadedManifest, LoadedHint)>, ControlObjectLoadError> {
        let mut hint = match self.load_hint().await {
            Ok(hint) => hint,
            Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let is_deleted =
            |manifest: &LoadedManifest| manifest.state.envelope.payload().status.is_deleted();
        loop {
            let mut manifest_no = hint.state.manifest_no;
            if manifest_no == loonfs_types::ManifestNo(0) {
                return Err(ControlObjectLoadError::Codec {
                    object_key: hint.object_key,
                    message: "manifest hint must be at least one".to_owned(),
                });
            }
            let mut current = self.load_manifest_by_no(manifest_no).await?;
            if let Some(loaded) = current.as_ref().filter(|loaded| !is_deleted(loaded)) {
                if let Some(newest) = self.find_newest_manifest_above(loaded).await? {
                    manifest_no = newest.state.envelope.payload().manifest_no;
                    current = Some(newest);
                }
            }
            // A tombstone is the last manifest of its namespace.
            if current.as_ref().is_some_and(is_deleted) {
                return Ok(current.map(|manifest| (manifest, hint)));
            }

            // A newer hint lets GC remove an old discovery path. Recheck after a
            // missing object before treating it as either absence or the tip.
            let refreshed = self.load_hint().await?;
            let newer = refreshed.state.manifest_no > manifest_no;
            hint = refreshed;
            if newer {
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

    /// Returns the newest manifest above `previous`, checked against `previous`
    /// (format spec, "Resolving the metadata basis"), or `None` when no manifest
    /// above `previous` exists.
    ///
    /// The probe sends one HEAD request for the number after `previous`, and a
    /// missing number ends it there. Otherwise it sends a HEAD request for each
    /// of the next [`MANIFEST_PROBE_WINDOW`] numbers at once and takes the
    /// highest number that exists, and when that number is the last of its
    /// window, it probes the next window above it. The call then loads the
    /// manifest at the highest number found with one GET, and checks it against
    /// `previous` without the manifests in between. The call returns `None` too
    /// if that manifest is gone by the GET, which garbage collection does only
    /// after the hint has moved past it.
    pub(crate) async fn find_newest_manifest_above(
        &self,
        previous: &LoadedManifest,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        let Ok(next) = previous.state.envelope.payload().manifest_no.successor() else {
            return Ok(None);
        };
        if !self.manifest_exists(next).await? {
            return Ok(None);
        }
        let mut newest = next;
        // Each window that continues the probe moves it up by a whole window, so
        // the loop runs at most once for each window of manifests above
        // `previous`, plus once.
        loop {
            let window: Vec<_> = (1..)
                .take(MANIFEST_PROBE_WINDOW)
                .map_while(|offset| newest.0.checked_add(offset).map(ManifestNo))
                .collect();
            let Some(&last) = window.last() else {
                break;
            };
            let found: Vec<_> = stream::iter(window)
                .map(|manifest_no| async move {
                    Ok::<_, ControlObjectLoadError>(
                        self.manifest_exists(manifest_no)
                            .await?
                            .then_some(manifest_no),
                    )
                })
                .buffered(MANIFEST_PROBE_WINDOW)
                .try_collect()
                .await?;
            let Some(highest) = found.into_iter().flatten().max() else {
                break;
            };
            newest = highest;
            if highest != last {
                break;
            }
        }
        let Some(manifest) = self.load_manifest_by_no(newest).await? else {
            return Ok(None);
        };
        previous
            .state
            .envelope
            .payload()
            .ensure_later(manifest.state.envelope.payload())
            .map_err(|error| ControlObjectLoadError::Codec {
                object_key: manifest.object_key.clone(),
                message: error.to_string(),
            })?;
        Ok(Some(manifest))
    }

    /// Returns whether the manifest numbered `manifest_no` exists, with one HEAD
    /// request.
    pub(crate) async fn manifest_exists(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<bool, ControlObjectLoadError> {
        let object_key =
            loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &manifest_no);
        Ok(self
            .store
            .head(&object_key)
            .await
            .map_err(|error| ControlObjectLoadError::Store {
                object_key: object_key.clone(),
                message: error.public_message().into_owned(),
                kind: error.kind(),
            })?
            .is_some())
    }

    pub(crate) async fn load_manifest_by_no(
        &self,
        manifest_no: loonfs_types::ManifestNo,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        let object_key =
            loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &manifest_no);
        let envelope = crate::manifest::load_namespace_manifest_envelope_if_present(
            &self.store,
            &self.namespace_id,
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

    pub async fn load_read_state(&self) -> Result<NamespaceReadState, ControlObjectLoadError> {
        let observed = crate::time::Observation::now(Arc::clone(&self.timer));
        Ok(self.load_read_anchor(observed).await?.read_state)
    }
}

pub type LoadedHint = LoadedControl<HintPayload>;

const _: () = assert!(
    0 < MANIFEST_PROBE_WINDOW && MANIFEST_PROBE_WINDOW <= crate::in_flight::STORE_READS_IN_FLIGHT
);

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationSelector,
        RecordedOperation, RecordingStore,
    };
    use std::assert_matches;

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
        crate::test_support::namespace_store(store, namespace_id)
            .load_hint()
            .await
            .expect("hint")
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
        let error = crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint(ManifestNo(2), Some(known))
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

    /// Checks that a raise whose compare-and-swap loses returns
    /// `store_contention` without loading the hint or writing it again.
    #[tokio::test]
    async fn a_hint_raise_that_loses_its_write_makes_no_second_attempt() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("contended");
        let known = seed_hint(&store, &namespace_id).await;
        let store = RecordingStore::new(
            FailStore::new(
                store,
                KeyPredicate::hint(&namespace_id),
                OperationSelector::CompareAndSwap,
                InjectedError::PreconditionFailed,
            ),
            KeyPredicate::any(),
        );
        store.inner().fail_all();
        let error = crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint(ManifestNo(2), Some(known))
            .await
            .expect_err("a lost write fails the raise");
        assert_matches!(error, CoreError::StoreContention { .. });
        let counts = store.counts();
        assert_eq!(
            (counts.gets_with_metadata, counts.compare_and_swaps),
            (0, 1)
        );
    }

    #[tokio::test]
    async fn a_hint_raise_loads_an_unknown_hint_and_writes_only_a_higher_number() {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("unknown");
        seed_hint(&store, &namespace_id).await;
        let store = RecordingStore::new(store, KeyPredicate::any());
        let raised = crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint(ManifestNo(3), None)
            .await
            .expect("raise");
        assert_eq!(raised.state.manifest_no, ManifestNo(3));
        let counts = store.counts();
        assert_eq!(
            (counts.gets_with_metadata, counts.compare_and_swaps),
            (1, 1)
        );
        store.reset();
        let kept = crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint(ManifestNo(2), Some(raised.clone()))
            .await
            .expect("a raise to a lower number");
        assert_eq!(kept, raised);
        assert!(store.take().is_empty());
        assert_eq!(
            crate::test_support::namespace_store(&store, &namespace_id)
                .load_hint()
                .await
                .expect("hint"),
            raised
        );
    }
}
