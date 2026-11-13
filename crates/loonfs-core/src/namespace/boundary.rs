//! Loads and raises a namespace's manifest boundary.

use crate::control_object::{
    decode_control_object_body, expect_namespace, load_control_object, present, store_load_error,
    ControlObjectLoadError, LoadedControl,
};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::error::CoreError;
use crate::namespace::store::NamespaceStore;
use bytes::Bytes;
use loonfs_objectstore::keys::boundary;
use loonfs_objectstore::{
    GetIfChangedOutcome, ObjectStore, ObjectStoreError, ObjectStoreExtension,
};
use loonfs_types::format::control::{
    encode_control_state, BoundaryPayload, ControlObjectKind, ManifestBoundary,
};
use loonfs_types::{ManifestNo, NamespaceId};

pub(crate) type LoadedBoundary = LoadedControl<BoundaryPayload>;

/// Returns the manifest number of `boundary`, or 0 when the boundary object
/// is missing.
pub(crate) fn boundary_manifest_no(boundary: Option<&LoadedBoundary>) -> ManifestNo {
    boundary.map_or(ManifestNo(0), |loaded| {
        let ManifestBoundary::Manifest { manifest_no } = loaded.state.boundary;
        manifest_no
    })
}

/// Checks the boundary at `object_key` that names `namespace_id`: the
/// namespace that its payload embeds and its number of at least one.
fn check_boundary(
    object_key: &str,
    namespace_id: &NamespaceId,
    state: &BoundaryPayload,
) -> Result<(), ControlObjectLoadError> {
    expect_namespace(object_key, namespace_id, &state.namespace_id)?;
    let ManifestBoundary::Manifest { manifest_no } = state.boundary;
    if manifest_no == ManifestNo(0) {
        return Err(ControlObjectLoadError::Codec {
            object_key: object_key.to_owned(),
            message: "manifest boundary must be at least one".to_owned(),
        });
    }
    Ok(())
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the namespace's manifest boundary, or returns `None` when the
    /// object is missing, which means that no manifest is at or below the
    /// boundary.
    pub(crate) async fn load_boundary(&self) -> crate::error::Result<Option<LoadedBoundary>> {
        present(
            load_control_object(
                &self.store,
                boundary(&self.namespace_id),
                ControlObjectKind::Boundary,
                |object_key, state| check_boundary(object_key, &self.namespace_id, state),
            )
            .await,
        )
    }

    /// Returns the namespace's manifest boundary as it is now, where `last`
    /// is the boundary as last loaded.
    ///
    /// When `last` is an object, the call reads the object only if its ETag
    /// isn't the one that `last` has, and returns `last` when it is. A
    /// boundary object that `last` found and that's now missing is a corrupt
    /// store, because nothing deletes the object. When `last` is `None`, the
    /// call loads the boundary.
    pub(crate) async fn reload_boundary(
        &self,
        last: Option<LoadedBoundary>,
    ) -> crate::error::Result<Option<LoadedBoundary>> {
        let Some(last) = last else {
            return self.load_boundary().await;
        };
        let object_key = last.object_key.clone();
        let read = self
            .store
            .get_if_changed(&object_key, &last.etag)
            .await
            .map_err(|error| store_load_error(&object_key, &error))?;
        let reloaded = match read {
            Some(GetIfChangedOutcome::Unchanged) => return Ok(Some(last)),
            Some(GetIfChangedOutcome::Changed(body)) => decode_control_object_body(
                object_key,
                body,
                ControlObjectKind::Boundary,
                |object_key, state| check_boundary(object_key, &self.namespace_id, state),
            )?,
            None => {
                return Err(ControlObjectLoadError::Codec {
                    object_key,
                    message: "the manifest boundary is missing after a read found it".to_owned(),
                }
                .into())
            }
        };
        Ok(Some(reloaded))
    }

    /// Raises the namespace's manifest boundary to at least `manifest_no`
    /// (format spec, "One complete collection").
    ///
    /// Each attempt loads the boundary and writes nothing when the stored
    /// number is already that high. Otherwise the attempt creates a missing
    /// object with put-if-absent, or writes `manifest_no` with a
    /// compare-and-swap on the loaded ETag, so no write lowers the number. An
    /// attempt that loses its write, or whose write has an unknown outcome,
    /// leaves the next attempt to load the boundary again.
    pub(crate) async fn raise_boundary(&self, manifest_no: ManifestNo) -> crate::error::Result<()> {
        let object_key = boundary(&self.namespace_id);
        let raised = BoundaryPayload {
            namespace_id: self.namespace_id.clone(),
            boundary: ManifestBoundary::Manifest { manifest_no },
        };
        let encoded = Bytes::from(
            encode_control_state(ControlObjectKind::Boundary, &raised)
                .expect("a manifest boundary should encode"),
        );
        retry_while_contended(|| async {
            let written = match self.load_boundary().await? {
                Some(loaded) => {
                    let ManifestBoundary::Manifest {
                        manifest_no: stored,
                    } = loaded.state.boundary;
                    if stored >= manifest_no {
                        return Ok(CasOutcome::Settled(()));
                    }
                    self.store
                        .compare_and_swap(&object_key, &loaded.etag, encoded.clone())
                        .await
                }
                None => self.store.put_if_absent(&object_key, encoded.clone()).await,
            };
            match written {
                Ok(_) => Ok(CasOutcome::Settled(())),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasOutcome::Contended(
                    CoreError::contention_exhausted(&object_key),
                )),
                // The next attempt loads the boundary again. If this write was
                // applied, that attempt finds the number and writes nothing.
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    Ok(CasOutcome::Contended(CoreError::store(&object_key, &error)))
                }
                Err(error) => Err(CoreError::store(&object_key, &error)),
            }
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::RecordingStore;
    use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector};

    async fn stored<S: ObjectStore>(namespace: &NamespaceStore<S>) -> Option<ManifestNo> {
        namespace
            .load_boundary()
            .await
            .expect("load the boundary")
            .map(|loaded| {
                let ManifestBoundary::Manifest { manifest_no } = loaded.state.boundary;
                manifest_no
            })
    }

    /// Checks that a raise creates a missing boundary, and that a raise to a
    /// lower number writes nothing and leaves the higher number.
    #[tokio::test]
    async fn a_raise_creates_a_missing_boundary_and_never_lowers_it() {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = loonfs_test_support::ids::namespace_id("boundary");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        assert_eq!(stored(&namespace).await, None);

        namespace
            .raise_boundary(ManifestNo(5))
            .await
            .expect("raise to 5");
        let counts = store.counts();
        assert_eq!(
            (counts.create_if_absent_puts, counts.compare_and_swaps),
            (1, 0)
        );
        store.reset();
        namespace
            .raise_boundary(ManifestNo(3))
            .await
            .expect("raise to 3");
        assert_eq!(store.counts().puts, 0);
        assert_eq!(stored(&namespace).await, Some(ManifestNo(5)));
    }

    /// Checks that a raise whose compare-and-swap loses to a higher raise
    /// loads the boundary again and ends without a second write.
    #[tokio::test]
    async fn a_raise_that_loses_to_a_higher_raise_writes_once() {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = loonfs_test_support::ids::namespace_id("boundary-race");
        let blocked = std::sync::Arc::new(BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::exact(boundary(&namespace_id)),
            OperationSelector::CompareAndSwap,
        ));
        crate::test_support::namespace_store(blocked.inner(), &namespace_id)
            .raise_boundary(ManifestNo(2))
            .await
            .expect("create the boundary");
        let store = RecordingStore::new(std::sync::Arc::clone(&blocked), KeyPredicate::any());
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        blocked.block_next();
        let (raised, ()) = tokio::join!(namespace.raise_boundary(ManifestNo(5)), async {
            blocked.wait_until_blocked().await;
            crate::test_support::namespace_store(blocked.inner(), &namespace_id)
                .raise_boundary(ManifestNo(7))
                .await
                .expect("the higher raise");
            blocked.release();
        },);
        raised.expect("the raise ends once the boundary is that high");
        let counts = store.counts();
        assert_eq!(
            (counts.gets_with_metadata, counts.compare_and_swaps),
            (2, 1)
        );
        assert_eq!(
            stored(&crate::test_support::namespace_store(
                blocked.inner(),
                &namespace_id
            ))
            .await,
            Some(ManifestNo(7))
        );
    }
}
