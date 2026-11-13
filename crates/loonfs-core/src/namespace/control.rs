//! Derives namespace read state from numbered manifests and the WAL tail.

use crate::control_object::ControlObjectLoadError;
use crate::error::CoreError;
use crate::limits::{MANIFEST_PROBE_WINDOW, MAX_CONTENTION_ATTEMPTS};
use crate::namespace::basis::PublicationBasis;
use crate::namespace::boundary::boundary_manifest_no;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
use loonfs_types::format::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
use loonfs_types::{CompactorEpoch, ManifestNo, NamespaceId};
use std::sync::Arc;

/// A namespace manifest loaded from the store, with its object key and stored
/// length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedManifest {
    pub object_key: String,
    /// Stored length of the manifest object, which is the compressed document.
    pub(crate) manifest_bytes: u64,
    pub(crate) envelope: Arc<NamespaceManifestEnvelope>,
}

impl LoadedManifest {
    /// Returns the manifest's decoded envelope.
    pub fn envelope(&self) -> &Arc<NamespaceManifestEnvelope> {
        &self.envelope
    }

    /// Returns the manifest's payload.
    pub fn payload(&self) -> &NamespaceManifestPayload {
        self.envelope.payload()
    }

    /// Returns the reference that identifies this manifest.
    pub fn manifest(&self) -> ManifestRef {
        crate::manifest::publish::manifest_ref_for(&self.envelope)
    }

    pub fn retention_floor_seq(&self) -> loonfs_types::CommitSeq {
        self.payload().retention_floor_seq
    }

    pub fn folded_wal_no(&self) -> loonfs_types::WalNo {
        self.payload().folded_wal_no
    }

    pub fn compactor_epoch(&self) -> CompactorEpoch {
        self.payload().compactor_epoch
    }
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

pub(crate) fn ensure_namespace_live(
    namespace_id: &NamespaceId,
    status: NamespaceStatus,
) -> crate::error::Result<()> {
    if status.is_deleted() {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }
    Ok(())
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the namespace's current manifest from the manifest boundary.
    pub async fn load_current_manifest(&self) -> crate::error::Result<LoadedManifest> {
        Ok(self.load_current_basis().await?.manifest)
    }

    /// Loads the namespace's current manifest from the manifest boundary,
    /// with the boundary of the last read.
    pub async fn load_current_basis(&self) -> crate::error::Result<PublicationBasis> {
        self.find_newest_manifest()
            .await?
            .ok_or_else(|| self.namespace_not_found())
    }

    pub(crate) async fn load_current_manifest_if_present(
        &self,
    ) -> crate::error::Result<Option<LoadedManifest>> {
        Ok(self
            .find_newest_manifest()
            .await?
            .map(|current| current.manifest))
    }

    /// Returns the error for a namespace that has no manifest above its
    /// manifest boundary and no boundary object.
    pub(crate) fn namespace_not_found(&self) -> CoreError {
        CoreError::NamespaceNotFound {
            namespace_id: self.namespace_id.clone(),
        }
    }

    /// Finds the namespace's newest manifest from the manifest boundary
    /// (format spec, "Resolving the metadata basis"), or returns `None` when
    /// no namespace has the ID.
    ///
    /// Each attempt reads the boundary, lists the manifests numbered above
    /// it, loads the highest-numbered manifest in the listing, and reads the
    /// boundary again. That manifest is the newest when it's still above the
    /// boundary. The attempt starts again when the manifest is gone by its
    /// load or is at or below the boundary by the second read, and when the
    /// listing is empty and the boundary changed. An empty listing under an
    /// unchanged missing boundary means that no namespace has the ID, and
    /// under an unchanged boundary object it means a corrupt store, because
    /// a collection never leaves a namespace without a manifest above its
    /// boundary. The call returns the manifest with the boundary of its last
    /// read, and returns `store_contention` after [`MAX_CONTENTION_ATTEMPTS`]
    /// attempts.
    pub(crate) async fn find_newest_manifest(
        &self,
    ) -> crate::error::Result<Option<PublicationBasis>> {
        let prefix = metadata_manifest_prefix(&self.namespace_id);
        for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
            let boundary = self.load_boundary().await?;
            let start_after = boundary.as_ref().map(|loaded| {
                metadata_manifest_object(&self.namespace_id, &boundary_manifest_no(Some(loaded)))
            });
            let highest = self
                .store
                .list_entries_from_stream(&prefix, start_after.as_deref())
                .map_err(|error| CoreError::store(&prefix, &error))
                .try_fold(None, |highest, entry| async move {
                    Ok::<_, CoreError>(manifest_no_of(&entry.key).max(highest))
                })
                .await?;
            let Some(manifest_no) = highest else {
                if self.reload_boundary(boundary.clone()).await? != boundary {
                    continue;
                }
                let Some(boundary) = boundary else {
                    return Ok(None);
                };
                return Err(ControlObjectLoadError::Codec {
                    message: format!(
                        "no manifest is above the manifest boundary `{}`",
                        boundary_manifest_no(Some(&boundary))
                    ),
                    object_key: boundary.object_key,
                }
                .into());
            };
            let Some(manifest) = self.load_manifest_by_no(manifest_no).await? else {
                continue;
            };
            let reloaded = self.reload_boundary(boundary).await?;
            if manifest_no > boundary_manifest_no(reloaded.as_ref()) {
                return Ok(Some(PublicationBasis {
                    manifest,
                    boundary: reloaded,
                }));
            }
        }
        Err(CoreError::contention_exhausted(
            &loonfs_objectstore::keys::boundary(&self.namespace_id),
        ))
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
    /// if that manifest is gone by the GET.
    pub(crate) async fn find_newest_manifest_above(
        &self,
        previous: &LoadedManifest,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        let Ok(next) = previous.payload().manifest_no.successor() else {
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
            .payload()
            .ensure_later(manifest.payload())
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
            envelope: Arc::new(envelope),
        }))
    }

    pub async fn load_read_state(&self) -> crate::error::Result<NamespaceReadState> {
        let observed = crate::time::Observation::now(Arc::clone(&self.timer));
        Ok(self.load_read_anchor(observed).await?.read_state)
    }
}

const _: () = assert!(
    0 < MANIFEST_PROBE_WINDOW && MANIFEST_PROBE_WINDOW <= crate::in_flight::STORE_READS_IN_FLIGHT
);
