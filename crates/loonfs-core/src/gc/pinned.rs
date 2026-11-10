//! The pins of one namespace, with their records and the manifests that they
//! keep, as both collections read them.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::manifest::load_namespace_manifest_envelope_if_present;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::pin_key_ids;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, pin_prefix};
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_types::format::control::PinPayload;
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::BTreeSet;
use std::sync::Arc;

/// One key that a listing of a namespace's pins returned.
pub(super) enum ListedPinKey {
    /// A key under the pins prefix that isn't a pin record.
    Unrecognized,
    Pin(ListedPin),
}

/// One listed pin, with its record and the manifest that it keeps.
pub(super) struct ListedPin {
    pub(super) key: String,
    /// The pin's record, or `None` when the record is gone.
    pub(super) record: Option<PinPayload>,
    pub(super) manifest: PinnedManifest,
}

/// The manifest that one listed pin keeps.
pub(super) enum PinnedManifest {
    /// The first listed pin of a manifest that no earlier root loaded.
    Loaded(LoadedManifest),
    /// A manifest that the current manifest or an earlier pin keeps.
    Rooted,
    /// A manifest that the store doesn't have.
    Missing(ManifestNo),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Lists the namespace's pins and reads each pin's record and the manifest
    /// that the pin keeps, with up to `STORE_READS_IN_FLIGHT` pins read at
    /// once, in listing order.
    ///
    /// The first listed pin for a manifest number other than
    /// `current_manifest_no` loads that manifest, and later pins for the
    /// number use the result.
    pub(super) fn list_pinned_manifests(
        &self,
        current_manifest_no: ManifestNo,
    ) -> BoxStream<'_, Result<ListedPinKey>> {
        let prefix = pin_prefix(&self.namespace_id);
        let mut requested = BTreeSet::from([current_manifest_no]);
        let mut rooted = BTreeSet::from([current_manifest_no]);
        self.store
            .list_prefix_stream(&prefix)
            .map_err(move |error| CoreError::store(&prefix, &error))
            .map_ok(move |key| {
                let manifest_no = pinned_manifest_no(&key);
                let first = matches!(manifest_no, Ok(Some(number)) if requested.insert(number));
                async move {
                    let Some(manifest_no) = manifest_no? else {
                        return Ok(None);
                    };
                    let manifest = async {
                        if first {
                            self.load_root_manifest(manifest_no).await.map(Some)
                        } else {
                            Ok(None)
                        }
                    };
                    let (loaded, record) =
                        futures::try_join!(manifest, self.load_listed_pin(&key))?;
                    Ok(Some((key, manifest_no, loaded, record)))
                }
            })
            // `try_buffered` keeps listing order, so a pin that reuses an
            // earlier pin's load arrives after that load's result.
            .try_buffered(STORE_READS_IN_FLIGHT)
            .map_ok(move |listed| {
                let Some((key, manifest_no, loaded, record)) = listed else {
                    return ListedPinKey::Unrecognized;
                };
                let manifest = match loaded {
                    Some(Some(manifest)) => {
                        rooted.insert(manifest_no);
                        PinnedManifest::Loaded(manifest)
                    }
                    _ if rooted.contains(&manifest_no) => PinnedManifest::Rooted,
                    _ => PinnedManifest::Missing(manifest_no),
                };
                ListedPinKey::Pin(ListedPin {
                    key,
                    record,
                    manifest,
                })
            })
            .boxed()
    }

    /// Loads one root manifest, or returns `None` when the manifest is missing.
    async fn load_root_manifest(&self, manifest_no: ManifestNo) -> Result<Option<LoadedManifest>> {
        let envelope = load_namespace_manifest_envelope_if_present(
            &self.store,
            &self.namespace_id,
            &manifest_no,
        )
        .await
        .map_err(MetadataProjectionLoadError::from)?;
        Ok(envelope.map(|(envelope, manifest_bytes)| LoadedManifest {
            object_key: metadata_manifest_object(&self.namespace_id, &manifest_no),
            manifest_bytes,
            envelope: Arc::new(envelope),
        }))
    }
}

/// Returns the manifest number in a pin key, or `None` for a key that isn't
/// a pin record.
fn pinned_manifest_no(key: &str) -> Result<Option<ManifestNo>> {
    if !super::families::CandidateFamily::Pins.recognizes(key) {
        return Ok(None);
    }
    let (_, pin_id) = pin_key_ids(key).map_err(CoreError::ControlObjectLoad)?;
    Ok(Some(pin_id.manifest_no()))
}

/// Returns the corruption error of a pin at `root_key` whose manifest
/// `manifest_no` of `namespace_id` is missing.
pub(super) fn missing_root_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    root_key: &str,
) -> CoreError {
    let key = metadata_manifest_object(namespace_id, &manifest_no);
    CoreError::NamespaceCorrupt(format!("root `{root_key}` pins missing manifest `{key}`"))
}
