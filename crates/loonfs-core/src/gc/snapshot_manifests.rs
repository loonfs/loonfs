//! The snapshots of one namespace, with their records and the manifests that they
//! keep, as both collections read them.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::manifest::load_namespace_manifest_envelope_if_present;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::snapshot_record_key_ids;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, snapshot_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::control::SnapshotPayload;
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::BTreeSet;
use std::sync::Arc;

/// One key that a listing of a namespace's snapshots returned.
pub(super) enum ListedSnapshotKey {
    /// A key under the snapshots prefix that isn't a snapshot record, with the
    /// object's size as the listing reported it.
    Unrecognized {
        size_bytes: u64,
    },
    Snapshot(ListedSnapshot),
}

/// One listed snapshot, with its record and the manifest that it keeps.
pub(super) struct ListedSnapshot {
    pub(super) key: String,
    /// The record's size, as the listing reported it.
    pub(super) size_bytes: u64,
    /// The snapshot's record, or `None` when the record is gone.
    pub(super) record: Option<SnapshotPayload>,
    pub(super) manifest: SnapshotManifest,
}

/// The manifest that one listed snapshot keeps.
pub(super) enum SnapshotManifest {
    /// The first listed snapshot of a manifest that no earlier root loaded.
    Loaded(LoadedManifest),
    /// A manifest that the current manifest or an earlier snapshot keeps.
    Rooted,
    /// A manifest that the store doesn't have.
    Missing(ManifestNo),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Lists the namespace's snapshots and reads each snapshot's record and the manifest
    /// that the snapshot keeps, with up to `STORE_READS_IN_FLIGHT` snapshots read at
    /// once, in listing order.
    ///
    /// The first listed snapshot for a manifest number other than
    /// `current_manifest_no` loads that manifest, and later snapshots for the
    /// number use the result.
    pub(super) fn list_snapshot_manifests(
        &self,
        current_manifest_no: ManifestNo,
    ) -> BoxStream<'_, Result<ListedSnapshotKey>> {
        let prefix = snapshot_prefix(&self.namespace_id);
        let mut requested = BTreeSet::from([current_manifest_no]);
        let mut rooted = BTreeSet::from([current_manifest_no]);
        self.store
            .list_entries_from_stream(&prefix, None)
            .map_err(move |error| CoreError::store(&prefix, &error))
            .map_ok(move |entry| {
                let ListedObject {
                    key, size_bytes, ..
                } = entry;
                let manifest_no = snapshot_manifest_no(&key);
                let first = matches!(manifest_no, Ok(Some(number)) if requested.insert(number));
                async move {
                    let Some(manifest_no) = manifest_no? else {
                        return Ok((size_bytes, None));
                    };
                    let manifest = async {
                        if first {
                            self.load_root_manifest(manifest_no).await.map(Some)
                        } else {
                            Ok(None)
                        }
                    };
                    let (loaded, record) =
                        futures::try_join!(manifest, self.load_listed_snapshot_record(&key))?;
                    Ok((size_bytes, Some((key, manifest_no, loaded, record))))
                }
            })
            // `try_buffered` keeps listing order, so a snapshot that reuses an
            // earlier snapshot's load arrives after that load's result.
            .try_buffered(STORE_READS_IN_FLIGHT)
            .map_ok(move |(size_bytes, listed)| {
                let Some((key, manifest_no, loaded, record)) = listed else {
                    return ListedSnapshotKey::Unrecognized { size_bytes };
                };
                let manifest = match loaded {
                    Some(Some(manifest)) => {
                        rooted.insert(manifest_no);
                        SnapshotManifest::Loaded(manifest)
                    }
                    _ if rooted.contains(&manifest_no) => SnapshotManifest::Rooted,
                    _ => SnapshotManifest::Missing(manifest_no),
                };
                ListedSnapshotKey::Snapshot(ListedSnapshot {
                    key,
                    size_bytes,
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

/// Returns the manifest number in a snapshot key, or `None` for a key that isn't
/// a snapshot record.
fn snapshot_manifest_no(key: &str) -> Result<Option<ManifestNo>> {
    if parse_object_key(key).map(|parsed| parsed.family()) != Some(DurableObjectFamily::Snapshot) {
        return Ok(None);
    }
    let (_, snapshot_id) = snapshot_record_key_ids(key).map_err(CoreError::ControlObjectLoad)?;
    Ok(Some(snapshot_id.manifest_no()))
}

/// Returns the corruption error of a snapshot at `root_key` whose manifest
/// `manifest_no` of `namespace_id` is missing.
pub(super) fn missing_root_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    root_key: &str,
) -> CoreError {
    let key = metadata_manifest_object(namespace_id, &manifest_no);
    CoreError::NamespaceCorrupt(format!("root `{root_key}` keeps missing manifest `{key}`"))
}
