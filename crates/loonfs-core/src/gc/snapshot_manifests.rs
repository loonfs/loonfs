//! The snapshots of one namespace and the manifests that they keep, as both
//! collections read them, with the snapshots' records for the namespace's
//! collection.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::snapshot_record_key_ids;
use futures::future::{self, Future};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, snapshot_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::control::SnapshotPayload;
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::BTreeSet;

/// One key that a listing of a namespace's snapshots returned, with what the
/// listing read of a snapshot's own record: the record for the namespace's
/// collection, and nothing for the pool's collection.
#[allow(
    clippy::large_enum_variant,
    reason = "almost every listed key is a snapshot, moved once: indirection would buy an allocation and save nothing"
)]
pub(super) enum ListedSnapshotKey<R = ()> {
    /// A key under the snapshots prefix that isn't a snapshot record, with the
    /// object's size as the listing reported it.
    Unrecognized {
        size_bytes: u64,
    },
    Snapshot(ListedSnapshot, R),
}

/// One listed snapshot, with the manifest that it keeps.
pub(super) struct ListedSnapshot {
    pub(super) key: String,
    /// The record's size, as the listing reported it.
    pub(super) size_bytes: u64,
    /// The record's provider modification time, when the listing reported one.
    pub(super) last_modified_at_ms: Option<u64>,
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
    /// Lists the namespace's snapshots and loads the manifest that each snapshot
    /// keeps, without reading the snapshots' records. The manifest number is part
    /// of the snapshot key.
    pub(super) fn list_snapshot_manifests(
        &self,
        current_manifest_no: ManifestNo,
    ) -> BoxStream<'_, Result<ListedSnapshotKey>> {
        self.list_snapshots(current_manifest_no, |_| future::ready(Ok(())))
    }

    /// Lists the namespace's snapshots and reads each snapshot's record and the
    /// manifest that the snapshot keeps. The record is `None` when the record is
    /// gone.
    pub(super) fn list_snapshot_records(
        &self,
        current_manifest_no: ManifestNo,
    ) -> BoxStream<'_, Result<ListedSnapshotKey<Option<SnapshotPayload>>>> {
        self.list_snapshots(current_manifest_no, move |key| {
            let key = key.to_owned();
            async move { self.load_listed_snapshot_record(&key).await }
        })
    }

    /// Lists the namespace's snapshots, and for each snapshot runs `read_record`
    /// and loads the manifest that the snapshot keeps, with up to
    /// `STORE_READS_IN_FLIGHT` snapshots read at once, in listing order.
    ///
    /// The first listed snapshot for a manifest number other than
    /// `current_manifest_no` loads that manifest, and later snapshots for the
    /// number use the result.
    fn list_snapshots<'a, R, F, Fut>(
        &'a self,
        current_manifest_no: ManifestNo,
        read_record: F,
    ) -> BoxStream<'a, Result<ListedSnapshotKey<R>>>
    where
        R: Send + 'a,
        F: Fn(&str) -> Fut + Send + 'a,
        Fut: Future<Output = Result<R>> + Send + 'a,
    {
        let prefix = snapshot_prefix(&self.namespace_id);
        let mut requested = BTreeSet::from([current_manifest_no]);
        let mut rooted = BTreeSet::from([current_manifest_no]);
        self.store
            .list_entries_from_stream(&prefix, None)
            .map_err(move |error| CoreError::store(&prefix, &error))
            .map_ok(move |entry| {
                let ListedObject {
                    key,
                    size_bytes,
                    last_modified_at_ms,
                } = entry;
                let manifest_no = snapshot_manifest_no(&key);
                let first = matches!(manifest_no, Ok(Some(number)) if requested.insert(number));
                let record = matches!(manifest_no, Ok(Some(_))).then(|| read_record(&key));
                async move {
                    let (Some(manifest_no), Some(record)) = (manifest_no?, record) else {
                        return Ok((size_bytes, last_modified_at_ms, None));
                    };
                    let manifest = async {
                        if first {
                            self.load_root_manifest(manifest_no).await.map(Some)
                        } else {
                            Ok(None)
                        }
                    };
                    let (loaded, record) = futures::try_join!(manifest, record)?;
                    Ok((
                        size_bytes,
                        last_modified_at_ms,
                        Some((key, manifest_no, loaded, record)),
                    ))
                }
            })
            // `try_buffered` keeps listing order, so a snapshot that reuses an
            // earlier snapshot's load arrives after that load's result.
            .try_buffered(STORE_READS_IN_FLIGHT)
            .map_ok(move |(size_bytes, last_modified_at_ms, listed)| {
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
                ListedSnapshotKey::Snapshot(
                    ListedSnapshot {
                        key,
                        size_bytes,
                        last_modified_at_ms,
                        manifest,
                    },
                    record,
                )
            })
            .boxed()
    }

    /// Loads one root manifest through the segment cache, or returns `None`
    /// when the manifest is missing.
    async fn load_root_manifest(&self, manifest_no: ManifestNo) -> Result<Option<LoadedManifest>> {
        Ok(self
            .load_manifest_if_present(manifest_no)
            .await
            .map_err(MetadataProjectionLoadError::from)?)
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
