//! Manifests and runs protected by the complete pin key listing.

use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::checkpoint::record::checkpoint_key_ids;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control_snapshot::NamespaceControlSnapshot;
use futures::StreamExt;
use loonfs_api::{ContentStoreId, ManifestNo, NamespaceId, WalNo};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key,
};
use loonfs_objectstore::{keys::wal_no_from_key, ObjectStore};
use std::collections::BTreeSet;

pub(super) struct LiveSet {
    pub(super) content_store_id: ContentStoreId,
    pub(super) namespace_deleted: bool,
    pub(super) reclaim_after_ms: Option<u64>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    folded_and_floor_wal_no: Option<WalNo>,
}

impl LiveSet {
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        snapshot: &NamespaceControlSnapshot,
    ) -> Result<Self> {
        let head = &snapshot.head.state;
        let mut live = Self {
            content_store_id: head.content_store_id.clone(),
            namespace_deleted: head.status.is_deleted(),
            reclaim_after_ms: head.status.reclaim_after_ms(),
            discovery_start_manifest_no: snapshot.root.discovery_start_manifest_no,
            objects: BTreeSet::from([snapshot.root.object_key.clone()]),
            folded_and_floor_wal_no: (!head.status.is_deleted())
                .then_some(head.last_folded_wal_no.min(head.retention_floor_wal_no)),
        };
        let mut manifests = BTreeSet::new();
        if !live.namespace_deleted {
            live.load_manifest(
                store,
                namespace_id,
                snapshot.basis().manifest().manifest_no,
                &snapshot.root.object_key,
                &mut manifests,
            )
            .await?;
        }
        let prefix = checkpoint_prefix(namespace_id);
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            if super::families::CandidateFamily::Checkpoints.recognizes(&key) {
                let (_, pin_id) = checkpoint_key_ids(&key).map_err(CoreError::ControlObjectLoad)?;
                live.load_manifest(
                    store,
                    namespace_id,
                    pin_id.manifest_no(),
                    &key,
                    &mut manifests,
                )
                .await?;
            }
        }
        Ok(live)
    }

    async fn load_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        manifest_no: ManifestNo,
        root_key: &str,
        manifests: &mut BTreeSet<ManifestNo>,
    ) -> Result<()> {
        if manifests.contains(&manifest_no) {
            return Ok(());
        }
        let key = metadata_manifest_object(namespace_id, &manifest_no);
        let envelope =
            load_namespace_manifest_envelope_if_present(store, namespace_id, &manifest_no, &key)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "root `{root_key}` pins missing manifest `{key}`"
                    ))
                })?;
        self.objects.insert(key);
        self.objects.extend(
            envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .map(metadata_segment_object_key),
        );
        manifests.insert(manifest_no);
        Ok(())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.folded_and_floor_wal_no
            .is_some_and(|floor| wal_no_from_key(key).is_none_or(|number| number > floor))
    }

    pub(super) fn retired_content(&self, now_ms: u64) -> bool {
        self.namespace_deleted
            && self
                .reclaim_after_ms
                .is_some_and(|deadline| deadline <= now_ms)
    }
}
