//! References protected by this call's head and complete checkpoint listing.

use super::fork_checkpoints::{classify_fork_checkpoint, ForkCheckpointReachability};
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::checkpoint::{
    ensure_manifest_reference_matches, load_namespace_manifest_envelope_if_present,
};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control_snapshot::NamespaceControlSnapshot;
use futures::StreamExt;
use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus, ManifestRef};
use loonfs_api::{ContentStoreId, ManifestNo, NamespaceId, WalNo};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key,
};
use loonfs_objectstore::{keys::wal_no_from_key, ObjectStore};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct LiveSet {
    pub(super) content_store_id: ContentStoreId,
    pub(super) namespace_deleted: bool,
    pub(super) reclaim_after_ms: Option<u64>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    pub(super) missing_basis_checkpoints: BTreeSet<String>,
    pub(super) checkpoints: Vec<String>,
    folded_and_floor_wal_no: Option<WalNo>,
}

impl LiveSet {
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        snapshot: &NamespaceControlSnapshot,
        context: &MutationContext,
    ) -> Result<Self> {
        let head = &snapshot.head.state;
        let mut live = Self {
            content_store_id: head.content_store_id.clone(),
            namespace_deleted: head.status.is_deleted(),
            reclaim_after_ms: head.status.reclaim_after_ms(),
            discovery_start_manifest_no: snapshot.root.discovery_start_manifest_no,
            objects: BTreeSet::new(),
            missing_basis_checkpoints: BTreeSet::new(),
            checkpoints: Vec::new(),
            folded_and_floor_wal_no: (!head.status.is_deleted())
                .then_some(head.last_folded_wal_no.min(head.retention_floor_wal_no)),
        };
        let mut manifests = BTreeMap::<String, Option<ManifestRef>>::new();
        live.objects.insert(snapshot.root.object_key.clone());
        if !live.namespace_deleted {
            live.load_manifest(store, snapshot.basis().manifest(), &mut manifests)
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
            if !super::families::CandidateFamily::Checkpoints.recognizes(&key) {
                live.checkpoints.push(key);
                continue;
            }
            let record = match load_checkpoint_record_at_key(store, &key).await {
                Ok(record) => record.state,
                Err(ControlObjectLoadError::MissingObject { .. }) => continue,
                Err(error) => return Err(CoreError::ControlObjectLoad(error)),
            };
            let active = record.status == (CheckpointStatus::Active {});
            let retained_fork = if let CheckpointOwner::Fork {
                target_namespace_id,
                expires_at_ms,
            } = &record.owner
            {
                matches!(
                    classify_fork_checkpoint(
                        store,
                        &record,
                        target_namespace_id,
                        *expires_at_ms,
                        context
                    )
                    .await?,
                    ForkCheckpointReachability::Retained { .. }
                )
            } else {
                false
            };
            if (active || retained_fork)
                && !live
                    .load_manifest(store, &record.manifest, &mut manifests)
                    .await?
            {
                live.missing_basis_checkpoints.insert(key.clone());
            }
            live.checkpoints.push(key);
        }
        Ok(live)
    }

    async fn load_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        reference: &ManifestRef,
        manifests: &mut BTreeMap<String, Option<ManifestRef>>,
    ) -> Result<bool> {
        let key = metadata_manifest_object(&reference.owner_namespace_id, &reference.manifest_no);
        if let Some(loaded) = manifests.get(&key) {
            if loaded.as_ref().is_some_and(|loaded| loaded != reference) {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "checkpoint manifest references disagree for `{key}`"
                )));
            }
            return Ok(loaded.is_some());
        }
        let envelope = load_namespace_manifest_envelope_if_present(
            store,
            &reference.owner_namespace_id,
            &reference.manifest_no,
            &key,
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
        let Some(envelope) = envelope else {
            manifests.insert(key, None);
            return Ok(false);
        };
        ensure_manifest_reference_matches("gc root", reference, &envelope)?;
        self.objects.insert(key.clone());
        self.objects.extend(
            envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .map(metadata_segment_object_key),
        );
        manifests.insert(key, Some(reference.clone()));
        Ok(true)
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
