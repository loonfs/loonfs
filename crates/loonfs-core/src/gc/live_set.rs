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
use loonfs_api::{ChangeSeq, ContentStoreId, ManifestNo, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key, wal_segment_prefix,
};
use loonfs_objectstore::{keys::wal_segment_id_from_key, ObjectStore};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct LiveSet {
    pub(super) content_store_id: ContentStoreId,
    pub(super) namespace_deleted: bool,
    pub(super) reclaim_after_ms: Option<u64>,
    pub(super) discovery_start_manifest_no: Option<ManifestNo>,
    pub(super) objects: BTreeSet<String>,
    pub(super) missing_basis_checkpoints: BTreeSet<String>,
    pub(super) checkpoints: Vec<String>,
    first_live_wal_seq: Option<ChangeSeq>,
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
            discovery_start_manifest_no: snapshot
                .root
                .as_ref()
                .map(|root| root.discovery_start_manifest_no),
            objects: BTreeSet::new(),
            missing_basis_checkpoints: BTreeSet::new(),
            checkpoints: Vec::new(),
            first_live_wal_seq: None,
        };
        let mut manifests = BTreeMap::<String, Option<ManifestRef>>::new();
        if !live.namespace_deleted {
            if let Some(reference) = snapshot.basis().manifest() {
                live.load_manifest(store, reference, &mut manifests).await?;
            }
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
        live.load_wal(store, namespace_id, snapshot).await?;
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

    async fn load_wal<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        snapshot: &NamespaceControlSnapshot,
    ) -> Result<()> {
        let prefix = wal_segment_prefix(namespace_id);
        if self.namespace_deleted {
            return Ok(());
        }
        let basis = snapshot.basis();
        let manifest_head_seq = basis
            .manifest()
            .map_or(ChangeSeq(0), |manifest| manifest.manifest_head_seq);
        let protected_floor = snapshot
            .retention_floor_seq
            .min(ChangeSeq(manifest_head_seq.0.saturating_add(1)));
        let mut first_live = None;
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            if let Some(seq) = wal_start_seq(&key).filter(|seq| *seq <= protected_floor) {
                first_live = Some(first_live.map_or(seq, |current: ChangeSeq| current.max(seq)));
            }
        }
        self.first_live_wal_seq = Some(first_live.unwrap_or(protected_floor));
        Ok(())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.first_live_wal_seq
            .is_some_and(|first_live| wal_start_seq(key).is_none_or(|seq| seq >= first_live))
    }

    pub(super) fn retired_content(&self, now_ms: u64) -> bool {
        self.namespace_deleted
            && self
                .reclaim_after_ms
                .is_some_and(|deadline| deadline <= now_ms)
    }
}

fn wal_start_seq(key: &str) -> Option<ChangeSeq> {
    wal_segment_id_from_key(key).and_then(loonfs_api::wal_segment_id_start_seq)
}
