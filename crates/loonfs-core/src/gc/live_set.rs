//! Manifests, runs, and WAL objects protected by namespace retention and pins.

use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::checkpoint::record::checkpoint_key_ids;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::NAMESPACE_RETIREMENT_GRACE_MS;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::wal::{object_is_required, required_from};
use futures::StreamExt;
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{
    CheckpointId, ContentStoreId, ManifestNo, NamespaceGeneration, NamespaceId, WalNo,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationState {
    Current,
    Pending { deadline_ms: u64 },
    Eligible,
    Reclaimed,
}

pub(super) struct RetiredPin {
    pub(super) key: String,
    pub(super) id: CheckpointId,
    pub(super) tombstone: NamespaceManifestPayload,
}

pub(super) struct LiveSet {
    pub(super) content_store_id: ContentStoreId,
    pub(super) owner_generation: NamespaceGeneration,
    pub(super) generation_first_manifest_no: ManifestNo,
    pub(super) namespace_deleted: bool,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) retired_pins: Vec<RetiredPin>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    listed_pins: BTreeSet<CheckpointId>,
    unrecognized_pin: bool,
    grace_window_ms: u64,
    now_ms: u64,
    required_wal_from: Option<WalNo>,
}

impl LiveSet {
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        anchor: &NamespaceReadAnchor,
        grace_window_ms: u64,
        now_ms: u64,
    ) -> Result<Self> {
        let head = anchor.manifest.envelope.payload();
        let mut live = Self {
            content_store_id: head.content_store_id.clone(),
            owner_generation: head.generation,
            generation_first_manifest_no: head.generation_first_manifest_no,
            namespace_deleted: head.status.is_deleted(),
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            retired_pins: Vec::new(),
            discovery_start_manifest_no: anchor.manifest.discovery_start_manifest_no,
            objects: BTreeSet::from([anchor.manifest.object_key.clone()]),
            listed_pins: BTreeSet::new(),
            unrecognized_pin: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_MS),
            now_ms,
            required_wal_from: required_from(&anchor.read_state),
        };
        let mut manifests = BTreeMap::new();
        if !live.namespace_deleted {
            live.load_manifest(
                store,
                namespace_id,
                head.manifest_no,
                &anchor.manifest.object_key,
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
            if !super::families::CandidateFamily::Checkpoints.recognizes(&key) {
                live.unrecognized_pin = true;
                continue;
            }
            let (_, pin_id) = checkpoint_key_ids(&key).map_err(CoreError::ControlObjectLoad)?;
            let payload = live
                .load_manifest(
                    store,
                    namespace_id,
                    pin_id.manifest_no(),
                    &key,
                    &mut manifests,
                )
                .await?;
            if super::fork_checkpoints::is_retired_pin(namespace_id, &pin_id) {
                if !payload.status.is_deleted() {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "retired pin `{key}` names an active manifest"
                    )));
                }
                live.retired_pins.push(RetiredPin {
                    key,
                    id: pin_id.clone(),
                    tombstone: payload,
                });
            }
            live.listed_pins.insert(pin_id);
        }
        live.retired_pins
            .sort_by(|left, right| left.key.cmp(&right.key));
        Ok(live)
    }

    async fn load_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        manifest_no: ManifestNo,
        root_key: &str,
        manifests: &mut BTreeMap<ManifestNo, NamespaceManifestPayload>,
    ) -> Result<NamespaceManifestPayload> {
        if let Some(payload) = manifests.get(&manifest_no) {
            return Ok(payload.clone());
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
        let payload = envelope.payload();
        self.objects.insert(key);
        if !payload.status.is_deleted() {
            self.objects.extend(
                payload
                    .runs
                    .iter()
                    .flat_map(|run| &run.segments)
                    .map(metadata_segment_object_key),
            );
        }
        manifests.insert(manifest_no, payload.clone());
        Ok(payload.clone())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.required_wal_from
            .is_some_and(|floor| object_is_required(key, floor))
    }

    pub(super) fn tombstones(&self) -> impl Iterator<Item = &NamespaceManifestPayload> {
        self.current_tombstone
            .iter()
            .chain(self.retired_pins.iter().map(|pin| &pin.tombstone))
    }

    pub(super) fn deadline(&self, tombstone: &NamespaceManifestPayload) -> u64 {
        tombstone
            .status
            .deleted_at_ms()
            .expect("a tombstone should carry its deletion stamp")
            .saturating_add(self.grace_window_ms)
    }

    pub(super) fn generation_state(&self, generation: NamespaceGeneration) -> GenerationState {
        if let Some(tombstone) = self
            .tombstones()
            .find(|tombstone| tombstone.generation == generation)
        {
            let deadline_ms = self.deadline(tombstone);
            let retired_id = CheckpointId::retired(&tombstone.namespace_id, tombstone.manifest_no);
            let pinned = self.listed_pins.iter().any(|id| {
                id != &retired_id
                    && id.manifest_no() >= tombstone.generation_first_manifest_no
                    && id.manifest_no() <= tombstone.manifest_no
            });
            if self.now_ms < deadline_ms || pinned || self.unrecognized_pin {
                GenerationState::Pending { deadline_ms }
            } else {
                GenerationState::Eligible
            }
        } else if generation < self.owner_generation {
            GenerationState::Reclaimed
        } else {
            GenerationState::Current
        }
    }

    pub(super) fn generation_content_store(
        &self,
        generation: NamespaceGeneration,
    ) -> &ContentStoreId {
        self.tombstones()
            .find(|tombstone| tombstone.generation == generation)
            .map_or(&self.content_store_id, |tombstone| {
                &tombstone.content_store_id
            })
    }
}
