//! Manifests, runs, and WAL objects protected by namespace retention and pins.

use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::checkpoint::record::checkpoint_key_ids;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::NAMESPACE_RETIREMENT_GRACE_MS;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::wal::{object_is_required, required_from};
use futures::StreamExt;
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, NamespaceGeneration, NamespaceId, PinId, WalNo};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationState {
    Current,
    /// Deleted and inside its retirement grace.
    Waiting {
        deadline_ms: u64,
    },
    /// Past its grace, but a pin in its manifest range still holds it.
    Held,
    Eligible,
    Reclaimed,
}

pub(super) struct RetiredGeneration {
    pub(super) key: String,
    pub(super) reference: loonfs_api::wire::control::ManifestRef,
    pub(super) tombstone: Option<NamespaceManifestPayload>,
}

pub(super) struct LiveSet {
    pub(super) owner_generation: NamespaceGeneration,
    pub(super) generation_first_manifest_no: ManifestNo,
    pub(super) namespace_deleted: bool,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) retired_generations: Vec<RetiredGeneration>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    listed_pins: BTreeSet<PinId>,
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
            owner_generation: head.generation,
            generation_first_manifest_no: head.generation_first_manifest_no,
            namespace_deleted: head.status.is_deleted(),
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            retired_generations: Vec::new(),
            discovery_start_manifest_no: anchor.manifest.discovery_start_manifest_no,
            objects: BTreeSet::from([anchor.manifest.object_key.clone()]),
            listed_pins: BTreeSet::new(),
            unrecognized_pin: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_MS),
            now_ms,
            required_wal_from: required_from(&anchor.read_state),
        };
        let mut manifests = BTreeSet::new();
        live.load_manifest(
            store,
            namespace_id,
            head.manifest_no,
            &anchor.manifest.object_key,
            &mut manifests,
        )
        .await?;
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
            live.load_manifest(
                store,
                namespace_id,
                pin_id.manifest_no(),
                &key,
                &mut manifests,
            )
            .await?;
            live.listed_pins.insert(pin_id);
        }
        live.load_retired_generations(store, namespace_id).await?;
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
                })?;
        let Some(envelope) = envelope else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "root `{root_key}` pins missing manifest `{key}`"
            )));
        };
        let payload = envelope.payload();
        self.protect_manifest(key, payload);
        manifests.insert(manifest_no);
        Ok(())
    }

    fn protect_manifest(&mut self, key: String, payload: &NamespaceManifestPayload) {
        self.objects.insert(key);
        self.objects.extend(
            payload
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .map(metadata_segment_object_key),
        );
    }

    async fn load_retired_generations<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<()> {
        let prefix = loonfs_objectstore::keys::retired_generation_prefix(namespace_id);
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            let Some(generation) = loonfs_objectstore::layout::retired_generation_of(&key) else {
                continue;
            };
            let Some(record) =
                crate::namespace::retired::load_retired_generation(store, namespace_id, generation)
                    .await?
            else {
                continue;
            };
            let tombstone =
                crate::namespace::retired::load_retired_tombstone(store, &record).await?;
            if let Some(tombstone) = &tombstone {
                self.protect_manifest(
                    metadata_manifest_object(namespace_id, &record.tombstone.manifest_no),
                    tombstone,
                );
            }
            self.retired_generations.push(RetiredGeneration {
                key,
                reference: record.tombstone,
                tombstone,
            });
        }
        self.retired_generations
            .sort_by(|left, right| left.key.cmp(&right.key));
        Ok(())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.required_wal_from
            .is_some_and(|floor| object_is_required(key, floor))
    }

    pub(super) fn tombstones(&self) -> impl Iterator<Item = &NamespaceManifestPayload> {
        self.current_tombstone.iter().chain(
            self.retired_generations
                .iter()
                .filter_map(|record| record.tombstone.as_ref()),
        )
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
            let pinned = self.listed_pins.iter().any(|id| {
                id.manifest_no() >= tombstone.generation_first_manifest_no
                    && id.manifest_no() <= tombstone.manifest_no
            });
            if self.now_ms < deadline_ms {
                GenerationState::Waiting { deadline_ms }
            } else if pinned || self.unrecognized_pin {
                GenerationState::Held
            } else {
                GenerationState::Eligible
            }
        } else if generation < self.owner_generation {
            GenerationState::Reclaimed
        } else {
            GenerationState::Current
        }
    }
}
