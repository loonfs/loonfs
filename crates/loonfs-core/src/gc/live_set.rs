//! Manifests, runs, and WAL objects protected by namespace retention and pins.

use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::checkpoint::record::checkpoint_key_ids;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::NAMESPACE_RETIREMENT_GRACE_MS;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::wal::{object_is_required, required_from};
use futures::StreamExt;
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, NamespaceId, WalNo};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_segment_object_key,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RetirementState {
    Active,
    Retained { until_ms: Option<u64> },
    Eligible,
}

pub(super) struct LiveSet {
    pub(super) namespace_deleted: bool,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    has_pins: bool,
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
            namespace_deleted: head.status.is_deleted(),
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.manifest.discovery_start_manifest_no,
            objects: BTreeSet::from([anchor.manifest.object_key.clone()]),
            has_pins: false,
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
            live.has_pins = true;
            if !super::families::CandidateFamily::Checkpoints.recognizes(&key) {
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

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.required_wal_from
            .is_some_and(|floor| object_is_required(key, floor))
    }

    pub(super) fn deadline(&self, tombstone: &NamespaceManifestPayload) -> u64 {
        tombstone
            .status
            .deleted_at_ms()
            .expect("a tombstone should carry its deletion stamp")
            .saturating_add(self.grace_window_ms)
    }

    pub(super) fn retirement_state(&self) -> RetirementState {
        let Some(tombstone) = &self.current_tombstone else {
            return RetirementState::Active;
        };
        let deadline_ms = self.deadline(tombstone);
        if self.now_ms < deadline_ms {
            RetirementState::Retained {
                until_ms: Some(deadline_ms),
            }
        } else if self.has_pins {
            RetirementState::Retained { until_ms: None }
        } else {
            RetirementState::Eligible
        }
    }
}
