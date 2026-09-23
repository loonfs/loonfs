//! Shared generation counters and retirement records for namespace recreation.

use super::control::LoadedManifest;
use crate::checkpoint::record::write_checkpoint_record_if_absent;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordState};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{CheckpointId, ManifestNo, NamespaceGeneration, WalNo, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(super) struct GenerationSuccessor {
    manifest_no: ManifestNo,
    generation: NamespaceGeneration,
    writer_epoch: WriterEpoch,
    compactor_epoch: u64,
    last_folded_wal_no: WalNo,
}

impl GenerationSuccessor {
    pub(super) fn from_tombstone(tombstone: &NamespaceManifestPayload) -> Result<Self> {
        Ok(Self {
            manifest_no: tombstone
                .manifest_no
                .successor()
                .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?,
            generation: tombstone
                .generation
                .successor()
                .map_err(|error| CoreError::Internal(format!("namespace generation {error}")))?,
            writer_epoch: tombstone
                .writer_epoch
                .successor()
                .map_err(|error| CoreError::Internal(format!("writer epoch {error}")))?,
            compactor_epoch: tombstone
                .compactor_epoch
                .checked_add(1)
                .ok_or_else(|| CoreError::Internal("compactor epoch overflow".to_owned()))?,
            last_folded_wal_no: tombstone.last_folded_wal_no,
        })
    }

    pub(super) fn apply_to(self, payload: &mut NamespaceManifestPayload) {
        payload.manifest_no = self.manifest_no;
        payload.generation = self.generation;
        payload.generation_first_manifest_no = self.manifest_no;
        payload.writer_epoch = self.writer_epoch;
        payload.compactor_epoch = self.compactor_epoch;
        payload.last_folded_wal_no = self.last_folded_wal_no;
    }
}

pub(super) async fn write_retired_pin<S: ObjectStore + ?Sized>(
    store: &S,
    current: &LoadedManifest,
    created_at_ms: u64,
) -> Result<()> {
    let tombstone = current.envelope.payload();
    let retired = CheckpointRecordState {
        namespace_id: tombstone.namespace_id.clone(),
        pin_id: CheckpointId::retired(&tombstone.namespace_id, tombstone.manifest_no),
        manifest_no: tombstone.manifest_no,
        manifest_head_seq: tombstone.head_seq,
        manifest_payload_checksum: current.state.manifest.manifest_payload_checksum.clone(),
        head_commit_id: tombstone.head_commit_id.clone(),
        created_at_ms,
        owner: CheckpointOwner::Retired {},
    };
    write_checkpoint_record_if_absent(store, &retired).await
}
