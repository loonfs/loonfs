use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::{read_head_and_metadata_root, ControlObjectLoadError};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::{wal_segment_id_start_seq, ChangeSeq, CheckpointId, ManifestId, NamespaceId};
use loonfs_objectstore::{
    keys::{namespace_config, wal_head, wal_segment_id_from_key, wal_segment_prefix},
    ObjectStore,
};
use serde::{Deserialize, Serialize};

/// Lightweight namespace head status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub current_manifest_id: Option<ManifestId>,
    pub latest_checkpoint_id: Option<CheckpointId>,
    /// WAL segment objects positioned past the loaded manifest.
    ///
    /// Derived from position-ordered object names, not from walking the
    /// chain: an inspection count for maintenance gating and operators, not
    /// a validated chain length.
    pub wal_tail_segments: u64,
    pub retention_floor_seq: ChangeSeq,
}

/// Opaque ETag probe for the namespace head object.
///
/// This only proves that the durable head object identity still matches a
/// previously loaded namespace view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadEtagProbe {
    pub head_etag: String,
}

pub async fn load_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadSummary, CoreError> {
    match namespace_initialization_state(store, expected_namespace).await {
        Ok(NamespaceInitializationState::Complete) => {}
        Ok(NamespaceInitializationState::Absent) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadNamespaceDescriptor(
                    ControlObjectLoadError::MissingObject {
                        object_key: namespace_config(expected_namespace.as_str()),
                    },
                ),
            ));
        }
        Ok(NamespaceInitializationState::Partial) => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: expected_namespace.clone(),
            });
        }
        Err(error) => return Err(map_namespace_initialization_error_to_core(error)),
    }

    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, expected_namespace)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if loaded_head.envelope.state.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace.clone(),
        });
    }
    let head = loaded_head.envelope.state;
    let root = loaded_root.envelope.state;
    let manifest = load_namespace_manifest_envelope(store, expected_namespace, root.manifest_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let manifest_head_seq = manifest.payload.head_seq;
    let latest_checkpoint_id = manifest
        .payload
        .checkpoints
        .last()
        .map(|record| record.checkpoint_id.clone());
    let wal_tail_segments = if head.visible_wal_tip.is_some() {
        count_wal_tail_segments_by_position(store, expected_namespace, manifest_head_seq).await?
    } else {
        0
    };
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id: Some(root.manifest_id),
        latest_checkpoint_id,
        wal_tail_segments,
        retention_floor_seq: head.retention_floor_seq,
    })
}

/// Counts WAL tail segments from their position-ordered object names.
///
/// Status is an inspection surface, so the count comes from one listing
/// instead of loading and validating segment bodies: segment file names
/// carry their `start_seq`, and every chain segment past the manifest starts
/// above it. Objects that lost a head race are counted until reclamation
/// removes them, which can only over-trigger maintenance, never starve it.
/// Recovery authority stays with the head and chain.
async fn count_wal_tail_segments_by_position<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_head_seq: ChangeSeq,
) -> Result<u64, CoreError> {
    let keys = store
        .list_prefix(&wal_segment_prefix(namespace_id.as_str()))
        .await
        .map_err(|error| CoreError::Store(format!("list WAL tail segments: {error}")))?;
    let tail_segments = keys
        .iter()
        .filter_map(|key| wal_segment_id_from_key(key))
        .filter_map(wal_segment_id_start_seq)
        .filter(|start_seq| *start_seq > manifest_head_seq)
        .count();
    u64::try_from(tail_segments)
        .map_err(|_| CoreError::Store("WAL tail segment count overflow".to_owned()))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "probe_namespace_head_etag", key_class = "wal_head")
)]
pub async fn probe_namespace_head_etag<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadEtagProbe, CoreError> {
    NamespaceId::parse(expected_namespace.as_str())?;
    let object_key = wal_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|err| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::Store(err.to_string()),
            ))
        })?
        .ok_or_else(|| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::MissingObject {
                    object_key: object_key.clone(),
                },
            ))
        })?;
    let head_etag = metadata.etag.ok_or(CoreError::MetadataProjection(
        MetadataProjectionLoadError::MissingHeadEtag { object_key },
    ))?;
    Ok(NamespaceHeadEtagProbe { head_etag })
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadNamespaceDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadContentStoreDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::InspectNamespaceDescriptor(_)
        | NamespaceInitializationError::InspectNamespaceHead(_) => {
            CoreError::Store(error.to_string())
        }
    }
}
