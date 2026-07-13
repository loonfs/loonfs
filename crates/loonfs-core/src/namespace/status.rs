//! Read-only namespace status: summarizes the head, root manifest, WAL
//! tail, and retention floor.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::{
    read_head_and_metadata_root, read_wal_floor_seq_or_zero, ControlObjectLoadError,
};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::{wal_segment_id_start_seq, ChangeSeq, ManifestId, NamespaceId};
use loonfs_objectstore::{
    keys::{namespace_config, wal_segment_id_from_key, wal_segment_prefix},
    ObjectStore,
};
use serde::{Deserialize, Serialize};

/// Lightweight namespace head status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub current_manifest_id: Option<ManifestId>,
    /// WAL segment objects positioned past the loaded manifest.
    ///
    /// Derived from position-ordered object names, not from walking the
    /// chain: an inspection count for maintenance gating and operators, not
    /// a validated chain length.
    pub wal_tail_segments: u64,
    pub retention_floor_seq: ChangeSeq,
}

pub async fn load_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceHeadSummary> {
    match namespace_initialization_state(store, expected_namespace_id).await {
        Ok(NamespaceInitializationState::Complete) => {}
        Ok(NamespaceInitializationState::Absent) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadNamespaceDescriptor(
                    ControlObjectLoadError::MissingObject {
                        object_key: namespace_config(expected_namespace_id.as_str()),
                    },
                ),
            ));
        }
        Ok(NamespaceInitializationState::Partial) => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: expected_namespace_id.clone(),
            });
        }
        Err(error) => return Err(map_namespace_initialization_error_to_core(error)),
    }

    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if loaded_head.envelope.state.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    let head = loaded_head.envelope.state;
    let root = loaded_root.envelope.state;
    let manifest =
        load_namespace_manifest_envelope(store, expected_namespace_id, &root.manifest_object_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    let manifest_head_seq = manifest.payload.head_seq;
    let wal_tail_segments = if head.visible_wal_tip.is_some() {
        count_wal_tail_segments_by_position(store, expected_namespace_id, manifest_head_seq).await?
    } else {
        0
    };
    let retention_floor_seq = read_wal_floor_seq_or_zero(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id: Some(root.manifest_id),
        wal_tail_segments,
        retention_floor_seq,
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
) -> Result<u64> {
    let wal_prefix = wal_segment_prefix(namespace_id.as_str());
    let keys = store
        .list_prefix(&wal_prefix)
        .await
        .map_err(|error| CoreError::store(&wal_prefix, &error))?;
    let tail_segments = keys
        .iter()
        .filter_map(|key| wal_segment_id_from_key(key))
        .filter_map(wal_segment_id_start_seq)
        .filter(|start_seq| *start_seq > manifest_head_seq)
        .count();
    u64::try_from(tail_segments)
        .map_err(|_| CoreError::Internal("WAL tail segment count overflow".to_owned()))
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
        NamespaceInitializationError::InspectNamespaceDescriptor {
            object_key,
            message,
        }
        | NamespaceInitializationError::InspectNamespaceHead {
            object_key,
            message,
        } => CoreError::Store {
            object_key,
            message,
        },
    }
}
