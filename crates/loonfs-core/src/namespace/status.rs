//! Read-only namespace status: summarizes the head, root manifest, WAL
//! tail, and retention floor.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::catalog::{
    map_namespace_initialization_error_to_core, namespace_initialization_state,
    NamespaceInitializationState,
};
use crate::namespace::control::{
    read_head_and_metadata_root, read_wal_floor_seq_or_zero, ControlObjectLoadError,
};
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use loonfs_objectstore::{keys::namespace_config, ObjectStore};
use serde::{Deserialize, Serialize};

/// Lightweight namespace head status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub current_manifest_id: Option<ManifestId>,
    /// Number of visible WAL segments after the current manifest.
    ///
    /// Counted from the head's chain pointers (`recent_segments`, published
    /// under the same CAS as the tip); segment bodies are fetched and
    /// validated only for a tail extending past the hinted window. An
    /// inspection count for maintenance gating and operators — replay
    /// consumers load the validated chain instead.
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
        Ok(NamespaceInitializationState::Partial | NamespaceInitializationState::PreHeadDebris) => {
            return Err(CoreError::NamespacePartial {
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
    let wal_tail_segments = count_visible_wal_tail_segments(
        store,
        WalChainLoadRequest {
            namespace_id: expected_namespace_id,
            chain_base_seq: manifest.payload.head_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
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
