//! Read-only namespace status: summarizes the head, its materialized
//! basis, the WAL tail, and the retention floor.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::basis::{read_head_and_metadata_basis, resolve_retention_floor_seq};
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};

/// Lightweight namespace head status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    /// Manifest this namespace has materialized for itself, or `None` when
    /// it has published none yet: a fresh namespace reads from the genesis
    /// state, and a fresh fork target reads from its source's manifest.
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
    let loaded = read_head_and_metadata_basis(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head = loaded.head.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    // The tail is counted from the basis manifest's coverage, whoever owns
    // it: a fork target that has not flushed counts from its fork point.
    let (current_manifest_id, basis_head_seq) = match loaded.basis.manifest() {
        Some(basis) => {
            let manifest = load_namespace_manifest_envelope(
                store,
                &basis.owner_namespace_id,
                &basis.manifest_object_id,
            )
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
            let own_manifest_id = loaded
                .basis
                .is_owned_by(expected_namespace_id)
                .then_some(basis.manifest_id);
            (own_manifest_id, manifest.payload.head_seq)
        }
        None => (None, ChangeSeq(0)),
    };
    let wal_tail_segments = count_visible_wal_tail_segments(
        store,
        WalChainLoadRequest {
            namespace_id: expected_namespace_id,
            chain_base_seq: basis_head_seq,
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
    let retention_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id,
        wal_tail_segments,
        retention_floor_seq,
    })
}

/// Summarizes a namespace whose head is a deletion tombstone.
///
/// Reads only the two control objects that outlive reclamation — the head
/// and the WAL floor — because garbage collection may already have reaped
/// the manifest and chain a live summary would consult. Callers reach for
/// this only after [`load_namespace_head_summary`] reported the deletion;
/// a live head here is an invariant breach, not a state to serve.
pub async fn load_deleted_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceHeadSummary> {
    let head = crate::namespace::control::read_head_object(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    if head.state != NamespaceState::Deleted {
        return Err(CoreError::Internal(format!(
            "namespace `{expected_namespace_id}` is not deleted; the live head summary serves it"
        )));
    }
    let retention_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id: None,
        wal_tail_segments: 0,
        retention_floor_seq,
    })
}
