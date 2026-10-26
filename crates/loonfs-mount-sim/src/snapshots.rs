//! Snapshots: create, retrieve, extend, and delete.
//!
//! A snapshot records the head it captured, and a read at the snapshot reads the model at that
//! sequence. The model keeps every row, so a snapshot needs no fold and pins nothing.

use crate::error::{MountSimError, Result};
use crate::state::ServerState;
use loonfs_types::api::v0::{
    CreateSnapshotRequest, DeleteSnapshotResponse, ExtendSnapshotRequest, Snapshot,
};
use loonfs_types::{ErrorCode, NamespaceId, PinId};

pub(crate) fn create_snapshot(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    request: &CreateSnapshotRequest,
) -> Result<Snapshot> {
    if request.ttl_ms == Some(0) {
        return Err(
            MountSimError::invalid_request("`ttl_ms` must be positive").with_param("/ttl_ms")
        );
    }
    let now_ms = state.now_ms();
    let captured_seq = state.namespace(namespace_id)?.head_seq();
    let snapshot = Snapshot {
        snapshot_id: state.ids.snapshot_id(captured_seq.0),
        namespace_id: namespace_id.clone(),
        name: request.name.clone(),
        captured_seq,
        created_at_ms: now_ms,
        expires_at_ms: request.ttl_ms.map(|ttl_ms| now_ms.saturating_add(ttl_ms)),
    };
    state
        .namespace_mut(namespace_id)?
        .snapshots
        .insert(snapshot.snapshot_id.clone(), snapshot.clone());
    Ok(snapshot)
}

/// Returns a snapshot, including an expired one, because nothing collects it.
pub(crate) fn get_snapshot(
    state: &ServerState,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
) -> Result<Snapshot> {
    state
        .namespace(namespace_id)?
        .snapshots
        .get(snapshot_id)
        .cloned()
        .ok_or_else(|| snapshot_not_found(snapshot_id))
}

/// Moves an unexpired snapshot's expiry to `ttl_ms` from now, and never earlier.
pub(crate) fn extend_snapshot(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    request: &ExtendSnapshotRequest,
) -> Result<Snapshot> {
    if request.ttl_ms == 0 {
        return Err(
            MountSimError::invalid_request("`ttl_ms` must be positive").with_param("/ttl_ms")
        );
    }
    let now_ms = state.now_ms();
    let namespace = state.namespace_mut(namespace_id)?;
    let snapshot = namespace
        .snapshots
        .get_mut(snapshot_id)
        .filter(|snapshot| snapshot.is_live(now_ms))
        .ok_or_else(|| snapshot_not_found(snapshot_id))?;
    if let Some(expires_at_ms) = snapshot.expires_at_ms {
        snapshot.expires_at_ms = Some(expires_at_ms.max(now_ms.saturating_add(request.ttl_ms)));
    }
    Ok(snapshot.clone())
}

/// Deletes a snapshot. Deleting a missing snapshot succeeds, so a retry is safe.
pub(crate) fn delete_snapshot(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
) -> Result<DeleteSnapshotResponse> {
    state
        .namespace_mut(namespace_id)?
        .snapshots
        .remove(snapshot_id);
    Ok(DeleteSnapshotResponse {
        namespace_id: namespace_id.clone(),
        snapshot_id: snapshot_id.clone(),
    })
}

fn snapshot_not_found(snapshot_id: &PinId) -> MountSimError {
    MountSimError::new(
        ErrorCode::SnapshotNotFound,
        format!("snapshot `{snapshot_id}` doesn't exist or has expired"),
    )
}
