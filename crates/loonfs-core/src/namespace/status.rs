//! Reads namespace state and storage diagnostics.

use crate::error::{CoreError, Result};
use crate::namespace::control_snapshot::{load_control_snapshot, load_head_and_retention_floor};
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{ChangeSeq, ManifestNo, Namespace, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Whether a namespace carries visible commits its basis manifest does not
/// cover, and the head sequence they run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceFlushBasis {
    pub head_seq: ChangeSeq,
    pub has_unflushed_wal_tail: bool,
}

/// Namespace storage diagnostics that do not require checkpoint enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStorageDiagnostics {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub retention_floor_seq: ChangeSeq,
    pub current_manifest_no: Option<ManifestNo>,
    pub wal_tail_segments: u64,
}

/// Namespace head state needed for storage diagnostics.
struct LoadedHeadBasis {
    head: NamespaceReadState,
    current_manifest_no: Option<ManifestNo>,
    /// Sequence the basis manifest covers; the visible tail sits above it.
    retention_floor_seq: ChangeSeq,
}

async fn load_namespace_head_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedHeadBasis> {
    let snapshot = load_control_snapshot(store, expected_namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let basis = snapshot.basis();
    let retention_floor_seq = snapshot.retention_floor_seq;
    let head = snapshot.head;
    super::control::ensure_namespace_live(&head)?;
    let current_manifest_no = Some(basis.manifest_no());
    Ok(LoadedHeadBasis {
        head,
        current_manifest_no,
        retention_floor_seq,
    })
}

/// Loads the current state of a live namespace.
pub async fn load_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<Namespace> {
    let (head, retention_floor_seq) = load_head_and_retention_floor(store, expected_namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    super::control::ensure_namespace_live(&head)?;
    Ok(Namespace {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq,
    })
}

pub async fn load_namespace_diagnostics<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceStorageDiagnostics> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    let wal_tail_segments = loaded.head.wal_no.0 - loaded.head.last_folded_wal_no.0;
    Ok(NamespaceStorageDiagnostics {
        namespace_id: loaded.head.namespace_id,
        head_seq: loaded.head.seq,
        retention_floor_seq: loaded.retention_floor_seq,
        current_manifest_no: loaded.current_manifest_no,
        wal_tail_segments,
    })
}

pub async fn load_namespace_flush_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceFlushBasis> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    Ok(NamespaceFlushBasis {
        head_seq: loaded.head.seq,
        has_unflushed_wal_tail: loaded.head.last_folded_wal_no < loaded.head.wal_no,
    })
}

/// Loads diagnostics for a deleted namespace.
///
/// Garbage collection may already have removed the manifest and WAL, so this
/// reads only the head and WAL floor. Call this only after
/// [`load_namespace_diagnostics`] reports that the namespace is deleted.
pub async fn load_deleted_namespace_diagnostics<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceStorageDiagnostics> {
    let (head, retention_floor_seq) = load_head_and_retention_floor(store, expected_namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if !head.status.is_deleted() {
        return Err(CoreError::Internal(format!(
            "namespace `{expected_namespace_id}` is live; deleted diagnostics require a deleted namespace"
        )));
    }
    Ok(NamespaceStorageDiagnostics {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq,
        current_manifest_no: None,
        wal_tail_segments: 0,
    })
}
