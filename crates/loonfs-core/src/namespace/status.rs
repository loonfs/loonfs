//! Reads namespace state and storage diagnostics.

use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control_snapshot::{load_control_snapshot, load_head_and_retention_floor};
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::wire::control::{HeadState, NamespaceStatus};
use loonfs_api::{ChangeSeq, ManifestNo, Namespace, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Whether a namespace carries visible commits its basis manifest does not
/// cover, and the head sequence they run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceFlushBasis {
    pub head_seq: ChangeSeq,
    pub has_unflushed_wal_tail: bool,
}

pub use loonfs_api::NamespaceStorageDiagnostics;

/// Namespace head state needed for storage diagnostics.
struct LoadedHeadBasis {
    head: HeadState,
    current_manifest_no: Option<ManifestNo>,
    /// Sequence the basis manifest covers; the visible tail sits above it.
    basis_head_seq: ChangeSeq,
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
    let head = snapshot.head.state;
    if head.status == (NamespaceStatus::Deleted {}) {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    // An unflushed fork measures its WAL tail from the fork point.
    let (current_manifest_no, basis_head_seq) = match basis.manifest() {
        Some(manifest) => (
            basis
                .is_owned_by(expected_namespace_id)
                .then_some(manifest.manifest_no),
            manifest.manifest_head_seq,
        ),
        None => (None, ChangeSeq(0)),
    };
    Ok(LoadedHeadBasis {
        head,
        current_manifest_no,
        basis_head_seq,
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
    let head = head.state;
    if head.status == (NamespaceStatus::Deleted {}) {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    Ok(Namespace {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq,
    })
}

/// Loads storage diagnostics for a live namespace.
///
/// The head stores enough recent segment IDs to count the visible WAL tail
/// without reading segment bodies. This returns an error if those IDs do not
/// cover the full tail. WAL readers validate the full chain separately.
pub async fn load_namespace_diagnostics<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceStorageDiagnostics> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    let wal_tail_segments = count_visible_wal_tail_segments(&WalChainLoadRequest {
        namespace_id: expected_namespace_id,
        chain_base_seq: loaded.basis_head_seq,
        head_seq: loaded.head.seq,
        visible_tip: loaded.head.visible_wal_tip.clone(),
        stop_after_seq: None,
        recent_segments: &loaded.head.recent_segments,
    })
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    Ok(NamespaceStorageDiagnostics {
        namespace_id: loaded.head.namespace_id,
        head_seq: loaded.head.seq,
        retention_floor_seq: loaded.retention_floor_seq,
        current_manifest_no: loaded.current_manifest_no,
        wal_tail_segments,
    })
}

/// Returns the head sequence and whether the namespace has WAL data to flush.
///
/// Unlike [`load_namespace_diagnostics`], this does not count WAL segments.
/// It can therefore be used to repair a head whose segment hints are incomplete.
pub async fn load_namespace_flush_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceFlushBasis> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    Ok(NamespaceFlushBasis {
        head_seq: loaded.head.seq,
        has_unflushed_wal_tail: loaded.basis_head_seq < loaded.head.seq,
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
    let head = head.state;
    if head.status != (NamespaceStatus::Deleted {}) {
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
