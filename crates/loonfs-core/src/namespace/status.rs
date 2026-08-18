//! Reads namespace state and storage diagnostics.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::basis::{load_head_and_metadata_basis, resolve_retention_floor_seq};
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::wire::control::{HeadState, NamespaceState};
use loonfs_api::{ChangeSeq, ManifestId, Namespace, NamespaceDiagnostics, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Whether a namespace carries visible commits its basis manifest does not
/// cover, and the head sequence they run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceFlushBasis {
    pub head_seq: ChangeSeq,
    pub has_unflushed_wal_tail: bool,
}

/// Namespace diagnostics before the WAL tail is counted.
struct LoadedHeadBasis {
    head: HeadState,
    current_manifest_id: Option<ManifestId>,
    /// Sequence the basis manifest covers; the visible tail sits above it.
    basis_head_seq: ChangeSeq,
    retention_floor_seq: ChangeSeq,
}

async fn load_namespace_head_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<LoadedHeadBasis> {
    let loaded = load_head_and_metadata_basis(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head = loaded.head.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    // The floor object is addressed by the head alone, so it is read beside
    // the basis manifest rather than after it. The tail is measured from the
    // basis manifest's coverage, whoever owns it: a fork target that has not
    // flushed measures from its fork point.
    let (basis, retention_floor_seq) = futures::join!(
        async {
            match loaded.basis.manifest() {
                Some(basis) => load_namespace_manifest_envelope(
                    store,
                    &basis.owner_namespace_id,
                    &basis.manifest_object_id,
                )
                .await
                .map(|manifest| {
                    let own_manifest_id = loaded
                        .basis
                        .is_owned_by(expected_namespace_id)
                        .then_some(basis.manifest_id);
                    (own_manifest_id, manifest.payload.head_seq)
                }),
                None => Ok((None, ChangeSeq(0))),
            }
        },
        resolve_retention_floor_seq(store, &head)
    );
    let (current_manifest_id, basis_head_seq) = basis.map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    let retention_floor_seq = retention_floor_seq.map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
    })?;
    Ok(LoadedHeadBasis {
        head,
        current_manifest_id,
        basis_head_seq,
        retention_floor_seq,
    })
}

/// Loads the current state of a live namespace.
pub async fn load_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<Namespace> {
    let head = crate::namespace::control::load_head_object(store, expected_namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace_id.clone(),
        });
    }
    let retention_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
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
) -> Result<NamespaceDiagnostics> {
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
    Ok(NamespaceDiagnostics {
        namespace_id: loaded.head.namespace_id,
        head_seq: loaded.head.seq,
        retention_floor_seq: loaded.retention_floor_seq,
        current_manifest_id: loaded.current_manifest_id,
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
) -> Result<NamespaceDiagnostics> {
    let head = crate::namespace::control::load_head_object(store, expected_namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if head.state != NamespaceState::Deleted {
        return Err(CoreError::Internal(format!(
            "namespace `{expected_namespace_id}` is live; deleted diagnostics require a deleted namespace"
        )));
    }
    let retention_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    Ok(NamespaceDiagnostics {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq,
        current_manifest_id: None,
        wal_tail_segments: 0,
    })
}
