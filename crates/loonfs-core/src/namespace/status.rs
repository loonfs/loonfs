//! Read-only namespace status: summarizes the head, its materialized
//! basis, the WAL tail, and the retention floor.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::basis::{read_head_and_metadata_basis, resolve_retention_floor_seq};
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::v0::NamespaceStatusResponse;
use loonfs_api::wire::control::{HeadState, NamespaceState};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Whether a namespace carries visible commits its basis manifest does not
/// cover, and the head sequence they run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceFoldBasis {
    pub head_seq: ChangeSeq,
    pub has_unflushed_wal_tail: bool,
}

/// The head-derived half of a status: everything it reports except the WAL
/// tail count.
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

/// Summarizes a live namespace head.
///
/// The WAL tail is counted from the head's chain pointers
/// (`recent_segments`, published under the same CAS as the tip and long
/// enough to name every segment a legal unflushed tail can hold); no segment
/// body is read. A head that under-describes its own tail fails the summary
/// rather than being walked. The count is for inspection and maintenance
/// gating — replay consumers load the validated chain instead.
pub async fn load_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceStatusResponse> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    let wal_tail_segments = count_visible_wal_tail_segments(WalChainLoadRequest {
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
    Ok(NamespaceStatusResponse {
        namespace_id: loaded.head.namespace_id,
        head_seq: loaded.head.seq,
        current_manifest_id: loaded.current_manifest_id,
        wal_tail_segments,
        retention_floor_seq: loaded.retention_floor_seq,
    })
}

/// Answers the one question a WAL fold decides on — is there anything to
/// fold — without sizing the tail.
///
/// [`load_namespace_head_summary`] answers it too, but only alongside a
/// segment count the head must be able to describe. A head that
/// under-describes its own tail is exactly the state an explicit fold
/// repairs, so the fold must not be gated on the count it cannot produce.
pub async fn load_namespace_fold_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceFoldBasis> {
    let loaded = load_namespace_head_basis(store, expected_namespace_id).await?;
    Ok(NamespaceFoldBasis {
        head_seq: loaded.head.seq,
        has_unflushed_wal_tail: loaded.basis_head_seq < loaded.head.seq,
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
) -> Result<NamespaceStatusResponse> {
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
    Ok(NamespaceStatusResponse {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id: None,
        wal_tail_segments: 0,
        retention_floor_seq,
    })
}
