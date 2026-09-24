//! Reads namespace state and storage diagnostics.

#[cfg(any(test, feature = "test-support"))]
use crate::error::CoreError;
use crate::error::Result;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::control::ForkBasis;
use loonfs_api::{ActorId, ChangeSeq, ManifestNo, Namespace, NamespaceForkBasis, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Whether a namespace carries visible commits its basis manifest does not
/// cover, and the head sequence they run to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceFlushBasis {
    pub head_seq: ChangeSeq,
    pub has_unflushed_wal_tail: bool,
}

/// Decoded WAL tail usage for test assertions.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceWalTailUsage {
    pub head_seq: ChangeSeq,
    pub wal_tail_segments: u64,
    pub wal_tail_inline_bytes: usize,
}

#[cfg(any(test, feature = "test-support"))]
pub async fn load_namespace_wal_tail_usage<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceWalTailUsage> {
    let loaded = crate::namespace::read_anchor::load_read_anchor(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    super::control::ensure_namespace_live(&loaded.read_state)?;
    let basis =
        crate::checkpoint::load_basis_metadata_segments(store, None, &loaded.basis()).await?;
    let tail = crate::wal::load_replayed_wal_tail(
        store,
        &basis.replay_head(&loaded.read_state),
        &loaded.read_state,
        &basis.base_state,
    )
    .await
    .map_err(CoreError::MetadataProjection)?;
    Ok(NamespaceWalTailUsage {
        head_seq: loaded.read_state.seq,
        wal_tail_segments: loaded.read_state.unfolded_wal_segments(),
        wal_tail_inline_bytes: tail.projected_tail.inline_bytes(),
    })
}

/// Namespace storage diagnostics that do not require checkpoint enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStorageDiagnostics {
    pub namespace_id: NamespaceId,
    pub created_at_ms: u64,
    pub created_by: ActorId,
    pub fork_basis: Option<NamespaceForkBasis>,
    pub head_seq: ChangeSeq,
    pub retention_floor_seq: ChangeSeq,
    pub current_manifest_no: ManifestNo,
    pub wal_tail_segments: u64,
}

impl NamespaceStorageDiagnostics {
    fn new(
        head: NamespaceReadState,
        retention_floor_seq: ChangeSeq,
        current_manifest_no: ManifestNo,
        wal_tail_segments: u64,
    ) -> Self {
        Self {
            created_at_ms: head.created_at_ms,
            created_by: head.created_by,
            fork_basis: fork_basis(head.fork_basis),
            namespace_id: head.namespace_id,
            head_seq: head.seq,
            retention_floor_seq,
            current_manifest_no,
            wal_tail_segments,
        }
    }
}

fn fork_basis(basis: Option<ForkBasis>) -> Option<NamespaceForkBasis> {
    basis.map(|basis| NamespaceForkBasis {
        source_namespace_id: basis.manifest.owner_namespace_id,
        source_head_seq: basis.manifest.head_seq,
    })
}

/// Loads the current state of a live namespace.
pub async fn load_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<Namespace> {
    let anchor = load_read_anchor(store, expected_namespace_id).await?;
    let retention_floor_seq = anchor.retention_floor_seq();
    let head = anchor.read_state;
    super::control::ensure_namespace_live(&head)?;
    Ok(Namespace {
        access: (&head.access).into(),
        created_at_ms: head.created_at_ms,
        created_by: head.created_by,
        fork_basis: fork_basis(head.fork_basis),
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq,
    })
}

pub async fn load_namespace_diagnostics<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceStorageDiagnostics> {
    let loaded = load_read_anchor(store, expected_namespace_id).await?;
    super::control::ensure_namespace_live(&loaded.read_state)?;
    let wal_tail_segments = loaded.read_state.unfolded_wal_segments();
    let retention_floor_seq = loaded.retention_floor_seq();
    let manifest_no = loaded.manifest.state.manifest.manifest_no;
    Ok(NamespaceStorageDiagnostics::new(
        loaded.read_state,
        retention_floor_seq,
        manifest_no,
        wal_tail_segments,
    ))
}

pub async fn load_namespace_flush_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceFlushBasis> {
    let loaded = load_read_anchor(store, expected_namespace_id).await?;
    super::control::ensure_namespace_live(&loaded.read_state)?;
    Ok(NamespaceFlushBasis {
        head_seq: loaded.read_state.seq,
        has_unflushed_wal_tail: loaded.read_state.folded_wal_no < loaded.read_state.wal_no,
    })
}
