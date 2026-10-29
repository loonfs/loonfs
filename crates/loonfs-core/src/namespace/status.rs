//! Reads namespace state and storage diagnostics.

#[cfg(any(test, feature = "test-support"))]
use crate::error::CoreError;
use crate::error::Result;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::Observation;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ForkBasis;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{
    ActorId, ChangeSeq, ManifestNo, NamespaceForkBasis, NamespaceId, NamespaceMetadata,
};
use std::num::NonZeroU64;
use std::sync::Arc;

/// Decoded WAL tail usage for test assertions.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceWalTailUsage {
    pub head_seq: ChangeSeq,
    pub wal_tail_objects: u64,
    pub wal_tail_inline_bytes: usize,
}

impl<S: ObjectStore> NamespaceStore<S> {
    #[cfg(any(test, feature = "test-support"))]
    pub async fn load_wal_tail_usage(&self) -> Result<NamespaceWalTailUsage> {
        let loaded = self
            .load_read_anchor(crate::test_support::observed_now())
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        super::control::ensure_namespace_live(&loaded.read_state)?;
        let basis =
            crate::manifest::load_basis_metadata_segments(&self.store, None, &loaded.basis())
                .await?;
        let tail = crate::wal::replay_discovered_tail(
            &basis.replay_head(&loaded.read_state),
            &basis.base_state,
            &loaded.tail,
        )
        .map_err(CoreError::MetadataProjection)?;
        Ok(NamespaceWalTailUsage {
            head_seq: loaded.read_state.seq,
            wal_tail_objects: loaded.read_state.unfolded_wal_objects(),
            wal_tail_inline_bytes: tail.projected_tail.inline_bytes(),
        })
    }

    /// Loads the current state of a live namespace.
    pub async fn load_metadata(&self) -> Result<NamespaceMetadata> {
        let anchor = self
            .load_read_anchor(Observation::now(Arc::clone(&self.timer)))
            .await?;
        super::control::ensure_namespace_live(&anchor.read_state)?;
        Ok(namespace_metadata(
            anchor.read_state,
            anchor.manifest.state.envelope.payload(),
        ))
    }

    pub async fn load_diagnostics(&self) -> Result<NamespaceStorageDiagnostics> {
        let anchor = self
            .load_live_read_anchor(Observation::now(Arc::clone(&self.timer)))
            .await?;
        Ok(NamespaceStorageDiagnostics::from(&anchor))
    }
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
    pub acknowledged_floor_seq: ChangeSeq,
    pub current_manifest_no: ManifestNo,
    pub wal_tail_objects: u64,
    pub wal_tail_newest_commit_at_ms: Option<u64>,
}

fn fork_basis(basis: Option<ForkBasis>) -> Option<NamespaceForkBasis> {
    basis.map(|basis| NamespaceForkBasis {
        source_namespace_id: basis.manifest.owner_namespace_id,
        source_head_seq: basis.manifest.head_seq,
    })
}

/// Returns the state of a namespace that this call created with `manifest`.
pub(crate) fn created_namespace(manifest: &NamespaceManifestPayload) -> NamespaceMetadata {
    namespace_metadata(NamespaceReadState::from(manifest), manifest)
}

/// Combines the head with the retention state of `manifest`, which is the
/// current manifest below that head.
fn namespace_metadata(
    head: NamespaceReadState,
    manifest: &NamespaceManifestPayload,
) -> NamespaceMetadata {
    NamespaceMetadata {
        access: (&head.access).into(),
        created_at_ms: head.created_at_ms,
        created_by: head.created_by,
        fork_basis: fork_basis(head.fork_basis),
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        retention_floor_seq: manifest.retention_floor_seq,
        retention_window_ms: manifest.retention_window_ms.map(NonZeroU64::get),
    }
}

impl From<&NamespaceReadAnchor> for NamespaceStorageDiagnostics {
    fn from(anchor: &NamespaceReadAnchor) -> Self {
        let head = &anchor.read_state;
        Self {
            namespace_id: head.namespace_id.clone(),
            created_at_ms: head.created_at_ms,
            created_by: head.created_by.clone(),
            fork_basis: fork_basis(head.fork_basis.clone()),
            head_seq: head.seq,
            retention_floor_seq: anchor.retention_floor_seq(),
            acknowledged_floor_seq: anchor
                .manifest
                .state
                .envelope
                .payload()
                .acknowledged_floor_seq,
            current_manifest_no: anchor.manifest.state.manifest().manifest_no,
            wal_tail_objects: head.unfolded_wal_objects(),
            wal_tail_newest_commit_at_ms: anchor.tail.newest_commit_at_ms(),
        }
    }
}
