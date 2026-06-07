//! Namespace control helpers.
//!
//! A namespace is one durable filesystem history. Most callers should use
//! [`crate::NamespaceEngine`]; these helpers are for runtime and admin code
//! that needs direct namespace inspection.

pub(crate) mod basis;
pub(crate) mod bootstrap;
pub(crate) mod catalog;
pub(crate) mod control;
pub(crate) mod fork;
pub(crate) mod lease;

pub use bootstrap::BootstrapNamespaceError;
pub use loon_api::wire::control::{HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope};
use loon_api::{FenceToken, NamespaceSummary};
use loon_objectstore::ObjectStore;
use thiserror::Error;

/// Lists complete namespaces in the object store.
pub fn list_namespaces<S: ObjectStore + ?Sized>(store: &S) -> crate::Result<Vec<NamespaceSummary>> {
    catalog::list_namespaces(store)
}

/// Returns true when the head and lease agree on the active writer fence.
pub fn head_and_lease_fence_tokens_agree(head: &HeadState, lease: &LeaseState) -> bool {
    head.namespace_id == lease.namespace_id && head.active_fence_token == lease.fence_token
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
/// Error while preparing a head for writer takeover.
pub enum HeadFenceTakeoverError {
    /// The fence token cannot be advanced.
    #[error("head fence token overflow from `{active:?}`")]
    FenceTokenOverflow { active: FenceToken },
}

/// Builds the head state a new writer should publish during takeover.
pub fn next_takeover_head(current_head: &HeadState) -> Result<HeadState, HeadFenceTakeoverError> {
    let next_fence = current_head.active_fence_token.0.checked_add(1).ok_or(
        HeadFenceTakeoverError::FenceTokenOverflow {
            active: current_head.active_fence_token,
        },
    )?;

    Ok(HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: current_head.seq,
        active_fence_token: FenceToken(next_fence),
        next_inode_id: current_head.next_inode_id,
        name_policy: current_head.name_policy,
        checkpoint_hint_seq: current_head.checkpoint_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: current_head.visible_wal_tip.clone(),
    })
}
