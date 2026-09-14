//! Per-operation options structs for the engine surface.

use loonfs_api::ChangeSeq;

/// Options for namespace bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapOptions {
    /// Application-supplied actor creating the namespace.
    pub actor_id: loonfs_api::ActorId,
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

impl BootstrapOptions {
    /// Requires an absent namespace.
    pub fn new(actor_id: loonfs_api::ActorId) -> Self {
        Self {
            actor_id,
            allow_existing: false,
        }
    }
}

/// Options for namespace deletion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteNamespaceOptions {
    /// Delete only if the head is still at this sequence. A mismatch fails
    /// with `stale_head` instead of deleting work the caller has not seen.
    pub expected_head_seq: Option<ChangeSeq>,
}
