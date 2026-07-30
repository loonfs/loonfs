//! Per-operation options structs for the engine surface.

use loonfs_api::ChangeSeq;

/// Options for namespace bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

/// Options for namespace deletion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteNamespaceOptions {
    /// Delete only if the head is still at this sequence. A mismatch fails
    /// with `stale_head` instead of deleting work the caller has not seen.
    pub expected_head_seq: Option<ChangeSeq>,
}
