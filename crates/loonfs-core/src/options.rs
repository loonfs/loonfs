//! Per-operation options structs for the engine surface.

use loonfs_types::ChangeSeq;
use std::num::NonZeroU64;

/// Options for creating a namespace.
///
/// The client's `CreateNamespaceOptions` has no `allow_existing`, because the
/// create request on the wire has no field for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNamespaceOptions {
    /// Access mode the namespace is created with.
    pub access: loonfs_types::NamespaceAccess,
    /// The namespace's retention window, which a host's scheduled retention
    /// advance uses instead of the host's own setting.
    pub retention_window_ms: Option<NonZeroU64>,
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

impl Default for CreateNamespaceOptions {
    fn default() -> Self {
        Self {
            access: loonfs_types::NamespaceAccess::Unrestricted {},
            retention_window_ms: None,
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
