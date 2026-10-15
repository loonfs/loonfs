//! Per-operation options structs for the engine surface.

use loonfs_types::ChangeSeq;

/// Options for creating a namespace.
///
/// The client's `CreateNamespaceOptions` has no `allow_existing`, because the
/// create request on the wire has no field for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNamespaceOptions {
    /// Access mode the namespace is created with.
    pub access: loonfs_types::NamespaceAccess,
    /// How sibling names compare in the namespace.
    pub naming: loonfs_types::NamespaceNaming,
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

impl Default for CreateNamespaceOptions {
    fn default() -> Self {
        Self {
            access: loonfs_types::NamespaceAccess::Unrestricted {},
            naming: loonfs_types::NamespaceNaming::CaseInsensitive,
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
