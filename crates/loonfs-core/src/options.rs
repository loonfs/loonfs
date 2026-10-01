//! Per-operation options structs for the engine surface.

use loonfs_api::ChangeSeq;

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNamespaceOptions {
    /// Access mode the namespace is created with.
    pub access: loonfs_api::NamespaceAccess,
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

impl Default for CreateNamespaceOptions {
    fn default() -> Self {
        Self {
            access: loonfs_api::NamespaceAccess::Unrestricted {},
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

#[cfg(test)]
mod tests {
    use super::CreateNamespaceOptions;

    #[test]
    fn create_namespace_options_default_to_an_absent_unrestricted_namespace() {
        assert_eq!(
            CreateNamespaceOptions::default(),
            CreateNamespaceOptions {
                access: loonfs_api::NamespaceAccess::Unrestricted {},
                allow_existing: false,
            }
        );
    }
}
