use crate::{
    manifest::MetadataTableCache, namespace::basis::VerifiedNamespaceBasis,
    path::write::PutFileBehavior,
};
use loon_api::wire::control::HeadState;
use loon_api::CommitId;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Options for namespace bootstrap.
pub struct BootstrapOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Options for namespace fork.
///
/// This is currently empty, but kept as the public shape for future fork
/// controls.
pub struct ForkOptions {}

#[derive(Debug, Clone)]
/// Controls where read operations get their namespace view.
///
/// The default prefers materialized metadata tables when they are available,
/// then falls back to rebuilding the full verified basis.
pub struct ReadOptions {
    source: ReadSource,
}

impl ReadOptions {
    /// Prefers fast materialized tables, with safe fallback to full basis reads.
    pub fn prefer_materialized() -> Self {
        Self {
            source: ReadSource::PreferMaterialized,
        }
    }

    /// Forces a complete verified basis reconstruction before reading.
    pub fn full_basis() -> Self {
        Self {
            source: ReadSource::FullBasis,
        }
    }

    /// Reuses a caller-supplied verified basis.
    ///
    /// This is useful when several reads should share the same namespace view.
    pub fn verified_basis(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            source: ReadSource::VerifiedBasis(basis),
        }
    }

    /// Reads from materialized tables pinned to an already-loaded head.
    ///
    /// Runtime code uses this when it has already validated the head and wants
    /// to reuse table cache state.
    pub fn materialized_tables_at_head(
        head: HeadState,
        table_cache: Option<Arc<MetadataTableCache>>,
    ) -> Self {
        Self {
            source: ReadSource::MaterializedTablesAtHead { head, table_cache },
        }
    }

    /// Returns the selected read source.
    pub fn source(&self) -> &ReadSource {
        &self.source
    }

    pub(crate) fn into_source(self) -> ReadSource {
        self.source
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self::prefer_materialized()
    }
}

#[derive(Debug, Clone)]
/// Source used by [`ReadOptions`].
pub enum ReadSource {
    /// Use materialized tables when possible, otherwise rebuild the full basis.
    PreferMaterialized,
    /// Always rebuild the full verified namespace basis.
    FullBasis,
    /// Use this already-verified namespace basis.
    VerifiedBasis(Arc<VerifiedNamespaceBasis>),
    /// Use materialized tables for a specific already-loaded head.
    MaterializedTablesAtHead {
        /// The namespace head to read against.
        head: HeadState,
        /// Optional decoded table cache.
        table_cache: Option<Arc<MetadataTableCache>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Options for path-oriented writes.
pub struct WriteOptions {
    /// Optional caller-provided idempotency key.
    ///
    /// If omitted, path helpers generate one internally.
    pub commit_id: Option<CommitId>,
    /// Whether a file put may replace an existing file.
    pub put_file_behavior: PutFileBehavior,
    /// Whether delete may remove a non-empty subtree.
    pub recursive_delete: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            commit_id: None,
            put_file_behavior: PutFileBehavior::CreateOnly,
            recursive_delete: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Options for explicit commit submission.
///
/// This is currently empty because the commit request carries the important
/// choices: commit id, preconditions, operations, message, and annotations.
pub struct CommitOptions {}
