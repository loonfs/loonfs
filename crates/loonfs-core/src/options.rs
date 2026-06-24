use crate::{
    checkpoint::{MetadataTableCache, WalTailProjectionCache},
    namespace::basis::VerifiedNamespaceBasis,
    path::write::PutFileBehavior,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::CommitId;
use std::sync::Arc;

/// Options for namespace bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

/// Options for namespace fork.
///
/// This is currently empty, but kept as the public shape for future fork
/// controls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions {}

/// Options for namespace deletion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteNamespaceOptions {
    /// Delete only if the head is still at this sequence. A mismatch fails
    /// with `stale_head` instead of deleting work the caller has not seen.
    pub expected_head_seq: Option<loonfs_api::ChangeSeq>,
}

/// Controls where read operations get their namespace view.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    source: ReadSource,
}

impl ReadOptions {
    /// Reads from the current manifest plus the visible WAL tail.
    pub fn manifest_plus_tail() -> Self {
        Self {
            source: ReadSource::ManifestPlusTail,
        }
    }

    /// Reuses a caller-supplied verified basis for maintenance/debug callers.
    ///
    /// This is useful when several reads should share the same namespace view.
    pub fn verified_basis(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            source: ReadSource::VerifiedBasis(basis),
        }
    }

    /// Reads from a manifest-plus-tail view pinned to an already-loaded head.
    ///
    /// Runtime code uses this when it has already validated the head and wants
    /// to reuse cache state.
    pub fn manifest_plus_tail_at_head(
        head: HeadState,
        head_etag: String,
        table_cache: Option<Arc<MetadataTableCache>>,
        tail_cache: Option<Arc<WalTailProjectionCache>>,
        max_wal_tail_segments: u64,
    ) -> Self {
        Self {
            source: ReadSource::ManifestPlusTailAtHead {
                head: Box::new(head),
                head_etag,
                table_cache,
                tail_cache,
                max_wal_tail_segments,
            },
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
        Self::manifest_plus_tail()
    }
}

/// Source used by [`ReadOptions`].
#[derive(Debug, Clone)]
pub enum ReadSource {
    /// Use the current manifest and bounded WAL tail.
    ManifestPlusTail,
    /// Use this already-verified namespace basis.
    VerifiedBasis(Arc<VerifiedNamespaceBasis>),
    /// Use manifest-plus-tail for a specific already-loaded head.
    ManifestPlusTailAtHead {
        /// The namespace head to read against.
        head: Box<HeadState>,
        /// Durable head object ETag that validated this head.
        head_etag: String,
        /// Optional decoded table cache.
        table_cache: Option<Arc<MetadataTableCache>>,
        /// Optional WAL-tail projection cache.
        tail_cache: Option<Arc<WalTailProjectionCache>>,
        /// Maximum visible WAL tail segments this read may project.
        max_wal_tail_segments: u64,
    },
}

/// Options for path-oriented writes.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Options for explicit commit submission.
///
/// This is currently empty because the commit request carries the important
/// choices: commit id, preconditions, operations, message, and annotations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitOptions {}
