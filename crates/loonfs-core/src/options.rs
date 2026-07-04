use loonfs_api::{CommitId, DeleteDirectoryBehavior, PutBehavior};

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
    pub expected_head_seq: Option<loonfs_api::ChangeSeq>,
}

/// Options for path-oriented writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    /// Optional caller-provided idempotency key.
    ///
    /// If omitted, path helpers generate one internally.
    pub commit_id: Option<CommitId>,
    /// Whether a file put may replace an existing file.
    pub put_behavior: PutBehavior,
    /// Whether delete may remove a non-empty subtree.
    pub delete_behavior: DeleteDirectoryBehavior,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            commit_id: None,
            put_behavior: PutBehavior::NoReplace,
            delete_behavior: DeleteDirectoryBehavior::NonRecursive,
        }
    }
}
