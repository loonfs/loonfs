//! Runtime limits and read policy.

use crate::trace::{TraceMode, TraceStoreKind};

/// Default minimum interval, in milliseconds, between checks for a successor
/// to a cached manifest.
pub(crate) const DEFAULT_MANIFEST_REVALIDATION_INTERVAL_MS: u64 = 1000;
/// Default minimum interval, in milliseconds, between publication starts
/// for one namespace (see [`crate::publisher`]). A request to an idle
/// namespace publishes immediately; the interval only paces requests that
/// queued behind a publish, so concurrent submissions amortize into fewer,
/// larger WAL objects. Zero
/// keeps only the batching that in-flight publications force.
pub(crate) const DEFAULT_MIN_PUBLISH_INTERVAL_MS: u64 = 15;
/// Default maximum WAL-tail folds one writer runs concurrently.
pub const DEFAULT_MAX_CONCURRENT_FOLDS: usize = 2;
/// Default maximum metadata merges, bounded or streaming, one writer runs
/// concurrently.
pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 2;
/// Default cap on concurrently running maintenance invocations.
/// Each job already runs at most once per namespace at a time; this bounds how many may run at
/// once, so a write burst across many namespaces cannot fan out into
/// unbounded concurrent maintenance. A run that waits for a permit is not
/// dropped: it takes the next one that frees.
pub const DEFAULT_MAX_CONCURRENT_MAINTENANCE: usize = 2;

/// Shared limits for queued and active publications owned by one writer.
///
/// Every admitted caller counts, including duplicate commits and namespace
/// deletes. Counts and bytes remain charged until the work settles, even if
/// the caller disconnects. Reaching either admission limit returns
/// `commit_queue_full`; admitted work waits for a publication slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationLimits {
    /// Maximum admitted requests across all namespaces. Defaults to 8,192.
    pub max_requests: std::num::NonZeroUsize,
    /// Maximum admitted requests for one namespace. Defaults to 1,024.
    pub max_requests_per_namespace: std::num::NonZeroUsize,
    /// Approximate retained request bytes across all namespaces. Defaults to
    /// 64 MiB. This includes request data, prepared proofs, and waiter overhead;
    /// it is not a bound on allocator capacity or working publication memory.
    pub max_estimated_bytes: std::num::NonZeroUsize,
    /// Approximate retained request bytes for one namespace. Defaults to 8 MiB.
    pub max_estimated_bytes_per_namespace: std::num::NonZeroUsize,
    /// Maximum publication batches or deletes running at once. Defaults to 8.
    pub max_concurrent_publications: std::num::NonZeroUsize,
}

impl Default for PublicationLimits {
    fn default() -> Self {
        Self {
            max_requests: std::num::NonZeroUsize::new(8192).expect("nonzero request limit"),
            max_requests_per_namespace: std::num::NonZeroUsize::new(1024)
                .expect("nonzero namespace request limit"),
            max_estimated_bytes: std::num::NonZeroUsize::new(64 * 1024 * 1024)
                .expect("nonzero request byte limit"),
            max_estimated_bytes_per_namespace: std::num::NonZeroUsize::new(8 * 1024 * 1024)
                .expect("nonzero namespace byte limit"),
            max_concurrent_publications: std::num::NonZeroUsize::new(8)
                .expect("nonzero publication limit"),
        }
    }
}

/// Writer policy for content carried in WAL objects.
/// Retrying a completed inline commit does not upload its content again while
/// the commit receipt is retained. See `docs/specs/api.md`, section 5.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineContentPolicy {
    /// Maximum size prepared inline; defaults to 64 KiB. `None` disables inline preparation.
    pub inline_content_threshold_bytes: Option<usize>,
    /// Maximum inline bytes in one WAL object; defaults to 1 MiB.
    pub inline_content_wal_object_budget_bytes: usize,
    /// Unfolded inline bytes that make a fold due; defaults to 2 MiB.
    pub inline_content_fold_at_bytes: usize,
    /// Limit on unfolded and admitted inline bytes; defaults to 32 MiB.
    /// Projection invalidation keeps the last count this session observed, and
    /// that count includes a put whose outcome is unknown. A session that has
    /// not observed the tail admits at most the WAL object budget. The tail can
    /// exceed the limit by at most the WAL object budget: for a new session's
    /// first inline commit, and after a put whose outcome is unknown.
    /// `MAX_UNFOLDED_WAL_OBJECTS` stops new commits regardless of this limit.
    pub inline_content_tail_limit_bytes: usize,
}

impl Default for InlineContentPolicy {
    fn default() -> Self {
        Self {
            inline_content_threshold_bytes: Some(64 * 1024),
            inline_content_wal_object_budget_bytes: 1024 * 1024,
            inline_content_fold_at_bytes: 2 * 1024 * 1024,
            inline_content_tail_limit_bytes: 32 * 1024 * 1024,
        }
    }
}

impl InlineContentPolicy {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        use loonfs_types::format::wal::{
            MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
        };
        if self
            .inline_content_threshold_bytes
            .is_some_and(|value| value > MAX_WAL_INLINE_CONTENT_BYTES)
        {
            return Err(crate::Error::Config(format!(
                "`inline_content_threshold_bytes` must not exceed {MAX_WAL_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_wal_object_budget_bytes == 0
            || self.inline_content_wal_object_budget_bytes > MAX_WAL_OBJECT_INLINE_CONTENT_BYTES
        {
            return Err(crate::Error::Config(format!(
                "`inline_content_wal_object_budget_bytes` must be between 1 and {MAX_WAL_OBJECT_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_fold_at_bytes == 0 || self.inline_content_tail_limit_bytes == 0 {
            return Err(crate::Error::Config(
                "`inline_content_fold_at_bytes` and `inline_content_tail_limit_bytes` must be greater than zero".to_owned()
            ));
        }
        if self.inline_content_fold_at_bytes > self.inline_content_tail_limit_bytes {
            return Err(crate::Error::Config(
                "`inline_content_fold_at_bytes` must not exceed `inline_content_tail_limit_bytes`"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Read configuration shared by all handles of one runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadConfig {
    /// Largest file content the buffered read APIs will materialize for one
    /// call, checked against resolved metadata before any content fetch.
    /// `None` (the embedded default) reads files of any size; servers set
    /// this so one proxied read cannot buffer arbitrarily large content.
    pub max_read_content_bytes: Option<u64>,
    /// Minimum monotonic interval between checks for a successor to a cached
    /// manifest. Zero checks on every read.
    pub manifest_revalidation_interval_ms: u64,
    /// Budgets for WAL folds and metadata compactions, and the block memo
    /// budget of every read, publication, and fold.
    pub metadata_lsm_policy: loonfs_core::MetadataLsmPolicy,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}
