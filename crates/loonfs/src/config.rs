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

/// The admission limits one writable runtime applies to each namespace's
/// publication requests.
///
/// Every admitted caller counts, including duplicate commits and namespace
/// deletes. Counts and bytes remain charged until the work settles, even if
/// the caller disconnects. Reaching either limit returns
/// `commit_queue_full`. The totals across namespaces, and across the runtimes
/// that share an execution budget, are limits of the
/// [`ExecutionBudget`](crate::ExecutionBudget).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationLimits {
    /// Maximum admitted requests for one namespace. Defaults to 1,024.
    pub max_requests_per_namespace: std::num::NonZeroUsize,
    /// Approximate retained request bytes for one namespace. Defaults to 8 MiB.
    pub max_estimated_bytes_per_namespace: std::num::NonZeroUsize,
}

impl Default for PublicationLimits {
    fn default() -> Self {
        Self {
            max_requests_per_namespace: std::num::NonZeroUsize::new(1024)
                .expect("nonzero namespace request limit"),
            max_estimated_bytes_per_namespace: std::num::NonZeroUsize::new(8 * 1024 * 1024)
                .expect("nonzero namespace byte limit"),
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
#[derive(Debug, Clone)]
pub(crate) struct ReadConfig {
    /// Largest file content the buffered read APIs will materialize for one
    /// call, checked against resolved metadata before any content fetch.
    /// `None` (the embedded default) reads files of any size; servers set
    /// this so one proxied read cannot buffer arbitrarily large content.
    pub max_read_content_bytes: Option<u64>,
    /// Minimum monotonic interval between checks for a successor to a cached
    /// manifest. Zero checks on every read.
    pub manifest_revalidation_interval_ms: u64,
    /// Layout and input limits for metadata compactions.
    pub metadata_lsm_policy: loonfs_core::MetadataLsmPolicy,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}
