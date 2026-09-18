//! Runtime limits and cache sizing.

use crate::trace::{TraceMode, TraceStoreKind};
use crate::MetadataSegmentCacheConfig;

/// Default maximum namespaces retained in runtime caches.
pub(crate) const DEFAULT_MAX_CACHED_NAMESPACES: usize = 64;
/// Default maximum metadata rows retained across cached WAL-tail projections.
pub(crate) const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS: usize =
    loonfs_core::cache::DEFAULT_WAL_TAIL_PROJECTION_ROWS;
/// Default decoded-byte budget for cached WAL-tail projections.
pub(crate) const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES: usize =
    loonfs_core::cache::DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES;
/// Default minimum interval, in milliseconds, between publication starts
/// for one namespace (see [`crate::publisher`]). A cold namespace
/// publishes immediately; the interval only paces follow-up batches, so
/// concurrent submissions amortize into fewer, larger WAL segments. Zero
/// keeps only the batching that in-flight publications force.
pub(crate) const DEFAULT_MIN_PUBLISH_INTERVAL_MS: u64 = 15;
/// Default maximum writer sessions held at once.
pub const DEFAULT_MAX_WRITER_SESSIONS: usize = 10_000;
/// Default maximum WAL-tail folds one writer runs concurrently.
pub const DEFAULT_MAX_CONCURRENT_FOLDS: usize = 2;
/// Default maximum streaming metadata compactions one job runs concurrently.
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

/// Writer policy for content carried in WAL segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineContentOptions {
    /// Maximum size prepared inline; defaults to 64 KiB. `None` disables inline preparation.
    pub inline_content_threshold_bytes: Option<usize>,
    /// Maximum inline bytes in one WAL segment; defaults to 1 MiB.
    pub inline_content_segment_budget_bytes: usize,
    /// Unfolded inline bytes that make a fold due; defaults to 2 MiB.
    pub inline_content_fold_at_bytes: usize,
    /// Limit on known unfolded and admitted inline bytes; defaults to 32 MiB.
    /// An absent projection counts as zero. After a process starts, the first
    /// inline commit in a namespace can exceed this limit by at most its own
    /// inline bytes, at most the segment budget, once per namespace per process start.
    pub inline_content_tail_limit_bytes: usize,
}

impl Default for InlineContentOptions {
    fn default() -> Self {
        Self {
            inline_content_threshold_bytes: Some(64 * 1024),
            inline_content_segment_budget_bytes: 1024 * 1024,
            inline_content_fold_at_bytes: 2 * 1024 * 1024,
            inline_content_tail_limit_bytes: 32 * 1024 * 1024,
        }
    }
}

impl InlineContentOptions {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        use loonfs_api::wire::wal::{
            MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES,
        };
        if self
            .inline_content_threshold_bytes
            .is_some_and(|value| value > MAX_WAL_INLINE_CONTENT_BYTES)
        {
            return Err(crate::RuntimeError::Config(format!(
                "`inline_content_threshold_bytes` must not exceed {MAX_WAL_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_segment_budget_bytes == 0
            || self.inline_content_segment_budget_bytes > MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES
        {
            return Err(crate::RuntimeError::Config(format!(
                "`inline_content_segment_budget_bytes` must be between 1 and {MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_fold_at_bytes == 0 || self.inline_content_tail_limit_bytes == 0 {
            return Err(crate::RuntimeError::Config(
                "`inline_content_fold_at_bytes` and `inline_content_tail_limit_bytes` must be greater than zero".to_owned()
            ));
        }
        if self.inline_content_fold_at_bytes > self.inline_content_tail_limit_bytes {
            return Err(crate::RuntimeError::Config(
                "`inline_content_fold_at_bytes` must not exceed `inline_content_tail_limit_bytes`"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Read and cache configuration shared by all handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadConfig {
    /// Largest file content the buffered read APIs will materialize for one
    /// call, checked against resolved metadata before any content fetch.
    /// `None` (the embedded default) reads files of any size; servers set
    /// this so one proxied read cannot buffer arbitrarily large content.
    pub max_read_content_bytes: Option<u64>,
    /// Cache configuration.
    pub runtime_cache: RuntimeCacheConfig,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}

/// Cache configuration for the embedded runtime. Every cache disables the
/// same way: a zero budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCacheConfig {
    /// Minimum monotonic interval between checks for a successor to the cached manifest.
    /// Also paces the writer's hint raise after publication. Defaults to 1000 milliseconds;
    /// zero checks on every read.
    pub manifest_revalidation_interval_ms: u64,
    /// Maximum namespaces retained by entry-counted runtime caches. Zero
    /// disables those caches. This does not affect maintenance scheduling.
    ///
    /// Writer session state is retained separately because an evicted fenced
    /// session could otherwise reacquire the epoch (see
    /// [`WriterSessionState`](loonfs_core::publish::WriterSessionState)).
    pub max_cached_namespaces: usize,
    /// Maximum metadata rows retained across WAL-tail projections. The read
    /// cache and the publish side each hold their own total against it, so
    /// this is the ceiling per side rather than for the process. Zero
    /// disables the projection cache.
    pub max_cached_wal_tail_projection_rows: usize,
    /// Approximate decoded-byte budget for WAL-tail projections, per side
    /// like the row budget. Both budgets also cap one projection: a publish
    /// whose tail outgrows either keeps nothing.
    pub max_cached_wal_tail_projection_decoded_bytes: usize,
    /// Cache settings for decoded metadata segments.
    pub metadata_segment_cache: MetadataSegmentCacheConfig,
}

impl RuntimeCacheConfig {
    /// Disables runtime caches by zeroing every budget.
    pub fn disabled() -> Self {
        Self {
            manifest_revalidation_interval_ms: 1000,
            max_cached_namespaces: 0,
            max_cached_wal_tail_projection_rows: 0,
            max_cached_wal_tail_projection_decoded_bytes: 0,
            metadata_segment_cache: MetadataSegmentCacheConfig {
                max_decoded_bytes: 0,
            },
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            manifest_revalidation_interval_ms: 1000,
            max_cached_namespaces: DEFAULT_MAX_CACHED_NAMESPACES,
            max_cached_wal_tail_projection_rows: DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS,
            max_cached_wal_tail_projection_decoded_bytes:
                DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES,
            metadata_segment_cache: MetadataSegmentCacheConfig::default(),
        }
    }
}
