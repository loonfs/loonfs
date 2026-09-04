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
            max_cached_namespaces: DEFAULT_MAX_CACHED_NAMESPACES,
            max_cached_wal_tail_projection_rows: DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS,
            max_cached_wal_tail_projection_decoded_bytes:
                DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES,
            metadata_segment_cache: MetadataSegmentCacheConfig::default(),
        }
    }
}
