//! Runtime configuration: read-side limits and cache sizing, plus the
//! writer-identity validation the write-capable builders apply, with the
//! defaults the rest of the crate advertises.

use crate::trace::{TraceMode, TraceStoreKind};
use crate::{MetadataTableCacheConfig, Result, RuntimeError};

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
/// Default cap on concurrently running writer-scheduled maintenance steps,
/// across every job and namespace of one handle. Each job already runs at
/// most one step per namespace at a time; this bounds how many may step at
/// once, so a write burst across many namespaces cannot fan out into
/// unbounded concurrent maintenance. A step that waits for a permit is not
/// dropped: it takes the next one that frees.
pub const DEFAULT_MAX_CONCURRENT_MAINTENANCE: usize = 2;

/// Configuration for one read core, assembled by the handle builders.
///
/// Everything here governs reading and caching, so every handle carries it.
/// Writer-side settings — the actor identity and publication pacing — belong
/// to the write-capable handles and never reach a reader.
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
    /// Maximum namespaces each entry-counted runtime cache retains: the
    /// head anchors, the read-side WAL-tail projection entries, and the tail
    /// projections a writer's namespace publishers hold. Setting this to
    /// zero disables those caches — a diagnostic mode that trades speed for
    /// re-reads. Cache settings never change behavior: maintenance
    /// scheduling is controlled only by
    /// [`FsBackgroundWork`](crate::FsBackgroundWork).
    ///
    /// It does not bound what a writer keeps per namespace it has mutated.
    /// One writer session — the epoch it acquired and its terminal fencing
    /// record — and the empty publisher around it are retained for every
    /// live namespace, at a few dozen bytes each. That is deliberate:
    /// nothing in the store can rebuild "this session was fenced", so
    /// evicting it would let a fenced writer reacquire the epoch (see
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
    /// Cache settings for decoded metadata tables.
    pub metadata_table_cache: MetadataTableCacheConfig,
}

impl RuntimeCacheConfig {
    /// Disables runtime caches by zeroing every budget.
    pub fn disabled() -> Self {
        Self {
            max_cached_namespaces: 0,
            max_cached_wal_tail_projection_rows: 0,
            max_cached_wal_tail_projection_decoded_bytes: 0,
            metadata_table_cache: MetadataTableCacheConfig {
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
            metadata_table_cache: MetadataTableCacheConfig::default(),
        }
    }
}

/// Checks the actor identity a write-capable handle will publish under.
///
/// Only the writer and admin builders call this; a reader has no identity to
/// check.
pub(crate) fn validate_writer_id(writer_id: &str) -> Result<()> {
    if writer_id.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_id must not be empty".to_owned(),
        ));
    }
    Ok(())
}
