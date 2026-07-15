//! Runtime configuration: writer identity, lease duration, and cache
//! sizing, with the defaults the rest of the crate advertises.

use crate::trace::{TraceMode, TraceStoreKind};
use crate::{GramIndexBuildPolicy, MetadataTableCacheConfig, Result, RuntimeError};

/// Default lease duration for write operations, in milliseconds.
/// Default visible WAL-tail length, in segments, at which a maintenance tick
/// publishes a checkpoint.
pub const DEFAULT_MAX_WAL_TAIL_SEGMENTS: u64 = 32;
/// Default maximum namespaces retained in runtime caches.
pub const DEFAULT_MAX_CACHED_NAMESPACES: usize = 64;
/// Default maximum metadata rows retained across cached WAL-tail projections.
pub const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS: usize =
    loonfs_core::cache::DEFAULT_WAL_TAIL_PROJECTION_ROWS;
/// Default decoded-byte budget for cached WAL-tail projections.
pub const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES: usize =
    loonfs_core::cache::DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES;
/// Default minimum interval, in milliseconds, between publication starts
/// for one namespace (see [`crate::publisher`]). A cold namespace
/// publishes immediately; the interval only paces follow-up batches, so
/// concurrent submissions amortize into fewer, larger WAL segments. Zero
/// keeps only the batching that in-flight publications force.
pub const DEFAULT_MIN_PUBLISH_INTERVAL_MS: u64 = 15;

/// Configuration for one runtime core, assembled by the handle builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FsConfig {
    /// Writer id used for namespace epoch acquisition and commits.
    pub writer_id: String,
    /// Writer version reported in mutation context.
    pub writer_version: String,
    /// Minimum interval between publication starts per namespace, in
    /// milliseconds; zero keeps only the batching that in-flight
    /// publications force.
    pub min_publish_interval_ms: u64,
    /// Cache configuration.
    pub runtime_cache: RuntimeCacheConfig,
    /// Budgets for the gram index build and fold steps this runtime's
    /// maintenance ticks run. Stored normalized (every budget at least one
    /// unit), so the config reads as the policy that will run.
    pub gram_index_build: GramIndexBuildPolicy,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}

/// Cache configuration for the embedded runtime. Every cache disables the
/// same way: a zero budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCacheConfig {
    /// Maximum namespaces retained in runtime caches (control anchors,
    /// catalogs, commit engines, and WAL-tail projection entries).
    /// Setting this to zero disables those caches — a diagnostic mode that
    /// also disables post-publish auto-maintenance, leaving only the hard
    /// publish backpressure. Not for production use.
    pub max_cached_namespaces: usize,
    /// Maximum metadata rows retained across cached WAL-tail projections;
    /// zero disables the projection cache.
    pub max_cached_wal_tail_projection_rows: usize,
    /// Approximate decoded-byte budget for cached WAL-tail projections.
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

pub(crate) fn default_writer_version() -> String {
    format!("loonfs/{}", env!("CARGO_PKG_VERSION"))
}

pub(crate) fn validate_config(config: &FsConfig) -> Result<()> {
    if config.writer_id.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_id must not be empty".to_owned(),
        ));
    }
    if config.writer_version.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_version must not be empty".to_owned(),
        ));
    }
    Ok(())
}
