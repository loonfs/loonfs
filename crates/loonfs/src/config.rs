//! Runtime configuration: writer identity, lease duration, and cache
//! sizing, with the defaults the rest of the crate advertises.

use crate::trace::{TraceMode, TraceStoreKind};
use crate::{MetadataTableCacheConfig, Result, RuntimeError};

/// Default lease duration for write operations, in milliseconds.
/// Default visible WAL-tail length, in segments, at which a maintenance tick
/// publishes a checkpoint.
pub const DEFAULT_MAX_WAL_TAIL_SEGMENTS: u64 = 32;
/// Default maximum namespaces retained in runtime caches.
pub const DEFAULT_MAX_CACHED_NAMESPACES: usize = 64;
/// Default maximum metadata rows retained across cached WAL-tail projections.
pub const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS: usize = 1_000_000;
/// Default decoded-byte budget for cached WAL-tail projections.
pub const DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES: usize = 256 * 1024 * 1024;
/// Default commit window, in milliseconds: how long a direct publish holds
/// its namespace's window open so concurrent publishes can join the same
/// flush — one WAL segment, one head CAS. Zero disables coalescing.
pub const DEFAULT_COMMIT_WINDOW_MS: u64 = 15;

/// Configuration for one runtime core, assembled by the handle builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FsConfig {
    /// Writer id used for namespace epoch acquisition and commits.
    pub writer_id: String,
    /// Writer version reported in mutation context.
    pub writer_version: String,
    /// Commit window for direct publishes, in milliseconds; zero disables
    /// coalescing.
    pub commit_window_ms: u64,
    /// Cache configuration.
    pub runtime_cache: RuntimeCacheConfig,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}

/// Cache configuration for the embedded runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCacheConfig {
    /// Enables WAL-tail projection caching.
    pub wal_tail_projection_cache_enabled: bool,
    /// Enables namespace control-object caching.
    pub control_cache_enabled: bool,
    /// Maximum namespaces retained in runtime caches.
    /// Setting this to zero disables the commit-engine cache — a diagnostic
    /// mode that also disables post-publish auto-maintenance, leaving only
    /// the hard publish backpressure. Not for production use.
    pub max_cached_namespaces: usize,
    /// Maximum metadata rows retained across cached WAL-tail projections.
    pub max_cached_wal_tail_projection_rows: usize,
    /// Approximate decoded-byte budget for cached WAL-tail projections.
    pub max_cached_wal_tail_projection_decoded_bytes: Option<usize>,
    /// Cache settings for decoded metadata tables.
    pub metadata_table_cache: MetadataTableCacheConfig,
}

impl RuntimeCacheConfig {
    /// Disables runtime caches.
    pub fn disabled() -> Self {
        Self {
            wal_tail_projection_cache_enabled: false,
            control_cache_enabled: false,
            max_cached_namespaces: 0,
            max_cached_wal_tail_projection_rows: 0,
            max_cached_wal_tail_projection_decoded_bytes: Some(0),
            metadata_table_cache: MetadataTableCacheConfig {
                enabled: false,
                max_blocks: 0,
                max_decoded_bytes: Some(0),
            },
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            wal_tail_projection_cache_enabled: true,
            control_cache_enabled: true,
            max_cached_namespaces: DEFAULT_MAX_CACHED_NAMESPACES,
            max_cached_wal_tail_projection_rows: DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS,
            max_cached_wal_tail_projection_decoded_bytes: Some(
                DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES,
            ),
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
