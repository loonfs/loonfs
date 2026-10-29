//! The server config file's `[metadata_cache]` and `[grep]` tables.

use super::JobMode;
use crate::{Result, ServerError};
use loonfs::SsdSegmentBlocks;
use loonfs_grep::GrepWorkerConfig;
use loonfs_types::config::ConfigFieldError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

/// The server's `[metadata_cache]` table: the limits of the one metadata
/// cache the server builds at startup.
///
/// Omitted fields are derived when sized, except `max_manifest_bytes` and
/// `max_head_bytes`, which default to 32 MiB and 16 MiB with or without
/// sizing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheConfig {
    pub max_segment_bytes: Option<usize>,
    pub max_manifest_bytes: Option<usize>,
    pub max_head_bytes: Option<usize>,
    pub max_wal_tail_bytes: Option<usize>,
    pub max_content_object_bytes: Option<usize>,
    /// The SSD tier of the metadata segment cache. The server has no SSD
    /// tier when the table is absent.
    pub ssd: Option<MetadataCacheSsdConfig>,
}

/// The server's `[metadata_cache.ssd]` table: where the SSD tier of the
/// metadata segment cache keeps its blocks, and how many bytes of blocks it
/// keeps. Both settings are required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheSsdConfig {
    /// The directory on the server's local SSD that the tier writes its
    /// blocks to. The server creates it if needed.
    pub directory: String,
    /// The most bytes of blocks that the tier keeps on disk, for every
    /// namespace that the server serves.
    pub max_segment_bytes: NonZeroUsize,
}

impl MetadataCacheSsdConfig {
    /// Opens the SSD tier in the configured directory, which reads the
    /// names of the files that the directory already has.
    pub(crate) async fn open(&self) -> Result<SsdSegmentBlocks> {
        SsdSegmentBlocks::open(&self.directory, self.max_segment_bytes)
            .await
            .map_err(|error| {
                self.invalid_directory(format!(
                    "the ssd tier can't open {:?}: {error}",
                    self.directory
                ))
            })
    }

    /// Creates the directory if it's missing, and checks that the server can
    /// write a file in it, by writing and removing a probe file. This is the
    /// SSD tier's part of `--check-config`.
    pub(crate) fn check_directory(&self) -> Result<()> {
        let directory = Path::new(&self.directory);
        fs::create_dir_all(directory).map_err(|error| {
            self.invalid_directory(format!(
                "the ssd tier can't create {:?}: {error}",
                self.directory
            ))
        })?;
        let probe = directory.join("check-config.probe");
        fs::write(&probe, b"probe")
            .and_then(|()| fs::remove_file(&probe))
            .map_err(|error| {
                self.invalid_directory(format!(
                    "the ssd tier can't write a file in {:?}: {error}",
                    self.directory
                ))
            })
    }

    fn invalid_directory(&self, reason: String) -> ServerError {
        ServerError::Config(ConfigFieldError::Invalid {
            field: "metadata_cache.ssd.directory",
            reason,
        })
    }
}

/// The server's `[grep]` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepConfig {
    /// Whether this server serves grep queries, and whether it serves the
    /// index-maintenance endpoints and its sweep builds and collects the
    /// index.
    pub mode: JobMode,
    pub max_cache_bytes: Option<usize>,
    /// Flattening preserves the existing `[grep]` keys while leaving the
    /// worker policy itself as their one in-memory owner.
    #[serde(flatten)]
    pub worker: GrepWorkerConfig,
}

impl Default for GrepConfig {
    fn default() -> Self {
        Self {
            mode: JobMode::Disabled,
            max_cache_bytes: None,
            worker: GrepWorkerConfig::default(),
        }
    }
}
