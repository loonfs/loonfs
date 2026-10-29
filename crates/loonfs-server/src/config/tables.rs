//! The server config file's `[inline_content]`, `[publication]`,
//! `[metadata_cache]`, and `[grep]` tables.

use super::JobMode;
use crate::{Result, ServerError};
use loonfs::SsdSegmentBlocks;
use loonfs_grep::GrepWorkerConfig;
use loonfs_types::config::ConfigFieldError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

/// Overrides the embedded writer's inline content policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InlineContentConfig {
    #[serde(
        serialize_with = "serialize_inline_content_threshold",
        deserialize_with = "deserialize_inline_content_threshold"
    )]
    pub inline_content_threshold_bytes: Option<usize>,
    pub inline_content_wal_object_budget_bytes: Option<usize>,
    pub inline_content_fold_at_bytes: Option<usize>,
    pub inline_content_tail_limit_bytes: Option<usize>,
}

impl Default for InlineContentConfig {
    fn default() -> Self {
        Self {
            inline_content_threshold_bytes: loonfs::InlineContentPolicy::default()
                .inline_content_threshold_bytes,
            inline_content_wal_object_budget_bytes: None,
            inline_content_fold_at_bytes: None,
            inline_content_tail_limit_bytes: None,
        }
    }
}

fn serialize_inline_content_threshold<S>(
    threshold: &Option<usize>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match threshold {
        Some(bytes) => bytes.serialize(serializer),
        None => serializer.serialize_bool(false),
    }
}

fn deserialize_inline_content_threshold<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Threshold {
        Bytes(usize),
        Enabled(bool),
    }

    match Threshold::deserialize(deserializer)? {
        Threshold::Bytes(bytes) => Ok(Some(bytes)),
        Threshold::Enabled(false) => Ok(None),
        Threshold::Enabled(true) => Err(serde::de::Error::custom(
            "`inline_content_threshold_bytes` must be a byte count or false",
        )),
    }
}

impl InlineContentConfig {
    pub(crate) fn effective_policy(&self) -> loonfs::InlineContentPolicy {
        let defaults = loonfs::InlineContentPolicy::default();
        loonfs::InlineContentPolicy {
            inline_content_threshold_bytes: self.inline_content_threshold_bytes,
            inline_content_wal_object_budget_bytes: self
                .inline_content_wal_object_budget_bytes
                .unwrap_or(defaults.inline_content_wal_object_budget_bytes),
            inline_content_fold_at_bytes: self
                .inline_content_fold_at_bytes
                .unwrap_or(defaults.inline_content_fold_at_bytes),
            inline_content_tail_limit_bytes: self
                .inline_content_tail_limit_bytes
                .unwrap_or(defaults.inline_content_tail_limit_bytes),
        }
    }
}

/// The server's `[publication]` table. Total bytes are derived when sized.
///
/// `max_requests`, `max_estimated_bytes`, and `max_concurrent_publications`
/// are limits of the server's execution budget. The two per-namespace fields
/// are the runtime's publication limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLimitsConfig {
    pub max_requests: Option<std::num::NonZeroUsize>,
    pub max_requests_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_concurrent_publications: Option<std::num::NonZeroUsize>,
}

impl PublicationLimitsConfig {
    /// The per-namespace limits the server's runtime applies.
    pub(crate) fn effective_policy(&self) -> loonfs::PublicationPolicy {
        let defaults = loonfs::PublicationPolicy::default();
        loonfs::PublicationPolicy {
            max_requests_per_namespace: self
                .max_requests_per_namespace
                .unwrap_or(defaults.max_requests_per_namespace),
            max_estimated_bytes_per_namespace: self
                .max_estimated_bytes_per_namespace
                .unwrap_or(defaults.max_estimated_bytes_per_namespace),
            ..defaults
        }
    }
}

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
    /// Whether this server answers grep queries, and whether it serves the
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
