//! Runtime options and request-to-options conversions.
//!
//! Options shared with the HTTP client are re-exported from
//! [`loonfs_api::options`]. Runtime-only options remain in this module.
//!
//! Results are the `loonfs-api` wire shapes themselves, the same way handles
//! already return `CommitResponse` and `FlushWalResponse`.

use crate::{EffectiveLimit, FrozenBasePolicy, GcConfig, Result, RuntimeError};
use loonfs_api::{CreateCheckpointRequest, GcRequest, MetadataMaintenanceRequest};
use loonfs_core::limits::{CHECKPOINT_AT_WAL_SEGMENTS, MAX_UNFLUSHED_WAL_SEGMENTS};
use std::num::NonZeroU64;

pub use loonfs_api::options::{
    CommitOptions, CopyOptions, CreateDirectoryOptions, DeleteOptions,
    DirectMultipartUploadOptions, ListInodeChildrenOptions, ListPathEntriesOptions, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, StatPathOptions, UndeleteOptions,
    UpdateAttributesOptions,
};

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMaintenanceOptions {
    /// Flush the visible WAL tail once it reaches this many segments.
    pub max_wal_tail_segments: NonZeroU64,
    /// How a bounded step treats a base run it cannot merge.
    pub frozen_base: FrozenBasePolicy,
}

impl Default for MetadataMaintenanceOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: const { NonZeroU64::new(CHECKPOINT_AT_WAL_SEGMENTS).unwrap() },
            frozen_base: FrozenBasePolicy::Amortized,
        }
    }
}

impl MetadataMaintenanceOptions {
    /// Resolves a wire-level metadata maintenance request.
    pub fn from_request(request: MetadataMaintenanceRequest) -> Result<Self> {
        let Some(threshold) = request.max_wal_tail_segments else {
            return Ok(Self::default());
        };
        let Some(max_wal_tail_segments) = NonZeroU64::new(threshold) else {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        };
        let reject_writes_at_segments = MAX_UNFLUSHED_WAL_SEGMENTS;
        if max_wal_tail_segments.get() > reject_writes_at_segments {
            return Err(RuntimeError::Config(format!(
                "max_wal_tail_segments may not exceed the write-rejection threshold \
                 ({reject_writes_at_segments})"
            )));
        }
        Ok(Self {
            max_wal_tail_segments,
            ..Self::default()
        })
    }

    /// Returns whether the WAL tail has reached the flush threshold.
    pub fn flush_is_due(&self, wal_tail_segments: u64) -> bool {
        wal_tail_segments >= self.max_wal_tail_segments.get()
    }
}

/// Resolves wire-level GC window overrides onto the conservative defaults.
///
/// [`GcRequest`] carries optional overrides; [`GcConfig`] carries the values
/// the pass actually runs with, so the two are deliberately distinct shapes.
pub fn gc_config_from_request(request: GcRequest) -> GcConfig {
    let defaults = GcConfig::default();
    GcConfig {
        grace_window_ms: request.grace_window_ms.unwrap_or(defaults.grace_window_ms),
        max_objects: request.max_objects,
        cursor: request.cursor,
    }
}

/// Options for creating a durable checkpoint pin.
///
/// The name is a label recorded on the record, not a key. No `Default`:
/// a checkpoint always names its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointOptions {
    /// Label recorded on the checkpoint record.
    pub name: String,
    /// Optional lifetime; the record's expiry is computed from the runtime's
    /// clock. Absent means the pin holds until explicitly released.
    pub ttl_ms: Option<u64>,
}

impl CreateCheckpointOptions {
    /// Resolves the wire-level create request onto runtime options.
    pub fn from_request(request: CreateCheckpointRequest) -> Self {
        Self {
            name: request.name,
            ttl_ms: request.ttl_ms,
        }
    }
}

/// Options for creating a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSnapshotOptions {
    /// A label that does not need to be unique.
    pub name: String,
    /// Expiry time in Unix milliseconds.
    pub expires_at_ms: u64,
}

/// Options for creating a namespace; feeds core's
/// [`loonfs_core::BootstrapOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateNamespaceOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

/// Options for reading the change feed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListChangesOptions {
    /// Page limit; `None` resolves the default pagination policy.
    pub limit: Option<EffectiveLimit>,
}

/// Options for a streaming file read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFileStreamOptions {
    /// Bytes one ranged read fetches, which is the most of the file the read
    /// holds at once. Defaults to
    /// [`CONTENT_READ_CHUNK_BYTES`](loonfs_core::CONTENT_READ_CHUNK_BYTES);
    /// a caller with a tighter memory budget than that says so here, the way
    /// a caller of [`FsReader::read_content_ref`](crate::FsReader::read_content_ref)
    /// declares its own. Non-zero by type, so there is no chunk size that
    /// makes no progress.
    pub chunk_bytes: NonZeroU64,
    /// Where the read starts, for a caller that already holds the bytes
    /// below it — an interrupted download picking up where it stopped.
    ///
    /// The read still reports on the whole object, so a nonzero offset
    /// obliges the caller to hand over what it holds through
    /// [`FileContentStream::fold_resumed_prefix`](loonfs_core::FileContentStream::fold_resumed_prefix)
    /// before driving the stream. Zero reads from the first byte and asks
    /// nothing of the caller.
    pub start_offset: u64,
}

impl Default for ReadFileStreamOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: const { NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES).unwrap() },
            start_offset: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gc_request_translates_without_resolving_its_budget() {
        let explicit = gc_config_from_request(GcRequest::default());
        assert_eq!(explicit.max_objects, None);
        assert_eq!(explicit.cursor, None);
    }

    #[test]
    fn an_absent_threshold_resolves_to_the_default() {
        assert_eq!(
            MetadataMaintenanceOptions::from_request(MetadataMaintenanceRequest::default())
                .expect("default metadata options should resolve"),
            MetadataMaintenanceOptions::default()
        );
    }

    #[test]
    fn a_useless_flush_threshold_is_rejected() {
        for threshold in [0, MAX_UNFLUSHED_WAL_SEGMENTS + 1] {
            let error = MetadataMaintenanceOptions::from_request(MetadataMaintenanceRequest {
                max_wal_tail_segments: Some(threshold),
            })
            .expect_err("the threshold is out of range");
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn gc_conversion_preserves_explicit_budget_and_cursor() {
        let gc = gc_config_from_request(GcRequest {
            max_objects: Some(7),
            cursor: Some("opaque".to_owned()),
            ..GcRequest::default()
        });
        assert_eq!(gc.max_objects, Some(7));
        assert_eq!(gc.cursor.as_deref(), Some("opaque"));
    }
}
