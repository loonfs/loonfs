//! Runtime options and request-to-options conversions.
//!
//! Options shared with the HTTP client are re-exported from
//! [`loonfs_types::options`]. Runtime-only options remain in this module.
//!
//! Results are the `loonfs-types` wire shapes themselves, the same way handles
//! already return `Commit` and `FoldWalResponse`.

use crate::{Error, MetadataCompactionPolicy, Result, RetentionTarget};
use loonfs_core::limits::{FOLD_AT_WAL_OBJECTS, MAX_UNFOLDED_WAL_OBJECTS};
use loonfs_types::{AdvanceRetentionRequest, MetadataMaintenanceRequest};
use std::num::NonZeroU64;

pub use loonfs_types::options::{
    AccessState, AppendFileByInodeOptions, AppendFileOptions, AttributeChanges, CommitOptions,
    Consistency, CopyOptions, CreateDirectoryOptions, DeleteByInodeOptions, DeleteOptions,
    DirectMultipartUploadOptions, ForkNamespaceOptions, ListOptions, MoveOptions, PutFileOptions,
    ReadAt, StatOptions, UndeleteDestination, UndeleteOptions, UpdateAccessByInodeOptions,
    UpdateAccessOptions, UpdateAttributesByInodeOptions, UpdateAttributesOptions,
};

/// Overrides for the metadata-upkeep action.
///
/// The inline fold size and the idle fold age are settings of the runtime
/// (see [`LoonFsBuilder::inline_content`](crate::LoonFsBuilder::inline_content)
/// and [`LoonFsBuilder::idle_fold_after_ms`](crate::LoonFsBuilder::idle_fold_after_ms)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintainMetadataOptions {
    /// Fold the visible WAL tail once it reaches this many WAL objects.
    pub max_wal_tail_objects: NonZeroU64,
    /// Whether run sizes must justify the rewrite before maintenance merges them.
    pub compaction_policy: MetadataCompactionPolicy,
}

impl Default for MaintainMetadataOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_objects: const { NonZeroU64::new(FOLD_AT_WAL_OBJECTS).unwrap() },
            compaction_policy: MetadataCompactionPolicy::SizeTiered,
        }
    }
}

impl MaintainMetadataOptions {
    /// Resolves a wire-level metadata maintenance request.
    pub fn from_request(request: MetadataMaintenanceRequest) -> Result<Self> {
        let Some(threshold) = request.max_wal_tail_objects else {
            return Ok(Self::default());
        };
        let Some(max_wal_tail_objects) = NonZeroU64::new(threshold) else {
            return Err(Error::InvalidRequest {
                message: "`max_wal_tail_objects` must be greater than zero".to_owned(),
                param: "/max_wal_tail_objects",
            });
        };
        let reject_writes_at_wal_objects = MAX_UNFOLDED_WAL_OBJECTS;
        if max_wal_tail_objects.get() > reject_writes_at_wal_objects {
            return Err(Error::InvalidRequest {
                message: format!(
                    "`max_wal_tail_objects` may not exceed the write-rejection threshold \
                 ({reject_writes_at_wal_objects})"
                ),
                param: "/max_wal_tail_objects",
            });
        }
        Ok(Self {
            max_wal_tail_objects,
            ..Self::default()
        })
    }
}

/// Options for advancing a namespace's retention floor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdvanceRetentionFloorOptions {
    /// Where the floor moves. The default is the folded manifest head.
    pub target: RetentionTarget,
}

impl AdvanceRetentionFloorOptions {
    /// Resolves a wire-level retention request, which names at most one
    /// target.
    pub fn from_request(request: AdvanceRetentionRequest) -> Result<Self> {
        let target = match (request.to_seq, request.cutoff_at_ms) {
            (None, None) => RetentionTarget::Head,
            (Some(seq), None) => RetentionTarget::Seq(seq),
            (None, Some(cutoff_at_ms)) => RetentionTarget::Cutoff { cutoff_at_ms },
            (Some(_), Some(_)) => {
                return Err(Error::InvalidRequest {
                    message: "`to_seq` cannot be combined with `cutoff_at_ms`".to_owned(),
                    param: "/cutoff_at_ms",
                })
            }
        };
        Ok(Self { target })
    }
}

/// Options for creating a snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateSnapshotOptions {
    /// The snapshot's lifetime in milliseconds, counted from the runtime's
    /// clock. Without it, the snapshot doesn't expire.
    pub ttl_ms: Option<u64>,
}

/// Options for a direct download.
///
/// The client's `DownloadOptions` also has `snapshot_id`, which this one does
/// not need because a snapshot is read through its own read view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DownloadOptions {
    /// The first byte the download reads, for a caller that already holds
    /// the bytes below it. The target names `[start_offset, size_bytes)` and
    /// nothing else, so a resume asks for a new target. An offset at or past
    /// the end answers `invalid_request`; a revision of zero bytes takes only
    /// 0.
    pub start_offset: u64,
}

/// Options for a streaming file read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFileStreamOptions {
    /// Bytes one ranged read fetches, which is the most of the file the read
    /// holds at once. Defaults to
    /// [`CONTENT_READ_CHUNK_BYTES`](loonfs_core::CONTENT_READ_CHUNK_BYTES);
    /// a caller with a tighter memory budget than that says so here, the way
    /// a caller of [`Namespace::read_content`](crate::Namespace::read_content)
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
    /// How recently the read confirms the live head, or `None` for the
    /// runtime's default.
    pub consistency: Option<Consistency>,
}

impl Default for ReadFileStreamOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: const { NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES).unwrap() },
            start_offset: 0,
            consistency: None,
        }
    }
}

/// Options for a buffered read of a file's current content.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadFileOptions {
    /// How recently the read confirms the live head, or `None` for the
    /// runtime's default.
    pub consistency: Option<Consistency>,
}

/// Options for reading the change feed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListChangesOptions {
    /// How recently each page confirms the live head, or `None` for the
    /// runtime's default.
    pub consistency: Option<Consistency>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_threshold_resolves_to_the_default() {
        assert_eq!(
            MaintainMetadataOptions::from_request(MetadataMaintenanceRequest::default())
                .expect("default metadata options should resolve"),
            MaintainMetadataOptions::default()
        );
    }

    #[test]
    fn a_useless_fold_threshold_is_rejected() {
        for threshold in [0, MAX_UNFOLDED_WAL_OBJECTS + 1] {
            let error = MaintainMetadataOptions::from_request(MetadataMaintenanceRequest {
                max_wal_tail_objects: Some(threshold),
            })
            .expect_err("the threshold is out of range");
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
        }
    }
}
