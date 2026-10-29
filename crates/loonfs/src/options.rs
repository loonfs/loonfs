//! Runtime options and request-to-options conversions.
//!
//! Options shared with the HTTP client are re-exported from
//! [`loonfs_types::options`]. Runtime-only options remain in this module.
//!
//! Results are the `loonfs-types` wire shapes themselves, the same way handles
//! already return `Commit` and `FoldWalResponse`.

use crate::{Error, MetadataCompactionPolicy, Result, RetentionTarget};
use loonfs_core::limits::{FOLD_AT_WAL_OBJECTS, MAX_UNFOLDED_WAL_OBJECTS};
use loonfs_core::StreamRange;
use loonfs_types::{AdvanceRetentionRequest, MetadataMaintenanceRequest};
use std::num::NonZeroU64;

pub use loonfs_types::options::{
    AccessState, AppendFileByInodeOptions, AppendFileOptions, AttributeChanges, CommitOptions,
    Consistency, CopyOptions, CreateDirectoryOptions, DeleteByInodeOptions, DeleteOptions,
    DirectMultipartUploadOptions, DownloadOptions, ForkNamespaceOptions, ListChangesOptions,
    ListFileRevisionsOptions, ListOptions, ListTrashOptions, MoveOptions, PutFileOptions, ReadAt,
    ReadConsistency, ReadFileOptions, ReadRange, StatOptions, UndeleteDestination, UndeleteOptions,
    UpdateAccessByInodeOptions, UpdateAccessOptions, UpdateAttributesByInodeOptions,
    UpdateAttributesOptions,
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
        let Some(max_wal_tail_objects) = request.max_wal_tail_objects else {
            return Ok(Self::default());
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
    pub ttl_ms: Option<NonZeroU64>,
}

/// Options for a streaming file read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFileStreamOptions {
    /// Bytes one ranged read fetches, which is the most of the file the read
    /// holds at once. Defaults to
    /// [`CONTENT_READ_CHUNK_BYTES`](loonfs_core::CONTENT_READ_CHUNK_BYTES);
    /// a caller with a tighter memory budget than that says so here, the way
    /// a caller of [`Namespace::read_content`](crate::Namespace::read_content)
    /// declares its own. Non-zero by type, so there is no chunk size that
    /// makes no progress.
    pub chunk_bytes: NonZeroU64,
    /// The bytes the read returns, by default the whole file from its first
    /// byte. See [`StreamRange`](loonfs_core::StreamRange) for how a resumed
    /// read and a ranged read are verified.
    pub range: StreamRange,
    /// The live head or the snapshot that the read reads, by default the
    /// live head at the runtime's default consistency. A snapshot read
    /// returns the file revision that the snapshot captured.
    pub at: ReadAt,
}

impl Default for ReadFileStreamOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: const { NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES).unwrap() },
            range: StreamRange::default(),
            at: ReadAt::default(),
        }
    }
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
        let error = MaintainMetadataOptions::from_request(MetadataMaintenanceRequest {
            max_wal_tail_objects: NonZeroU64::new(MAX_UNFOLDED_WAL_OBJECTS + 1),
        })
        .expect_err("the threshold is out of range");
        assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
    }
}
