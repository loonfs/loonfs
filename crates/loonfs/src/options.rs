//! Runtime options and request-to-options conversions.
//!
//! Options shared with the HTTP client are re-exported from
//! [`loonfs_api::options`]. Runtime-only options remain in this module.
//!
//! Results are the `loonfs-api` wire shapes themselves, the same way handles
//! already return `Commit` and `FoldWalResponse`.

use crate::{EffectiveLimit, Error, MetadataCompactionPolicy, Result};
use loonfs_api::{CreateCheckpointRequest, MetadataMaintenanceRequest};
use loonfs_core::limits::{FOLD_AT_WAL_OBJECTS, MAX_UNFOLDED_WAL_OBJECTS};
use std::num::{NonZeroU64, NonZeroUsize};

pub use loonfs_api::options::{
    CommitOptions, CopyOptions, CreateDirectoryOptions, DeleteOptions,
    DirectMultipartUploadOptions, ForkNamespaceOptions, ListInodeChildrenOptions,
    ListPathEntriesOptions, MoveOptions, PutFileOptions, RestoreRevisionOptions, StatPathOptions,
    UndeleteOptions, UpdateAccessOptions, UpdateAttributesOptions,
};

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMaintenanceOptions {
    /// Fold the visible WAL tail once it reaches this many WAL objects.
    pub max_wal_tail_objects: NonZeroU64,
    /// Fold once unfolded inline bytes reach this size; defaults to 2 MiB.
    /// Applies only when the runtime's publisher knows the count.
    pub inline_content_fold_at_bytes: NonZeroUsize,
    /// Fold a WAL tail of any size once its newest commit is this old on
    /// the runtime's wall clock; defaults to 15 minutes. Zero
    /// turns this off.
    pub idle_fold_after_ms: u64,
    /// Whether run sizes must justify the rewrite before maintenance merges them.
    pub compaction_policy: MetadataCompactionPolicy,
}

impl Default for MetadataMaintenanceOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_objects: const { NonZeroU64::new(FOLD_AT_WAL_OBJECTS).unwrap() },
            inline_content_fold_at_bytes: NonZeroUsize::new(
                crate::InlineContentPolicy::default().inline_content_fold_at_bytes,
            )
            .expect("default inline fold threshold should be nonzero"),
            idle_fold_after_ms: 15 * 60 * 1_000,
            compaction_policy: MetadataCompactionPolicy::SizeTiered,
        }
    }
}

impl MetadataMaintenanceOptions {
    /// Resolves a wire-level metadata maintenance request.
    pub fn from_request(request: MetadataMaintenanceRequest) -> Result<Self> {
        let Some(threshold) = request.max_wal_tail_objects else {
            return Ok(Self::default());
        };
        let Some(max_wal_tail_objects) = NonZeroU64::new(threshold) else {
            return Err(Error::InvalidRequest {
                message: "max_wal_tail_objects must be greater than zero".to_owned(),
                param: "/max_wal_tail_objects",
            });
        };
        let reject_writes_at_wal_objects = MAX_UNFOLDED_WAL_OBJECTS;
        if max_wal_tail_objects.get() > reject_writes_at_wal_objects {
            return Err(Error::InvalidRequest {
                message: format!(
                    "max_wal_tail_objects may not exceed the write-rejection threshold \
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

    /// Returns whether the WAL tail has reached the fold threshold.
    pub fn fold_is_due(&self, wal_tail_objects: u64, wal_tail_inline_bytes: usize) -> bool {
        wal_tail_objects >= self.max_wal_tail_objects.get()
            || wal_tail_inline_bytes >= self.inline_content_fold_at_bytes.get()
    }

    /// Returns whether a WAL tail has gone idle: it holds a commit, and the
    /// newest one is at least `idle_fold_after_ms` old at `now_ms`.
    pub(crate) fn idle_fold_is_due(
        &self,
        wal_tail_newest_commit_at_ms: Option<u64>,
        now_ms: u64,
    ) -> bool {
        self.idle_fold_due_in_ms(wal_tail_newest_commit_at_ms, now_ms) == Some(0)
    }

    /// Returns how long after `now_ms` a WAL tail goes idle, or zero once it
    /// has. A commit stamped after `now_ms` counts as zero milliseconds old.
    /// Returns `None` when the tail holds no commit or the rule is off.
    pub(crate) fn idle_fold_due_in_ms(
        &self,
        wal_tail_newest_commit_at_ms: Option<u64>,
        now_ms: u64,
    ) -> Option<u64> {
        let committed_at_ms =
            wal_tail_newest_commit_at_ms.filter(|_| self.idle_fold_after_ms > 0)?;
        Some(
            self.idle_fold_after_ms
                .saturating_sub(now_ms.saturating_sub(committed_at_ms)),
        )
    }
}

/// Options for creating a durable checkpoint pin.
///
/// The name is a label recorded on the record, not a key. No `Default`:
/// a checkpoint always names its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointOptions {
    /// Label recorded on the pin.
    pub name: String,
    /// Optional lifetime; the record's expiry is computed from the runtime's
    /// clock. Absent means the pin holds until deleted.
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

/// Options for reading the change feed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListChangesOptions {
    /// Page limit; `None` resolves the default pagination policy.
    pub limit: Option<EffectiveLimit>,
}

/// Options for a streaming file read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFileStreamOptions {
    /// Read this retained revision instead of the current content.
    pub revision_no: Option<loonfs_api::RevisionNo>,
    /// Bytes one ranged read fetches, which is the most of the file the read
    /// holds at once. Defaults to
    /// [`CONTENT_READ_CHUNK_BYTES`](loonfs_core::CONTENT_READ_CHUNK_BYTES);
    /// a caller with a tighter memory budget than that says so here, the way
    /// a caller of [`Namespace::read_content_ref`](crate::Namespace::read_content_ref)
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
            revision_no: None,
            chunk_bytes: const { NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES).unwrap() },
            start_offset: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_threshold_resolves_to_the_default() {
        assert_eq!(
            MetadataMaintenanceOptions::from_request(MetadataMaintenanceRequest::default())
                .expect("default metadata options should resolve"),
            MetadataMaintenanceOptions::default()
        );
    }

    #[test]
    fn a_useless_fold_threshold_is_rejected() {
        for threshold in [0, MAX_UNFOLDED_WAL_OBJECTS + 1] {
            let error = MetadataMaintenanceOptions::from_request(MetadataMaintenanceRequest {
                max_wal_tail_objects: Some(threshold),
            })
            .expect_err("the threshold is out of range");
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
        }
    }
}
