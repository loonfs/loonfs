//! The lifecycle of grep's index and its in-progress reorganization, as
//! grep's entry records them.

use loonfs_types::{CommitSeq, IndexSegmentId, InodeId, RunNo, SnapshotId};
use serde::{Deserialize, Serialize};

/// Durable status of grep indexing for one namespace.
///
/// Each phase stores only the position it needs. A backfill records its
/// target and progress. An active index records how far indexing has
/// progressed.
///
/// The wire twin is `GrepIndexStatus` in `loonfs-types`. The two types share a
/// name because they describe the same status, and their fields differ because
/// each follows the conventions of its own surface: this record tags variants
/// with `kind` and writes inode IDs as numbers, and the API response tags
/// variants with `status` and writes inode IDs in their public `ino_` form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrepIndexStatus {
    /// The initial build is walking the files that a snapshot keeps.
    Backfilling {
        /// Namespace sequence the snapshot captured. The walk ends
        /// at exactly this state however far the namespace has moved since.
        captured_seq: CommitSeq,
        /// Inode the next backfill step resumes strictly after; absent means
        /// the start. Snapshot file enumeration is ordered by ascending
        /// inode id, so one id is the whole resume position.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_inode_id: Option<InodeId>,
        /// The operation snapshot whose manifest this backfill walks.
        snapshot_id: SnapshotId,
    },
    /// Backfill is complete and changes are consumed incrementally.
    Active {
        /// Namespace sequence that the backfill which built this index
        /// captured. It identifies the build: a rebootstrap rebuilds the
        /// index from a later backfill, and a query cursor that refers
        /// to another build is invalid.
        captured_seq: CommitSeq,
        /// Sequence of the commit at the index cursor. Everything at or
        /// below it is indexed, subject to `next_event_index`.
        built_through_seq: CommitSeq,
        /// Offset of the next change event within `built_through_seq`, or
        /// zero when the cursor is at the commit boundary and the whole
        /// commit is represented.
        ///
        /// A commit's events are one per internal operation in request
        /// order, derived from its durable delta vector; incremental
        /// indexing relies on that stable order when a step's budget stops
        /// it inside a commit.
        ///
        /// Zero is written like any other value. A status that omits the
        /// field fails to decode.
        next_event_index: u32,
    },
    /// Grep indexing and queries are disabled for this namespace.
    ///
    /// This closed status has no phase-specific state.
    Disabled {},
}

impl GrepIndexStatus {
    /// Returns the index position for the active status.
    pub fn active_watermark(&self) -> Option<ChangeFeedResume> {
        match self {
            Self::Active {
                built_through_seq,
                next_event_index,
                ..
            } => Some(ChangeFeedResume::new(*built_through_seq, *next_event_index)),
            Self::Backfilling { .. } | Self::Disabled {} => None,
        }
    }

    /// Returns the sequence that the active index's backfill captured,
    /// which identifies the build of the index.
    pub fn active_captured_seq(&self) -> Option<CommitSeq> {
        match self {
            Self::Active { captured_seq, .. } => Some(*captured_seq),
            Self::Backfilling { .. } | Self::Disabled {} => None,
        }
    }

    /// Returns the snapshot a backfilling status walks.
    pub fn backfill_snapshot_id(&self) -> Option<&SnapshotId> {
        match self {
            Self::Backfilling { snapshot_id, .. } => Some(snapshot_id),
            Self::Active { .. } | Self::Disabled {} => None,
        }
    }
}

impl From<&GrepIndexStatus> for loonfs_types::GrepIndexStatus {
    fn from(status: &GrepIndexStatus) -> Self {
        match status {
            GrepIndexStatus::Disabled {} => Self::Disabled,
            GrepIndexStatus::Backfilling {
                captured_seq,
                cursor_inode_id,
                snapshot_id,
            } => Self::Backfilling {
                captured_seq: *captured_seq,
                cursor_inode_id: *cursor_inode_id,
                snapshot_id: snapshot_id.clone(),
            },
            GrepIndexStatus::Active {
                built_through_seq,
                next_event_index,
                ..
            } => Self::Active {
                built_through_seq: *built_through_seq,
                next_event_index: *next_event_index,
            },
        }
    }
}

/// Resumable state for one partitioned segment reorganize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepReorganizeState {
    /// Segments this reorganize rewrites, chosen when it starts and kept until
    /// the manifest that completes it is published.
    pub input_segment_ids: Vec<IndexSegmentId>,
    /// Outputs written by completed reorganize steps and retained until publication.
    pub output_segment_ids: Vec<IndexSegmentId>,
    /// Inclusive row key at which the next reorganize step resumes.
    pub row_key_cursor: String,
    /// Level stamped on every output segment of this reorganize.
    pub output_level: u32,
    /// Run number stamped on every output segment of this reorganize.
    pub run_no: RunNo,
    /// The revisions whose content values and `content_revisions` rows a
    /// reorganization into the base level keeps. Its first step records
    /// them, builds the pruning filters from one scan of them, and writes the
    /// output run's value table and revision filter, which later steps read
    /// back. Absent for a reorganization into a lower level, which drops
    /// nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruning: Option<GrepPruning>,
}

/// The revisions whose content values a reorganization into the base level
/// keeps.
///
/// They're every revision at or below `filter_seq` that's newer than
/// `retention_floor_seq`, and each file's newest revision at or below
/// `retention_floor_seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepPruning {
    /// The sequence that the filters describe, the head when the first step
    /// built them.
    pub filter_seq: CommitSeq,
    /// The retention floor when the first step built the filters, at or
    /// below `filter_seq`.
    pub retention_floor_seq: CommitSeq,
}

/// Change-feed resume point derived from the grep watermark pair.
///
/// A commit-boundary cursor (`next_event_index == 0`) resumes strictly after
/// `built_through_seq`. An in-commit cursor reloads that commit and skips the
/// already represented event prefix, keeping feed selection and event
/// selection complementary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeFeedResume {
    built_through_seq: CommitSeq,
    next_event_index: u32,
}

impl ChangeFeedResume {
    pub fn new(built_through_seq: CommitSeq, next_event_index: u32) -> Self {
        Self {
            built_through_seq,
            next_event_index,
        }
    }

    pub fn after_seq(self) -> CommitSeq {
        if self.next_event_index == 0 {
            self.built_through_seq
        } else {
            CommitSeq(self.built_through_seq.0.saturating_sub(1))
        }
    }

    pub fn built_through_seq(self) -> CommitSeq {
        self.built_through_seq
    }

    pub(crate) fn start_event_index(self, change_seq: CommitSeq) -> usize {
        if change_seq == self.built_through_seq {
            self.next_event_index as usize
        } else {
            0
        }
    }

    pub fn next_event_index(self) -> u32 {
        self.next_event_index
    }
}
