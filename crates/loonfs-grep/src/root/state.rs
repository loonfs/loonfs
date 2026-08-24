//! Constructor-validated grep manifest payload and its root pointer.

use super::error::GrepManifestStateError;
use loonfs_api::wire::sst_blocks::BlockHandle;
pub use loonfs_api::GrepManifestObjectId;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId, NamespaceId, RunNo};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Small mutable control payload installed at `extensions/grep/root.json`.
///
/// The pointer names the manifest and carries its digest, so the object the
/// key resolves to is bound to the bytes the publisher installed even though
/// nothing about the id derives from them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepRootPointer {
    namespace_id: NamespaceId,
    manifest_object_id: GrepManifestObjectId,
    manifest_payload_checksum: String,
}

impl GrepRootPointer {
    pub fn new(
        namespace_id: NamespaceId,
        manifest_object_id: GrepManifestObjectId,
        manifest_payload_checksum: String,
    ) -> Self {
        Self {
            namespace_id,
            manifest_object_id,
            manifest_payload_checksum,
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn manifest_object_id(&self) -> &GrepManifestObjectId {
        &self.manifest_object_id
    }

    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub fn manifest_payload_checksum(&self) -> &str {
        &self.manifest_payload_checksum
    }
}

/// Durable status of grep indexing for one namespace.
///
/// Each phase stores only the position it needs. A backfill records its
/// target and progress. An active index records how far indexing has
/// progressed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrepIndexStatus {
    /// Initial materialization is walking the checkpointed file set.
    Backfilling {
        /// Namespace sequence the pinned checkpoint captured. The walk ends
        /// at exactly this state however far the namespace has moved since.
        target_seq: ChangeSeq,
        /// Inode the next backfill step resumes strictly after; absent means
        /// the start. Checkpoint file enumeration is ordered by ascending
        /// inode id, so one id is the whole resume position.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_inode_id: Option<InodeId>,
        /// User-checkpoint pin backing this immutable manifest walk.
        checkpoint_id: CheckpointId,
    },
    /// Backfill is complete and changes are consumed incrementally.
    Active {
        /// Sequence of the commit at the index cursor. Everything at or
        /// below it is indexed, subject to `next_event_index`.
        built_through_seq: ChangeSeq,
        /// Offset of the next change event within `built_through_seq`, or
        /// zero when the cursor is at the commit boundary and the whole
        /// commit is represented.
        ///
        /// A commit's events are one per committed operation in request
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
    /// The braces spell this status the same way as the other two and as
    /// every durable control object's status. A manifest is immutable, so
    /// this decoder still tolerates fields it does not know.
    Disabled {},
}

impl GrepIndexStatus {
    /// Returns the index position for the active status.
    pub fn active_watermark(&self) -> Option<(ChangeSeq, u32)> {
        match self {
            Self::Active {
                built_through_seq,
                next_event_index,
            } => Some((*built_through_seq, *next_event_index)),
            Self::Backfilling { .. } | Self::Disabled {} => None,
        }
    }
}

impl From<&GrepIndexStatus> for loonfs_api::v0::GrepIndexLifecycle {
    fn from(status: &GrepIndexStatus) -> Self {
        match status {
            GrepIndexStatus::Disabled {} => Self::Disabled,
            GrepIndexStatus::Backfilling {
                target_seq,
                cursor_inode_id,
                checkpoint_id,
            } => Self::Backfilling {
                target_seq: *target_seq,
                cursor_inode_id: *cursor_inode_id,
                checkpoint_id: checkpoint_id.clone(),
            },
            GrepIndexStatus::Active {
                built_through_seq,
                next_event_index,
            } => Self::Active {
                built_through_seq: *built_through_seq,
                next_event_index: *next_event_index,
            },
        }
    }
}

/// Resumable state for one partitioned segment reorganize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepReorganizeState {
    /// Fixed input snapshot retained until the completing root swap.
    pub snapshot_segment_ids: Vec<IndexSegmentId>,
    /// Outputs written by completed reorganize steps and retained until the swap.
    pub output_segment_ids: Vec<IndexSegmentId>,
    /// Inclusive row key at which the next reorganize step resumes.
    pub row_key_cursor: String,
    /// Level stamped on every output segment of this reorganize.
    pub output_level: u32,
    /// Run number stamped on every output segment of this reorganize.
    pub run_no: RunNo,
}

/// Durable index bookkeeping paired with the visible segment set.
///
/// Segments can be written and reorganized during both backfill and active
/// indexing. Shared state therefore lives here, while phase-specific state
/// lives in [`GrepIndexStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepIndexState {
    /// One in-progress partitioned reorganize, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorganize: Option<GrepReorganizeState>,
    /// Run number the next producer allocates, atomically with a root
    /// update. Every segment's `run_no` is below it.
    pub next_run_no: RunNo,
}

/// Change-feed resume point derived from the grep watermark pair.
///
/// A commit-boundary cursor (`next_event_index == 0`) resumes strictly after
/// `built_through_seq`. An in-commit cursor reloads that commit and skips the
/// already represented event prefix, keeping feed selection and event
/// selection complementary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangeFeedResume {
    built_through_seq: ChangeSeq,
    next_event_index: u32,
}

impl ChangeFeedResume {
    pub(crate) fn new(built_through_seq: ChangeSeq, next_event_index: u32) -> Self {
        Self {
            built_through_seq,
            next_event_index,
        }
    }

    pub(crate) fn after_seq(self) -> ChangeSeq {
        if self.next_event_index == 0 {
            self.built_through_seq
        } else {
            ChangeSeq(self.built_through_seq.0.saturating_sub(1))
        }
    }

    pub(crate) fn start_event_index(
        self,
        change_seq: ChangeSeq,
    ) -> std::result::Result<usize, std::num::TryFromIntError> {
        if change_seq == self.built_through_seq {
            usize::try_from(self.next_event_index)
        } else {
            Ok(0)
        }
    }

    pub(crate) fn next_event_index(self) -> u32 {
        self.next_event_index
    }
}

/// Query-visible descriptor for one immutable grep segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepSegmentRef {
    pub segment_id: IndexSegmentId,
    pub run_no: RunNo,
    pub run_seq: ChangeSeq,
    pub level: u32,
    pub segment_index: u32,
    pub min_row_key: String,
    pub max_row_key: String,
    /// Entry point for the segment's data-block index and its CRC.
    pub index_block: BlockHandle,
    /// Bloom-filter layout and CRC for query probes.
    pub filter_block: BlockHandle,
    /// Small filters may be inlined while retaining the same block handle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_inline: Option<String>,
    /// SHA-256 of the complete stored segment, formatted as
    /// `sha256:<64 lowercase hex>`. Caches and offline verification use this
    /// value. Ranged reads verify each block with its CRC32C instead.
    pub object_checksum: String,
}

/// One namespace's complete immutable grep manifest state.
///
/// Fields stay private so every constructed or decoded manifest passes the
/// inexpensive reorganize/segment and run-allocation checks in [`Self::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepManifestState {
    namespace_id: NamespaceId,
    status: GrepIndexStatus,
    index: GrepIndexState,
    segments: Vec<GrepSegmentRef>,
}

impl GrepManifestState {
    /// Creates a manifest payload after validating its cross-field invariants.
    pub fn new(
        namespace_id: NamespaceId,
        status: GrepIndexStatus,
        index: GrepIndexState,
        segments: Vec<GrepSegmentRef>,
    ) -> Result<Self, GrepManifestStateError> {
        let state = Self {
            namespace_id,
            status,
            index,
            segments,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn status(&self) -> &GrepIndexStatus {
        &self.status
    }

    pub fn index(&self) -> &GrepIndexState {
        &self.index
    }

    pub fn segments(&self) -> &[GrepSegmentRef] {
        &self.segments
    }

    pub(super) fn validate(&self) -> Result<(), GrepManifestStateError> {
        if matches!(self.status, GrepIndexStatus::Disabled {}) {
            if !self.segments.is_empty() {
                return Err(GrepManifestStateError::DisabledHasSegments);
            }
            if self.index.reorganize.is_some() {
                return Err(GrepManifestStateError::DisabledHasReorganize);
            }
        }

        let mut by_id = BTreeMap::new();
        for segment in &self.segments {
            if segment.min_row_key > segment.max_row_key {
                return Err(GrepManifestStateError::InvalidSegmentRange {
                    segment_id: segment.segment_id.clone(),
                });
            }
            if segment.run_no >= self.index.next_run_no {
                return Err(GrepManifestStateError::UnallocatedSegmentRunNo {
                    segment_id: segment.segment_id.clone(),
                    run_no: segment.run_no,
                    next_run_no: self.index.next_run_no,
                });
            }
            if by_id.insert(&segment.segment_id, segment).is_some() {
                return Err(GrepManifestStateError::DuplicateSegmentId {
                    segment_id: segment.segment_id.clone(),
                });
            }
        }

        if let Some(reorganize) = &self.index.reorganize {
            validate_reorganize(reorganize, self.index.next_run_no, &by_id)?;
        }
        Ok(())
    }
}

fn validate_reorganize(
    reorganize: &GrepReorganizeState,
    next_run_no: RunNo,
    segments: &BTreeMap<&IndexSegmentId, &GrepSegmentRef>,
) -> Result<(), GrepManifestStateError> {
    if reorganize.run_no >= next_run_no {
        return Err(GrepManifestStateError::UnallocatedReorganizeRunNo {
            run_no: reorganize.run_no,
            next_run_no,
        });
    }

    let mut snapshot_ids = BTreeSet::new();
    for segment_id in &reorganize.snapshot_segment_ids {
        if !snapshot_ids.insert(segment_id) {
            return Err(GrepManifestStateError::DuplicateReorganizeSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        if !segments.contains_key(segment_id) {
            return Err(GrepManifestStateError::MissingReorganizeSnapshotSegment {
                segment_id: segment_id.clone(),
            });
        }
    }

    let mut output_ids = BTreeSet::new();
    for segment_id in &reorganize.output_segment_ids {
        if snapshot_ids.contains(segment_id) || !output_ids.insert(segment_id) {
            return Err(GrepManifestStateError::DuplicateReorganizeSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        let Some(segment) = segments.get(segment_id) else {
            return Err(GrepManifestStateError::MissingReorganizeOutputSegment {
                segment_id: segment_id.clone(),
            });
        };
        if segment.level != reorganize.output_level || segment.run_no != reorganize.run_no {
            return Err(GrepManifestStateError::ReorganizeOutputDescriptorMismatch {
                segment_id: segment_id.clone(),
            });
        }
    }
    Ok(())
}
