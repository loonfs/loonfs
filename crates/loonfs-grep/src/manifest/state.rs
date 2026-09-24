//! Constructor-validated grep manifest payload and its discovery hint.

use super::error::GrepManifestStateError;
use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, IndexSegmentId, InodeId, ManifestNo, NamespaceId, PinId, RunNo};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepHint {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
}

/// Durable status of grep indexing for one namespace.
///
/// Each phase stores only the position it needs. A backfill records its
/// target and progress. An active index records how far indexing has
/// progressed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
        checkpoint_id: PinId,
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
    /// This closed status carries no phase-specific state.
    Disabled {},
}

impl GrepIndexStatus {
    /// Returns the index position for the active status.
    pub fn active_watermark(&self) -> Option<ChangeFeedResume> {
        match self {
            Self::Active {
                built_through_seq,
                next_event_index,
            } => Some(ChangeFeedResume::new(*built_through_seq, *next_event_index)),
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
#[serde(deny_unknown_fields)]
pub struct GrepReorganizeState {
    /// Fixed input snapshot retained until the completing manifest publication.
    pub snapshot_segment_ids: Vec<IndexSegmentId>,
    /// Outputs written by completed reorganize steps and retained until publication.
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
#[serde(deny_unknown_fields)]
pub struct GrepIndexState {
    /// One in-progress partitioned reorganize, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorganize: Option<GrepReorganizeState>,
    /// Run number the next producer allocates, atomically with a manifest
    /// publication. Every segment's `run_no` is below it.
    pub next_run_no: RunNo,
}

/// Change-feed resume point derived from the grep watermark pair.
///
/// A commit-boundary cursor (`next_event_index == 0`) resumes strictly after
/// `built_through_seq`. An in-commit cursor reloads that commit and skips the
/// already represented event prefix, keeping feed selection and event
/// selection complementary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeFeedResume {
    built_through_seq: ChangeSeq,
    next_event_index: u32,
}

impl ChangeFeedResume {
    pub fn new(built_through_seq: ChangeSeq, next_event_index: u32) -> Self {
        Self {
            built_through_seq,
            next_event_index,
        }
    }

    pub fn after_seq(self) -> ChangeSeq {
        if self.next_event_index == 0 {
            self.built_through_seq
        } else {
            ChangeSeq(self.built_through_seq.0.saturating_sub(1))
        }
    }

    pub fn built_through_seq(self) -> ChangeSeq {
        self.built_through_seq
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

    pub fn next_event_index(self) -> u32 {
        self.next_event_index
    }
}

/// Query-visible descriptor for one immutable grep segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepSegmentRef {
    pub segment_id: IndexSegmentId,
    pub run_no: RunNo,
    pub level: u32,
    pub row_count: u64,
    pub min_row_key: String,
    pub max_row_key: String,
    /// Entry point for the segment's data-block index and its CRC.
    pub index_block: BlockHandle,
    /// Bloom-filter layout and CRC for query probes.
    pub filter_block: BlockHandle,
    /// Small filters may be inlined while retaining the same block handle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_inline: Option<String>,
}

/// One namespace's complete immutable grep manifest state.
///
/// Fields stay private so every constructed or decoded manifest passes the
/// inexpensive reorganize/segment and run-allocation checks in [`Self::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepManifestState {
    namespace_id: NamespaceId,
    manifest_no: ManifestNo,
    status: GrepIndexStatus,
    index: GrepIndexState,
    segments: Vec<GrepSegmentRef>,
}

impl GrepManifestState {
    /// Creates a manifest payload after validating its cross-field invariants.
    pub fn new(
        namespace_id: NamespaceId,
        manifest_no: ManifestNo,
        status: GrepIndexStatus,
        index: GrepIndexState,
        segments: Vec<GrepSegmentRef>,
    ) -> Result<Self, GrepManifestStateError> {
        let state = Self {
            namespace_id,
            manifest_no,
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

    pub fn manifest_no(&self) -> ManifestNo {
        self.manifest_no
    }

    pub fn status(&self) -> &GrepIndexStatus {
        &self.status
    }

    pub fn index(&self) -> &GrepIndexState {
        &self.index
    }

    /// Total stored bytes of segments referenced by this manifest, without
    /// reading the segment objects. Pair this with `manifest_no` and `status`;
    /// grep's position is independent of the core manifest's folded head.
    pub fn index_stored_bytes(&self) -> crate::Result<u64> {
        self.segments.iter().try_fold(0_u64, |total, segment| {
            let key = crate::keyspace::segment_key(&self.namespace_id, &segment.segment_id);
            let bytes = crate::index_read::segment_object_len(&key, segment)?;
            total
                .checked_add(bytes)
                .ok_or_else(|| crate::GrepError::CorruptIndex {
                    message: "index byte count overflow".to_owned(),
                })
        })
    }

    pub fn segments(&self) -> &[GrepSegmentRef] {
        &self.segments
    }

    pub(super) fn ensure_successor(
        &self,
        successor: &Self,
    ) -> Result<(), loonfs_api::wire::manifest::ManifestChainError> {
        let invalid = |field: &str| {
            Err(loonfs_api::wire::manifest::ManifestChainError {
                field: field.to_owned(),
            })
        };
        if successor.namespace_id != self.namespace_id {
            return invalid("namespace_id");
        }
        if self.manifest_no.successor().ok() != Some(successor.manifest_no) {
            return invalid("manifest_no");
        }
        if successor.index.next_run_no < self.index.next_run_no {
            return invalid("next_run_no");
        }
        match (&self.status, &successor.status) {
            (
                GrepIndexStatus::Active {
                    built_through_seq: before,
                    next_event_index: before_event,
                },
                GrepIndexStatus::Active {
                    built_through_seq: after,
                    next_event_index: after_event,
                },
            ) => {
                if after < before
                    || (after == before && *before_event == 0 && *after_event != 0)
                    || (after == before && *after_event != 0 && after_event < before_event)
                {
                    return invalid("status");
                }
            }
            (
                GrepIndexStatus::Backfilling {
                    target_seq,
                    checkpoint_id,
                    cursor_inode_id,
                },
                GrepIndexStatus::Backfilling {
                    target_seq: next_target,
                    checkpoint_id: next_checkpoint,
                    cursor_inode_id: next_cursor,
                },
            ) => {
                if next_target < target_seq
                    || (next_checkpoint == checkpoint_id
                        && (next_target != target_seq || next_cursor < cursor_inode_id))
                {
                    return invalid("status");
                }
            }
            (
                GrepIndexStatus::Backfilling { target_seq, .. },
                GrepIndexStatus::Active {
                    built_through_seq, ..
                },
            ) if built_through_seq < target_seq => return invalid("status"),
            (
                GrepIndexStatus::Active {
                    built_through_seq, ..
                },
                GrepIndexStatus::Backfilling { target_seq, .. },
            ) if target_seq < built_through_seq => return invalid("status"),
            _ => {}
        }
        Ok(())
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
            if segment.row_count == 0 {
                return Err(GrepManifestStateError::EmptySegment {
                    segment_id: segment.segment_id.clone(),
                });
            }
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
