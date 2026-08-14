//! Constructor-validated grep manifest payload and its root pointer.

use super::error::{GrepManifestIdError, GrepManifestStateError};
use loonfs_api::generated_id;
use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, InodeId, NamespaceId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

const GREP_MANIFEST_ID_PREFIX: &str = "gmf";
const GREP_MANIFEST_ID_BODY_LEN: usize = 32;

/// Version of the grep index state nested inside a v1 manifest.
///
/// Before the first stable release this schema evolves in place at version 1;
/// compatibility starts with the released format, not intermediate
/// pre-release encodings.
pub const GREP_INDEX_FORMAT_VERSION: u32 = 1;

/// Durable object id for one immutable grep manifest candidate.
///
/// The id is drawn fresh for every candidate and names *which object*, never
/// what it contains. A content-derived id would make an identical rebuild
/// reuse the object an earlier publication left behind, and that reuse is
/// what lets collection race a publication for a manifest the winner is
/// about to point at. Integrity evidence rides
/// [`GrepRootPointer::manifest_payload_checksum`] beside the id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GrepManifestId(String);

impl GrepManifestId {
    /// Generates an id no earlier candidate can carry.
    pub fn generate() -> Self {
        Self(generated_id(GREP_MANIFEST_ID_PREFIX))
    }

    /// Parses the `gmf_` marker plus a 32-character lowercase hex body.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, GrepManifestIdError> {
        let value = value.as_ref();
        let expected_prefix = format!("{GREP_MANIFEST_ID_PREFIX}_");
        let Some(body) = value.strip_prefix(&expected_prefix) else {
            return Err(GrepManifestIdError {
                value: value.to_owned(),
                reason: format!("must start with `{expected_prefix}`"),
            });
        };
        if body.len() != GREP_MANIFEST_ID_BODY_LEN {
            return Err(GrepManifestIdError {
                value: value.to_owned(),
                reason: format!(
                    "body must be {GREP_MANIFEST_ID_BODY_LEN} lowercase hex characters"
                ),
            });
        }
        if !body
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(GrepManifestIdError {
                value: value.to_owned(),
                reason: "body must contain only lowercase hex characters".to_owned(),
            });
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for GrepManifestId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for GrepManifestId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for GrepManifestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GrepManifestId {
    type Err = GrepManifestIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for GrepManifestId {
    type Error = GrepManifestIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for GrepManifestId {
    type Error = GrepManifestIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl Serialize for GrepManifestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GrepManifestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// Small mutable control payload installed at `extensions/grep/root.json`.
///
/// The pointer names the manifest and carries its digest, so the object the
/// key resolves to is bound to the bytes the publisher installed even though
/// nothing about the id derives from them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepRootPointer {
    namespace_id: NamespaceId,
    manifest_id: GrepManifestId,
    manifest_payload_checksum: String,
}

impl GrepRootPointer {
    pub fn new(
        namespace_id: NamespaceId,
        manifest_id: GrepManifestId,
        manifest_payload_checksum: String,
    ) -> Self {
        Self {
            namespace_id,
            manifest_id,
            manifest_payload_checksum,
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn manifest_id(&self) -> &GrepManifestId {
        &self.manifest_id
    }

    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub fn manifest_payload_checksum(&self) -> &str {
        &self.manifest_payload_checksum
    }
}

/// Durable lifecycle of grep indexing for one namespace.
///
/// Each phase carries its own position and nothing else's. A backfill knows
/// the sequence it is walking toward and how far the walk got; a steady
/// index knows the sequence it has really built through. Neither can report
/// the other's number, because neither has a field to put it in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrepLifecycle {
    /// Initial materialization is walking the checkpointed file set.
    Backfilling {
        /// Namespace sequence the pinned checkpoint captured. The walk ends
        /// at exactly this state however far the namespace has moved since.
        target_seq: ChangeSeq,
        /// Inode the next backfill step resumes strictly after; absent means
        /// the start. Checkpoint file enumeration is ordered by ascending
        /// inode id, so one id is the whole resume position.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<InodeId>,
        /// User-checkpoint pin backing this immutable manifest walk.
        checkpoint_id: CheckpointId,
    },
    /// Backfill is complete and changes are consumed incrementally.
    Steady {
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
        #[serde(default, skip_serializing_if = "is_zero")]
        next_event_index: u32,
    },
    /// Grep indexing and queries are disabled for this namespace.
    Disabled,
}

impl GrepLifecycle {
    /// The steady watermark pair, or `None` in any phase that has none.
    pub fn steady_watermark(&self) -> Option<(ChangeSeq, u32)> {
        match self {
            Self::Steady {
                built_through_seq,
                next_event_index,
            } => Some((*built_through_seq, *next_event_index)),
            Self::Backfilling { .. } | Self::Disabled => None,
        }
    }
}

/// The durable lifecycle as the admin plane reports it.
///
/// The wire enum mirrors the durable one state for state. The durable
/// `Steady` spelling maps to the admin API's `Active` spelling without
/// changing the durable encoding, and no host invents a number for a state
/// that does not have one.
impl From<&GrepLifecycle> for loonfs_api::v0::GrepIndexLifecycle {
    fn from(lifecycle: &GrepLifecycle) -> Self {
        match lifecycle {
            GrepLifecycle::Disabled => Self::Disabled,
            GrepLifecycle::Backfilling {
                target_seq,
                cursor,
                checkpoint_id,
            } => Self::Backfilling {
                target_seq: *target_seq,
                cursor_inode_id: *cursor,
                checkpoint_id: checkpoint_id.clone(),
            },
            GrepLifecycle::Steady {
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
    /// Logical run identity stamped on every output segment of this reorganize.
    pub run_ordinal: u64,
}

/// Durable index bookkeeping paired with the visible segment set.
///
/// What lives here is what every phase has: segments are written and folded
/// during a backfill and during steady indexing alike, so the reorganize
/// state and the run allocator belong to the index rather than to a phase.
/// Anything only one phase has lives in [`GrepLifecycle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepIndexState {
    /// Version of this nested index-state schema.
    pub format_version: u32,
    /// One in-progress partitioned reorganize, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorganize: Option<GrepReorganizeState>,
    /// Next logical run ordinal to allocate atomically with a root update.
    pub next_run_ordinal: u64,
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

impl GrepIndexState {
    /// Creates index bookkeeping in the current grep-owned format.
    pub fn new(reorganize: Option<GrepReorganizeState>, next_run_ordinal: u64) -> Self {
        Self {
            format_version: GREP_INDEX_FORMAT_VERSION,
            reorganize,
            next_run_ordinal,
        }
    }
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Query-visible descriptor for one immutable grep segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepSegmentRef {
    pub segment_id: IndexSegmentId,
    pub run_seq: ChangeSeq,
    pub run_ordinal: u64,
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
    /// SHA-256 identity of the full immutable segment payload.
    pub payload_checksum: String,
}

/// One namespace's complete immutable grep manifest state.
///
/// Fields stay private so every constructed or decoded manifest passes the
/// inexpensive reorganize/segment and run-allocation checks in [`Self::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepManifestState {
    namespace_id: NamespaceId,
    lifecycle: GrepLifecycle,
    index: GrepIndexState,
    segments: Vec<GrepSegmentRef>,
}

impl GrepManifestState {
    /// Creates a manifest payload after validating its cross-field invariants.
    pub fn new(
        namespace_id: NamespaceId,
        lifecycle: GrepLifecycle,
        index: GrepIndexState,
        segments: Vec<GrepSegmentRef>,
    ) -> Result<Self, GrepManifestStateError> {
        let state = Self {
            namespace_id,
            lifecycle,
            index,
            segments,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn lifecycle(&self) -> &GrepLifecycle {
        &self.lifecycle
    }

    pub fn index(&self) -> &GrepIndexState {
        &self.index
    }

    pub fn segments(&self) -> &[GrepSegmentRef] {
        &self.segments
    }

    pub(super) fn validate(&self) -> Result<(), GrepManifestStateError> {
        if self.index.format_version != GREP_INDEX_FORMAT_VERSION {
            return Err(GrepManifestStateError::UnsupportedIndexFormatVersion {
                found: self.index.format_version,
                supported: GREP_INDEX_FORMAT_VERSION,
            });
        }
        if matches!(self.lifecycle, GrepLifecycle::Disabled) {
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
            if segment.run_ordinal >= self.index.next_run_ordinal {
                return Err(GrepManifestStateError::UnallocatedSegmentRunOrdinal {
                    segment_id: segment.segment_id.clone(),
                    run_ordinal: segment.run_ordinal,
                    next_run_ordinal: self.index.next_run_ordinal,
                });
            }
            if by_id.insert(&segment.segment_id, segment).is_some() {
                return Err(GrepManifestStateError::DuplicateSegmentId {
                    segment_id: segment.segment_id.clone(),
                });
            }
        }

        if let Some(reorganize) = &self.index.reorganize {
            validate_reorganize(reorganize, self.index.next_run_ordinal, &by_id)?;
        }
        Ok(())
    }
}

fn validate_reorganize(
    reorganize: &GrepReorganizeState,
    next_run_ordinal: u64,
    segments: &BTreeMap<&IndexSegmentId, &GrepSegmentRef>,
) -> Result<(), GrepManifestStateError> {
    if reorganize.run_ordinal >= next_run_ordinal {
        return Err(GrepManifestStateError::UnallocatedReorganizeRunOrdinal {
            run_ordinal: reorganize.run_ordinal,
            next_run_ordinal,
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
        if segment.level != reorganize.output_level || segment.run_ordinal != reorganize.run_ordinal
        {
            return Err(GrepManifestStateError::ReorganizeOutputDescriptorMismatch {
                segment_id: segment_id.clone(),
            });
        }
    }
    Ok(())
}
