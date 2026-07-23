//! Constructor-validated grep manifest payload and its root pointer.

use super::error::{GrepManifestIdError, GrepRootStateError};
use loonfs_api::sha256_digest;
use loonfs_api::wire::sst_blocks::BlockHandle;
use loonfs_api::{ChangeSeq, CheckpointId, IndexSegmentId, NamespaceId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

const SHA256_PREFIX: &str = "sha256:";
const SHA256_HEX_LEN: usize = 64;

/// Version of the grep index state nested inside a v1 manifest.
pub const GREP_INDEX_FORMAT_VERSION: u32 = 1;

/// Content-derived identity of one immutable grep manifest payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GrepManifestId(String);

impl GrepManifestId {
    /// Parses the manifest id's 64-character lowercase SHA-256 hex form.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, GrepManifestIdError> {
        let value = value.as_ref();
        if value.len() != SHA256_HEX_LEN {
            return Err(GrepManifestIdError {
                value: value.to_owned(),
                reason: format!("must contain exactly {SHA256_HEX_LEN} lowercase hex characters"),
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(GrepManifestIdError {
                value: value.to_owned(),
                reason: "must contain only lowercase hex characters".to_owned(),
            });
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn for_payload(payload: &[u8]) -> Self {
        let digest = sha256_digest(payload);
        Self(
            digest
                .strip_prefix(SHA256_PREFIX)
                .expect("sha256_digest should carry the sha256 prefix")
                .to_owned(),
        )
    }

    pub(crate) fn payload_checksum(&self) -> String {
        format!("{SHA256_PREFIX}{}", self.0)
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepRootPointer {
    namespace_id: NamespaceId,
    manifest_id: GrepManifestId,
}

impl GrepRootPointer {
    pub fn new(namespace_id: NamespaceId, manifest_id: GrepManifestId) -> Self {
        Self {
            namespace_id,
            manifest_id,
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn manifest_id(&self) -> &GrepManifestId {
        &self.manifest_id
    }
}

/// Durable lifecycle of grep indexing for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrepLifecycle {
    /// Initial materialization is walking existing revisions.
    Backfilling {
        /// Resume key for the next backfill step; empty means the start.
        backfill_cursor: String,
        /// User-checkpoint pin backing this immutable manifest walk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_id: Option<CheckpointId>,
    },
    /// Backfill is complete and changes are consumed incrementally.
    Steady,
    /// Grep indexing and queries are disabled for this namespace.
    Disabled,
}

/// Resumable state for one partitioned segment fold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepFoldState {
    /// Fixed input snapshot retained until the completing root swap.
    pub snapshot_segment_ids: Vec<IndexSegmentId>,
    /// Outputs written by completed fold steps and retained until the swap.
    pub output_segment_ids: Vec<IndexSegmentId>,
    /// Inclusive row key at which the next fold step resumes.
    pub row_key_cursor: String,
    /// Level stamped on every output segment of this fold.
    pub output_level: u32,
    /// Logical run identity stamped on every output segment of this fold.
    pub run_ordinal: u64,
}

/// Durable index bookkeeping paired with the visible segment set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepIndexState {
    /// Version of this nested index-state schema.
    pub format_version: u32,
    /// Changes at or below this sequence are represented by `segments`.
    pub built_through_seq: ChangeSeq,
    /// One in-progress partitioned fold, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fold: Option<GrepFoldState>,
    /// Next logical run ordinal to allocate atomically with a root update.
    pub next_run_ordinal: u64,
}

impl GrepIndexState {
    /// Creates index bookkeeping in the current grep-owned format.
    pub fn new(
        built_through_seq: ChangeSeq,
        fold: Option<GrepFoldState>,
        next_run_ordinal: u64,
    ) -> Self {
        Self {
            format_version: GREP_INDEX_FORMAT_VERSION,
            built_through_seq,
            fold,
            next_run_ordinal,
        }
    }
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
/// inexpensive fold/segment and run-allocation checks in [`Self::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepRootState {
    namespace_id: NamespaceId,
    lifecycle: GrepLifecycle,
    index: GrepIndexState,
    segments: Vec<GrepSegmentRef>,
}

impl GrepRootState {
    /// Creates a root after validating its cross-field invariants.
    pub fn new(
        namespace_id: NamespaceId,
        lifecycle: GrepLifecycle,
        index: GrepIndexState,
        segments: Vec<GrepSegmentRef>,
    ) -> Result<Self, GrepRootStateError> {
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

    pub(super) fn validate(&self) -> Result<(), GrepRootStateError> {
        if self.index.format_version != GREP_INDEX_FORMAT_VERSION {
            return Err(GrepRootStateError::UnsupportedIndexFormatVersion {
                found: self.index.format_version,
                supported: GREP_INDEX_FORMAT_VERSION,
            });
        }
        if matches!(self.lifecycle, GrepLifecycle::Disabled) {
            if !self.segments.is_empty() {
                return Err(GrepRootStateError::DisabledHasSegments);
            }
            if self.index.fold.is_some() {
                return Err(GrepRootStateError::DisabledHasFold);
            }
        }

        let mut by_id = BTreeMap::new();
        for segment in &self.segments {
            if segment.min_row_key > segment.max_row_key {
                return Err(GrepRootStateError::InvalidSegmentRange {
                    segment_id: segment.segment_id.clone(),
                });
            }
            if segment.run_ordinal >= self.index.next_run_ordinal {
                return Err(GrepRootStateError::UnallocatedSegmentRunOrdinal {
                    segment_id: segment.segment_id.clone(),
                    run_ordinal: segment.run_ordinal,
                    next_run_ordinal: self.index.next_run_ordinal,
                });
            }
            if by_id.insert(&segment.segment_id, segment).is_some() {
                return Err(GrepRootStateError::DuplicateSegmentId {
                    segment_id: segment.segment_id.clone(),
                });
            }
        }

        if let Some(fold) = &self.index.fold {
            validate_fold(fold, self.index.next_run_ordinal, &by_id)?;
        }
        Ok(())
    }
}

fn validate_fold(
    fold: &GrepFoldState,
    next_run_ordinal: u64,
    segments: &BTreeMap<&IndexSegmentId, &GrepSegmentRef>,
) -> Result<(), GrepRootStateError> {
    if fold.run_ordinal >= next_run_ordinal {
        return Err(GrepRootStateError::UnallocatedFoldRunOrdinal {
            run_ordinal: fold.run_ordinal,
            next_run_ordinal,
        });
    }

    let mut snapshot_ids = BTreeSet::new();
    for segment_id in &fold.snapshot_segment_ids {
        if !snapshot_ids.insert(segment_id) {
            return Err(GrepRootStateError::DuplicateFoldSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        if !segments.contains_key(segment_id) {
            return Err(GrepRootStateError::MissingFoldSnapshotSegment {
                segment_id: segment_id.clone(),
            });
        }
    }

    let mut output_ids = BTreeSet::new();
    for segment_id in &fold.output_segment_ids {
        if snapshot_ids.contains(segment_id) || !output_ids.insert(segment_id) {
            return Err(GrepRootStateError::DuplicateFoldSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        let Some(segment) = segments.get(segment_id) else {
            return Err(GrepRootStateError::MissingFoldOutputSegment {
                segment_id: segment_id.clone(),
            });
        };
        if segment.level != fold.output_level || segment.run_ordinal != fold.run_ordinal {
            return Err(GrepRootStateError::FoldOutputDescriptorMismatch {
                segment_id: segment_id.clone(),
            });
        }
    }
    Ok(())
}
