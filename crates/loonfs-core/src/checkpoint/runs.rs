//! The LSM run model: how a manifest's flat `segments` list groups
//! into ordered runs and row families, plus the layout policy constants.

use loonfs_api::wire::manifest::{MetadataRowFamily, MetadataSegmentRef, NamespaceManifestPayload};
use loonfs_api::ChangeSeq;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

pub(super) use loonfs_api::wire::sst_blocks::{
    DEFAULT_MAX_L0_RUNS as DEFAULT_MAX_CHECKPOINT_L0_RUNS, DEFAULT_MAX_REORGANIZATION_INPUT_BYTES,
    DEFAULT_MAX_REORGANIZATION_INPUT_ROWS, DEFAULT_MAX_REORGANIZATION_INPUT_RUNS,
    DEFAULT_MAX_ROWS_PER_SEGMENT as DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
};

pub(super) const MAX_MAINTENANCE_SEGMENT_IO: usize = 8;
pub(super) const CHECKPOINT_L0_RUN_LEVEL: u32 = 0;
pub(super) const CHECKPOINT_BASE_RUN_LEVEL: u32 = 1;

pub(super) const CHECKPOINT_ROW_FAMILIES: [MetadataRowFamily; 10] = [
    MetadataRowFamily::Inodes,
    MetadataRowFamily::DirentryBinds,
    MetadataRowFamily::DirentryChildBinds,
    MetadataRowFamily::DirentryUnbinds,
    MetadataRowFamily::Revisions,
    MetadataRowFamily::RevisionsByInodeDesc,
    MetadataRowFamily::Tombstones,
    MetadataRowFamily::ActiveDeletions,
    MetadataRowFamily::CommitReceipts,
    MetadataRowFamily::Attributes,
];

/// Metadata families merged together as one consistency unit.
///
/// Families whose retention rules depend on each other are grouped, and each
/// secondary index is grouped with its canonical family. The closed enum lets
/// planning and validation use the group identity directly. Manifest loading
/// also enforces the layout rule that each group has at most one base-tier
/// run.
///
/// Declaration order determines which group is selected when multiple groups
/// have the same amount of pending work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetadataFamilyGroup {
    /// A directory binding, the reverse index that finds it by child, and the
    /// unbind that retires it.
    Bindings,
    /// File revision history and its newest-first index.
    Revisions,
    Inodes,
    Tombstones,
    /// Active deletions fold alone: a removal marker is cancelled by the
    /// listed row it names, and both live in this family.
    ActiveDeletions,
    CommitReceipts,
    /// Attributes fold alone too: a revision supersedes the revisions of the
    /// same inode, and they all live in this family. The family has no
    /// secondary index to travel with.
    Attributes,
}

impl MetadataFamilyGroup {
    /// The families this group merges together, in the order a run writes
    /// them. What a caller reports; never what it keys a group by.
    pub const fn families(self) -> &'static [MetadataRowFamily] {
        match self {
            Self::Bindings => &[
                MetadataRowFamily::DirentryBinds,
                MetadataRowFamily::DirentryChildBinds,
                MetadataRowFamily::DirentryUnbinds,
            ],
            Self::Revisions => &[
                MetadataRowFamily::Revisions,
                MetadataRowFamily::RevisionsByInodeDesc,
            ],
            Self::Inodes => &[MetadataRowFamily::Inodes],
            Self::Tombstones => &[MetadataRowFamily::Tombstones],
            Self::ActiveDeletions => &[MetadataRowFamily::ActiveDeletions],
            Self::CommitReceipts => &[MetadataRowFamily::CommitReceipts],
            Self::Attributes => &[MetadataRowFamily::Attributes],
        }
    }

    pub(super) fn contains(self, family: MetadataRowFamily) -> bool {
        self.families().contains(&family)
    }
}

pub(super) const REORGANIZE_FAMILY_GROUPS: [MetadataFamilyGroup; 7] = [
    MetadataFamilyGroup::Bindings,
    MetadataFamilyGroup::Revisions,
    MetadataFamilyGroup::Inodes,
    MetadataFamilyGroup::Tombstones,
    MetadataFamilyGroup::ActiveDeletions,
    MetadataFamilyGroup::CommitReceipts,
    MetadataFamilyGroup::Attributes,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataFamilySegments {
    pub(super) family: MetadataRowFamily,
    pub(super) segments: Vec<MetadataSegmentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataRunManifest {
    pub(super) run_seq: ChangeSeq,
    pub(super) level: u32,
    pub(super) segments: Vec<MetadataFamilySegments>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetadataLsmPolicy {
    pub max_l0_runs: NonZeroUsize,
    pub max_rows_per_segment: NonZeroUsize,
    /// Complete logical runs one reorganization step may inspect and merge.
    pub max_input_runs_per_step: NonZeroUsize,
    /// Row payloads one reorganization step may decode across its selected runs.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
    /// Decoded SST data-block bytes one reorganization step may materialize.
    pub max_decoded_input_bytes_per_step: NonZeroUsize,
}

impl Default for MetadataLsmPolicy {
    fn default() -> Self {
        Self {
            max_l0_runs: const { NonZeroUsize::new(DEFAULT_MAX_CHECKPOINT_L0_RUNS).unwrap() },
            max_rows_per_segment: const {
                NonZeroUsize::new(DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT).unwrap()
            },
            max_input_runs_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_RUNS).unwrap()
            },
            max_decoded_input_rows_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_ROWS).unwrap()
            },
            max_decoded_input_bytes_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_BYTES).unwrap()
            },
        }
    }
}

pub(super) fn l0_run_count(payload: &NamespaceManifestPayload) -> usize {
    runs_from_segments(payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .count()
}

pub(super) fn runs_in_scan_order(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs = runs_from_segments(payload);
    runs.sort_by(|left, right| {
        left.level
            .cmp(&right.level)
            .then(right.run_seq.cmp(&left.run_seq))
    });
    runs
}

pub(super) fn runs_in_materialization_order(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
    let mut runs = runs_from_segments(payload);
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.level.cmp(&left.level))
    });
    runs
}

pub(super) fn runs_from_segments(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs: BTreeMap<(ChangeSeq, u32), BTreeMap<MetadataRowFamily, Vec<MetadataSegmentRef>>> =
        BTreeMap::new();
    for descriptor in &payload.segments {
        runs.entry((descriptor.run_seq, descriptor.level))
            .or_default()
            .entry(descriptor.family)
            .or_default()
            .push(descriptor.clone());
    }
    runs.into_iter()
        .map(
            |((run_seq, level), segments_by_family)| MetadataRunManifest {
                run_seq,
                level,
                segments: CHECKPOINT_ROW_FAMILIES
                    .into_iter()
                    .map(|family| {
                        let mut segments =
                            segments_by_family.get(&family).cloned().unwrap_or_default();
                        segments.sort_by_key(|descriptor| descriptor.segment_index);
                        MetadataFamilySegments { family, segments }
                    })
                    .collect(),
            },
        )
        .collect()
}

pub(super) fn flatten_manifest_segments(
    segments_by_family: Vec<MetadataFamilySegments>,
) -> Vec<MetadataSegmentRef> {
    segments_by_family
        .into_iter()
        .flat_map(|family_segments| family_segments.segments)
        .collect()
}
