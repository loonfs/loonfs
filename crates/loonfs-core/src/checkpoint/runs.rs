//! The LSM run model: ordered manifest runs, row families, and layout policy.

use loonfs_api::wire::manifest::{
    MetadataRowFamily, MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_api::{ChangeSeq, RunNo};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

pub(super) use loonfs_api::wire::sst_blocks::{
    DEFAULT_MAX_DELTA_RUNS as DEFAULT_MAX_CHECKPOINT_DELTA_RUNS,
    DEFAULT_MAX_REORGANIZATION_INPUT_BYTES, DEFAULT_MAX_REORGANIZATION_INPUT_ROWS,
    DEFAULT_MAX_REORGANIZATION_INPUT_RUNS,
    DEFAULT_MAX_ROWS_PER_SEGMENT as DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
};

pub(super) const MAX_MAINTENANCE_SEGMENT_IO: usize = 8;

/// Segment tail and row-block fetches issued in one bounded wave.
pub(super) const MAX_MATERIALIZED_TABLE_LOADS: usize = 16;

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

/// Path resolution families come first so open prefetching serves point lookups first.
pub(super) const OPEN_PREFETCH_ROW_FAMILIES: [MetadataRowFamily; 10] = [
    MetadataRowFamily::DirentryBinds,
    MetadataRowFamily::Inodes,
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
    pub(super) run_no: RunNo,
    pub(super) run_seq: ChangeSeq,
    pub(super) tier: RunTier,
    pub(super) segments: Vec<MetadataFamilySegments>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetadataLsmPolicy {
    pub max_delta_runs: NonZeroUsize,
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
            max_delta_runs: const { NonZeroUsize::new(DEFAULT_MAX_CHECKPOINT_DELTA_RUNS).unwrap() },
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

pub(super) fn delta_run_count(payload: &NamespaceManifestPayload) -> usize {
    payload
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count()
}

/// Orders runs for reorganization planning: delta runs before base runs,
/// later sequences before earlier ones, and later run numbers first.
///
/// Two runs may carry the same sequence and tier, because a reorganization
/// writes its output beside the runs it did not consume. They hold different
/// families in that case, and the later-allocated run number keeps the order
/// total.
pub(super) fn runs_in_reorganization_order(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
    let mut runs = payload
        .runs
        .iter()
        .map(metadata_run_manifest)
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.tier
            .cmp(&right.tier)
            .then(right.run_seq.cmp(&left.run_seq))
            .then(right.run_no.cmp(&left.run_no))
    });
    runs
}

/// Oldest run first, which is the order a materialization applies rows in.
/// It is the reverse of the reorganization order, down to the same tiebreak.
pub(super) fn runs_in_materialization_order(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
    let mut runs = payload
        .runs
        .iter()
        .map(metadata_run_manifest)
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.tier.cmp(&left.tier))
            .then(left.run_no.cmp(&right.run_no))
    });
    runs
}

/// Orders candidate runs for folding: base before delta, then oldest first.
pub(super) fn runs_in_fold_order(mut runs: Vec<&MetadataRunManifest>) -> Vec<&MetadataRunManifest> {
    runs.sort_by(|left, right| {
        right
            .tier
            .cmp(&left.tier)
            .then(left.run_seq.cmp(&right.run_seq))
            .then(left.run_no.cmp(&right.run_no))
    });
    runs
}

fn metadata_run_manifest(run: &MetadataRunRef) -> MetadataRunManifest {
    MetadataRunManifest {
        run_no: run.run_no,
        run_seq: run.run_seq,
        tier: run.tier,
        segments: CHECKPOINT_ROW_FAMILIES
            .into_iter()
            .map(|family| {
                let mut segments = run
                    .segments
                    .iter()
                    .filter(|descriptor| descriptor.family == family)
                    .cloned()
                    .collect::<Vec<_>>();
                segments.sort_by_key(|descriptor| descriptor.segment_index);
                MetadataFamilySegments { family, segments }
            })
            .collect(),
    }
}

pub(super) fn flatten_manifest_segments(
    segments_by_family: Vec<MetadataFamilySegments>,
) -> Vec<MetadataSegmentRef> {
    segments_by_family
        .into_iter()
        .flat_map(|family_segments| family_segments.segments)
        .collect()
}
