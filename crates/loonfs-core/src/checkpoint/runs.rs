//! The LSM run model: how a manifest's flat `metadata_files` list groups
//! into ordered runs and tables, plus the layout policy constants.

use loonfs_api::wire::manifest::{MetadataFileRef, MetadataTableFamily, NamespaceManifestPayload};
use loonfs_api::ChangeSeq;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

pub(super) use loonfs_api::wire::sst_blocks::{
    DEFAULT_MAX_L0_RUNS as DEFAULT_MAX_CHECKPOINT_L0_RUNS, DEFAULT_MAX_REORGANIZATION_INPUT_BYTES,
    DEFAULT_MAX_REORGANIZATION_INPUT_ROWS, DEFAULT_MAX_REORGANIZATION_INPUT_RUNS,
    DEFAULT_MAX_ROWS_PER_SEGMENT as DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
};

pub(super) const MAX_MAINTENANCE_TABLE_IO: usize = 8;
pub(super) const CHECKPOINT_L0_RUN_LEVEL: u32 = 0;
pub(super) const CHECKPOINT_BASE_RUN_LEVEL: u32 = 1;

pub(super) const CHECKPOINT_TABLE_FAMILIES: [MetadataTableFamily; 10] = [
    MetadataTableFamily::Inodes,
    MetadataTableFamily::DirentryBinds,
    MetadataTableFamily::DirentryChildBinds,
    MetadataTableFamily::DirentryUnbinds,
    MetadataTableFamily::Revisions,
    MetadataTableFamily::RevisionsByInodeDesc,
    MetadataTableFamily::Tombstones,
    MetadataTableFamily::ActiveDeletions,
    MetadataTableFamily::CommitReceipts,
    MetadataTableFamily::Attributes,
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
    pub const fn families(self) -> &'static [MetadataTableFamily] {
        match self {
            Self::Bindings => &[
                MetadataTableFamily::DirentryBinds,
                MetadataTableFamily::DirentryChildBinds,
                MetadataTableFamily::DirentryUnbinds,
            ],
            Self::Revisions => &[
                MetadataTableFamily::Revisions,
                MetadataTableFamily::RevisionsByInodeDesc,
            ],
            Self::Inodes => &[MetadataTableFamily::Inodes],
            Self::Tombstones => &[MetadataTableFamily::Tombstones],
            Self::ActiveDeletions => &[MetadataTableFamily::ActiveDeletions],
            Self::CommitReceipts => &[MetadataTableFamily::CommitReceipts],
            Self::Attributes => &[MetadataTableFamily::Attributes],
        }
    }

    pub(super) fn contains(self, family: MetadataTableFamily) -> bool {
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
pub(super) struct MetadataTableManifest {
    pub(super) family: MetadataTableFamily,
    pub(super) segments: Vec<MetadataFileRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataRunManifest {
    pub(super) run_seq: ChangeSeq,
    pub(super) level: u32,
    pub(super) tables: Vec<MetadataTableManifest>,
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
    runs_from_metadata_files(payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .count()
}

pub(super) fn runs_in_scan_order(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs = runs_from_metadata_files(payload);
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
    let mut runs = runs_from_metadata_files(payload);
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.level.cmp(&left.level))
    });
    runs
}

pub(super) fn runs_from_metadata_files(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
    let mut runs: BTreeMap<(ChangeSeq, u32), BTreeMap<MetadataTableFamily, Vec<MetadataFileRef>>> =
        BTreeMap::new();
    for metadata_file in &payload.metadata_files {
        runs.entry((metadata_file.run_seq, metadata_file.level))
            .or_default()
            .entry(metadata_file.family)
            .or_default()
            .push(metadata_file.clone());
    }
    runs.into_iter()
        .map(|((run_seq, level), tables_by_family)| MetadataRunManifest {
            run_seq,
            level,
            tables: CHECKPOINT_TABLE_FAMILIES
                .into_iter()
                .map(|family| {
                    let mut segments = tables_by_family.get(&family).cloned().unwrap_or_default();
                    segments.sort_by_key(|segment| segment.segment_index);
                    MetadataTableManifest { family, segments }
                })
                .collect(),
        })
        .collect()
}

pub(super) fn flatten_manifest_tables(tables: Vec<MetadataTableManifest>) -> Vec<MetadataFileRef> {
    tables
        .into_iter()
        .flat_map(|table| table.segments)
        .collect()
}
