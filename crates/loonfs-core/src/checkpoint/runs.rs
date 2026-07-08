//! The LSM run model: how a manifest's flat `metadata_files` list groups
//! into ordered runs and tables, plus the layout policy constants.

use loonfs_api::wire::manifest::{MetadataFileRef, MetadataTableFamily, NamespaceManifestPayload};
use loonfs_api::ChangeSeq;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(super) const DEFAULT_MAX_CHECKPOINT_L0_RUNS: usize = 8;
/// With block-granular reads the segment is no longer the read unit, so
/// base segments are sized large to amortize their index and filter
/// sections across many lookups (design doc, "Sizing").
pub(super) const DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT: usize = 65_536;

pub(super) const MAX_MAINTENANCE_TABLE_IO: usize = 8;
#[cfg(test)]
pub(super) const MAX_CHECKPOINT_L0_RUNS: usize = DEFAULT_MAX_CHECKPOINT_L0_RUNS;
pub(super) const CHECKPOINT_L0_RUN_LEVEL: u32 = 0;
pub(super) const CHECKPOINT_BASE_RUN_LEVEL: u32 = 1;
pub(super) const CHECKPOINT_TABLE_FAMILIES: [MetadataTableFamily; 8] = [
    MetadataTableFamily::Inodes,
    MetadataTableFamily::DirentryBinds,
    MetadataTableFamily::DirentryChildBinds,
    MetadataTableFamily::DirentryUnbinds,
    MetadataTableFamily::Revisions,
    MetadataTableFamily::RevisionsByInodeDesc,
    MetadataTableFamily::Tombstones,
    MetadataTableFamily::CommitReceipts,
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
pub struct MetadataLsmPolicy {
    pub max_l0_runs: usize,
    pub max_rows_per_segment: usize,
}

impl Default for MetadataLsmPolicy {
    fn default() -> Self {
        Self {
            max_l0_runs: DEFAULT_MAX_CHECKPOINT_L0_RUNS,
            max_rows_per_segment: DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
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
